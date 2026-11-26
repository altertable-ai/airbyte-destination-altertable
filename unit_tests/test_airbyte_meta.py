from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Generator
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from airbyte_cdk.models.airbyte_protocol import (
    AirbyteRecordMessageMeta,
    AirbyteRecordMessageMetaChange,
    Change,
    Reason,
)

from destination_altertable.altertable_writer import (
    AIRBYTE_META_SCHEMA,
    AltertableWriter,
)


CONFIG = {
    "host": "localhost",
    "port": 15002,
    "username": "test",
    "password": "test",
    "catalog": "my_catalog",
    "schema": "main",
    "tls": False,
}

ALTER_STATEMENTS = [
    'ALTER TABLE IF EXISTS "my_catalog"."main"."users" ADD COLUMN IF NOT EXISTS "_airbyte_raw_id" VARCHAR',
    'ALTER TABLE IF EXISTS "my_catalog"."main"."users" ADD COLUMN IF NOT EXISTS "_airbyte_extracted_at" TIMESTAMPTZ',
    'ALTER TABLE IF EXISTS "my_catalog"."main"."users" ADD COLUMN IF NOT EXISTS "_airbyte_meta" JSON',
    'ALTER TABLE IF EXISTS "my_catalog"."main"."users" ADD COLUMN IF NOT EXISTS "_airbyte_generation_id" BIGINT',
]


def make_stream(
    destination_sync_mode: DestinationSyncMode = DestinationSyncMode.append,
    properties: dict | None = None,
    generation_id: int | None = 4,
    sync_id: int | None = 42,
) -> ConfiguredAirbyteStream:
    return ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name="users",
            json_schema={
                "type": "object",
                "properties": properties
                or {"id": {"type": "integer"}, "name": {"type": "string"}},
            },
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
        ),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=destination_sync_mode,
        primary_key=[["id"]],
        generation_id=generation_id,
        minimum_generation_id=0,
        sync_id=sync_id,
    )


def make_record(
    record_id: int,
    emitted_at: int = 1764064800000,
    meta: AirbyteRecordMessageMeta | None = None,
) -> AirbyteRecordMessage:
    return AirbyteRecordMessage(
        stream="users",
        data={"id": record_id, "name": "Alice"},
        emitted_at=emitted_at,
        meta=meta,
    )


@pytest.fixture
def client() -> Generator[MagicMock, None, None]:
    with patch(
        "destination_altertable.altertable_writer.altertable_flightsql.Client"
    ) as client_class:
        yield client_class.return_value


def make_writer(stream: ConfiguredAirbyteStream) -> AltertableWriter:
    writer = AltertableWriter(CONFIG)
    writer.set_streams(ConfiguredAirbyteCatalog(streams=[stream]))
    return writer


def written_batches(client: MagicMock) -> list[pa.RecordBatch]:
    writer = client.ingest.return_value.__enter__.return_value
    return [c.args[0] for c in writer.write_batch.call_args_list]


def executed(client: MagicMock) -> list[str]:
    return [c.args[0] for c in client.execute.call_args_list]


def test_metadata_columns_are_written_after_data_columns(client: MagicMock) -> None:
    writer = make_writer(make_stream())

    writer.buffer_record(make_record(1))
    writer.flush()

    [batch] = written_batches(client)
    assert batch.schema == pa.schema(
        [("id", pa.int64()), ("name", pa.string()), *AIRBYTE_META_SCHEMA]
    )
    assert client.ingest.call_args.kwargs["schema"] == batch.schema


def test_metadata_column_values(client: MagicMock) -> None:
    writer = make_writer(make_stream())

    writer.buffer_record(make_record(1, emitted_at=1764064800123))
    writer.buffer_record(make_record(2))
    writer.flush()

    [batch] = written_batches(client)
    rows = batch.to_pylist()
    raw_ids = [row["_airbyte_raw_id"] for row in rows]
    assert len(set(raw_ids)) == 2
    assert all(uuid.UUID(raw_id).version == 4 for raw_id in raw_ids)
    assert rows[0]["_airbyte_extracted_at"] == datetime(
        2025, 11, 25, 10, 0, 0, 123000, tzinfo=timezone.utc
    )
    assert json.loads(rows[0]["_airbyte_meta"]) == {"sync_id": 42, "changes": []}
    assert [row["_airbyte_generation_id"] for row in rows] == [4, 4]


def test_meta_records_source_changes(client: MagicMock) -> None:
    writer = make_writer(make_stream())
    change = AirbyteRecordMessageMetaChange(
        field="name",
        change=Change.TRUNCATED,
        reason=Reason.SOURCE_FIELD_SIZE_LIMITATION,
    )

    writer.buffer_record(
        make_record(1, meta=AirbyteRecordMessageMeta(changes=[change]))
    )
    writer.buffer_record(make_record(2))
    writer.flush()

    [batch] = written_batches(client)
    metas = [json.loads(meta) for meta in batch.column("_airbyte_meta").to_pylist()]
    assert metas == [
        {
            "sync_id": 42,
            "changes": [
                {
                    "field": "name",
                    "change": "TRUNCATED",
                    "reason": "SOURCE_FIELD_SIZE_LIMITATION",
                }
            ],
        },
        {"sync_id": 42, "changes": []},
    ]


def test_missing_generation_and_sync_ids(client: MagicMock) -> None:
    writer = make_writer(make_stream(generation_id=None, sync_id=None))

    writer.buffer_record(make_record(1))
    writer.flush()

    [row] = written_batches(client)[0].to_pylist()
    assert row["_airbyte_generation_id"] == 0
    assert json.loads(row["_airbyte_meta"]) == {"sync_id": None, "changes": []}


def test_source_fields_named_like_metadata_columns_are_dropped(
    client: MagicMock,
) -> None:
    writer = make_writer(
        make_stream(
            properties={
                "id": {"type": "integer"},
                "_airbyte_raw_id": {"type": "string"},
            }
        )
    )

    writer.buffer_record(
        AirbyteRecordMessage(
            stream="users",
            data={"id": 1, "_airbyte_raw_id": "from source"},
            emitted_at=1000,
        )
    )
    writer.flush()

    [batch] = written_batches(client)
    assert batch.schema.names == ["id", *AIRBYTE_META_SCHEMA.names]
    assert batch.column("_airbyte_raw_id")[0].as_py() != "from source"


@pytest.mark.parametrize(
    "sync_mode", [DestinationSyncMode.append, DestinationSyncMode.append_dedup]
)
def test_existing_tables_get_metadata_columns_once(
    client: MagicMock, sync_mode: DestinationSyncMode
) -> None:
    writer = make_writer(make_stream(sync_mode))

    writer.buffer_record(make_record(1))
    writer.flush()
    writer.buffer_record(make_record(2))
    writer.flush()

    assert executed(client) == ALTER_STATEMENTS
    assert client.ingest.call_count == 2


def test_replaced_tables_are_not_altered(client: MagicMock) -> None:
    writer = make_writer(make_stream(DestinationSyncMode.overwrite))

    writer.buffer_record(make_record(1))
    writer.flush()
    writer.buffer_record(make_record(2))
    writer.flush()

    assert executed(client) == []


def test_cleared_table_has_metadata_columns(client: MagicMock) -> None:
    stream = make_stream(DestinationSyncMode.append)
    stream.minimum_generation_id = stream.generation_id
    writer = make_writer(stream)

    writer.complete_stream(None, "users")

    [batch] = written_batches(client)
    assert batch.num_rows == 0
    assert batch.schema == pa.schema(
        [("id", pa.int64()), ("name", pa.string()), *AIRBYTE_META_SCHEMA]
    )

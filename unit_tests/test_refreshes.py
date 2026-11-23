from __future__ import annotations

from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from altertable_flightsql.client import IngestTableMode

from destination_altertable.altertable_writer import AltertableWriter


CONFIG = {
    "host": "localhost",
    "port": 15002,
    "username": "test",
    "password": "test",
    "catalog": "my_catalog",
    "schema": "main",
    "tls": False,
}


def make_stream(
    destination_sync_mode: DestinationSyncMode,
    generation_id: int,
    minimum_generation_id: int,
) -> ConfiguredAirbyteStream:
    return ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name="users",
            json_schema={
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                },
            },
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
        ),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=destination_sync_mode,
        primary_key=[["id"]],
        generation_id=generation_id,
        minimum_generation_id=minimum_generation_id,
        sync_id=1,
    )


def make_record(record_id: int) -> AirbyteRecordMessage:
    return AirbyteRecordMessage(
        stream="users", data={"id": record_id, "name": "Alice"}, emitted_at=1000
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


def ingest_modes(client: MagicMock) -> list[IngestTableMode]:
    return [c.kwargs["mode"] for c in client.ingest.call_args_list]


def written_batches(client: MagicMock) -> list:
    writer = client.ingest.return_value.__enter__.return_value
    return [c.args[0] for c in writer.write_batch.call_args_list]


def test_refresh_removing_records_replaces_the_table_once(client: MagicMock) -> None:
    writer = make_writer(make_stream(DestinationSyncMode.append_dedup, 2, 2))

    writer.buffer_record(make_record(1))
    writer.flush()
    writer.buffer_record(make_record(2))
    writer.flush()
    writer.complete_stream(None, "users")

    assert ingest_modes(client) == [
        IngestTableMode.REPLACE,
        IngestTableMode.CREATE_APPEND,
    ]


def test_refresh_retaining_records_appends(client: MagicMock) -> None:
    writer = make_writer(make_stream(DestinationSyncMode.append_dedup, 2, 0))

    writer.buffer_record(make_record(1))
    writer.flush()
    writer.complete_stream(None, "users")

    assert ingest_modes(client) == [IngestTableMode.CREATE_APPEND]


def test_clear_replaces_the_table_with_an_empty_one(client: MagicMock) -> None:
    writer = make_writer(make_stream(DestinationSyncMode.append, 3, 3))

    writer.complete_stream(None, "users")

    assert ingest_modes(client) == [IngestTableMode.REPLACE]
    assert client.ingest.call_args.kwargs["table_name"] == "users"
    [batch] = written_batches(client)
    assert batch.num_rows == 0
    assert batch.schema.names == ["id", "name"]


def test_complete_flushes_buffered_records(client: MagicMock) -> None:
    writer = make_writer(make_stream(DestinationSyncMode.append, 3, 3))

    writer.buffer_record(make_record(1))
    writer.complete_stream(None, "users")

    assert ingest_modes(client) == [IngestTableMode.REPLACE]
    [batch] = written_batches(client)
    assert batch.num_rows == 1


def test_complete_without_records_keeps_non_truncated_table(
    client: MagicMock,
) -> None:
    writer = make_writer(make_stream(DestinationSyncMode.append, 3, 0))

    writer.complete_stream(None, "users")

    client.ingest.assert_not_called()


def test_complete_ignores_unknown_streams(client: MagicMock) -> None:
    writer = make_writer(make_stream(DestinationSyncMode.append, 3, 3))

    writer.complete_stream(None, "unknown")

    client.ingest.assert_not_called()

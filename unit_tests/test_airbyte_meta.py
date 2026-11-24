from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
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
from airbyte_cdk.models.airbyte_protocol import (
    AirbyteRecordMessageMeta,
    AirbyteRecordMessageMetaChange,
    Change,
    Reason,
)

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


@pytest.fixture
def client() -> Generator[MagicMock, None, None]:
    with patch(
        "destination_altertable.altertable_writer.altertable_flightsql.Client"
    ) as client_class:
        yield client_class.return_value


def test_records_carry_airbyte_meta_columns(client: MagicMock) -> None:
    # A stream whose only column is its primary key: the metadata columns leave
    # something to update when deduping.
    stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name="events",
            json_schema={
                "type": "object",
                "properties": {"event_name": {"type": "string"}},
            },
            supported_sync_modes=[SyncMode.incremental],
        ),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append_dedup,
        primary_key=[["event_name"]],
        generation_id=3,
        minimum_generation_id=0,
        sync_id=42,
    )
    writer = AltertableWriter(CONFIG)
    writer.set_streams(ConfiguredAirbyteCatalog(streams=[stream]))

    writer.buffer_record(
        AirbyteRecordMessage(
            stream="events",
            data={"event_name": "signup"},
            emitted_at=1_700_000_000_123,
            meta=AirbyteRecordMessageMeta(
                changes=[
                    AirbyteRecordMessageMetaChange(
                        field="event_name",
                        change=Change.TRUNCATED,
                        reason=Reason.SOURCE_RECORD_SIZE_LIMITATION,
                    )
                ]
            ),
        )
    )
    writer.flush()

    batch = (
        client.ingest.return_value.__enter__.return_value.write_batch.call_args.args[0]
    )
    row = batch.to_pylist()[0]

    assert batch.schema.names == [
        "event_name",
        "_airbyte_raw_id",
        "_airbyte_extracted_at",
        "_airbyte_meta",
        "_airbyte_generation_id",
    ]
    uuid.UUID(row["_airbyte_raw_id"])
    assert row["_airbyte_extracted_at"] == datetime(
        2023, 11, 14, 22, 13, 20, 123000, tzinfo=timezone.utc
    )
    assert json.loads(row["_airbyte_meta"]) == {
        "changes": [
            {
                "field": "event_name",
                "change": "TRUNCATED",
                "reason": "SOURCE_RECORD_SIZE_LIMITATION",
            }
        ],
        "sync_id": 42,
    }
    assert row["_airbyte_generation_id"] == 3

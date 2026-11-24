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


def make_writer(destination_sync_mode: DestinationSyncMode) -> AltertableWriter:
    writer = AltertableWriter(CONFIG)
    writer.set_streams(
        ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=AirbyteStream(
                        name="users",
                        json_schema={
                            "type": "object",
                            "properties": {"id": {"type": "integer"}},
                        },
                        supported_sync_modes=[SyncMode.full_refresh],
                    ),
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=destination_sync_mode,
                )
            ]
        )
    )
    return writer


def make_record(record_id: int) -> AirbyteRecordMessage:
    return AirbyteRecordMessage(stream="users", data={"id": record_id}, emitted_at=1000)


@pytest.fixture
def client() -> Generator[MagicMock, None, None]:
    with patch(
        "destination_altertable.altertable_writer.altertable_flightsql.Client"
    ) as client_class:
        yield client_class.return_value


def test_flushes_when_the_buffer_is_full(client: MagicMock) -> None:
    writer = make_writer(DestinationSyncMode.append)

    with patch("destination_altertable.altertable_writer.MAX_BUFFERED_RECORDS", 3):
        for record_id in range(7):
            writer.buffer_record(make_record(record_id))

    # Two full buffers were written, the last record is still buffered.
    assert client.ingest.call_count == 2
    assert len(writer.buffer[(None, "users")]) == 1


def test_overwrite_replaces_once_then_appends(client: MagicMock) -> None:
    writer = make_writer(DestinationSyncMode.overwrite)

    with patch("destination_altertable.altertable_writer.MAX_BUFFERED_RECORDS", 2):
        for record_id in range(4):
            writer.buffer_record(make_record(record_id))

    modes = [c.kwargs["mode"] for c in client.ingest.call_args_list]
    assert modes == [IngestTableMode.REPLACE, IngestTableMode.CREATE_APPEND]

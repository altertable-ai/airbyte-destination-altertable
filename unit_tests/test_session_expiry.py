from __future__ import annotations

from typing import Generator
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.flight as flight
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

SESSION_EXPIRED = flight.FlightServerError(
    "Session expired. Open a new session. gRPC client debug context: ..."
)


def make_stream(
    destination_sync_mode: DestinationSyncMode = DestinationSyncMode.append,
    namespace: str | None = None,
) -> ConfiguredAirbyteStream:
    return ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name="users",
            namespace=namespace,
            json_schema={
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                },
            },
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
        ),
        sync_mode=SyncMode.full_refresh,
        destination_sync_mode=destination_sync_mode,
        generation_id=1,
        minimum_generation_id=0,
        sync_id=1,
    )


def make_record(record_id: int, namespace: str | None = None) -> AirbyteRecordMessage:
    return AirbyteRecordMessage(
        stream="users",
        namespace=namespace,
        data={"id": record_id, "name": "Alice"},
        emitted_at=1000,
    )


@pytest.fixture
def clients() -> Generator[list[MagicMock], None, None]:
    """Each new Flight client the writer opens, in order."""
    opened: list[MagicMock] = []

    def connect(**_kwargs) -> MagicMock:
        client = MagicMock()
        opened.append(client)
        return client

    with patch(
        "destination_altertable.altertable_writer.altertable_flightsql.Client",
        side_effect=connect,
    ):
        yield opened


def make_writer(stream: ConfiguredAirbyteStream) -> AltertableWriter:
    writer = AltertableWriter(CONFIG)
    writer.set_streams(ConfiguredAirbyteCatalog(streams=[stream]))
    return writer


def written_batches(client: MagicMock) -> list[pa.RecordBatch]:
    writer = client.ingest.return_value.__enter__.return_value
    return [c.args[0] for c in writer.write_batch.call_args_list]


def test_expired_session_reopens_and_retries_the_ingest(
    clients: list[MagicMock],
) -> None:
    writer = make_writer(make_stream(DestinationSyncMode.overwrite))
    expired = clients[0]
    expired.ingest.return_value.__enter__.return_value.write_batch.side_effect = (
        SESSION_EXPIRED
    )

    writer.buffer_record(make_record(1))
    writer.flush()

    assert len(clients) == 2
    expired.close.assert_called_once()
    renewed = clients[1]
    assert writer.client is renewed
    assert renewed.ingest.call_args.kwargs["mode"] == IngestTableMode.REPLACE
    [batch] = written_batches(renewed)
    assert batch.num_rows == 1

    # Later writes keep using the new session, as appends after the replace.
    writer.buffer_record(make_record(2))
    writer.flush()
    assert len(clients) == 2
    assert renewed.ingest.call_args.kwargs["mode"] == IngestTableMode.CREATE_APPEND


def test_expired_session_reopens_and_retries_schema_creation(
    clients: list[MagicMock],
) -> None:
    writer = make_writer(make_stream(namespace="sales"))
    clients[0].execute.side_effect = SESSION_EXPIRED

    writer.buffer_record(make_record(1, namespace="sales"))
    writer.flush()

    assert len(clients) == 2
    renewed = clients[1]
    renewed.execute.assert_called_once_with(
        'CREATE SCHEMA IF NOT EXISTS "my_catalog"."sales"'
    )
    renewed.ingest.assert_called_once()
    clients[0].ingest.assert_not_called()


def test_other_errors_are_raised_without_reopening(
    clients: list[MagicMock],
) -> None:
    writer = make_writer(make_stream())
    error = flight.FlightServerError("Table users has 3 columns but 2 were supplied")
    clients[
        0
    ].ingest.return_value.__enter__.return_value.write_batch.side_effect = error

    writer.buffer_record(make_record(1))
    with pytest.raises(flight.FlightServerError, match="3 columns"):
        writer.flush()

    assert len(clients) == 1


def test_session_expiring_again_after_reopening_is_raised(
    clients: list[MagicMock],
) -> None:
    writer = make_writer(make_stream(namespace="sales"))
    still_expired = MagicMock()
    still_expired.execute.side_effect = SESSION_EXPIRED
    clients[0].execute.side_effect = SESSION_EXPIRED

    writer.buffer_record(make_record(1, namespace="sales"))
    with patch.object(writer, "_connect", return_value=still_expired) as reconnect:
        with pytest.raises(flight.FlightServerError, match="Session expired"):
            writer.flush()

    reconnect.assert_called_once()
    still_expired.ingest.assert_not_called()

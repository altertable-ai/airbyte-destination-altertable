from __future__ import annotations

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

TIKTOK_AD_ID = 7628540698668761000


@pytest.fixture
def client() -> Generator[MagicMock, None, None]:
    with patch(
        "destination_altertable.altertable_writer.altertable_flightsql.Client"
    ) as client_class:
        yield client_class.return_value


def make_writer() -> AltertableWriter:
    stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name="ads",
            json_schema={
                "type": "object",
                "properties": {
                    "ad_id": {"type": ["null", "number"]},
                    "spend": {"type": ["null", "number"]},
                },
            },
            supported_sync_modes=[SyncMode.incremental],
        ),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append,
    )
    writer = AltertableWriter(CONFIG)
    writer.set_streams(ConfiguredAirbyteCatalog(streams=[stream]))
    return writer


def written_batches(client: MagicMock) -> list[pa.RecordBatch]:
    """Written batches, without the Airbyte metadata columns."""
    writer = client.ingest.return_value.__enter__.return_value
    return [
        c.args[0].select(["ad_id", "spend"]) for c in writer.write_batch.call_args_list
    ]


def buffer_and_flush(writer: AltertableWriter, rows: list[dict]) -> None:
    for data in rows:
        writer.buffer_record(
            AirbyteRecordMessage(stream="ads", data=data, emitted_at=1000)
        )
    writer.flush()


def test_flush_keeps_integers_beyond_float64_precision(client: MagicMock) -> None:
    writer = make_writer()
    for data in [{"ad_id": TIKTOK_AD_ID, "spend": 12.5}, {"ad_id": None, "spend": 3}]:
        writer.buffer_record(
            AirbyteRecordMessage(stream="ads", data=data, emitted_at=1000)
        )

    writer.flush()

    [batch] = written_batches(client)
    assert batch.schema == pa.schema([("ad_id", pa.int64()), ("spend", pa.float64())])
    assert batch.to_pylist() == [
        {"ad_id": TIKTOK_AD_ID, "spend": 12.5},
        {"ad_id": None, "spend": 3.0},
    ]


def test_flush_types_number_columns_on_all_chunks(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("destination_altertable.altertable_writer.ROWS_PER_CHUNK", 1)
    writer = make_writer()
    buffer_and_flush(
        writer,
        [
            {"ad_id": TIKTOK_AD_ID, "spend": 12.5},
            {"ad_id": 1.0, "spend": 2**60},
            {"ad_id": None, "spend": 3},
        ],
    )

    batches = written_batches(client)
    schema = pa.schema([("ad_id", pa.int64()), ("spend", pa.float64())])
    assert [batch.schema for batch in batches] == [schema] * 3
    assert [row for batch in batches for row in batch.to_pylist()] == [
        {"ad_id": TIKTOK_AD_ID, "spend": 12.5},
        {"ad_id": 1, "spend": float(2**60)},
        {"ad_id": None, "spend": 3.0},
    ]


def test_flush_keeps_number_column_types_of_previous_flushes(client: MagicMock) -> None:
    writer = make_writer()
    buffer_and_flush(writer, [{"ad_id": TIKTOK_AD_ID, "spend": 12.5}])
    buffer_and_flush(writer, [{"ad_id": None, "spend": 3}])

    schemas = [call.kwargs["schema"] for call in client.ingest.call_args_list]
    schema = pa.schema(
        [("ad_id", pa.int64()), ("spend", pa.float64()), *AIRBYTE_META_SCHEMA]
    )
    assert schemas == [schema] * 2
    assert [row for batch in written_batches(client) for row in batch.to_pylist()] == [
        {"ad_id": TIKTOK_AD_ID, "spend": 12.5},
        {"ad_id": None, "spend": 3.0},
    ]


@pytest.mark.parametrize(
    "first, second",
    [
        ({"ad_id": 1, "spend": 1.0}, {"ad_id": TIKTOK_AD_ID, "spend": 1.0}),
        ({"ad_id": TIKTOK_AD_ID, "spend": 1.0}, {"ad_id": 1.5, "spend": 1.0}),
    ],
)
def test_flush_rejects_lossy_number_casts_across_flushes(
    client: MagicMock, first: dict, second: dict
) -> None:
    writer = make_writer()
    buffer_and_flush(writer, [first])

    with pytest.raises(ValueError, match="can't be written exactly"):
        buffer_and_flush(writer, [second])

    assert client.ingest.call_count == 1

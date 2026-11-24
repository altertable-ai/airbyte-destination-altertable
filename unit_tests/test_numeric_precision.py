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
    writer = client.ingest.return_value.__enter__.return_value
    return [c.args[0] for c in writer.write_batch.call_args_list]


def test_flush_keeps_integers_beyond_float64_precision(client: MagicMock) -> None:
    writer = make_writer()
    for data in [{"ad_id": TIKTOK_AD_ID, "spend": 12.5}, {"ad_id": None, "spend": 3}]:
        writer.buffer_record(
            AirbyteRecordMessage(stream="ads", data=data, emitted_at=1000)
        )

    writer.flush()

    [batch] = written_batches(client)
    batch = batch.select(["ad_id", "spend"])
    assert batch.schema == pa.schema([("ad_id", pa.int64()), ("spend", pa.float64())])
    assert batch.to_pylist() == [
        {"ad_id": TIKTOK_AD_ID, "spend": 12.5},
        {"ad_id": None, "spend": 3.0},
    ]

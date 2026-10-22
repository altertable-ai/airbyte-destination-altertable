from __future__ import annotations

from unittest.mock import patch

import pytest
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)

from destination_altertable import altertable_writer
from destination_altertable.altertable_writer import (
    DEFAULT_FLUSH_THRESHOLD,
    MEMORY_LIMIT_TO_FLUSH_THRESHOLD_RATIO,
    AltertableWriter,
    get_flush_threshold,
    get_memory_limit,
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

CATALOG = ConfiguredAirbyteCatalog(
    streams=[
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="test_table",
                json_schema={
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                    },
                },
                supported_sync_modes=[SyncMode.full_refresh],
            ),
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.append,
        )
    ]
)


@pytest.fixture
def cgroup(tmp_path, monkeypatch):
    """Point the cgroup memory limit files to a temporary directory."""
    v2 = tmp_path / "memory.max"
    v1 = tmp_path / "memory.limit_in_bytes"
    monkeypatch.setattr(altertable_writer, "CGROUP_V2_MEMORY_LIMIT", v2)
    monkeypatch.setattr(altertable_writer, "CGROUP_V1_MEMORY_LIMIT", v1)
    return v2, v1


def make_writer(config: dict) -> AltertableWriter:
    with patch("altertable_flightsql.Client"):
        return AltertableWriter(config)


def make_record(i: int) -> AirbyteRecordMessage:
    return AirbyteRecordMessage(
        stream="test_table", data={"id": i, "name": f"Person {i}"}, emitted_at=1000
    )


def test_memory_limit_from_cgroup_v2(cgroup) -> None:
    v2, _ = cgroup
    v2.write_text("2147483648\n")
    assert get_memory_limit() == 2147483648


def test_memory_limit_cgroup_v2_unlimited(cgroup) -> None:
    v2, _ = cgroup
    v2.write_text("max\n")
    assert get_memory_limit() is None


def test_memory_limit_from_cgroup_v1(cgroup) -> None:
    _, v1 = cgroup
    v1.write_text("2147483648\n")
    assert get_memory_limit() == 2147483648


def test_memory_limit_cgroup_v1_unlimited(cgroup) -> None:
    _, v1 = cgroup
    v1.write_text("9223372036854771712\n")
    assert get_memory_limit() is None


def test_memory_limit_without_cgroup(cgroup) -> None:
    assert get_memory_limit() is None


def test_flush_threshold_from_config(cgroup) -> None:
    v2, _ = cgroup
    v2.write_text("2147483648\n")
    assert get_flush_threshold({"flush_threshold_mb": 10}) == 10 * 1024 * 1024


def test_flush_threshold_from_memory_limit(cgroup) -> None:
    v2, _ = cgroup
    v2.write_text("2147483648\n")
    assert (
        get_flush_threshold({}) == 2147483648 // MEMORY_LIMIT_TO_FLUSH_THRESHOLD_RATIO
    )


def test_flush_threshold_default(cgroup) -> None:
    assert get_flush_threshold({}) == DEFAULT_FLUSH_THRESHOLD


def test_should_flush_once_threshold_is_reached(cgroup, monkeypatch) -> None:
    monkeypatch.setattr(altertable_writer, "ROWS_PER_CHUNK", 2)
    writer = make_writer(CONFIG)
    writer.set_streams(CATALOG)
    writer.flush_threshold = 1

    # Pending records are only counted once converted to Arrow
    writer.buffer_record(make_record(1))
    assert writer.buffered_bytes == 0
    assert not writer.should_flush()

    writer.buffer_record(make_record(2))
    assert writer.buffered_bytes == writer.buffer["test_table"].batches[0].nbytes
    assert writer.should_flush()


def test_records_are_converted_to_arrow_in_chunks(cgroup, monkeypatch) -> None:
    monkeypatch.setattr(altertable_writer, "ROWS_PER_CHUNK", 2)
    writer = make_writer(CONFIG)
    writer.set_streams(CATALOG)
    for i in range(5):
        writer.buffer_record(make_record(i))

    assert [b.num_rows for b in writer.buffer["test_table"].batches] == [2, 2]

    writer.flush()

    ingest_writer = writer.client.ingest.return_value.__enter__.return_value
    batches = [call.args[0] for call in ingest_writer.write_batch.call_args_list]
    assert [b.num_rows for b in batches] == [2, 2, 1]
    assert [row for b in batches for row in b.to_pylist()] == [
        {"id": i, "name": f"Person {i}"} for i in range(5)
    ]


def test_flush_resets_buffered_bytes(cgroup) -> None:
    writer = make_writer(CONFIG)
    writer.set_streams(CATALOG)
    writer.buffer_record(make_record(1))

    writer.flush()

    assert writer.buffered_bytes == 0
    assert not writer.should_flush()
    writer.client.ingest.assert_called_once()

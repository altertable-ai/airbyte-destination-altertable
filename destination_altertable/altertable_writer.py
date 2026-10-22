from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import altertable_flightsql
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode
import pyarrow as pa
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
)

from .type_conversion import convert_to_arrow


# Maximum GRPC message size is 512MB. We use 400MB as a safety margin because
# we only have a message size average.
MAX_BATCH_SIZE = 400 * 1024 * 1024

# Records are converted to Arrow in chunks of this many rows, so that at most
# one chunk per stream is held as Python objects.
ROWS_PER_CHUNK = 1000

# Buffered records take ~2x their Arrow size in memory at flush time, so
# flushing every 1/5th of the memory limit keeps the peak below half of it.
MEMORY_LIMIT_TO_FLUSH_THRESHOLD_RATIO = 5
DEFAULT_FLUSH_THRESHOLD = 100 * 1024 * 1024

CGROUP_V2_MEMORY_LIMIT = Path("/sys/fs/cgroup/memory.max")
CGROUP_V1_MEMORY_LIMIT = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
# cgroup v1 reports "no limit" as a page-aligned value close to 2**63.
CGROUP_V1_UNLIMITED = 2**62


def get_memory_limit() -> Optional[int]:
    """Return the container memory limit in bytes, or None if unlimited or unknown."""
    for path in (CGROUP_V2_MEMORY_LIMIT, CGROUP_V1_MEMORY_LIMIT):
        try:
            value = path.read_text().strip()
        except OSError:
            continue
        if value == "max":
            return None
        limit = int(value)
        return limit if limit < CGROUP_V1_UNLIMITED else None
    return None


def get_flush_threshold(config: Dict[str, Any]) -> int:
    """Return the buffered size in bytes above which records are flushed."""
    if flush_threshold_mb := config.get("flush_threshold_mb"):
        return flush_threshold_mb * 1024 * 1024
    if memory_limit := get_memory_limit():
        return memory_limit // MEMORY_LIMIT_TO_FLUSH_THRESHOLD_RATIO
    return DEFAULT_FLUSH_THRESHOLD


@dataclass(frozen=True)
class AirbyteStream:
    name: str
    properties: dict[str, Any]
    sync_mode: DestinationSyncMode
    primary_key: list[str] = field(default_factory=list)
    cursor_field: list[str] = field(default_factory=list)

    @property
    def arrow_schema(self) -> pa.Schema:
        # Arrow types only depend on the JSON schema, not on the values.
        return pa.schema(
            (name, convert_to_arrow(None, prop)[0])
            for name, prop in self.properties.items()
        )


class StreamBuffer:
    """Records of a stream, converted to Arrow as they are buffered."""

    def __init__(self, stream: AirbyteStream):
        self.properties = stream.properties
        self.schema = stream.arrow_schema
        self.batches: list[pa.RecordBatch] = []
        self._columns: dict[str, list] = {name: [] for name in self.properties}
        self._pending_rows = 0

    def append(self, data: dict[str, Any]) -> int:
        """Buffer a record, returning the number of Arrow bytes it added."""
        for name, prop in self.properties.items():
            self._columns[name].append(convert_to_arrow(data.get(name), prop)[1])
        self._pending_rows += 1
        if self._pending_rows >= ROWS_PER_CHUNK:
            return self.convert_pending()
        return 0

    def convert_pending(self) -> int:
        """Convert pending records to an Arrow batch, returning its size in bytes."""
        if not self._pending_rows:
            return 0
        batch = pa.RecordBatch.from_pydict(self._columns, schema=self.schema)
        self.batches.append(batch)
        self._columns = {name: [] for name in self.properties}
        self._pending_rows = 0
        return batch.nbytes


class AltertableWriter:
    def __init__(self, config: Dict[str, Any]):
        self.client = altertable_flightsql.Client(
            username=config["username"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            tls=config.get("tls", True),
            catalog=config.get("catalog"),
            schema=config.get("schema"),
        )

        self.config = config
        self.buffer: Dict[str, StreamBuffer] = {}
        self.buffered_bytes = 0
        self.flush_threshold = get_flush_threshold(config)
        self.streams: Dict[str, AirbyteStream] = {}
        self._replaced_streams: set[str] = set()

    def __enter__(self):
        """Enter context manager"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and close client"""
        self.close()
        return False

    def close(self):
        """Close the Flight client connection"""
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass  # Ignore errors on close

    def set_streams(self, catalog: ConfiguredAirbyteCatalog):
        self.streams.update(
            {
                stream.stream.name: AirbyteStream(
                    name=stream.stream.name,
                    properties=stream.stream.json_schema["properties"],
                    sync_mode=stream.destination_sync_mode,
                    primary_key=self._get_primary_key(
                        stream.primary_key or stream.stream.source_defined_primary_key
                    ),
                    cursor_field=stream.cursor_field
                    or stream.stream.source_defined_cursor
                    or [],
                )
                for stream in catalog.streams
            }
        )

    def test_connection(self):
        stream = self.client.query("SELECT 1")
        stream.read_all()

    def buffer_record(self, record: AirbyteRecordMessage):
        if (buffer := self.buffer.get(record.stream)) is None:
            buffer = self.buffer[record.stream] = StreamBuffer(
                self.streams[record.stream]
            )
        self.buffered_bytes += buffer.append(record.data)

    def should_flush(self) -> bool:
        return self.buffered_bytes >= self.flush_threshold

    def _get_primary_key(self, primary_key: list[list[str]] | None) -> list[str]:
        if primary_key is None:
            return []
        if any(len(pkey) > 1 for pkey in primary_key):
            raise NotImplementedError("Nested primary keys are not supported yet")
        return [pkey[0] for pkey in primary_key]

    def _estimate_rows_per_batch(self, table: pa.Table) -> int:
        if rows_per_batch := self.config.get("rows_per_batch"):
            return rows_per_batch

        # Split table into batches to avoid gRPC message size limits (512MB default)
        # Use memory size to determine batch boundaries (400MB per batch to stay safely under 512MB limit)
        total_size = table.get_total_buffer_size()
        if total_size > MAX_BATCH_SIZE:
            return max(1, int(len(table) * MAX_BATCH_SIZE / total_size))
        else:
            return len(table)

    def _get_incremental_options(
        self, stream_config: AirbyteStream
    ) -> Optional[IngestIncrementalOptions]:
        if stream_config.sync_mode not in (
            DestinationSyncMode.append_dedup,
            DestinationSyncMode.update,
        ):
            return None

        if not stream_config.primary_key:
            raise ValueError(
                f"Stream {stream_config.name} has no primary key but {stream_config.sync_mode} requires one"
            )

        return IngestIncrementalOptions(
            primary_key=stream_config.primary_key,
            cursor_field=stream_config.cursor_field,
        )

    def _get_ingest_mode(
        self, stream: str, sync_mode: DestinationSyncMode
    ) -> IngestTableMode:
        if sync_mode != DestinationSyncMode.overwrite:
            return IngestTableMode.CREATE_APPEND
        if stream in self._replaced_streams:
            return IngestTableMode.CREATE_APPEND
        return IngestTableMode.REPLACE

    def flush(self):
        for stream, buffer in self.buffer.items():
            buffer.convert_pending()
            if not buffer.batches:
                continue

            stream_config = self.streams[stream]
            table = pa.Table.from_batches(buffer.batches, schema=buffer.schema)
            mode = self._get_ingest_mode(stream, stream_config.sync_mode)

            with self.client.ingest(
                table_name=stream,
                schema=table.schema,
                schema_name=self.config.get("schema", ""),
                catalog_name=self.config.get("catalog", ""),
                mode=mode,
                incremental_options=self._get_incremental_options(stream_config),
            ) as writer:
                estimated_rows_per_batch = self._estimate_rows_per_batch(table)
                batches = table.to_batches(max_chunksize=estimated_rows_per_batch)

                for batch in batches:
                    writer.write_batch(batch)

            if mode == IngestTableMode.REPLACE:
                self._replaced_streams.add(stream)

        self.buffer.clear()
        self.buffered_bytes = 0

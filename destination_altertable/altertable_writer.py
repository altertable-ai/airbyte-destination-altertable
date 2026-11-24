from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

import altertable_flightsql
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode
import pyarrow as pa
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
)

from .type_conversion import (
    convert_number_column,
    convert_to_arrow,
    is_number_column,
    unify_number_columns,
)


SESSION_EXPIRED_MESSAGE = "Session expired"

StreamKey = tuple[Optional[str], str]

T = TypeVar("T")

logger = getLogger("airbyte")


def is_session_expired(exc: BaseException) -> bool:
    """True when the server no longer knows the Flight session (e.g. it expired while idle)."""
    return isinstance(exc, pa.ArrowException) and SESSION_EXPIRED_MESSAGE in str(exc)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


# Records are converted to Arrow in chunks of at most this many rows or string
# bytes, so that at most one chunk per stream is held as Python objects. Chunks
# are written as is, so they must stay below the 512MB gRPC message limit.
ROWS_PER_CHUNK = 1000
BYTES_PER_CHUNK = 16 * 1024 * 1024

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
    namespace: Optional[str]
    properties: dict[str, Any]
    sync_mode: DestinationSyncMode
    truncate: bool
    primary_key: list[str] = field(default_factory=list)
    cursor_field: list[str] = field(default_factory=list)

    @property
    def arrow_schema(self) -> pa.Schema:
        """Arrow schema of the stream when it has no values.

        Arrow types only depend on the JSON schema, except for number columns.
        """
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
        # Number columns are typed on all their values, so they are converted per chunk.
        self._number_columns = {
            name for name, prop in self.properties.items() if is_number_column(prop)
        }
        self._batches_bytes = 0
        self._columns: dict[str, list] = {name: [] for name in self.properties}
        self._pending_rows = 0
        self._pending_bytes = 0

    @property
    def nbytes(self) -> int:
        """Arrow size of the converted records plus string size of the pending ones."""
        return self._batches_bytes + self._pending_bytes

    def append(self, data: dict[str, Any]) -> int:
        """Buffer a record, returning the number of bytes it added to the buffer."""
        nbytes = self.nbytes
        for name, prop in self.properties.items():
            value = data.get(name)
            if name not in self._number_columns:
                value = convert_to_arrow(value, prop)[1]
            self._columns[name].append(value)
            # Strings make records large, every other type is fixed width.
            if isinstance(value, str):
                self._pending_bytes += len(value)
        self._pending_rows += 1
        if (
            self._pending_rows >= ROWS_PER_CHUNK
            or self._pending_bytes >= BYTES_PER_CHUNK
        ):
            self.convert_pending()
        return self.nbytes - nbytes

    def convert_pending(self):
        """Convert pending records to an Arrow batch."""
        if not self._pending_rows:
            return
        arrays = []
        for field_ in self.schema:
            pa_type, values = field_.type, self._columns[field_.name]
            if field_.name in self._number_columns:
                pa_type, values = convert_number_column(values)
            arrays.append(pa.array(values, type=pa_type))
        batch = pa.RecordBatch.from_arrays(arrays, names=self.schema.names)
        self.batches.append(batch)
        self._batches_bytes += batch.nbytes
        self._columns = {name: [] for name in self.properties}
        self._pending_rows = 0
        self._pending_bytes = 0

    def take_batches(self) -> list[pa.RecordBatch]:
        """Return the buffered records as batches with the same schema, emptying the buffer."""
        self.convert_pending()
        batches = unify_number_columns(self.batches)
        self.batches = []
        self._batches_bytes = 0
        return batches


class AltertableWriter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = self._connect()
        self.buffer: Dict[StreamKey, StreamBuffer] = {}
        self.buffered_bytes = 0
        self.flush_threshold = get_flush_threshold(config)
        self.streams: Dict[StreamKey, AirbyteStream] = {}
        self._replaced_streams: set[StreamKey] = set()
        self._created_schemas: set[str] = set()
        self._written_schemas: Dict[StreamKey, pa.Schema] = {}

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

    def _connect(self) -> altertable_flightsql.Client:
        return altertable_flightsql.Client(
            username=self.config["username"],
            password=self.config["password"],
            host=self.config["host"],
            port=self.config["port"],
            tls=self.config.get("tls", True),
            catalog=self.config.get("catalog"),
            schema=self.config.get("schema"),
        )

    def _with_session_retry(self, call: Callable[[], T]) -> T:
        """Run `call`, opening a new Flight session and retrying once if the session expired.

        The session expires when the source keeps the destination idle for longer than the
        server's lease TTL (e.g. while reading a large stream). The server rejects an expired
        session before running anything, so the retry can't apply a write twice.
        """
        try:
            return call()
        except Exception as e:
            if not is_session_expired(e):
                raise
        logger.warning("Flight session expired; opening a new session")
        self.close()
        self.client = self._connect()
        return call()

    def set_streams(self, catalog: ConfiguredAirbyteCatalog):
        self.streams.update(
            {
                (stream.stream.namespace, stream.stream.name): AirbyteStream(
                    name=stream.stream.name,
                    namespace=stream.stream.namespace,
                    properties=stream.stream.json_schema["properties"],
                    sync_mode=stream.destination_sync_mode,
                    truncate=self._is_truncate(stream),
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
        stream_key = (record.namespace, record.stream)
        if (buffer := self.buffer.get(stream_key)) is None:
            buffer = self.buffer[stream_key] = StreamBuffer(self.streams[stream_key])
        self.buffered_bytes += buffer.append(record.data)

    def should_flush(self) -> bool:
        return self.buffered_bytes >= self.flush_threshold

    def complete_stream(self, namespace: Optional[str], name: str):
        self.flush()

        stream_key = (namespace, name)
        stream_config = self.streams.get(stream_key)
        if (
            stream_config is None
            or not stream_config.truncate
            or stream_key in self._replaced_streams
        ):
            return

        # The stream received no records (e.g. a clear): replace the table with
        # an empty one. The server ignores ingests without any batch.
        schema = stream_config.arrow_schema
        self._ingest(
            stream_key,
            stream_config,
            schema,
            [pa.RecordBatch.from_pylist([], schema=schema)],
            incremental_options=None,
        )

    def _is_truncate(self, stream: ConfiguredAirbyteStream) -> bool:
        if stream.destination_sync_mode == DestinationSyncMode.overwrite:
            return True
        # A minimum generation of 0 asks to keep all previous generations.
        return bool(stream.minimum_generation_id) and (
            stream.minimum_generation_id == stream.generation_id
        )

    def _get_primary_key(self, primary_key: list[list[str]] | None) -> list[str]:
        if primary_key is None:
            return []
        if any(len(pkey) > 1 for pkey in primary_key):
            raise NotImplementedError("Nested primary keys are not supported yet")
        return [pkey[0] for pkey in primary_key]

    def _get_schema_name(self, stream_config: AirbyteStream) -> str:
        return stream_config.namespace or self.config.get("schema", "")

    def _ensure_schema(self, schema_name: str):
        if schema_name in self._created_schemas:
            return

        catalog_name = self.config.get("catalog", "")
        qualified_name = quote_identifier(schema_name)
        if catalog_name:
            qualified_name = f"{quote_identifier(catalog_name)}.{qualified_name}"
        self._with_session_retry(
            lambda: self.client.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name}")
        )
        self._created_schemas.add(schema_name)

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
        self, stream_key: StreamKey, stream_config: AirbyteStream
    ) -> IngestTableMode:
        if not stream_config.truncate:
            return IngestTableMode.CREATE_APPEND
        if stream_key in self._replaced_streams:
            return IngestTableMode.CREATE_APPEND
        return IngestTableMode.REPLACE

    def _ingest(
        self,
        stream_key: StreamKey,
        stream_config: AirbyteStream,
        schema: pa.Schema,
        batches: list[pa.RecordBatch],
        incremental_options: Optional[IngestIncrementalOptions],
    ):
        mode = self._get_ingest_mode(stream_key, stream_config)

        schema_name = self._get_schema_name(stream_config)
        # The configured schema is expected to exist, namespaces are created on demand.
        if stream_config.namespace:
            self._ensure_schema(schema_name)

        def write():
            with self.client.ingest(
                table_name=stream_config.name,
                schema=schema,
                schema_name=schema_name,
                catalog_name=self.config.get("catalog", ""),
                mode=mode,
                incremental_options=incremental_options,
            ) as writer:
                for batch in batches:
                    writer.write_batch(batch)

        self._with_session_retry(write)

        if mode == IngestTableMode.REPLACE:
            self._replaced_streams.add(stream_key)

    def _cast_to_written_schema(
        self, stream_key: StreamKey, batches: list[pa.RecordBatch]
    ) -> list[pa.RecordBatch]:
        """Give number columns the type of their first flush.

        The server silently casts batches to the table's column types, so lossy casts
        (e.g. rounding large integers to float64) are rejected here instead.
        """
        schema = self._written_schemas.setdefault(stream_key, batches[0].schema)
        if batches[0].schema == schema:
            return batches
        try:
            return [batch.cast(schema) for batch in batches]
        except pa.ArrowInvalid as e:
            raise ValueError(
                f"Stream {stream_key[1]} can't be written exactly with the column types of its previous records: {e}"
            ) from e

    def flush(self):
        for stream_key, buffer in self.buffer.items():
            batches = buffer.take_batches()
            if not batches:
                continue
            batches = self._cast_to_written_schema(stream_key, batches)

            stream_config = self.streams[stream_key]
            self._ingest(
                stream_key,
                stream_config,
                batches[0].schema,
                batches,
                self._get_incremental_options(stream_config),
            )

        self.buffer.clear()
        self.buffered_bytes = 0

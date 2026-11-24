from collections import defaultdict
from dataclasses import dataclass, field
from logging import getLogger
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

from .type_conversion import convert_to_arrow


# Maximum GRPC message size is 512MB. We use 400MB as a safety margin because
# we only have a message size average.
MAX_BATCH_SIZE = 400 * 1024 * 1024

SESSION_EXPIRED_MESSAGE = "Session expired"

StreamKey = tuple[Optional[str], str]

T = TypeVar("T")

logger = getLogger("airbyte")


def is_session_expired(exc: BaseException) -> bool:
    """True when the server no longer knows the Flight session (e.g. it expired while idle)."""
    return isinstance(exc, pa.ArrowException) and SESSION_EXPIRED_MESSAGE in str(exc)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@dataclass(frozen=True)
class AirbyteStream:
    name: str
    namespace: Optional[str]
    properties: dict[str, Any]
    sync_mode: DestinationSyncMode
    truncate: bool
    primary_key: list[str] = field(default_factory=list)
    cursor_field: list[str] = field(default_factory=list)


class AltertableWriter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = self._connect()
        self.buffer = defaultdict(list)
        self.streams: Dict[StreamKey, AirbyteStream] = {}
        self._replaced_streams: set[StreamKey] = set()
        self._created_schemas: set[str] = set()

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
        self.buffer[(record.namespace, record.stream)].append(record)

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
        schema = self._convert_records_to_pyarrow_table(stream_config, []).schema
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

    def _convert_records_to_pyarrow_table(
        self, stream_config: AirbyteStream, records: list[AirbyteRecordMessage]
    ) -> pa.Table:
        properties = stream_config.properties

        fields = []
        converted_rows = [{} for _ in records]

        for field_name, prop in properties.items():
            # Get the first non-null value to determine type (or use None)
            sample_value = next(
                (
                    r.data.get(field_name)
                    for r in records
                    if r.data.get(field_name) is not None
                ),
                None,
            )
            pa_type, _ = convert_to_arrow(sample_value, prop)
            fields.append((field_name, pa_type))

            # Convert all values for this field
            for i, record in enumerate(records):
                _, converted = convert_to_arrow(record.data.get(field_name), prop)
                converted_rows[i][field_name] = converted

        schema = pa.schema(fields)
        return pa.Table.from_pylist(converted_rows, schema=schema)

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

    def flush(self):
        for stream_key, records in self.buffer.items():
            if not records:
                continue

            stream_config = self.streams[stream_key]
            table = self._convert_records_to_pyarrow_table(stream_config, records)
            estimated_rows_per_batch = self._estimate_rows_per_batch(table)
            self._ingest(
                stream_key,
                stream_config,
                table.schema,
                table.to_batches(max_chunksize=estimated_rows_per_batch),
                self._get_incremental_options(stream_config),
            )

        self.buffer.clear()

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import altertable_flightsql
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode
import pyarrow as pa
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
)


# Maximum GRPC message size is 4MB. We use 3MB as a safety margin because
# we only have a message size average.
MAX_BATCH_SIZE = 3 * 1024 * 1024


@dataclass(frozen=True)
class AirbyteStream:
    name: str
    properties: dict[str, Any]
    sync_mode: DestinationSyncMode
    primary_key: list[str] = field(default_factory=list)
    cursor_field: list[str] = field(default_factory=list)


class AltertableWriter:
    def __init__(self, config: Dict[str, Any]):
        self.client = altertable_flightsql.Client(
            username=config["username"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            tls=config.get("tls", True),
        )

        self.config = config
        self.buffer = defaultdict(list)
        self.streams: Dict[str, AirbyteStream] = {}

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
        stream.readall()

    def buffer_record(self, record: AirbyteRecordMessage):
        self.buffer[record.stream].append(record)

    def _get_primary_key(self, primary_key: list[list[str]] | None) -> list[str]:
        if primary_key is None:
            return []
        if any(len(pkey) > 1 for pkey in primary_key):
            raise NotImplementedError("Nested primary keys are not supported yet")
        return [pkey[0] for pkey in primary_key]

    def _airbyte_type_to_arrow_type(self, airbyte_type: str) -> pa.DataType:
        if isinstance(airbyte_type, list):
            airbyte_type = next((t for t in airbyte_type if t != "null"))

        return {
            "string": pa.string(),
            "integer": pa.int64(),
            "number": pa.float64(),
            "boolean": pa.bool_(),
        }.get(airbyte_type, pa.string())

    def _convert_records_to_pyarrow_table(
        self, stream: str, records: list[AirbyteRecordMessage]
    ) -> pa.Table:
        schema = pa.schema(
            [
                (field, self._airbyte_type_to_arrow_type(property.get("type")))
                for field, property in self.streams[stream].properties.items()
            ]
        )
        rows = [
            {k: json.dumps(v) if isinstance(v, dict) else v for k, v in r.data.items()}
            for r in records
        ]

        return pa.Table.from_pylist(rows, schema=schema)

    def _estimate_rows_per_batch(self, table: pa.Table) -> int:
        if rows_per_batch := self.config.get("rows_per_batch"):
            return rows_per_batch

        # Split table into batches to avoid gRPC message size limits (4MB default)
        # Use memory size to determine batch boundaries (3MB per batch to stay safely under 4MB limit)
        total_size = table.get_total_buffer_size()
        if total_size > MAX_BATCH_SIZE:
            return int(len(table) * MAX_BATCH_SIZE / total_size)
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

    def flush(self):
        for stream, records in self.buffer.items():
            if not records:
                continue

            stream_config = self.streams[stream]
            table = self._convert_records_to_pyarrow_table(stream, records)

            with self.client.ingest(
                table_name=stream,
                schema=table.schema,
                schema_name=self.config.get("schema", ""),
                catalog_name=self.config.get("catalog", ""),
                mode=IngestTableMode.REPLACE
                if stream_config.sync_mode == DestinationSyncMode.overwrite
                else IngestTableMode.CREATE_APPEND,
                incremental_options=self._get_incremental_options(stream_config),
            ) as writer:
                estimated_rows_per_batch = self._estimate_rows_per_batch(table)
                batches = table.to_batches(max_chunksize=estimated_rows_per_batch)

                for batch in batches:
                    writer.write_batch(batch)

        self.buffer.clear()

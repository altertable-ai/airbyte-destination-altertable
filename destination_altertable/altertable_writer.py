import base64
import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict

import pyarrow as pa
import pyarrow.flight
import pyarrow.ipc as ipc
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
)
from google.protobuf.any_pb2 import Any as ProtobufAny

from destination_altertable import arrow_flight_sql_definitions


@dataclass(frozen=True)
class AirbyteStream:
    name: str
    properties: dict[str, Any]
    sync_mode: DestinationSyncMode
    primary_key: list[list[str]] | None = None


class AltertableWriter:
    def __init__(self, config: Dict[str, Any]):
        scheme = "grpc+tls" if config.get("tls", True) else "grpc"
        location = f"{scheme}://{config['host']}:{config['port']}"

        self.config = config
        self.client = pyarrow.flight.FlightClient(location)
        self.auth_header = self._get_auth_header(config["username"], config["password"])
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

    @classmethod
    def _get_auth_header(cls, username, password) -> str:
        """Generate basic auth header value"""
        credentials = f"{username}:{password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Bearer {encoded}".encode()

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
                )
                for stream in catalog.streams
            }
        )

    def test_connection(self):
        list(self.client.list_actions())

    def buffer_record(self, record: AirbyteRecordMessage):
        self.buffer[record.stream].append(record)

    def _get_primary_key(self, primary_key: list[list[str]] | None) -> list[str] | None:
        if primary_key is None:
            return None
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

    def _convert_airbyte_sync_mode_to_arrow_flight_mode(
        self, sync_mode: DestinationSyncMode
    ) -> tuple:
        """Convert Airbyte sync mode to Arrow Flight SQL table definition options.

        Returns:
            tuple: (if_not_exist_option, if_exists_option)
        """
        if sync_mode == DestinationSyncMode.overwrite:
            return (
                arrow_flight_sql_definitions.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption.TABLE_NOT_EXIST_OPTION_CREATE,
                arrow_flight_sql_definitions.CommandStatementIngest.TableDefinitionOptions.TableExistsOption.TABLE_EXISTS_OPTION_REPLACE,
            )
        else:  # append/create_append
            return (
                arrow_flight_sql_definitions.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption.TABLE_NOT_EXIST_OPTION_CREATE,
                arrow_flight_sql_definitions.CommandStatementIngest.TableDefinitionOptions.TableExistsOption.TABLE_EXISTS_OPTION_APPEND,
            )

    def _estimate_rows_per_batch(self, table: pa.Table) -> int:
        if rows_per_batch := self.config.get("rows_per_batch"):
            return rows_per_batch

        # Split table into batches to avoid gRPC message size limits (4MB default)
        # Use memory size to determine batch boundaries (3MB per batch to stay safely under 4MB limit)
        total_size = table.get_total_buffer_size()
        if total_size > 3 * 1024 * 1024:
            return int(len(table) * 3 * 1024 * 1024 / total_size)
        else:
            return len(table)

    def flush(self):
        for stream, records in self.buffer.items():
            if not records:
                continue

            stream_config = self.streams[stream]

            table = self._convert_records_to_pyarrow_table(stream, records)
            if_not_exist, if_exists = (
                self._convert_airbyte_sync_mode_to_arrow_flight_mode(
                    stream_config.sync_mode
                )
            )

            command = arrow_flight_sql_definitions.CommandStatementIngest()
            command.table = stream
            command.schema = self.config.get("schema", "")
            command.catalog = self.config.get("catalog", "")
            command.table_definition_options.if_not_exist = if_not_exist
            command.table_definition_options.if_exists = if_exists

            if self.streams[stream].sync_mode in (
                DestinationSyncMode.append_dedup,
                DestinationSyncMode.update,
            ):
                if not stream_config.primary_key:
                    raise ValueError(f"Stream {stream} has no primary key but {stream_config.sync_mode} requires one")
                command.options["upsert_keys"] = json.dumps(stream_config.primary_key)


            command_bytes = command.SerializeToString()
            any_message = ProtobufAny()
            any_message.type_url = (
                "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementIngest"
            )
            any_message.value = command_bytes

            any_bytes = any_message.SerializeToString()
            descriptor = pyarrow.flight.FlightDescriptor.for_command(any_bytes)

            call_options = pyarrow.flight.FlightCallOptions(
                headers=[(b"authorization", self.auth_header)]
            )

            estimated_rows_per_batch = self._estimate_rows_per_batch(table)
            batches = table.to_batches(max_chunksize=estimated_rows_per_batch)

            writer, _ = self.client.do_put(
                descriptor, table.schema, options=call_options
            )
            with writer:
                for batch in batches:
                    writer.write_batch(batch)

        self.buffer.clear()

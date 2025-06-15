import trino
import base64
import json
import pyarrow as pa
import pyarrow.parquet as pq
import io
import requests
from typing import Dict, Any
from collections import defaultdict
from enum import Enum
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
)


class ParquetUploaderMode(Enum):
    append = "append"
    append_dedup = "append_dedup"


class ParquetUploader:
    def __init__(
        self,
        host: str,
        port: int,
        http_port: int | None,
        catalog: str,
        schema: str,
        username: str,
        password: str,
        verify: bool,
    ):
        auth_str = f"{username}:{password}"
        auth_bytes = auth_str.encode("ascii")
        self.base64_auth = base64.b64encode(auth_bytes).decode("ascii")
        scheme = "http" if http_port else "https"
        self.url = f"{scheme}://{host}:{http_port or port}/upload"
        self.catalog = catalog
        self.schema = schema
        self.verify = verify

    def upload(
        self,
        table: str,
        buffer: io.BytesIO,
        mode: ParquetUploaderMode,
        primary_key: str | None,
    ):
        params = {
            "catalog": self.catalog,
            "schema": self.schema,
            "table": table,
            "format": "parquet",
            "mode": mode.value,
            "primary_key": primary_key,
        }

        headers = {
            "Authorization": f"Basic {self.base64_auth}",
        }

        try:
            response = requests.post(
                self.url,
                params=params,
                headers=headers,
                data=buffer,
                verify=self.verify,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(
                f"Failed to upload parquet: {e.response.status_code} {e.response.text}"
            )
            raise


def get_primary_key(primary_key: list[list[str]] | None) -> str | None:
    if primary_key is None:
        return None
    if len(primary_key) > 1:
        raise NotImplementedError("Composite primary keys are not supported yet")
    if len(primary_key[0]) > 1:
        raise NotImplementedError("Nested primary keys are not supported yet")
    return primary_key[0][0]


class AltertableWriter:
    def __init__(self, config: Dict[str, Any]):
        self.conn = trino.dbapi.connect(
            host=config["host"],
            port=config["port"],
            user=config["username"],
            auth=trino.auth.BasicAuthentication(config["username"], config["password"]),
            catalog=config["catalog"],
            schema=config["schema"],
            http_scheme="https",
            verify=config["verify_ssl_certificate"],
        )
        self.parquet_uploader = ParquetUploader(
            host=config["host"],
            port=config["port"],
            http_port=config.get("http_port"),
            catalog=config["catalog"],
            schema=config["schema"],
            username=config["username"],
            password=config["password"],
            verify=config["verify_ssl_certificate"],
        )
        self.config = config
        self.buffer = defaultdict(list)
        self.streams = {}

    def set_catalog(self, catalog: ConfiguredAirbyteCatalog):
        for stream in catalog.streams:
            stream_name = stream.stream.name
            self.streams[stream_name] = {
                "properties": stream.stream.json_schema["properties"],
                "sync_mode": stream.destination_sync_mode,
                "primary_key": get_primary_key(
                    stream.primary_key or stream.stream.source_defined_primary_key
                ),
            }

    def test_connection(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()

    def drop_tables_if_overwrite(self):
        cursor = self.conn.cursor()
        for stream, params in self.streams.items():
            if params["sync_mode"] == DestinationSyncMode.overwrite:
                cursor.execute(f"DROP TABLE IF EXISTS {stream}")

    def buffer_record(self, record: AirbyteRecordMessage):
        self.buffer[record.stream].append(record)

    def _create_table_if_not_exists(self, stream: str):
        columns = []

        for field, property in self.streams[stream]["properties"].items():
            airbyte_type = property.get("type")
            iceberg_type = self._airbyte_type_to_iceberg_type(airbyte_type)
            columns.append(f"{field} {iceberg_type}")

        columns_sql = ", ".join(columns)
        query = f"CREATE TABLE IF NOT EXISTS {stream} ({columns_sql})"
        cursor = self.conn.cursor()
        cursor.execute(query)

    def _airbyte_type_to_iceberg_type(self, airbyte_type: str) -> str:
        if isinstance(airbyte_type, list):
            airbyte_type = [t for t in airbyte_type if t != "null"][0]
        return {
            "string": "VARCHAR",
            "integer": "BIGINT",
            "number": "DOUBLE",
            "boolean": "BOOLEAN",
        }.get(airbyte_type, "VARCHAR")

    def _airbyte_type_to_arrow_type(self, airbyte_type: str) -> str:
        if isinstance(airbyte_type, list):
            airbyte_type = [t for t in airbyte_type if t != "null"][0]
        return {
            "string": pa.string(),
            "integer": pa.int64(),
            "number": pa.float64(),
            "boolean": pa.bool_(),
        }.get(airbyte_type, pa.string())

    def _convert_records_to_parquet(
        self, stream: str, records: list[AirbyteRecordMessage]
    ) -> io.BytesIO:
        schema = pa.schema(
            [
                (field, self._airbyte_type_to_arrow_type(property.get("type")))
                for field, property in self.streams[stream]["properties"].items()
            ]
        )
        rows = [
            {k: json.dumps(v) if isinstance(v, dict) else v for k, v in r.data.items()}
            for r in records
        ]
        table = pa.Table.from_pylist(rows, schema=schema)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        return buffer

    def flush(self):
        for stream, records in self.buffer.items():
            if not records:
                continue

            self._create_table_if_not_exists(stream)

            buffer = self._convert_records_to_parquet(stream, records)
            params = self.streams[stream]
            sync_mode = (
                ParquetUploaderMode.append_dedup
                if params["sync_mode"] == DestinationSyncMode.append_dedup
                else ParquetUploaderMode.append
            )
            self.parquet_uploader.upload(
                stream,
                buffer,
                sync_mode,
                params["primary_key"],
            )

        self.buffer.clear()

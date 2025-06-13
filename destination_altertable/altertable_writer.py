import trino
import base64
import json
import pyarrow as pa
import pyarrow.parquet as pq
import io
import requests
from typing import Dict, Any
from collections import defaultdict
from airbyte_cdk.models import AirbyteRecordMessage, ConfiguredAirbyteCatalog


class ParquetUploader:
    def __init__(
        self,
        host: str,
        port: int,
        catalog: str,
        schema: str,
        username: str,
        password: str,
    ):
        auth_str = f"{username}:{password}"
        auth_bytes = auth_str.encode("ascii")
        self.base64_auth = base64.b64encode(auth_bytes).decode("ascii")
        self.url = f"http://{host}:{port}/upload"
        self.catalog = catalog
        self.schema = schema

    def upload(self, table: str, buffer: io.BytesIO):
        params = {
            "catalog": self.catalog,
            "schema": self.schema,
            "table": table,
            "format": "parquet",
            "mode": "append",
        }

        headers = {
            "Authorization": f"Basic {self.base64_auth}",
        }

        try:
            response = requests.post(
                self.url, params=params, headers=headers, data=buffer
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(
                f"Failed to upload parquet: {e.response.status_code} {e.response.text}"
            )
            raise


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
            verify=False,
        )
        self.parquet_uploader = ParquetUploader(
            host=config["host"],
            port=config.get("http_port", config["port"]),
            catalog=config["catalog"],
            schema=config["schema"],
            username=config["username"],
            password=config["password"],
        )
        self.config = config
        self.buffer = defaultdict(list)
        self.stream_properties = {}

    def test_connection(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()

    def set_catalog(self, catalog: ConfiguredAirbyteCatalog):
        for stream in catalog.streams:
            stream_name = stream.stream.name
            self.stream_properties[stream_name] = stream.stream.json_schema[
                "properties"
            ]

    def drop_tables(self):
        cursor = self.conn.cursor()
        for stream in self.stream_properties.keys():
            cursor.execute(f"DROP TABLE IF EXISTS {stream}")

    def buffer_record(self, record: AirbyteRecordMessage):
        self.buffer[record.stream].append(record)

    def _create_table_if_not_exists(self, stream: str):
        columns = []

        for field, definition in self.stream_properties[stream].items():
            airbyte_type = definition.get("type")
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
                (field, self._airbyte_type_to_arrow_type(definition.get("type")))
                for field, definition in self.stream_properties[stream].items()
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
            self.parquet_uploader.upload(stream, buffer)

        self.buffer.clear()

import trino
import json
from typing import Dict, Any
from collections import defaultdict
from airbyte_cdk.models import AirbyteRecordMessage, ConfiguredAirbyteCatalog


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
            json_type = definition.get("type", "string")
            sql_type = self._json_type_to_sql_type(json_type)
            columns.append(f"{field} {sql_type}")

        columns_sql = ", ".join(columns)
        query = f"CREATE TABLE IF NOT EXISTS {stream} ({columns_sql})"
        cursor = self.conn.cursor()
        cursor.execute(query)

    def _json_type_to_sql_type(self, json_type: str) -> str:
        if isinstance(json_type, list):
            json_type = [t for t in json_type if t != "null"][0]
        return {
            "string": "VARCHAR",
            "integer": "BIGINT",
            "number": "DOUBLE",
            "boolean": "BOOLEAN",
        }.get(json_type, "VARCHAR")

    def flush(self):
        for stream, records in self.buffer.items():
            if not records:
                continue

            self._create_table_if_not_exists(stream)
            fields = list(self.stream_properties[stream].keys())

            value_rows = []
            for rec in records:
                values = []
                for field in self.stream_properties[stream].keys():
                    v = rec.data.get(field)
                    if isinstance(v, str):
                        values.append(f"'{v.replace('\'', '\'\'')}'")
                    elif isinstance(v, bool):
                        values.append(str(v).upper())
                    elif isinstance(v, dict):
                        json_str = json.dumps(v)
                        values.append(f"'{json_str.replace('\'', '\'\'')}'")
                    elif v is None:
                        values.append("NULL")
                    else:
                        values.append(str(v))
                value_rows.append(f"({', '.join(values)})")

            sql_fields = ", ".join(fields)
            sql_values = ",\n".join(value_rows)

            sql = f"INSERT INTO {stream} ({sql_fields}) VALUES {sql_values}"
            self.conn.cursor().execute(sql)

        self.buffer.clear()

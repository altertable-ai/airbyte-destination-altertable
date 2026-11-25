from __future__ import annotations

from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from altertable_flightsql.client import IngestTableMode

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


def make_stream(
    name: str,
    namespace: str | None = None,
    destination_sync_mode: DestinationSyncMode = DestinationSyncMode.append,
) -> ConfiguredAirbyteStream:
    return ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name=name,
            namespace=namespace,
            json_schema={"type": "object", "properties": {"id": {"type": "integer"}}},
            supported_sync_modes=[SyncMode.full_refresh],
        ),
        sync_mode=SyncMode.full_refresh,
        destination_sync_mode=destination_sync_mode,
    )


def make_record(
    stream: str, record_id: int, namespace: str | None = None
) -> AirbyteRecordMessage:
    return AirbyteRecordMessage(
        stream=stream, namespace=namespace, data={"id": record_id}, emitted_at=1000
    )


@pytest.fixture
def client() -> Generator[MagicMock, None, None]:
    with patch(
        "destination_altertable.altertable_writer.altertable_flightsql.Client"
    ) as client_class:
        yield client_class.return_value


def ingested_tables(client: MagicMock) -> list[tuple[str, str, str]]:
    return [
        (c.kwargs["catalog_name"], c.kwargs["schema_name"], c.kwargs["table_name"])
        for c in client.ingest.call_args_list
    ]


def ingested_ids(client: MagicMock) -> list[list[int]]:
    writer = client.ingest.return_value.__enter__.return_value
    return [
        batch.args[0].column("id").to_pylist()
        for batch in writer.write_batch.call_args_list
    ]


def created_schemas(client: MagicMock) -> list[str]:
    return [
        c.args[0]
        for c in client.execute.call_args_list
        if c.args[0].startswith("CREATE SCHEMA")
    ]


def test_stream_without_namespace_uses_configured_schema(client: MagicMock) -> None:
    writer = AltertableWriter(CONFIG)
    writer.set_streams(ConfiguredAirbyteCatalog(streams=[make_stream("forms")]))

    writer.buffer_record(make_record("forms", 1))
    writer.flush()

    assert ingested_tables(client) == [("my_catalog", "main", "forms")]
    assert created_schemas(client) == []


def test_stream_namespace_maps_to_schema(client: MagicMock) -> None:
    writer = AltertableWriter(CONFIG)
    writer.set_streams(
        ConfiguredAirbyteCatalog(
            streams=[
                make_stream("forms", "AIRBYTE_TYPEFORM"),
                make_stream("responses", "AIRBYTE_TYPEFORM"),
            ]
        )
    )

    writer.buffer_record(make_record("forms", 1, "AIRBYTE_TYPEFORM"))
    writer.buffer_record(make_record("responses", 2, "AIRBYTE_TYPEFORM"))
    writer.flush()
    writer.buffer_record(make_record("forms", 3, "AIRBYTE_TYPEFORM"))
    writer.flush()

    assert ingested_tables(client) == [
        ("my_catalog", "AIRBYTE_TYPEFORM", "forms"),
        ("my_catalog", "AIRBYTE_TYPEFORM", "responses"),
        ("my_catalog", "AIRBYTE_TYPEFORM", "forms"),
    ]
    assert created_schemas(client) == [
        'CREATE SCHEMA IF NOT EXISTS "my_catalog"."AIRBYTE_TYPEFORM"'
    ]


def test_namespace_is_quoted_in_create_schema(client: MagicMock) -> None:
    writer = AltertableWriter(CONFIG)
    writer.set_streams(
        ConfiguredAirbyteCatalog(streams=[make_stream("forms", 'my "ns"')])
    )

    writer.buffer_record(make_record("forms", 1, 'my "ns"'))
    writer.flush()

    assert created_schemas(client) == [
        'CREATE SCHEMA IF NOT EXISTS "my_catalog"."my ""ns"""'
    ]


def test_same_stream_name_in_different_namespaces(client: MagicMock) -> None:
    writer = AltertableWriter(CONFIG)
    writer.set_streams(
        ConfiguredAirbyteCatalog(
            streams=[make_stream("users", "app_a"), make_stream("users", "app_b")]
        )
    )

    writer.buffer_record(make_record("users", 1, "app_a"))
    writer.buffer_record(make_record("users", 2, "app_b"))
    writer.flush()

    assert ingested_tables(client) == [
        ("my_catalog", "app_a", "users"),
        ("my_catalog", "app_b", "users"),
    ]
    assert ingested_ids(client) == [[1], [2]]
    assert created_schemas(client) == [
        'CREATE SCHEMA IF NOT EXISTS "my_catalog"."app_a"',
        'CREATE SCHEMA IF NOT EXISTS "my_catalog"."app_b"',
    ]


def test_overwrite_replaces_once_per_namespace(client: MagicMock) -> None:
    writer = AltertableWriter(CONFIG)
    writer.set_streams(
        ConfiguredAirbyteCatalog(
            streams=[
                make_stream("users", "app_a", DestinationSyncMode.overwrite),
                make_stream("users", "app_b", DestinationSyncMode.overwrite),
            ]
        )
    )

    writer.buffer_record(make_record("users", 1, "app_a"))
    writer.flush()
    writer.buffer_record(make_record("users", 2, "app_a"))
    writer.buffer_record(make_record("users", 3, "app_b"))
    writer.flush()

    assert [c.kwargs["mode"] for c in client.ingest.call_args_list] == [
        IngestTableMode.REPLACE,
        IngestTableMode.CREATE_APPEND,
        IngestTableMode.REPLACE,
    ]

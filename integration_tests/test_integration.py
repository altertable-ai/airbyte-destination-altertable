"""
Integration tests for DestinationAltertable using testcontainers.

Uses ghcr.io/altertable-ai/altertable-mock to simulate the Altertable backend.
"""

import uuid
from unittest.mock import patch

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    AirbyteStreamState,
    AirbyteStreamStatus,
    AirbyteStreamStatusTraceMessage,
    AirbyteTraceMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    StreamDescriptor,
    SyncMode,
    TraceType,
    Type,
)

from destination_altertable.altertable_writer import AltertableWriter

JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "created_at": {"type": "string", "format": "date-time"},
    },
}
PRIMARY_KEY = [["id"]]
CURSOR_FIELD = ["created_at"]


def make_configured_catalog(
    stream_name: str,
    destination_sync_mode: DestinationSyncMode,
    json_schema: dict,
    primary_key: list[list[str]] | None = None,
    cursor_field: list[str] | None = None,
    source_defined_cursor: bool | None = None,
    namespace: str | None = None,
    generation_id: int | None = None,
    minimum_generation_id: int | None = None,
) -> ConfiguredAirbyteCatalog:
    """Helper to create a ConfiguredAirbyteCatalog for testing."""
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=stream_name,
                    namespace=namespace,
                    json_schema=json_schema,
                    supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
                    source_defined_primary_key=primary_key,
                    source_defined_cursor=source_defined_cursor,
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=destination_sync_mode,
                primary_key=primary_key,
                cursor_field=cursor_field,
                generation_id=generation_id,
                minimum_generation_id=minimum_generation_id,
            )
        ]
    )


def make_record_message(
    stream: str, data: dict, emitted_at: int = 1000, namespace: str | None = None
) -> AirbyteMessage:
    """Helper to create an AirbyteMessage with a record."""
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(
            stream=stream,
            namespace=namespace,
            data=data,
            emitted_at=emitted_at,
        ),
    )


def make_state_message(stream: str) -> AirbyteMessage:
    """Helper to create an AirbyteMessage with a state."""
    return AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name=stream),
            ),
        ),
    )


def make_stream_complete_message(stream: str) -> AirbyteMessage:
    """Helper to create the stream completion message sent by the platform."""
    return AirbyteMessage(
        type=Type.TRACE,
        trace=AirbyteTraceMessage(
            type=TraceType.STREAM_STATUS,
            emitted_at=1000,
            stream_status=AirbyteStreamStatusTraceMessage(
                stream_descriptor=StreamDescriptor(name=stream),
                status=AirbyteStreamStatus.COMPLETE,
            ),
        ),
    )


def query_table(client, catalog: str, schema: str, table: str) -> list[dict]:
    """Query a table and return results as a list of dicts."""
    stream = client.query(f'SELECT * FROM "{catalog}"."{schema}"."{table}"')
    table_result = stream.read_all()
    return table_result.to_pylist()


class TestCheck:
    """Test the check() method."""

    def test_check_success(self, destination, altertable_service):
        """Test that check() succeeds with valid altertable_service."""
        result = destination.check(logger=None, config=altertable_service)

        assert result.status == Status.SUCCEEDED
        assert result.message is None

    def test_check_failure_invalid_host(self, destination, altertable_service):
        """Test that check() fails with invalid host."""
        invalid_config = {
            **altertable_service,
            "host": "invalid-host-that-does-not-exist",
        }

        result = destination.check(logger=None, config=invalid_config)

        assert result.status == Status.FAILED
        assert result.message is not None


class TestOverwriteSyncMode:
    """Test the overwrite destination sync mode."""

    def test_overwrite_replaces_all_data(
        self, destination, altertable_service, client, data
    ):
        """Test that overwrite mode replaces all existing data."""
        catalog_name, schema_name, table_name = data
        test_config = {
            **altertable_service,
            "catalog": catalog_name,
            "schema": schema_name,
        }

        catalog = make_configured_catalog(
            stream_name=table_name,
            destination_sync_mode=DestinationSyncMode.overwrite,
            json_schema=JSON_SCHEMA,
        )

        # Overwrite with new data
        new_records = [
            make_record_message(
                table_name,
                {"id": 10, "name": "New Person", "created_at": "2024-01-01T00:00:00Z"},
            ),
            make_state_message(table_name),
        ]

        messages = list(destination.write(test_config, catalog, iter(new_records)))
        assert len(messages) == 1

        # Verify data was replaced (only new record should exist)
        results = query_table(client, catalog_name, schema_name, table_name)
        assert len(results) == 1
        assert results[0]["id"] == 10
        assert results[0]["name"] == "New Person"

    def test_overwrite_multiple_batches_preserves_all_records(
        self, destination, altertable_service, client, data
    ):
        """Test that overwrite mode keeps records from all batches, not just the last one.

        Airbyte sends data in multiple batches separated by state messages.
        The table should be replaced once (on the first batch) and subsequent
        batches should append so that all records survive.
        """
        catalog_name, schema_name, table_name = data
        test_config = {
            **altertable_service,
            "catalog": catalog_name,
            "schema": schema_name,
        }

        catalog = make_configured_catalog(
            stream_name=table_name,
            destination_sync_mode=DestinationSyncMode.overwrite,
            json_schema=JSON_SCHEMA,
        )

        # Simulate three batches, each flushed by a state message
        input_messages = [
            # Batch 1
            make_record_message(
                table_name,
                {"id": 10, "name": "Batch 1 - A", "created_at": "2024-01-01T00:00:00Z"},
            ),
            make_record_message(
                table_name,
                {"id": 11, "name": "Batch 1 - B", "created_at": "2024-01-01T00:00:00Z"},
            ),
            make_state_message(table_name),
            # Batch 2
            make_record_message(
                table_name,
                {"id": 12, "name": "Batch 2 - A", "created_at": "2024-01-02T00:00:00Z"},
            ),
            make_state_message(table_name),
            # Batch 3
            make_record_message(
                table_name,
                {"id": 13, "name": "Batch 3 - A", "created_at": "2024-01-03T00:00:00Z"},
            ),
            make_record_message(
                table_name,
                {"id": 14, "name": "Batch 3 - B", "created_at": "2024-01-03T00:00:00Z"},
            ),
            make_state_message(table_name),
        ]

        messages = list(destination.write(test_config, catalog, iter(input_messages)))
        assert len(messages) == 3  # one state message echoed per batch

        results = query_table(client, catalog_name, schema_name, table_name)

        # Pre-existing rows (ids 1,2,3) should be gone; all 5 new records should be present
        assert len(results) == 5
        assert {r["id"] for r in results} == {10, 11, 12, 13, 14}

    def test_overwrite_flushes_early_between_states(
        self, destination, altertable_service, client, data
    ):
        """Test that records are flushed once the buffer is full, without waiting
        for a state message, and that only the first flush replaces the table."""
        catalog_name, schema_name, table_name = data
        test_config = {
            **altertable_service,
            "catalog": catalog_name,
            "schema": schema_name,
            "flush_threshold_mb": 1,
        }

        catalog = make_configured_catalog(
            stream_name=table_name,
            destination_sync_mode=DestinationSyncMode.overwrite,
            json_schema=JSON_SCHEMA,
        )

        # ~70 bytes per record, so ~3MB in total
        record_count = 40_000
        input_messages = [
            make_record_message(
                table_name,
                {"id": i, "name": f"Person {i}", "created_at": "2024-01-01T00:00:00Z"},
            )
            for i in range(record_count)
        ] + [make_state_message(table_name)]

        with patch.object(
            AltertableWriter, "flush", autospec=True, side_effect=AltertableWriter.flush
        ) as flush:
            messages = list(
                destination.write(test_config, catalog, iter(input_messages))
            )

        assert len(messages) == 1
        assert flush.call_count > 1

        results = query_table(client, catalog_name, schema_name, table_name)
        assert len(results) == record_count
        assert {r["id"] for r in results} == set(range(record_count))


class TestAppendSyncMode:
    """Test the append destination sync mode."""

    def test_append_adds_to_existing_data(
        self, destination, altertable_service, client, data
    ):
        """Test that append mode adds new records without replacing existing ones."""
        catalog_name, schema_name, table_name = data
        test_config = {
            **altertable_service,
            "catalog": catalog_name,
            "schema": schema_name,
        }

        # Verify pre-existing data (3 rows from fixture)
        results = query_table(client, catalog_name, schema_name, table_name)
        assert len(results) == 3

        catalog = make_configured_catalog(
            stream_name=table_name,
            destination_sync_mode=DestinationSyncMode.append,
            json_schema=JSON_SCHEMA,
        )

        # Append new data
        new_records = [
            make_record_message(
                table_name,
                {
                    "id": 10,
                    "name": "New Person 1",
                    "created_at": "2024-01-01T00:00:00Z",
                },
            ),
            make_record_message(
                table_name,
                {
                    "id": 11,
                    "name": "New Person 2",
                    "created_at": "2024-01-01T00:00:00Z",
                },
            ),
            make_state_message(table_name),
        ]

        messages = list(destination.write(test_config, catalog, iter(new_records)))
        assert len(messages) == 1

        # Verify all data is present (original 3 + new 2)
        results = query_table(client, catalog_name, schema_name, table_name)
        assert len(results) == 5
        assert {r["id"] for r in results} == {1, 2, 3, 10, 11}


class TestAppendDedupSyncMode:
    """Test the append_dedup destination sync mode with primary key and cursor."""

    def test_append_dedup_upserts_by_primary_key(
        self, destination, altertable_service, client, data
    ):
        """Test that append_dedup mode deduplicates by primary key using cursor field."""
        catalog_name, schema_name, table_name = data
        test_config = {
            **altertable_service,
            "catalog": catalog_name,
            "schema": schema_name,
        }

        # Verify pre-existing data (3 rows from fixture: ids 1, 2, 3)
        results = query_table(client, catalog_name, schema_name, table_name)
        assert len(results) == 3

        catalog = make_configured_catalog(
            stream_name=table_name,
            destination_sync_mode=DestinationSyncMode.append_dedup,
            json_schema=JSON_SCHEMA,
            primary_key=PRIMARY_KEY,
            cursor_field=CURSOR_FIELD,
            source_defined_cursor=True,
        )

        # Update existing record (id=1) and add new one (id=10)
        update_records = [
            make_record_message(
                table_name,
                {
                    "id": 1,
                    "name": "John Doe Updated",
                    "created_at": "2024-01-01T00:00:00Z",
                },
            ),
            make_record_message(
                table_name,
                {"id": 10, "name": "New Person", "created_at": "2024-01-01T00:00:00Z"},
            ),
            make_state_message(table_name),
        ]

        messages = list(destination.write(test_config, catalog, iter(update_records)))
        assert len(messages) == 1

        # Verify: id=1 updated, id=2 and id=3 unchanged, id=10 added
        results = query_table(client, catalog_name, schema_name, table_name)
        assert len(results) == 4

        results_by_id = {r["id"]: r for r in results}

        # id=1 should be updated
        assert results_by_id[1]["name"] == "John Doe Updated"

        # id=2 and id=3 should be unchanged
        assert results_by_id[2]["name"] == "Jane Doe"
        assert results_by_id[3]["name"] == "Jim Doe"

        # id=10 should be added
        assert results_by_id[10]["name"] == "New Person"


class TestNamespaces:
    """Test that stream namespaces map to Altertable schemas."""

    def test_namespace_creates_schema_and_writes_into_it(
        self, destination, altertable_service, client, data
    ):
        """Test that a stream namespace is used as the schema, creating it if needed."""
        catalog_name, schema_name, table_name = data
        namespace = f"ns_{uuid.uuid4().hex[:8]}"
        test_config = {
            **altertable_service,
            "catalog": catalog_name,
            "schema": schema_name,
        }

        catalog = make_configured_catalog(
            stream_name=table_name,
            destination_sync_mode=DestinationSyncMode.append,
            json_schema=JSON_SCHEMA,
            namespace=namespace,
        )

        records = [
            make_record_message(
                table_name,
                {"id": 10, "name": "Namespaced", "created_at": "2024-01-01T00:00:00Z"},
                namespace=namespace,
            ),
            make_state_message(table_name),
        ]

        messages = list(destination.write(test_config, catalog, iter(records)))
        assert len(messages) == 1

        results = query_table(client, catalog_name, namespace, table_name)
        assert [r["id"] for r in results] == [10]

        # The table in the configured schema is left untouched
        results = query_table(client, catalog_name, schema_name, table_name)
        assert {r["id"] for r in results} == {1, 2, 3}


class TestRefreshes:
    """Test refreshes and clears, driven by the stream generations."""

    def test_refresh_removing_records_replaces_existing_data(
        self, destination, altertable_service, client, data
    ):
        """Test that a refresh removing records only keeps the newly synced records."""
        catalog_name, schema_name, table_name = data
        test_config = {
            **altertable_service,
            "catalog": catalog_name,
            "schema": schema_name,
        }

        catalog = make_configured_catalog(
            stream_name=table_name,
            destination_sync_mode=DestinationSyncMode.append_dedup,
            json_schema=JSON_SCHEMA,
            primary_key=PRIMARY_KEY,
            cursor_field=CURSOR_FIELD,
            source_defined_cursor=True,
            generation_id=2,
            minimum_generation_id=2,
        )

        input_messages = [
            make_record_message(
                table_name,
                {"id": 1, "name": "Refreshed", "created_at": "2024-01-01T00:00:00Z"},
            ),
            make_state_message(table_name),
            make_stream_complete_message(table_name),
        ]

        messages = list(destination.write(test_config, catalog, iter(input_messages)))
        assert len(messages) == 1

        results = query_table(client, catalog_name, schema_name, table_name)
        assert [(r["id"], r["name"]) for r in results] == [(1, "Refreshed")]

    def test_clear_empties_the_table(
        self, destination, altertable_service, client, data
    ):
        """Test that a clear, which sends no records, empties the table."""
        catalog_name, schema_name, table_name = data
        test_config = {
            **altertable_service,
            "catalog": catalog_name,
            "schema": schema_name,
        }

        catalog = make_configured_catalog(
            stream_name=table_name,
            destination_sync_mode=DestinationSyncMode.append,
            json_schema=JSON_SCHEMA,
            generation_id=2,
            minimum_generation_id=2,
        )

        input_messages = [
            make_state_message(table_name),
            make_stream_complete_message(table_name),
        ]

        messages = list(destination.write(test_config, catalog, iter(input_messages)))
        assert len(messages) == 1

        assert query_table(client, catalog_name, schema_name, table_name) == []

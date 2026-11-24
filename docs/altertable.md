# Altertable

This page guides you through the process of setting up the [Altertable](https://altertable.ai) destination connector.

## Prerequisites

1. An [Altertable](https://altertable.ai) account.
2. Your lakehouse credentials (username and password) — see the [Altertable documentation](https://altertable.ai/docs/analytical-database/authentication) to retrieve them.
3. An existing catalog — see the [Altertable documentation](https://altertable.ai/docs/analytical-database/altertable-catalogs) to create one.

## Connection parameters

- **Username**
  - Your Altertable lakehouse username.
- **Password**
  - Your Altertable lakehouse password.
- **Catalog**
  - The target catalog where data will be written.
- **Schema**
  - The default schema within the catalog to write data to. Streams with a namespace are written to the schema named after it.
- **Host** _(optional)_
  - Hostname of the Altertable Flight SQL server. Defaults to `flight.altertable.ai`.
- **Port** _(optional)_
  - Port of the Altertable Flight SQL server. Defaults to `443`.
- **Use TLS** _(optional)_
  - Whether to use TLS for the connection. Defaults to `true`. Recommended for production.

## Supported sync modes

| Sync mode                                                                                                                                     | Supported? |
| :-------------------------------------------------------------------------------------------------------------------------------------------- | :--------- |
| [Full Refresh - Overwrite](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/full-refresh-overwrite)                   | Yes        |
| [Full Refresh - Append](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/full-refresh-append)                         | Yes        |
| [Full Refresh - Overwrite + Deduped](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/full-refresh-overwrite-deduped) | No         |
| [Incremental Sync - Append](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/incremental-append)                      | Yes        |
| [Incremental Sync - Append + Deduped](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/incremental-append-deduped)    | Yes        |

## Output schema

Each stream is written to its own table within the configured catalog, in the schema matching its namespace (see [Namespace support](#namespace-support)). Columns are mapped from the source JSON schema to DuckDB types.

## Data type map

Airbyte JSON Schema types are mapped to Altertable (DuckDB) types as follows:

| Airbyte Type                                                                | Altertable Type |
| :-------------------------------------------------------------------------- | :-------------- |
| `string`                                                                    | `VARCHAR`       |
| `string` + `format: date`                                                   | `DATE`          |
| `string` + `format: date-time`                                              | `TIMESTAMPTZ`   |
| `string` + `format: date-time` + `airbyte_type: timestamp_without_timezone` | `TIMESTAMP`     |
| `string` + `format: time`                                                   | `TIME`          |
| `integer`                                                                   | `BIGINT`        |
| `number` + `airbyte_type: integer`                                          | `BIGINT`        |
| `number`                                                                    | `DOUBLE`        |
| `boolean`                                                                   | `BOOLEAN`       |
| `object` / `array` / any other type                                         | `VARCHAR`       |

Objects and arrays are stored as JSON-encoded strings.

## Namespace support

This destination supports [namespaces](https://docs.airbyte.com/platform/using-airbyte/core-concepts/namespaces). The namespace maps to an Altertable schema within the configured catalog, which is created if it does not exist. Streams without a namespace (e.g. when the connection uses the "Destination default" namespace) are written to the configured schema.

## Refreshes and clears

This destination supports [refreshes](https://docs.airbyte.com/platform/operator-guides/refreshes) and [clears](https://docs.airbyte.com/platform/operator-guides/clear):

- **Refresh and retain records** appends the reloaded records to the existing table.
- **Refresh and remove records** replaces the table when the first batch of reloaded records is written, so previous records are no longer available while the rest of the refresh runs.
- **Clear** empties the table once the sync completes.

Records are not tagged with an `_airbyte_generation_id` column.

## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date       | Pull Request | Subject         |
| :------ | :--------- | :----------- | :-------------- |
| 0.1.4   | 2026-11-24 | [21](https://github.com/altertable-ai/airbyte-destination-altertable/pull/21) | Reopen the Flight session when it expires during a sync |
| 0.1.3   | 2026-11-23 | [20](https://github.com/altertable-ai/airbyte-destination-altertable/pull/20) | Support refreshes and clears |
| 0.1.2   | 2026-11-23 | [19](https://github.com/altertable-ai/airbyte-destination-altertable/pull/19) | Write streams to the schema matching their namespace |
| 0.1.1   | 2026-11-23 | [18](https://github.com/altertable-ai/airbyte-destination-altertable/pull/18) | Bump altertable-flightsql to 0.4.1 to fix session options encoding |
| 0.1.0   | 2026-04-08 |              | Initial release |

</details>

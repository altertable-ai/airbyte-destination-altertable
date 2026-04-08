# Altertable

This page guides you through the process of setting up the [Altertable](https://altertable.ai) destination connector.

## Prerequisites

- An [Altertable](https://altertable.ai) account with credentials (username and password)
- A catalog and schema created in your Altertable workspace

## Setup guide

1. Log in to your [Altertable](https://altertable.ai) account.
2. Create a catalog and schema where Airbyte will write data.
3. Note your connection details: username, password, catalog, and schema.

## Connection parameters

- **Host**
  - Hostname of the Altertable Flight SQL server. Defaults to `flight.altertable.ai`.
- **Port**
  - Port of the Altertable Flight SQL server. Defaults to `443`.
- **Username**
  - Your Altertable account username.
- **Password**
  - Your Altertable account password.
- **Catalog**
  - The target catalog where data will be written.
- **Schema**
  - The schema within the catalog to write data to.
- **Use TLS**
  - Whether to use TLS for the connection to Altertable. Defaults to `true`. Recommended for production.

## Supported sync modes

| Sync mode | Supported? |
| :--- | :--- |
| [Full Refresh - Overwrite](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/full-refresh-overwrite) | Yes |
| [Full Refresh - Append](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/full-refresh-append) | Yes |
| [Full Refresh - Overwrite + Deduped](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/full-refresh-overwrite-deduped) | No |
| [Incremental Sync - Append](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/incremental-append) | Yes |
| [Incremental Sync - Append + Deduped](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/incremental-append-deduped) | Yes |

## Output schema

Each stream is written to its own table within the configured catalog and schema. Columns are mapped from the source JSON schema to Apache Arrow types.

## Data type map

Airbyte JSON Schema types are mapped to Altertable (Apache Arrow) types as follows:

| Airbyte Type | Altertable Type |
| :--- | :--- |
| `string` | `string` |
| `string` + `format: date` | `date32` |
| `string` + `format: date-time` | `timestamp[us, UTC]` |
| `string` + `format: date-time` + `airbyte_type: timestamp_without_timezone` | `timestamp[us]` |
| `string` + `format: time` | `time64[us]` |
| `integer` | `int64` |
| `number` | `float64` |
| `boolean` | `bool` |
| `object` / `array` | `string` (JSON serialized) |

## Namespace support

This destination supports [namespaces](https://docs.airbyte.com/platform/using-airbyte/core-concepts/namespaces). The namespace maps to an Altertable schema.

## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date       | Pull Request | Subject         |
| :------ | :--------- | :----------- | :-------------- |
| 0.1.0   | 2026-04-05 |              | Initial release |

</details>

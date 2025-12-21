# Altertable's Airbyte Destination

A custom Airbyte destination connector for [Altertable](https://altertable.ai).

## Installation

The destination is available as a Docker image at:

```
ghcr.io/altertable-ai/airbyte-destination-altertable
```

### Platform Support

This connector is built as a multi-platform Docker image supporting:
- **linux/amd64** - Intel/AMD 64-bit systems
- **linux/arm64** - ARM 64-bit systems (Apple Silicon, AWS Graviton, etc.)

Docker will automatically select the appropriate image for your platform.

### Adding the Custom Destination in Airbyte

1. Open the Airbyte web UI
2. Navigate to **Settings** → **Destinations**
3. Click **+ New connector** → **Add a new Docker connector**
4. Fill in the connector details:
   - **Connector display name**: `Altertable`
   - **Docker repository name**: `ghcr.io/altertable-ai/airbyte-destination-altertable`
   - **Docker image tag**: `latest` (or a specific version)
5. Click **Add** to save

For more details on using custom connectors, see [Airbyte's official documentation](https://docs.airbyte.com/operator-guides/using-custom-connectors).

## Configuration

When creating a new destination, you'll need to provide the following required parameters:

| Parameter    | Description                                   |
| ------------ | --------------------------------------------- |
| **Username** | Your Altertable account username              |
| **Password** | Your Altertable account password              |
| **Catalog**  | The target catalog where data will be written |
| **Schema**   | The schema within the catalog to use          |

For more information on setting up your Altertable account and obtaining credentials, see the [Altertable documentation](https://docs.altertable.ai).

## Run locally

- Setup your Python venv:

```
python -m venv venv
. venv/bin/activate # change depending on your shell
```

- Discover the source schema and generate the catalog.json file:

```
docker run --rm -v $PWD/scripts/faker:/data airbyte/source-faker:latest discover --config /data/config.json > scripts/faker/discover.json
python scripts/gen_catalog.py scripts/faker/discover.json scripts/faker/catalog.json
```

- Run the ETL:

```
./scripts/run.sh faker
```

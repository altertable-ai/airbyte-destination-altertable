# Altertable's Airbyte destination

## Run locally

- Setup your Python venv:
```
python -m venv .venv
. .venv/bin/activate.fish # change depending on your shell
```

- Discover the source schema and generate the catalog.json file:
```
docker run --rm -v $PWD/scripts/faker:/data airbyte/source-faker:latest discover --config /data/config.json > scripts/faker/discover.json
.venv/bin/python scripts/gen_catalog.py scripts/faker/discover.json scripts/faker/catalog.json
```

- Run the ETL:
```
./scripts/run.sh faker
```

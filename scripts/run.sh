#!/bin/bash

set -e

if [ "$1" = "faker" ]; then
  docker run --rm -v $PWD/scripts/faker:/data airbyte/source-faker:latest read \
    --config /data/config.json \
    --catalog /data/catalog.json \
    | python main.py write --config scripts/destination_config.local.json --catalog scripts/faker/catalog.json
elif [ "$1" = "postgres" ]; then
  docker run --rm \
    --network local_default \
    -v $PWD/scripts/postgres:/data \
    airbyte/source-postgres:3.6.32 read \
    --config /data/config.json \
    --catalog /data/catalog.json \
    | python main.py write --config scripts/destination_config.local.json --catalog scripts/postgres/catalog.json
else
  echo "Invalid source: $1"
  exit 1
fi

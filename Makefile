all: lint build

lint:
	ruff format .
	ruff check --fix .

build:
	docker build -t altertable-ai/airbyte-destination-altertable .

.PHONY: all lint build
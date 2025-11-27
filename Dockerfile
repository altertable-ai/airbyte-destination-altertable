FROM python:3.12-slim-bookworm AS builder

RUN mkdir -p /airbyte-destination-altertable
WORKDIR /airbyte-destination-altertable

COPY main.py ./
COPY pyproject.toml ./
COPY destination_altertable ./destination_altertable

RUN python -m venv venv
RUN ./venv/bin/pip install .

FROM python:3.12-slim-bookworm

COPY --from=builder /airbyte-destination-altertable /airbyte-destination-altertable
RUN mkdir -p /airbyte

RUN useradd -m airbyte
RUN chown -R airbyte:airbyte /airbyte-destination-altertable /airbyte

USER airbyte

ENV AIRBYTE_ENTRYPOINT="/airbyte-destination-altertable/venv/bin/python /airbyte-destination-altertable/main.py"
ENTRYPOINT ["/airbyte-destination-altertable/venv/bin/python", "/airbyte-destination-altertable/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=altertable-ai/airbyte-destination-altertable

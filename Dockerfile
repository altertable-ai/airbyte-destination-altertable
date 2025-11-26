FROM python:3.12.10

RUN apt-get update && apt-get -y upgrade && pip install --upgrade pip
RUN useradd -m airbyte
RUN mkdir -p /airbyte/integration_code && chown -R airbyte:airbyte /airbyte/integration_code

USER airbyte

WORKDIR /airbyte/integration_code

COPY main.py ./
COPY pyproject.toml ./
COPY destination_altertable ./destination_altertable

RUN pip install .

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=altertable-ai/airbyte-destination-altertable

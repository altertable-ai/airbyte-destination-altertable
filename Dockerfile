FROM python:3.12.10

RUN apt-get update && apt-get -y upgrade && pip install --upgrade pip

WORKDIR /airbyte/integration_code

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY main.py ./
COPY destination_altertable ./destination_altertable

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/destination-altertable

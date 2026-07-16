FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        default-jdk \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME

WORKDIR /opt/dagster/app

COPY ./docker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pyspark==3.5.0

COPY ./docker/dagster.yaml ./docker/workspace.yaml $DAGSTER_HOME
COPY ./ecom_dagster/ecom_dagster /opt/dagster/app/ecom_dagster
COPY ./ecom_dbt /opt/dagster/app/ecom_dbt
COPY ./spark_jobs /opt/dagster/app/spark_jobs

EXPOSE 4000
EXPOSE 3000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "ecom_dagster.definitions"]
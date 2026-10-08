# syntax=docker/dockerfile:1.7
# Airflow image with Java and the `epl` CLI, so LocalExecutor tasks run Spark in-container.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12
ARG AIRFLOW_VERSION

USER root
RUN apt-get update \
    && (apt-get install -y --no-install-recommends openjdk-17-jre-headless \
        || apt-get install -y --no-install-recommends openjdk-21-jre-headless) \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/epl/jars /opt/airflow/lake \
    && chown -R airflow:root /opt/epl /opt/airflow/lake

USER airflow
COPY --chown=airflow:root pyproject.toml README.md LICENSE /opt/epl/
COPY --chown=airflow:root src /opt/epl/src
# Pin Airflow in the same install so pip can never up/downgrade it.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" /opt/epl

RUN python -c "from epl_lakehouse.config import Settings; from epl_lakehouse.spark_session import build_spark; build_spark(Settings(data_root='/tmp/warmup')).stop()" \
    && cp ~/.ivy2.5.2/jars/*.jar /opt/epl/jars/ \
    && rm -rf /tmp/warmup ~/.ivy2.5.2

ENV EPL_SPARK_JARS=/opt/epl/jars/*.jar \
    EPL_DATA_ROOT=/opt/airflow/lake

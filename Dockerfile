# syntax=docker/dockerfile:1.7
# Standalone pipeline image: `docker run epl-fpl-lakehouse run` (or any `epl` subcommand).
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 epl \
    && mkdir -p /opt/epl/jars /data \
    && chown -R epl /opt/epl /data

WORKDIR /opt/epl
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install .

USER epl
# Resolve the Delta Lake jars once at build time and bake them in, so containers never
# need Maven Central at runtime (reproducible, works in locked-down networks).
RUN python -c "from epl_lakehouse.config import Settings; from epl_lakehouse.spark_session import build_spark; build_spark(Settings(data_root='/tmp/warmup')).stop()" \
    && cp ~/.ivy2.5.2/jars/*.jar /opt/epl/jars/ \
    && rm -rf /tmp/warmup ~/.ivy2.5.2

ENV EPL_SPARK_JARS=/opt/epl/jars/*.jar \
    EPL_DATA_ROOT=/data
VOLUME ["/data"]
ENTRYPOINT ["epl"]
CMD ["--help"]

"""Operational metadata tables: load bookkeeping and incremental watermarks."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from epl_lakehouse import delta_io
from epl_lakehouse.config import Settings

_WATERMARK_SCHEMA = StructType(
    [
        StructField("stage", StringType(), False),
        StructField("source", StringType(), False),
        StructField("high_watermark", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

_LOADED_RUNS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("files", StringType(), False),
        StructField("loaded_at", TimestampType(), False),
    ]
)


def get_watermarks(spark: SparkSession, settings: Settings, stage: str) -> dict[str, datetime]:
    path = settings.table_path("ops", "watermarks")
    if not delta_io.table_exists(spark, path):
        return {}
    rows = delta_io.read(spark, path).filter(F.col("stage") == stage).collect()
    return {row["source"]: row["high_watermark"] for row in rows}


def set_watermarks(
    spark: SparkSession, settings: Settings, stage: str, marks: dict[str, datetime]
) -> None:
    if not marks:
        return
    now = datetime.now(UTC)
    rows = [(stage, source, mark, now) for source, mark in marks.items()]
    df = spark.createDataFrame(rows, _WATERMARK_SCHEMA)
    delta_io.upsert(
        spark,
        df,
        settings.table_path("ops", "watermarks"),
        ["stage", "source"],
        order_col="high_watermark",
        partition_by=(),
    )


def loaded_run_ids(spark: SparkSession, settings: Settings) -> set[str]:
    path = settings.table_path("ops", "bronze_loaded_runs")
    if not delta_io.table_exists(spark, path):
        return set()
    return {row["run_id"] for row in delta_io.read(spark, path).select("run_id").collect()}


def record_loaded_run(spark: SparkSession, settings: Settings, run_id: str, files: int) -> None:
    df = spark.createDataFrame([(run_id, str(files), datetime.now(UTC))], _LOADED_RUNS_SCHEMA)
    delta_io.upsert(
        spark,
        df,
        settings.table_path("ops", "bronze_loaded_runs"),
        ["run_id"],
        order_col="loaded_at",
        partition_by=(),
    )

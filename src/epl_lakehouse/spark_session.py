"""SparkSession factory with Delta Lake enabled."""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from epl_lakehouse.config import Settings


def build_spark(settings: Settings, app_name: str = "epl-fpl-lakehouse") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(settings.root / "_spark_warehouse"))
        .config("spark.ui.showConsoleProgress", "false")
        # Data-column predicates in replaceWhere (season AND gameweek) are relied upon.
        .config("spark.databricks.delta.replaceWhere.dataColumns.enabled", "true")
        # Delta replays each table's log with 50 tasks by default; this lake's tables are
        # small, so match the shuffle parallelism instead (big win for local runs).
        .config("spark.databricks.delta.snapshotPartitions", str(settings.spark_shuffle_partitions))
        .config("spark.default.parallelism", str(settings.spark_shuffle_partitions))
    )
    if settings.spark_jars:
        # Pre-provisioned jars (the Docker images bake them in): no Maven access at runtime.
        spark = builder.config("spark.jars", settings.spark_jars).getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

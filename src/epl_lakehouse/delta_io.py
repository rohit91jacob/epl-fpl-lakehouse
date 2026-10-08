"""Idempotent Delta Lake write primitives shared by every layer."""

from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def table_exists(spark: SparkSession, path: str) -> bool:
    return bool(DeltaTable.isDeltaTable(spark, path))


def read(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def latest_per_key(df: DataFrame, keys: Sequence[str], order_col: str) -> DataFrame:
    """Keep the newest row per key; MERGE needs a source with unique keys."""
    window = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def upsert(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: Sequence[str],
    *,
    order_col: str = "fetched_at",
    partition_by: Sequence[str] = ("season",),
) -> None:
    """MERGE rows by key. A row only replaces the target when it is strictly newer, so
    replays and out-of-order loads converge to the same table state."""
    source = latest_per_key(df, keys, order_col)
    if not table_exists(spark, path):
        source.write.format("delta").partitionBy(*partition_by).save(path)
        return
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(source.alias("s"), condition)
        .withSchemaEvolution()
        .whenMatchedUpdateAll(condition=f"s.`{order_col}` > t.`{order_col}`")
        .whenNotMatchedInsertAll()
        .execute()
    )


def replace_where(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    predicate: str,
    *,
    partition_by: Sequence[str] = ("season",),
) -> None:
    """Atomically replace exactly the rows matching ``predicate`` with ``df``."""
    if not table_exists(spark, path):
        df.write.format("delta").partitionBy(*partition_by).save(path)
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .option("mergeSchema", "true")
        .save(path)
    )


def sql_in(column: str, values: Sequence[object]) -> str:
    """Render ``column IN (...)`` for replaceWhere predicates (values are ints or strings)."""
    if not values:
        return "false"
    rendered = ", ".join(
        str(int(v)) if isinstance(v, int) else "'" + str(v).replace("'", "''") + "'" for v in values
    )
    return f"`{column}` IN ({rendered})"

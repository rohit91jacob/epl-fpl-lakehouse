"""Bronze: load landed raw responses into an append-only Delta table, one row per response.

Each row keeps the payload verbatim plus lineage (run, source URL, checksum, raw path),
so silver can always be rebuilt from bronze alone.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from epl_lakehouse import delta_io, ops
from epl_lakehouse.config import Settings
from epl_lakehouse.ingestion.landing import Manifest, RawLandingZone
from epl_lakehouse.schemas import BRONZE_SCHEMA

log = logging.getLogger(__name__)

BRONZE_TABLE = "fpl_responses"
BRONZE_KEYS = ["endpoint", "resource_id", "season", "run_id"]


def _rows(
    zone: RawLandingZone, manifest: Manifest, loaded_at: datetime
) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for f in manifest.files:
        path = zone.root / f.path
        fetched_at = datetime.fromisoformat(f.fetched_at)
        rows.append(
            (
                f.endpoint,
                f.resource_id,
                f.season,
                fetched_at.date(),
                manifest.run_id,
                fetched_at,
                f.source_url,
                f.sha256,
                f.size_bytes,
                f.is_final,
                f.path,
                path.read_text(encoding="utf-8"),
                loaded_at,
            )
        )
    return rows


def run_bronze(spark: SparkSession, settings: Settings, run_ids: list[str] | None = None) -> int:
    """Load every successful, not-yet-loaded ingestion run. Returns the number of runs loaded."""
    zone = RawLandingZone(settings.raw_root)
    already = ops.loaded_run_ids(spark, settings)
    manifests = [
        m
        for m in zone.list_manifests(status="succeeded")
        if m.run_id not in already and (run_ids is None or m.run_id in run_ids)
    ]
    path = settings.table_path("bronze", BRONZE_TABLE)
    for manifest in manifests:
        if manifest.files:
            rows = _rows(zone, manifest, datetime.now(UTC))
            # Spread payloads over several partitions; one task per run would be huge.
            rdd = spark.sparkContext.parallelize(rows, max(1, min(len(rows) // 25, 64)))
            df = spark.createDataFrame(rdd, BRONZE_SCHEMA)
            # Insert-only MERGE: reloading a run after a crash never duplicates rows.
            if delta_io.table_exists(spark, path):
                condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in BRONZE_KEYS)
                (
                    DeltaTable.forPath(spark, path)
                    .alias("t")
                    .merge(df.alias("s"), condition)
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                df.write.format("delta").partitionBy("endpoint", "season").save(path)
        ops.record_loaded_run(spark, settings, manifest.run_id, len(manifest.files))
        log.info(
            "bronze loaded run",
            extra={"run_id": manifest.run_id, "files": len(manifest.files)},
        )
    return len(manifests)


def bronze_frame(spark: SparkSession, settings: Settings, endpoint: str) -> DataFrame:
    return delta_io.read(spark, settings.table_path("bronze", BRONZE_TABLE)).filter(
        F.col("endpoint") == endpoint
    )

"""Delta table housekeeping: compact small files and vacuum unreferenced ones."""

from __future__ import annotations

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from epl_lakehouse.config import Settings

log = logging.getLogger(__name__)


def delta_tables(settings: Settings) -> list[str]:
    tables = []
    for layer in ("bronze", "silver", "gold", "ops"):
        base = settings.root / layer
        if base.exists():
            tables += [str(p.parent) for p in sorted(base.glob("*/_delta_log"))]
    return tables


def run_maintenance(
    spark: SparkSession, settings: Settings, retention_hours: int | None = None
) -> list[str]:
    hours = settings.vacuum_retention_hours if retention_hours is None else retention_hours
    tables = delta_tables(settings)
    for path in tables:
        table = DeltaTable.forPath(spark, path)
        table.optimize().executeCompaction()
        table.vacuum(hours)
        log.info("maintained table", extra={"table": Path(path).name, "retention_hours": hours})
    return tables

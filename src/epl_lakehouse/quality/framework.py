"""A small, dependency-free data-quality framework on top of Spark DataFrames.

A :class:`Check` evaluates to a violation count; zero means it passed. ``error`` checks
fail the pipeline, ``warn`` checks are recorded and logged only.
"""

from __future__ import annotations

import logging
import operator
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from epl_lakehouse.quality.results import CheckResult, DataQualityError, Severity

log = logging.getLogger(__name__)

Loader = Callable[[str], DataFrame | None]


@dataclass(frozen=True)
class Check:
    table: str
    name: str
    severity: Severity
    violations: Callable[[Loader], int]
    description: str = ""


class MissingTableError(LookupError):
    pass


def require(load: Loader, table: str) -> DataFrame:
    df = load(table)
    if df is None:
        raise MissingTableError(table)
    return df


# -- check builders --------------------------------------------------------------------------


def unique(table: str, keys: Sequence[str], severity: Severity = Severity.ERROR) -> Check:
    def run(load: Loader) -> int:
        dupes = require(load, table).groupBy(*keys).count().filter("count > 1")
        return dupes.count()

    return Check(table, f"unique({', '.join(keys)})", severity, run, "key groups with duplicates")


def not_null(table: str, columns: Sequence[str], severity: Severity = Severity.ERROR) -> Check:
    def run(load: Loader) -> int:
        cond = reduce(operator.or_, [F.col(c).isNull() for c in columns])
        return require(load, table).filter(cond).count()

    return Check(table, f"not_null({', '.join(columns)})", severity, run, "rows with nulls")


def expect(table: str, name: str, condition: str, severity: Severity = Severity.ERROR) -> Check:
    """Every row must satisfy the SQL ``condition`` (null counts as a violation)."""

    def run(load: Loader) -> int:
        return require(load, table).filter(f"NOT coalesce(({condition}), false)").count()

    return Check(table, name, severity, run, f"rows violating: {condition}")


def between(
    table: str, column: str, low: float, high: float, severity: Severity = Severity.WARN
) -> Check:
    return expect(
        table,
        f"between({column}, {low}, {high})",
        f"`{column}` IS NULL OR `{column}` BETWEEN {low} AND {high}",
        severity,
    )


def row_count(
    table: str,
    expected: int,
    group_by: Sequence[str] = ("season",),
    severity: Severity = Severity.ERROR,
) -> Check:
    def run(load: Loader) -> int:
        counts = require(load, table).groupBy(*group_by).count()
        return counts.filter(F.col("count") != expected).count()

    return Check(table, f"row_count == {expected} per {', '.join(group_by)}", severity, run)


def references(
    table: str,
    columns: Sequence[str],
    parent: str,
    parent_columns: Sequence[str],
    severity: Severity = Severity.ERROR,
) -> Check:
    def run(load: Loader) -> int:
        child = require(load, table).select(*columns).dropna().distinct()
        keys = require(load, parent).select(
            *[F.col(p).alias(c) for p, c in zip(parent_columns, columns, strict=True)]
        )
        return child.join(keys, list(columns), "left_anti").count()

    return Check(
        table,
        f"references {parent}({', '.join(parent_columns)})",
        severity,
        run,
        "orphaned key values",
    )


def custom(
    table: str, name: str, fn: Callable[[Loader], int], severity: Severity = Severity.ERROR
) -> Check:
    return Check(table, name, severity, fn)


# -- runner -------------------------------------------------------------------------------------

RESULT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table", StringType(), False),
        StructField("check", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("passed", BooleanType(), False),
        StructField("violations", LongType(), False),
        StructField("details", StringType(), True),
        StructField("checked_at", TimestampType(), False),
    ]
)


def run_checks(
    spark: SparkSession,
    checks: Sequence[Check],
    load: Loader,
    *,
    run_id: str,
    layer: str,
    results_path: str | None,
    optional_tables: frozenset[str] = frozenset(),
) -> list[CheckResult]:
    results: list[CheckResult] = []
    for check in checks:
        try:
            violations = check.violations(load)
            details = check.description
        except MissingTableError as exc:
            if str(exc) in optional_tables or check.table in optional_tables:
                results.append(
                    CheckResult(
                        check.table, check.name, check.severity, True, 0, "skipped: no table"
                    )
                )
                continue
            violations, details = 1, f"table not found: {exc}"
        result = CheckResult(
            check.table, check.name, check.severity, violations == 0, violations, details
        )
        results.append(result)
        if not result.passed:
            level = logging.ERROR if check.severity is Severity.ERROR else logging.WARNING
            log.log(
                level,
                "data-quality check failed",
                extra={
                    "table": check.table,
                    "check": check.name,
                    "severity": str(check.severity),
                    "violations": violations,
                },
            )

    if results_path is not None:
        now = datetime.now(UTC)
        rows = [
            (
                run_id,
                layer,
                r.table,
                r.check,
                str(r.severity),
                r.passed,
                r.violations,
                r.details,
                now,
            )
            for r in results
        ]
        spark.createDataFrame(rows, RESULT_SCHEMA).write.format("delta").mode("append").save(
            results_path
        )

    failures = [r for r in results if not r.passed and r.severity is Severity.ERROR]
    log.info(
        "data-quality suite finished",
        extra={
            "layer": layer,
            "checks": len(results),
            "failed_error": len(failures),
            "failed_warn": sum(1 for r in results if not r.passed and r.severity is Severity.WARN),
        },
    )
    if failures:
        raise DataQualityError(failures)
    return results

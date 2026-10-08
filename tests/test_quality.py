from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from epl_lakehouse import delta_io
from epl_lakehouse.quality.framework import (
    DataQualityError,
    Severity,
    between,
    expect,
    not_null,
    references,
    row_count,
    run_checks,
    unique,
)

pytestmark = pytest.mark.spark


@pytest.fixture
def tables(spark: Any) -> dict[str, Any]:
    teams = spark.createDataFrame(
        [("s", 1, "Alpha"), ("s", 2, "Beta"), ("s", 2, "Beta again"), ("s", 3, None)],
        "season string, team_id int, team_name string",
    )
    players = spark.createDataFrame(
        [("s", 10, 1, 45), ("s", 11, 9, 300)],
        "season string, player_id int, team_id int, now_cost int",
    )
    return {"silver.teams": teams, "silver.players": players}


def evaluate(
    spark: Any, tables: dict[str, Any], checks: list[Any], **kwargs: Any
) -> dict[str, int]:
    results = run_checks(
        spark, checks, tables.get, run_id="t", layer="test", results_path=None, **kwargs
    )
    return {r.check: r.violations for r in results}


def test_each_builder_counts_violations(spark: Any, tables: dict[str, Any]) -> None:
    checks = [
        unique("silver.teams", ["season", "team_id"], Severity.WARN),
        not_null("silver.teams", ["team_name"], Severity.WARN),
        row_count("silver.teams", 4, severity=Severity.WARN),
        references(
            "silver.players",
            ["season", "team_id"],
            "silver.teams",
            ["season", "team_id"],
            Severity.WARN,
        ),
        between("silver.players", "now_cost", 30, 200),
        expect("silver.players", "positive_cost", "now_cost > 0", Severity.WARN),
    ]
    assert evaluate(spark, tables, checks) == {
        "unique(season, team_id)": 1,
        "not_null(team_name)": 1,
        "row_count == 4 per season": 0,
        "references silver.teams(season, team_id)": 1,
        "between(now_cost, 30, 200)": 1,
        "positive_cost": 0,
    }


def test_error_severity_fails_the_suite(spark: Any, tables: dict[str, Any]) -> None:
    with pytest.raises(DataQualityError, match=r"silver\.teams:unique"):
        evaluate(spark, tables, [unique("silver.teams", ["season", "team_id"])])


def test_missing_tables_fail_unless_optional(spark: Any, tables: dict[str, Any]) -> None:
    check = unique("silver.history", ["id"])
    with pytest.raises(DataQualityError, match=r"silver\.history"):
        evaluate(spark, tables, [check])
    assert evaluate(spark, tables, [check], optional_tables=frozenset({"silver.history"})) == {
        "unique(id)": 0
    }


def test_results_are_persisted(spark: Any, tables: dict[str, Any], tmp_path: Path) -> None:
    path = str(tmp_path / "dq_results")
    run_checks(
        spark,
        [not_null("silver.teams", ["team_name"], Severity.WARN)],
        tables.get,
        run_id="run-42",
        layer="silver",
        results_path=path,
    )
    row = delta_io.read(spark, path).collect()[0]
    assert (row["run_id"], row["passed"], row["violations"]) == ("run-42", False, 1)

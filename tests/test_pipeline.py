"""End-to-end pipeline tests on the synthetic API (needs Java 17+ for Spark).

One lake is built once per session and walked through a realistic sequence:
GW5 run -> no-op rerun -> GW6 run. Each stage's outputs are captured, then asserted
against an independent pure-Python oracle computed straight from the API JSON.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from epl_lakehouse import delta_io
from epl_lakehouse.bronze import run_bronze
from epl_lakehouse.config import Settings
from epl_lakehouse.gold import run_gold
from epl_lakehouse.ingestion.pipeline import run_ingestion
from epl_lakehouse.quality.framework import CheckResult
from epl_lakehouse.quality.suites import run_suite
from epl_lakehouse.silver import run_silver
from tests.helpers import GW5_CLOCK, GW6_CLOCK, fixed_clock, lake_settings, load_api

pytestmark = pytest.mark.spark

SILVER = [
    "teams",
    "positions",
    "gameweeks",
    "players",
    "player_snapshots",
    "fixtures",
    "fixture_player_stats",
    "player_gameweek_stats",
    "player_gameweek_explain",
    "player_fixture_history",
    "player_past_seasons",
]


@dataclass
class Lake:
    settings: Settings
    counts5: dict[str, int] = field(default_factory=dict)
    league5: list[dict[str, Any]] = field(default_factory=list)
    league6: list[dict[str, Any]] = field(default_factory=list)
    dq5: list[CheckResult] = field(default_factory=list)
    dq6: list[CheckResult] = field(default_factory=list)
    noop_processed: dict[str, int] = field(default_factory=dict)
    versions_before_noop: dict[str, int] = field(default_factory=dict)
    versions_after_noop: dict[str, int] = field(default_factory=dict)
    league_after_gold_rerun: list[dict[str, Any]] = field(default_factory=list)
    pgw5: list[dict[str, Any]] = field(default_factory=list)
    dim_player6: list[dict[str, Any]] = field(default_factory=list)
    ticker5: list[dict[str, Any]] = field(default_factory=list)


def _rows(spark: Any, settings: Settings, table: str, order: str) -> list[dict[str, Any]]:
    layer, name = table.split(".")
    df = delta_io.read(spark, settings.table_path(layer, name))
    return [r.asDict(recursive=True) for r in df.orderBy(*order.split(",")).collect()]


def _version(spark: Any, settings: Settings, name: str) -> int:
    from delta.tables import DeltaTable

    history = DeltaTable.forPath(spark, settings.table_path("silver", name)).history(1)
    return int(history.collect()[0]["version"])


@pytest.fixture(scope="module")
def lake(
    spark: Any, tmp_path_factory: pytest.TempPathFactory, sample_api: Path, sample_api_gw6: Path
) -> Lake:
    root = tmp_path_factory.mktemp("lake")
    s5 = lake_settings(root, sample_api)
    lake = Lake(settings=s5)

    def cycle(settings: Settings, run_id: str, clock: Any) -> list[CheckResult]:
        run_ingestion(settings, run_id=run_id, clock=clock)
        run_bronze(spark, settings)
        run_silver(spark, settings)
        results = run_suite(spark, settings, "silver", run_id)
        run_gold(spark, settings)
        return results + run_suite(spark, settings, "gold", run_id)

    lake.dq5 = cycle(s5, "gw5", fixed_clock(GW5_CLOCK))
    lake.counts5 = {
        name: delta_io.read(spark, s5.table_path("silver", name)).count() for name in SILVER
    }
    lake.league5 = _rows(spark, s5, "gold.mart_league_table", "as_of_gameweek,position")
    lake.pgw5 = _rows(spark, s5, "gold.fct_player_gameweek", "gameweek_id,player_id")
    lake.ticker5 = _rows(spark, s5, "gold.mart_fixture_ticker", "team_id")

    # A rerun with no new bronze data must not commit anything to silver.
    lake.versions_before_noop = {name: _version(spark, s5, name) for name in SILVER}
    lake.noop_processed = run_silver(spark, s5)
    lake.versions_after_noop = {name: _version(spark, s5, name) for name in SILVER}
    run_gold(spark, s5)
    lake.league_after_gold_rerun = _rows(
        spark, s5, "gold.mart_league_table", "as_of_gameweek,position"
    )

    s6 = lake_settings(root, sample_api_gw6)
    lake.dq6 = cycle(s6, "gw6", fixed_clock(GW6_CLOCK))
    lake.league6 = _rows(spark, s6, "gold.mart_league_table", "as_of_gameweek,position")
    lake.dim_player6 = _rows(spark, s6, "gold.dim_player", "player_id,valid_from")
    return lake


def oracle_table(fixtures: list[dict[str, Any]], names: dict[int, str], as_of: int) -> list[tuple]:
    """Standings computed independently of Spark, straight from the fixtures JSON."""
    agg: dict[int, list[int]] = defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0])  # P W D L F A Pts
    for team in names:
        agg[team]
    for f in fixtures:
        if not f["finished"] or f["event"] is None or f["event"] > as_of:
            continue
        for team, gf, ga in (
            (f["team_h"], f["team_h_score"], f["team_a_score"]),
            (f["team_a"], f["team_a_score"], f["team_h_score"]),
        ):
            row = agg[team]
            row[0] += 1
            row[4] += gf
            row[5] += ga
            if gf > ga:
                row[1] += 1
                row[6] += 3
            elif gf == ga:
                row[2] += 1
                row[6] += 1
            else:
                row[3] += 1
    order = sorted(agg, key=lambda t: (-agg[t][6], -(agg[t][4] - agg[t][5]), -agg[t][4], names[t]))
    return [(pos, t, *agg[t]) for pos, t in enumerate(order, start=1)]


def test_silver_row_counts(lake: Lake) -> None:
    c = lake.counts5
    assert c["teams"] == 20
    assert c["positions"] == 4
    assert c["gameweeks"] == 38
    assert c["players"] == 300
    assert c["player_snapshots"] == 300
    assert c["fixtures"] == 380
    assert c["player_gameweek_stats"] == 300 * 5
    assert c["player_fixture_history"] > 0
    assert c["fixture_player_stats"] > 0


def test_data_quality_suites_pass(lake: Lake) -> None:
    for results in (lake.dq5, lake.dq6):
        failed = [(r.table, r.check, r.violations) for r in results if not r.passed]
        assert failed == []
        assert len(results) > 40


@pytest.mark.parametrize("as_of", [1, 3, 5])
def test_league_table_matches_independent_oracle(lake: Lake, sample_api: Path, as_of: int) -> None:
    fixtures = load_api(sample_api, "fixtures")
    names = {t["id"]: t["name"] for t in load_api(sample_api, "bootstrap-static")["teams"]}
    expected = oracle_table(fixtures, names, as_of)
    actual = [
        (
            r["position"],
            r["team_id"],
            r["played"],
            r["won"],
            r["drawn"],
            r["lost"],
            r["goals_for"],
            r["goals_against"],
            r["points"],
        )
        for r in lake.league5
        if r["as_of_gameweek"] == as_of
    ]
    assert actual == expected


def test_blank_and_double_gameweeks(lake: Lake, sample_api: Path) -> None:
    fixtures = load_api(sample_api, "fixtures")
    gw5 = [f for f in fixtures if f["event"] == 5]
    doubled = {
        t
        for t in {f["team_h"] for f in gw5} | {f["team_a"] for f in gw5}
        if sum(t in (f["team_h"], f["team_a"]) for f in gw5) == 2
    }
    assert len(doubled) == 2
    rows = [r for r in lake.pgw5 if r["gameweek_id"] == 5]
    assert {r["team_id"] for r in rows if r["fixtures_played"] == 2} == doubled
    blank = [r for r in lake.pgw5 if r["gameweek_id"] == 3 and r["team_id"] in doubled]
    assert blank
    assert all(r["fixtures_played"] == 0 for r in blank)
    assert all(r["minutes"] == 0 for r in blank)
    # Standings after GW3: the two clubs have a game in hand.
    after3 = {r["team_id"]: r["played"] for r in lake.league5 if r["as_of_gameweek"] == 3}
    assert {after3[t] for t in doubled} == {2}


def test_price_is_as_of_the_gameweek(lake: Lake, sample_api: Path) -> None:
    for row in [r for r in lake.pgw5 if r["player_id"] in (1, 77, 250)]:
        history = load_api(sample_api, f"element-summary/{row['player_id']}")["history"]
        values = {h["round"]: h["value"] for h in history}
        if row["gameweek_id"] in values:
            assert float(row["price_m"]) == values[row["gameweek_id"]] / 10


def test_fixture_ticker_lists_next_five_gameweeks(lake: Lake) -> None:
    assert len(lake.ticker5) == 20
    for row in lake.ticker5:
        assert row["from_gameweek"] == 6
        gameweeks = [f["gameweek_id"] for f in row["fixtures"]]
        assert gameweeks == sorted(gameweeks)
        assert set(gameweeks) <= {6, 7, 8, 9, 10}
    # The fixture postponed out of GW8 leaves two clubs with four fixtures.
    assert sorted(r["fixture_count"] for r in lake.ticker5).count(4) == 2


def test_noop_rerun_is_idempotent(lake: Lake) -> None:
    assert lake.noop_processed == {}
    assert lake.versions_after_noop == lake.versions_before_noop
    assert lake.league_after_gold_rerun == lake.league5


def test_incremental_run_extends_history_without_rewriting_it(lake: Lake) -> None:
    before = list(lake.league5)
    after = [r for r in lake.league6 if r["as_of_gameweek"] <= 5]
    assert after == before
    assert {r["as_of_gameweek"] for r in lake.league6} == {1, 2, 3, 4, 5, 6}


def test_scd2_tracks_price_changes(lake: Lake, sample_api: Path, sample_api_gw6: Path) -> None:
    cost5 = {e["id"]: e["now_cost"] for e in load_api(sample_api, "bootstrap-static")["elements"]}
    cost6 = {
        e["id"]: e["now_cost"] for e in load_api(sample_api_gw6, "bootstrap-static")["elements"]
    }
    changed = {pid for pid in cost5 if cost5[pid] != cost6[pid]}
    assert changed, "the sample season should include price moves"

    versions: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in lake.dim_player6:
        versions[row["player_id"]].append(row)
    assert {pid for pid, v in versions.items() if len(v) == 2} == changed
    for pid in changed:
        old, new = versions[pid]
        assert (old["now_cost"], new["now_cost"]) == (cost5[pid], cost6[pid])
        assert str(old["valid_to"]) == "2026-09-21"
        assert old["is_current"] is False
        assert new["valid_to"] is None
        assert new["is_current"] is True


def test_results_site_renders_from_gold(lake: Lake, spark: Any, tmp_path: Path) -> None:
    import json

    from epl_lakehouse.report import build_report

    page = build_report(spark, lake.settings, tmp_path / "site")
    html = page.read_text()
    data = json.loads((tmp_path / "site" / "data.json").read_text())
    assert data["as_of_gameweek"] == 6
    assert len(data["league_table"]) == 20
    leader = data["league_table"][0]
    assert leader["team_name"] in html
    assert html.count("<tr>") >= 20 + 10 + 20 + 20  # league, form, ticker, xG rows
    assert "<script" not in html and "http://" not in html  # self-contained page
    assert len(data["in_form"]) == 10
    assert {r["position_id"] for r in data["value_picks"]} <= {1, 2, 3, 4}

"""The data-quality suites for the silver and gold layers."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from epl_lakehouse import delta_io
from epl_lakehouse.config import Settings
from epl_lakehouse.quality.framework import (
    Check,
    CheckResult,
    Loader,
    Severity,
    between,
    custom,
    expect,
    not_null,
    references,
    require,
    row_count,
    run_checks,
    unique,
)

E, W = Severity.ERROR, Severity.WARN
OPTIONAL_TABLES = frozenset(
    {
        "silver.player_fixture_history",
        "silver.player_past_seasons",
        "silver.player_gameweek_stats",
        "silver.player_gameweek_explain",
        "gold.fct_player_gameweek",
        "gold.mart_player_form",
        "gold.mart_team_gameweek",
    }
)


def _fixture_goals_reconcile(load: Loader) -> int:
    """Scorelines must equal goals credited to players plus opponents' own goals."""
    fixtures = require(load, "silver.fixtures")
    stats = require(load, "silver.fixture_player_stats")
    credited = stats.groupBy("season", "fixture_id").agg(
        F.sum(
            F.when((F.col("stat") == "goals_scored") & (F.col("side") == "h"), F.col("value"))
            .when((F.col("stat") == "own_goals") & (F.col("side") == "a"), F.col("value"))
            .otherwise(0)
        ).alias("home_credited"),
        F.sum(
            F.when((F.col("stat") == "goals_scored") & (F.col("side") == "a"), F.col("value"))
            .when((F.col("stat") == "own_goals") & (F.col("side") == "h"), F.col("value"))
            .otherwise(0)
        ).alias("away_credited"),
    )
    finished = fixtures.filter("finished")
    joined = finished.join(credited, ["season", "fixture_id"], "left").fillna(
        0, ["home_credited", "away_credited"]
    )
    return joined.filter(
        (F.col("home_score") != F.col("home_credited"))
        | (F.col("away_score") != F.col("away_credited"))
    ).count()


def _explain_matches_points(load: Loader) -> int:
    """FPL's own breakdown should add up to each player's gameweek total."""
    stats, explain = load("silver.player_gameweek_stats"), load("silver.player_gameweek_explain")
    if stats is None or explain is None:
        return 0
    summed = explain.groupBy("season", "gameweek_id", "player_id").agg(
        F.sum(F.col("points") + F.coalesce("points_modification", F.lit(0))).alias("explained")
    )
    return (
        stats.filter("fixtures_played > 0")
        .join(summed, ["season", "gameweek_id", "player_id"], "left")
        .filter(F.coalesce("explained", F.lit(0)) != F.col("total_points"))
        .count()
    )


def silver_checks(settings: Settings) -> list[Check]:
    teams = settings.expected_team_count
    return [
        unique("silver.teams", ["season", "team_id"]),
        not_null("silver.teams", ["team_name", "short_name"]),
        row_count("silver.teams", teams),
        unique("silver.positions", ["season", "position_id"]),
        unique("silver.gameweeks", ["season", "gameweek_id"]),
        not_null("silver.gameweeks", ["deadline_time"]),
        row_count("silver.gameweeks", 38, severity=W),
        unique("silver.players", ["season", "player_id"]),
        not_null("silver.players", ["web_name", "team_id", "position_id", "now_cost"]),
        references("silver.players", ["season", "team_id"], "silver.teams", ["season", "team_id"]),
        references(
            "silver.players",
            ["season", "position_id"],
            "silver.positions",
            ["season", "position_id"],
        ),
        between("silver.players", "now_cost", 30, 200),
        unique("silver.player_snapshots", ["season", "player_id", "snapshot_date"]),
        unique("silver.fixtures", ["season", "fixture_id"]),
        expect("silver.fixtures", "home_differs_from_away", "home_team_id <> away_team_id"),
        expect(
            "silver.fixtures",
            "finished_fixtures_have_scores",
            "NOT finished OR (home_score IS NOT NULL AND away_score IS NOT NULL)",
        ),
        references(
            "silver.fixtures", ["season", "home_team_id"], "silver.teams", ["season", "team_id"]
        ),
        references(
            "silver.fixtures", ["season", "away_team_id"], "silver.teams", ["season", "team_id"]
        ),
        row_count("silver.fixtures", teams * (teams - 1), severity=W),
        unique(
            "silver.fixture_player_stats", ["season", "fixture_id", "stat", "side", "player_id"]
        ),
        custom("silver.fixtures", "scores_reconcile_with_goal_scorers", _fixture_goals_reconcile),
        unique("silver.player_gameweek_stats", ["season", "gameweek_id", "player_id"]),
        between("silver.player_gameweek_stats", "minutes", 0, 180, severity=E),
        between("silver.player_gameweek_stats", "total_points", -15, 60),
        references(
            "silver.player_gameweek_stats",
            ["season", "player_id"],
            "silver.players",
            ["season", "player_id"],
            severity=W,
        ),
        custom(
            "silver.player_gameweek_stats",
            "explain_breakdown_sums_to_total_points",
            _explain_matches_points,
            severity=W,
        ),
        unique("silver.player_fixture_history", ["season", "player_id", "fixture_id"]),
        unique("silver.player_past_seasons", ["player_code", "season_name"]),
    ]


def _one_current_version(load: Loader) -> int:
    dim = require(load, "gold.dim_player")
    per_player = dim.groupBy("season", "player_id").agg(
        F.sum(F.col("is_current").cast("int")).alias("current_versions")
    )
    return per_player.filter("current_versions != 1").count()


def _no_overlapping_versions(load: Loader) -> int:
    dim = require(load, "gold.dim_player")
    a, b = dim.alias("a"), dim.alias("b")
    overlaps = a.join(
        b,
        (F.col("a.season") == F.col("b.season"))
        & (F.col("a.player_id") == F.col("b.player_id"))
        & (F.col("a.player_sk") != F.col("b.player_sk"))
        & (
            F.col("a.valid_from")
            < F.coalesce(F.col("b.valid_to"), F.lit("9999-12-31").cast("date"))
        )
        & (
            F.col("b.valid_from")
            < F.coalesce(F.col("a.valid_to"), F.lit("9999-12-31").cast("date"))
        ),
    )
    return overlaps.count()


def _league_table_balances(load: Loader) -> int:
    """League-wide, goals for == goals against and wins == losses after every gameweek."""
    table = require(load, "gold.mart_league_table")
    totals = table.groupBy("season", "as_of_gameweek").agg(
        F.sum("goals_for").alias("gf"),
        F.sum("goals_against").alias("ga"),
        F.sum("won").alias("w"),
        F.sum("lost").alias("l"),
        F.count("*").alias("teams"),
        F.countDistinct("position").alias("positions"),
        F.max("position").alias("max_position"),
    )
    return totals.filter(
        (F.col("gf") != F.col("ga"))
        | (F.col("w") != F.col("l"))
        | (F.col("positions") != F.col("teams"))
        | (F.col("max_position") != F.col("teams"))
    ).count()


def _standings_match_fpl(load: Loader) -> int:
    """Our latest standings vs the position FPL publishes per team (informational)."""
    table = require(load, "gold.mart_league_table")
    teams = require(load, "silver.teams")
    gameweeks = require(load, "silver.gameweeks")
    latest = (
        gameweeks.filter("finished AND data_checked")
        .groupBy("season")
        .agg(F.max("gameweek_id").alias("as_of_gameweek"))
    )
    ours = table.join(latest, ["season", "as_of_gameweek"])
    published = teams.select("season", "team_id", "fpl_league_position").filter(
        F.col("fpl_league_position") > 0
    )
    return (
        ours.join(published, ["season", "team_id"])
        .filter(F.col("position") != F.col("fpl_league_position"))
        .count()
    )


def gold_checks(settings: Settings) -> list[Check]:
    return [
        unique("gold.dim_team", ["season", "team_id"]),
        unique("gold.dim_gameweek", ["season", "gameweek_id"]),
        unique("gold.dim_player", ["player_sk"]),
        custom("gold.dim_player", "exactly_one_current_version", _one_current_version),
        custom("gold.dim_player", "versions_do_not_overlap", _no_overlapping_versions),
        unique("gold.fct_fixture", ["season", "fixture_id"]),
        expect(
            "gold.fct_fixture",
            "finished_fixtures_have_result",
            "NOT finished OR result IN ('H', 'D', 'A')",
        ),
        unique("gold.fct_player_gameweek", ["season", "gameweek_id", "player_id"]),
        not_null("gold.fct_player_gameweek", ["team_id", "price_m"], severity=W),
        unique("gold.mart_league_table", ["season", "as_of_gameweek", "team_id"]),
        expect("gold.mart_league_table", "points_are_3w_plus_d", "points = 3 * won + drawn"),
        expect("gold.mart_league_table", "played_is_w_d_l", "played = won + drawn + lost"),
        expect("gold.mart_league_table", "played_at_most_38", "played <= 38"),
        custom("gold.mart_league_table", "league_totals_balance", _league_table_balances),
        custom("gold.mart_league_table", "standings_match_fpl_published", _standings_match_fpl, W),
        unique("gold.mart_player_form", ["season", "gameweek_id", "player_id"]),
        unique("gold.mart_team_gameweek", ["season", "gameweek_id", "team_id"]),
        unique("gold.mart_fixture_ticker", ["season", "team_id"]),
    ]


def table_loader(spark: SparkSession, settings: Settings) -> Callable[[str], DataFrame | None]:
    cache: dict[str, DataFrame | None] = {}

    def load(qualified: str) -> DataFrame | None:
        if qualified not in cache:
            layer, name = qualified.split(".", 1)
            path = settings.table_path(layer, name)
            # Cached: a suite runs several checks against the same table.
            cache[qualified] = (
                delta_io.read(spark, path).cache() if delta_io.table_exists(spark, path) else None
            )
        return cache[qualified]

    return load


def run_suite(
    spark: SparkSession, settings: Settings, layer: str, run_id: str
) -> list[CheckResult]:
    checks = {"silver": silver_checks, "gold": gold_checks}[layer](settings)
    return run_checks(
        spark,
        checks,
        table_loader(spark, settings),
        run_id=run_id,
        layer=layer,
        results_path=settings.table_path("ops", "dq_results"),
        optional_tables=OPTIONAL_TABLES,
    )

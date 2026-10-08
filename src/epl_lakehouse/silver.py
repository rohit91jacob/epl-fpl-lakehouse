"""Silver: typed, deduplicated, conformed tables parsed from bronze payloads.

Write semantics per table:
* entity tables (teams, players, fixtures, ...) are MERGEd by business key, newest wins;
* per-resource fact tables (gameweek stats, fixture stats) are replaced per resource with
  ``replaceWhere``, because a newer payload can *remove* rows (e.g. provisional bonus);
* every table carries ``season``, because FPL ids are only unique within a season.

Only bronze rows newer than the stored watermark are processed unless ``full_refresh``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from epl_lakehouse import delta_io, ops
from epl_lakehouse.bronze import bronze_frame
from epl_lakehouse.config import Settings
from epl_lakehouse.schemas import (
    BOOTSTRAP_SCHEMA,
    DECIMAL_STATS,
    ELEMENT_SUMMARY_SCHEMA,
    EVENT_LIVE_SCHEMA,
    FIXTURES_SCHEMA,
    INT_STATS,
)

log = logging.getLogger(__name__)
STAGE = "silver"


def _ts(col: Column) -> Column:
    return F.try_to_timestamp(col)


def _dec(col: Column, dtype: str) -> Column:
    return col.try_cast(dtype)


def _match_stats(prefix: str) -> list[Column]:
    """Typed per-match stat columns from a struct column named ``prefix``."""
    cols = [F.col(f"{prefix}.{name}").alias(name) for name in INT_STATS]
    cols += [
        _dec(F.col(f"{prefix}.{name}"), dtype).alias(name) for name, dtype in DECIMAL_STATS.items()
    ]
    return cols


# -- bootstrap-static ----------------------------------------------------------------------


def parse_bootstrap(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        "season", "fetched_at", F.from_json("payload", BOOTSTRAP_SCHEMA).alias("p")
    )


def teams(parsed: DataFrame) -> DataFrame:
    return parsed.select("season", "fetched_at", F.explode("p.teams").alias("t")).select(
        "season",
        F.col("t.id").alias("team_id"),
        F.col("t.code").alias("team_code"),
        F.col("t.name").alias("team_name"),
        F.col("t.short_name").alias("short_name"),
        F.col("t.strength").alias("strength"),
        F.col("t.strength_overall_home").alias("strength_overall_home"),
        F.col("t.strength_overall_away").alias("strength_overall_away"),
        F.col("t.strength_attack_home").alias("strength_attack_home"),
        F.col("t.strength_attack_away").alias("strength_attack_away"),
        F.col("t.strength_defence_home").alias("strength_defence_home"),
        F.col("t.strength_defence_away").alias("strength_defence_away"),
        F.col("t.position").alias("fpl_league_position"),
        "fetched_at",
    )


def positions(parsed: DataFrame) -> DataFrame:
    return parsed.select("season", "fetched_at", F.explode("p.element_types").alias("e")).select(
        "season",
        F.col("e.id").alias("position_id"),
        F.col("e.singular_name").alias("position_name"),
        F.col("e.singular_name_short").alias("position_short"),
        F.col("e.squad_select").alias("squad_select"),
        F.col("e.squad_min_play").alias("squad_min_play"),
        F.col("e.squad_max_play").alias("squad_max_play"),
        "fetched_at",
    )


def gameweeks(parsed: DataFrame) -> DataFrame:
    return parsed.select("season", "fetched_at", F.explode("p.events").alias("e")).select(
        "season",
        F.col("e.id").alias("gameweek_id"),
        F.col("e.name").alias("gameweek_name"),
        _ts(F.col("e.deadline_time")).alias("deadline_time"),
        F.col("e.finished").alias("finished"),
        F.col("e.data_checked").alias("data_checked"),
        F.col("e.is_previous").alias("is_previous"),
        F.col("e.is_current").alias("is_current"),
        F.col("e.is_next").alias("is_next"),
        F.col("e.average_entry_score").alias("average_entry_score"),
        F.col("e.highest_score").alias("highest_score"),
        F.col("e.most_selected").alias("most_selected_player_id"),
        F.col("e.most_transferred_in").alias("most_transferred_in_player_id"),
        F.col("e.most_captained").alias("most_captained_player_id"),
        F.col("e.most_vice_captained").alias("most_vice_captained_player_id"),
        F.col("e.top_element").alias("top_player_id"),
        F.col("e.transfers_made").alias("transfers_made"),
        "fetched_at",
    )


def gameweek_chip_plays(parsed: DataFrame) -> DataFrame:
    return (
        parsed.select("season", "fetched_at", F.explode("p.events").alias("e"))
        .select(
            "season",
            "fetched_at",
            F.col("e.id").alias("gameweek_id"),
            F.explode("e.chip_plays").alias("c"),
        )
        .select(
            "season",
            "gameweek_id",
            F.col("c.chip_name").alias("chip_name"),
            F.col("c.num_played").alias("num_played"),
            "fetched_at",
        )
    )


def _player_columns() -> list[Column]:
    return [
        F.col("e.id").alias("player_id"),
        F.col("e.code").alias("player_code"),
        F.col("e.first_name").alias("first_name"),
        F.col("e.second_name").alias("second_name"),
        F.col("e.web_name").alias("web_name"),
        F.col("e.team").alias("team_id"),
        F.col("e.team_code").alias("team_code"),
        F.col("e.element_type").alias("position_id"),
        F.col("e.status").alias("status"),
        F.col("e.now_cost").alias("now_cost"),
        (F.col("e.now_cost") / F.lit(10)).cast("decimal(4,1)").alias("price_m"),
        F.col("e.cost_change_start").alias("cost_change_start"),
        _dec(F.col("e.selected_by_percent"), "decimal(5,2)").alias("selected_by_percent"),
        _dec(F.col("e.form"), "decimal(5,1)").alias("form"),
        _dec(F.col("e.points_per_game"), "decimal(5,1)").alias("points_per_game"),
        F.col("e.total_points").alias("total_points"),
        F.col("e.event_points").alias("event_points"),
        *_match_stats("e"),
        F.col("e.transfers_in").alias("transfers_in"),
        F.col("e.transfers_out").alias("transfers_out"),
        F.col("e.transfers_in_event").alias("transfers_in_event"),
        F.col("e.transfers_out_event").alias("transfers_out_event"),
        F.col("e.chance_of_playing_next_round").alias("chance_of_playing_next_round"),
        F.col("e.news").alias("news"),
        _ts(F.col("e.news_added")).alias("news_added"),
        F.try_to_date(F.col("e.birth_date")).alias("birth_date"),
        F.try_to_date(F.col("e.team_join_date")).alias("team_join_date"),
        F.col("e.opta_code").alias("opta_code"),
        F.coalesce(F.col("e.removed"), F.lit(False)).alias("is_removed"),
    ]


def players(parsed: DataFrame) -> DataFrame:
    return parsed.select("season", "fetched_at", F.explode("p.elements").alias("e")).select(
        "season", *_player_columns(), "fetched_at"
    )


def player_snapshots(parsed: DataFrame) -> DataFrame:
    """Daily snapshot of the slowly changing player attributes (feeds the SCD2 dimension)."""
    return players(parsed).select(
        "season",
        "player_id",
        F.to_date("fetched_at").alias("snapshot_date"),
        "team_id",
        "position_id",
        "status",
        "now_cost",
        "price_m",
        "selected_by_percent",
        "form",
        "total_points",
        "transfers_in_event",
        "transfers_out_event",
        "chance_of_playing_next_round",
        "news",
        "fetched_at",
    )


# -- fixtures --------------------------------------------------------------------------------


def parse_fixtures(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        "season", "fetched_at", F.explode(F.from_json("payload", FIXTURES_SCHEMA)).alias("f")
    )


def fixtures(parsed: DataFrame) -> DataFrame:
    return parsed.select(
        "season",
        F.col("f.id").alias("fixture_id"),
        F.col("f.code").alias("fixture_code"),
        F.col("f.event").alias("gameweek_id"),
        _ts(F.col("f.kickoff_time")).alias("kickoff_time"),
        F.col("f.team_h").alias("home_team_id"),
        F.col("f.team_a").alias("away_team_id"),
        F.col("f.team_h_score").alias("home_score"),
        F.col("f.team_a_score").alias("away_score"),
        F.coalesce(F.col("f.finished"), F.lit(False)).alias("finished"),
        F.col("f.finished_provisional").alias("finished_provisional"),
        F.col("f.started").alias("started"),
        F.col("f.minutes").alias("minutes"),
        F.col("f.provisional_start_time").alias("provisional_start_time"),
        F.col("f.team_h_difficulty").alias("home_difficulty"),
        F.col("f.team_a_difficulty").alias("away_difficulty"),
        "fetched_at",
    )


def fixture_player_stats(parsed: DataFrame) -> DataFrame:
    """One row per (fixture, stat, side, player) from the ``stats`` arrays of each fixture."""
    stats = parsed.select(
        "season",
        "fetched_at",
        F.col("f.id").alias("fixture_id"),
        F.col("f.team_h").alias("home_team_id"),
        F.col("f.team_a").alias("away_team_id"),
        F.explode("f.stats").alias("s"),
    )
    sides = []
    for side, team_col in (("h", "home_team_id"), ("a", "away_team_id")):
        sides.append(
            stats.select(
                "season",
                "fixture_id",
                F.col("s.identifier").alias("stat"),
                F.lit(side).alias("side"),
                F.col(team_col).alias("team_id"),
                F.explode(f"s.{side}").alias("x"),
                "fetched_at",
            ).select(
                "season",
                "fixture_id",
                "stat",
                "side",
                "team_id",
                F.col("x.element").alias("player_id"),
                F.col("x.value").alias("value"),
                "fetched_at",
            )
        )
    return sides[0].unionByName(sides[1])


# -- event/<gw>/live -----------------------------------------------------------------------


def parse_event_live(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        "season",
        F.col("resource_id").cast("int").alias("gameweek_id"),
        "is_final",
        "fetched_at",
        F.from_json("payload", EVENT_LIVE_SCHEMA).alias("p"),
    )


def player_gameweek_stats(parsed: DataFrame) -> DataFrame:
    return parsed.select(
        "season", "gameweek_id", "is_final", "fetched_at", F.explode("p.elements").alias("e")
    ).select(
        "season",
        "gameweek_id",
        F.col("e.id").alias("player_id"),
        *_match_stats("e.stats"),
        F.col("e.stats.total_points").alias("total_points"),
        F.coalesce(F.col("e.stats.in_dreamteam"), F.lit(False)).alias("in_dreamteam"),
        F.size(F.coalesce(F.col("e.explain"), F.array())).alias("fixtures_played"),
        F.coalesce(F.col("is_final"), F.lit(False)).alias("is_final"),
        "fetched_at",
    )


def player_gameweek_explain(parsed: DataFrame) -> DataFrame:
    """FPL's own points breakdown, one row per (gameweek, player, fixture, stat)."""
    return (
        parsed.select("season", "gameweek_id", "fetched_at", F.explode("p.elements").alias("e"))
        .select(
            "season",
            "gameweek_id",
            "fetched_at",
            F.col("e.id").alias("player_id"),
            F.explode("e.explain").alias("x"),
        )
        .select(
            "season",
            "gameweek_id",
            "player_id",
            F.col("x.fixture").alias("fixture_id"),
            F.explode("x.stats").alias("s"),
            "fetched_at",
        )
        .select(
            "season",
            "gameweek_id",
            "player_id",
            "fixture_id",
            F.col("s.identifier").alias("stat"),
            F.col("s.value").alias("value"),
            F.col("s.points").alias("points"),
            F.col("s.points_modification").alias("points_modification"),
            "fetched_at",
        )
    )


# -- element-summary/<id> -------------------------------------------------------------------


def parse_element_summary(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        "season",
        F.col("resource_id").cast("int").alias("player_id"),
        "fetched_at",
        F.from_json("payload", ELEMENT_SUMMARY_SCHEMA).alias("p"),
    )


def player_fixture_history(parsed: DataFrame) -> DataFrame:
    return parsed.select("season", "fetched_at", F.explode("p.history").alias("h")).select(
        "season",
        F.col("h.element").alias("player_id"),
        F.col("h.fixture").alias("fixture_id"),
        F.col("h.round").alias("gameweek_id"),
        F.col("h.opponent_team").alias("opponent_team_id"),
        F.col("h.was_home").alias("was_home"),
        _ts(F.col("h.kickoff_time")).alias("kickoff_time"),
        F.col("h.total_points").alias("total_points"),
        *_match_stats("h"),
        F.col("h.value").alias("value"),
        (F.col("h.value") / F.lit(10)).cast("decimal(4,1)").alias("price_m"),
        F.col("h.selected").alias("selected"),
        F.col("h.transfers_in").alias("transfers_in"),
        F.col("h.transfers_out").alias("transfers_out"),
        F.col("h.transfers_balance").alias("transfers_balance"),
        "fetched_at",
    )


def player_past_seasons(parsed: DataFrame) -> DataFrame:
    return parsed.select("fetched_at", F.explode("p.history_past").alias("h")).select(
        F.col("h.element_code").alias("player_code"),
        F.col("h.season_name").alias("season_name"),
        F.col("h.start_cost").alias("start_cost"),
        F.col("h.end_cost").alias("end_cost"),
        F.col("h.total_points").alias("total_points"),
        *_match_stats("h"),
        "fetched_at",
    )


# -- orchestration --------------------------------------------------------------------------


def _new_rows(bronze: DataFrame, watermark: datetime | None) -> DataFrame:
    return bronze if watermark is None else bronze.filter(F.col("fetched_at") > F.lit(watermark))


def _replace_resources(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    resource_col: str,
    batch: DataFrame,
) -> None:
    """Replace a fact table's rows per (season, resource) — only where the batch is newer."""
    newest = batch.groupBy("season", resource_col).agg(F.max("fetched_at").alias("batch_at"))
    if delta_io.table_exists(spark, path):
        current = (
            delta_io.read(spark, path)
            .groupBy("season", resource_col)
            .agg(F.max("fetched_at").alias("table_at"))
        )
        newest = newest.join(current, ["season", resource_col], "left").filter(
            F.col("table_at").isNull() | (F.col("batch_at") > F.col("table_at"))
        )
    targets = newest.select("season", resource_col).collect()
    for season in sorted({r["season"] for r in targets}):
        ids = sorted(r[resource_col] for r in targets if r["season"] == season)
        rows = df.filter((F.col("season") == season) & F.col(resource_col).isin(ids))
        predicate = (
            f"{delta_io.sql_in('season', [season])} AND {delta_io.sql_in(resource_col, ids)}"
        )
        delta_io.replace_where(spark, rows, path, predicate)


def _latest_payload_per(batch: DataFrame, keys: Iterable[str]) -> DataFrame:
    return delta_io.latest_per_key(batch, list(keys), "fetched_at")


def run_silver(
    spark: SparkSession, settings: Settings, *, full_refresh: bool = False
) -> dict[str, int]:
    """Process new bronze rows into silver. Returns bronze payloads processed per endpoint."""
    marks = {} if full_refresh else ops.get_watermarks(spark, settings, STAGE)
    path = lambda name: settings.table_path("silver", name)  # noqa: E731
    processed: dict[str, int] = {}
    new_marks: dict[str, datetime] = {}

    def batch_for(endpoint: str) -> DataFrame | None:
        if not delta_io.table_exists(spark, settings.table_path("bronze", "fpl_responses")):
            return None
        batch = _new_rows(bronze_frame(spark, settings, endpoint), marks.get(endpoint)).cache()
        stats = batch.agg(F.count("*").alias("n"), F.max("fetched_at").alias("hw")).first()
        if stats is None or stats["n"] == 0:
            batch.unpersist()
            return None
        processed[endpoint] = int(stats["n"])
        new_marks[endpoint] = stats["hw"]
        return batch

    if (batch := batch_for("bootstrap-static")) is not None:
        parsed = parse_bootstrap(batch).cache()
        delta_io.upsert(spark, teams(parsed), path("teams"), ["season", "team_id"])
        delta_io.upsert(spark, positions(parsed), path("positions"), ["season", "position_id"])
        delta_io.upsert(spark, gameweeks(parsed), path("gameweeks"), ["season", "gameweek_id"])
        delta_io.upsert(
            spark,
            gameweek_chip_plays(parsed),
            path("gameweek_chip_plays"),
            ["season", "gameweek_id", "chip_name"],
        )
        delta_io.upsert(spark, players(parsed), path("players"), ["season", "player_id"])
        delta_io.upsert(
            spark,
            player_snapshots(parsed),
            path("player_snapshots"),
            ["season", "player_id", "snapshot_date"],
        )
        parsed.unpersist()

    if (batch := batch_for("fixtures")) is not None:
        parsed = parse_fixtures(_latest_payload_per(batch, ["season"])).cache()
        delta_io.upsert(spark, fixtures(parsed), path("fixtures"), ["season", "fixture_id"])
        # Each fixtures payload is the complete season, so its stats replace the season's.
        stats = fixture_player_stats(parsed)
        seasons = sorted({r["season"] for r in parsed.select("season").distinct().collect()})
        delta_io.replace_where(
            spark, stats, path("fixture_player_stats"), delta_io.sql_in("season", seasons)
        )
        parsed.unpersist()

    if (batch := batch_for("event-live")) is not None:
        latest = _latest_payload_per(batch, ["season", "resource_id"])
        parsed = parse_event_live(latest).cache()
        stats = player_gameweek_stats(parsed)
        explain = player_gameweek_explain(parsed)
        _replace_resources(spark, stats, path("player_gameweek_stats"), "gameweek_id", stats)
        _replace_resources(spark, explain, path("player_gameweek_explain"), "gameweek_id", stats)
        parsed.unpersist()

    if (batch := batch_for("element-summary")) is not None:
        parsed = parse_element_summary(
            _latest_payload_per(batch, ["season", "resource_id"])
        ).cache()
        delta_io.upsert(
            spark,
            player_fixture_history(parsed),
            path("player_fixture_history"),
            ["season", "player_id", "fixture_id"],
        )
        delta_io.upsert(
            spark,
            player_past_seasons(parsed),
            path("player_past_seasons"),
            ["player_code", "season_name"],
            partition_by=(),
        )
        parsed.unpersist()

    ops.set_watermarks(spark, settings, STAGE, new_marks)
    log.info("silver complete", extra={"processed": processed, "full_refresh": full_refresh})
    return processed

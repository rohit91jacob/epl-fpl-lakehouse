"""Gold: dimensional model and analytics marts, rebuilt per season from silver.

A season is small (20 teams, ~700 players, 380 fixtures), so each run recomputes the
affected seasons in full and swaps them in atomically with ``replaceWhere``. That keeps
gold deterministic: the same silver state always yields byte-for-byte identical tables.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from epl_lakehouse import delta_io
from epl_lakehouse.config import Settings

log = logging.getLogger(__name__)

FAR_FUTURE = "9999-12-31"  # open-ended SCD2 versions compare against this date


def dim_team(teams: DataFrame) -> DataFrame:
    return teams.select(
        "season",
        "team_id",
        "team_code",
        "team_name",
        "short_name",
        "strength",
        "strength_overall_home",
        "strength_overall_away",
        "strength_attack_home",
        "strength_attack_away",
        "strength_defence_home",
        "strength_defence_away",
    )


def dim_gameweek(gameweeks: DataFrame, chip_plays: DataFrame) -> DataFrame:
    chips = chip_plays.groupBy("season", "gameweek_id").agg(
        F.sum("num_played").alias("chips_played"),
        F.map_from_entries(F.collect_list(F.struct("chip_name", "num_played"))).alias(
            "chips_played_by_type"
        ),
    )
    return gameweeks.join(chips, ["season", "gameweek_id"], "left").select(
        "season",
        "gameweek_id",
        "gameweek_name",
        "deadline_time",
        F.to_date("deadline_time").alias("deadline_date"),
        "finished",
        "data_checked",
        "is_current",
        "is_next",
        "average_entry_score",
        "highest_score",
        "most_captained_player_id",
        "top_player_id",
        "transfers_made",
        F.coalesce("chips_played", F.lit(0)).alias("chips_played"),
        "chips_played_by_type",
    )


def dim_player(snapshots: DataFrame, players: DataFrame, positions: DataFrame) -> DataFrame:
    """Type-2 slowly changing dimension over (team, position, price, status).

    ``valid_to`` is exclusive; the current version has ``valid_to`` null.
    """
    tracked = F.concat_ws("|", "team_id", "position_id", "now_cost", "status")
    by_player = Window.partitionBy("season", "player_id").orderBy("snapshot_date")
    versions = (
        snapshots.withColumn("_attrs", tracked)
        .withColumn("_changed", (F.lag("_attrs").over(by_player) != F.col("_attrs")).cast("int"))
        .withColumn("_version", F.sum(F.coalesce("_changed", F.lit(1))).over(by_player))
        .groupBy("season", "player_id", "_version")
        .agg(
            F.min("snapshot_date").alias("valid_from"),
            F.first("team_id").alias("team_id"),
            F.first("position_id").alias("position_id"),
            F.first("now_cost").alias("now_cost"),
            F.first("price_m").alias("price_m"),
            F.first("status").alias("status"),
        )
        .withColumn(
            "valid_to",
            F.lead("valid_from").over(
                Window.partitionBy("season", "player_id").orderBy("valid_from")
            ),
        )
    )
    names = players.select(
        "season", "player_id", "player_code", "web_name", "first_name", "second_name"
    )
    pos = positions.select("season", "position_id", "position_short")
    return (
        versions.join(names, ["season", "player_id"], "left")
        .join(pos, ["season", "position_id"], "left")
        .select(
            F.xxhash64("season", "player_id", "valid_from").alias("player_sk"),
            "season",
            "player_id",
            "player_code",
            "web_name",
            F.concat_ws(" ", "first_name", "second_name").alias("full_name"),
            "team_id",
            "position_id",
            F.col("position_short").alias("position"),
            "now_cost",
            "price_m",
            "status",
            "valid_from",
            "valid_to",
            F.col("valid_to").isNull().alias("is_current"),
        )
    )


def fct_fixture(fixtures: DataFrame, teams: DataFrame) -> DataFrame:
    home = teams.select(
        "season",
        F.col("team_id").alias("home_team_id"),
        F.col("team_name").alias("home_team"),
        F.col("short_name").alias("home_short"),
    )
    away = teams.select(
        "season",
        F.col("team_id").alias("away_team_id"),
        F.col("team_name").alias("away_team"),
        F.col("short_name").alias("away_short"),
    )
    result = (
        F.when(~F.col("finished"), F.lit(None))
        .when(F.col("home_score") > F.col("away_score"), "H")
        .when(F.col("home_score") < F.col("away_score"), "A")
        .otherwise("D")
    )
    status = (
        F.when(F.col("finished"), "finished")
        .when(F.col("gameweek_id").isNull(), "unscheduled")
        .when(F.col("started"), "in_play")
        .otherwise("scheduled")
    )
    return (
        fixtures.join(home, ["season", "home_team_id"], "left")
        .join(away, ["season", "away_team_id"], "left")
        .select(
            "season",
            "fixture_id",
            "gameweek_id",
            "kickoff_time",
            "home_team_id",
            "home_team",
            "home_short",
            "away_team_id",
            "away_team",
            "away_short",
            "home_score",
            "away_score",
            result.alias("result"),
            (F.col("home_score") + F.col("away_score")).alias("total_goals"),
            "home_difficulty",
            "away_difficulty",
            "finished",
            status.alias("status"),
        )
    )


def _team_matches(fct: DataFrame) -> DataFrame:
    """Finished fixtures from each team's perspective (two rows per fixture)."""
    played = fct.filter(F.col("finished") & F.col("gameweek_id").isNotNull())
    sides = []
    for team, opp, gf, ga in (
        ("home_team_id", "away_team_id", "home_score", "away_score"),
        ("away_team_id", "home_team_id", "away_score", "home_score"),
    ):
        sides.append(
            played.select(
                "season",
                "fixture_id",
                "gameweek_id",
                "kickoff_time",
                F.col(team).alias("team_id"),
                F.col(opp).alias("opponent_team_id"),
                F.lit(team == "home_team_id").alias("is_home"),
                F.col(gf).alias("goals_for"),
                F.col(ga).alias("goals_against"),
            )
        )
    tm = sides[0].unionByName(sides[1])
    result = (
        F.when(F.col("goals_for") > F.col("goals_against"), "W")
        .when(F.col("goals_for") == F.col("goals_against"), "D")
        .otherwise("L")
    )
    return tm.withColumn("result", result).withColumn(
        "points", F.when(F.col("result") == "W", 3).when(F.col("result") == "D", 1).otherwise(0)
    )


def mart_league_table(fct: DataFrame, teams: DataFrame, gameweeks: DataFrame) -> DataFrame:
    """Standings after every started gameweek.

    Ordering: points, goal difference, goals scored, then team name. (The Premier League
    then uses head-to-head record, which only matters for exact ties late in a season.)
    """
    tm = _team_matches(fct).alias("m")
    as_of = gameweeks.filter(F.col("finished") | F.col("is_current")).select(
        "season", F.col("gameweek_id").alias("as_of_gameweek")
    )
    grid = teams.select("season", "team_id", "team_name", "short_name").join(as_of, "season")
    joined = grid.alias("g").join(
        tm,
        (F.col("g.season") == F.col("m.season"))
        & (F.col("g.team_id") == F.col("m.team_id"))
        & (F.col("m.gameweek_id") <= F.col("g.as_of_gameweek")),
        "left",
    )
    table = joined.groupBy(
        F.col("g.season").alias("season"),
        F.col("g.as_of_gameweek").alias("as_of_gameweek"),
        F.col("g.team_id").alias("team_id"),
        F.col("g.team_name").alias("team_name"),
        F.col("g.short_name").alias("short_name"),
    ).agg(
        F.count("m.fixture_id").alias("played"),
        F.sum(F.when(F.col("m.result") == "W", 1).otherwise(0)).alias("won"),
        F.sum(F.when(F.col("m.result") == "D", 1).otherwise(0)).alias("drawn"),
        F.sum(F.when(F.col("m.result") == "L", 1).otherwise(0)).alias("lost"),
        F.coalesce(F.sum("m.goals_for"), F.lit(0)).alias("goals_for"),
        F.coalesce(F.sum("m.goals_against"), F.lit(0)).alias("goals_against"),
        F.coalesce(F.sum("m.points"), F.lit(0)).alias("points"),
        F.array_sort(
            F.collect_list(
                F.when(F.col("m.fixture_id").isNotNull(), F.struct("m.kickoff_time", "m.result"))
            )
        ).alias("_results"),
    )
    last5 = F.slice("_results", F.greatest(F.size("_results") - 4, F.lit(1)), 5)
    ranking = Window.partitionBy("season", "as_of_gameweek").orderBy(
        F.col("points").desc(),
        F.col("goal_difference").desc(),
        F.col("goals_for").desc(),
        F.col("team_name").asc(),
    )
    return (
        table.withColumn("goal_difference", F.col("goals_for") - F.col("goals_against"))
        .withColumn("form_last5", F.concat_ws("", F.transform(last5, lambda r: r["result"])))
        .withColumn("points_per_game", F.try_divide("points", "played").cast("decimal(4,2)"))
        .withColumn("position", F.row_number().over(ranking))
        .select(
            "season",
            "as_of_gameweek",
            "position",
            "team_id",
            "team_name",
            "short_name",
            "played",
            "won",
            "drawn",
            "lost",
            "goals_for",
            "goals_against",
            "goal_difference",
            "points",
            "points_per_game",
            "form_last5",
        )
    )


def fct_player_gameweek(
    stats: DataFrame,
    dim: DataFrame,
    gameweeks: DataFrame,
    fixtures: DataFrame,
    *,
    history: DataFrame | None,
    players: DataFrame,
) -> DataFrame:
    """Player x gameweek facts with team and price *as of that gameweek*.

    Price/team come from per-fixture history when ingested (exact), else from the SCD2
    dimension as of the gameweek deadline, else from the player's current record.
    """
    deadlines = gameweeks.select("season", "gameweek_id", F.to_date("deadline_time").alias("_d"))
    s = stats.join(deadlines, ["season", "gameweek_id"], "left")

    scd = dim.select(
        "season",
        "player_id",
        "valid_from",
        "valid_to",
        F.col("team_id").alias("_scd_team"),
        F.col("price_m").alias("_scd_price"),
    )
    s = s.join(
        scd,
        (s["season"] == scd["season"])
        & (s["player_id"] == scd["player_id"])
        & (scd["valid_from"] <= s["_d"])
        & (s["_d"] < F.coalesce(scd["valid_to"], F.lit(FAR_FUTURE).cast("date"))),
        "left",
    )
    s = s.drop(scd["season"]).drop(scd["player_id"]).drop("valid_from", "valid_to")

    if history is not None:
        fx = fixtures.select("season", "fixture_id", "home_team_id", "away_team_id")
        first_fixture = Window.partitionBy("season", "player_id", "gameweek_id").orderBy(
            "kickoff_time"
        )
        hist = (
            history.join(fx, ["season", "fixture_id"], "left")
            .withColumn(
                "_hist_team",
                F.when(F.col("was_home"), F.col("home_team_id")).otherwise(F.col("away_team_id")),
            )
            .withColumn("_rn", F.row_number().over(first_fixture))
            .filter("_rn = 1")
            .select(
                "season",
                "player_id",
                "gameweek_id",
                "_hist_team",
                F.col("price_m").alias("_hist_price"),
            )
        )
        s = s.join(hist, ["season", "player_id", "gameweek_id"], "left")
    else:
        s = s.withColumn("_hist_team", F.lit(None).cast("int")).withColumn(
            "_hist_price", F.lit(None).cast("decimal(4,1)")
        )

    current = players.select(
        "season",
        "player_id",
        "web_name",
        "position_id",
        F.col("team_id").alias("_cur_team"),
        F.col("price_m").alias("_cur_price"),
    )
    s = s.join(current, ["season", "player_id"], "left")
    price = F.coalesce("_hist_price", "_scd_price", "_cur_price")
    return s.select(
        "season",
        "gameweek_id",
        "player_id",
        "web_name",
        F.coalesce("_hist_team", "_scd_team", "_cur_team").alias("team_id"),
        "position_id",
        price.alias("price_m"),
        "fixtures_played",
        "minutes",
        "starts",
        "goals_scored",
        "assists",
        (F.col("goals_scored") + F.col("assists")).alias("goal_involvements"),
        "clean_sheets",
        "goals_conceded",
        "own_goals",
        "penalties_saved",
        "penalties_missed",
        "yellow_cards",
        "red_cards",
        "saves",
        "bonus",
        "bps",
        "defensive_contribution",
        "influence",
        "creativity",
        "threat",
        "ict_index",
        "expected_goals",
        "expected_assists",
        "expected_goal_involvements",
        "expected_goals_conceded",
        "total_points",
        F.try_divide(F.col("total_points"), price).cast("decimal(6,2)").alias("points_per_million"),
        "in_dreamteam",
        "is_final",
    )


def mart_player_form(fct_pgw: DataFrame, gameweeks: DataFrame, players: DataFrame) -> DataFrame:
    """Rolling 3- and 5-gameweek form for every player over every finished gameweek.

    Gameweeks where a player has no stats row count as zero, so rolling windows are true
    calendar windows rather than "last five appearances".
    """
    finished = gameweeks.filter("finished").select("season", "gameweek_id")
    grid = players.select("season", "player_id", "web_name", "position_id", "price_m").join(
        finished, "season"
    )
    f = grid.join(
        fct_pgw.select(
            "season",
            "player_id",
            "gameweek_id",
            "team_id",
            F.col("price_m").alias("_gw_price"),
            "minutes",
            "total_points",
            "goal_involvements",
            "expected_goal_involvements",
        ),
        ["season", "player_id", "gameweek_id"],
        "left",
    )
    for c in ("minutes", "total_points", "goal_involvements"):
        f = f.withColumn(c, F.coalesce(c, F.lit(0)))
    f = f.withColumn(
        "expected_goal_involvements",
        F.coalesce("expected_goal_involvements", F.lit(0).cast("decimal(7,2)")),
    )
    ordered = Window.partitionBy("season", "player_id").orderBy("gameweek_id")
    w3, w5 = ordered.rowsBetween(-2, 0), ordered.rowsBetween(-4, 0)
    price = F.coalesce("_gw_price", "price_m")
    out = (
        f.withColumn("points_last3", F.sum("total_points").over(w3))
        .withColumn("points_last5", F.sum("total_points").over(w5))
        .withColumn("minutes_last5", F.sum("minutes").over(w5))
        .withColumn("appearances_last5", F.sum((F.col("minutes") > 0).cast("int")).over(w5))
        .withColumn("goal_involvements_last5", F.sum("goal_involvements").over(w5))
        .withColumn("xgi_last5", F.sum("expected_goal_involvements").over(w5).cast("decimal(7,2)"))
        .withColumn("price_m", price)
        .withColumn(
            "points_per_million_last5", F.try_divide("points_last5", price).cast("decimal(6,2)")
        )
    )
    rank = Window.partitionBy("season", "gameweek_id", "position_id").orderBy(
        F.col("points_last5").desc(), F.col("xgi_last5").desc()
    )
    return out.withColumn("form_rank_in_position", F.dense_rank().over(rank)).select(
        "season",
        "gameweek_id",
        "player_id",
        "web_name",
        "position_id",
        "price_m",
        "total_points",
        "points_last3",
        "points_last5",
        "minutes_last5",
        "appearances_last5",
        "goal_involvements_last5",
        "xgi_last5",
        "points_per_million_last5",
        "form_rank_in_position",
    )


def mart_team_gameweek(fct: DataFrame, fct_pgw: DataFrame) -> DataFrame:
    """Team results and expected-goals profile per gameweek.

    ``xg_against`` is approximated as the largest ``expected_goals_conceded`` among the
    team's players that gameweek (the player on the pitch longest, normally the keeper).
    """
    tm = (
        _team_matches(fct)
        .groupBy("season", "gameweek_id", "team_id")
        .agg(
            F.count("*").alias("fixtures_played"),
            F.sum("goals_for").alias("goals_for"),
            F.sum("goals_against").alias("goals_against"),
            F.sum("points").alias("points"),
            F.sum((F.col("goals_against") == 0).cast("int")).alias("clean_sheets"),
        )
    )
    xg = fct_pgw.groupBy("season", "gameweek_id", "team_id").agg(
        F.sum("expected_goals").cast("decimal(7,2)").alias("xg_for"),
        F.max("expected_goals_conceded").cast("decimal(7,2)").alias("xg_against"),
        F.sum("total_points").alias("fpl_points"),
    )
    return tm.join(xg, ["season", "gameweek_id", "team_id"], "left").select(
        "season",
        "gameweek_id",
        "team_id",
        "fixtures_played",
        "goals_for",
        "goals_against",
        "points",
        "clean_sheets",
        "xg_for",
        "xg_against",
        (F.col("goals_for") - F.col("xg_for")).cast("decimal(7,2)").alias("goals_minus_xg"),
        "fpl_points",
    )


def mart_fixture_ticker(fct: DataFrame, gameweeks: DataFrame, horizon: int = 5) -> DataFrame:
    """Each team's next ``horizon`` gameweeks of fixtures with FPL difficulty ratings."""
    nxt = gameweeks.filter("is_next").select("season", F.col("gameweek_id").alias("from_gameweek"))
    upcoming = (
        fct.filter(~F.col("finished") & F.col("gameweek_id").isNotNull())
        .join(nxt, "season")
        .filter(
            (F.col("gameweek_id") >= F.col("from_gameweek"))
            & (F.col("gameweek_id") < F.col("from_gameweek") + horizon)
        )
    )
    sides = []
    for team, opp_short, venue, difficulty in (
        ("home_team_id", "away_short", "H", "home_difficulty"),
        ("away_team_id", "home_short", "A", "away_difficulty"),
    ):
        sides.append(
            upcoming.select(
                "season",
                "from_gameweek",
                F.col(team).alias("team_id"),
                F.struct(
                    F.col("gameweek_id").alias("gameweek_id"),
                    F.col("kickoff_time").alias("kickoff_time"),
                    F.col(opp_short).alias("opponent"),
                    F.lit(venue).alias("venue"),
                    F.col(difficulty).alias("difficulty"),
                ).alias("fx"),
            )
        )
    rows = sides[0].unionByName(sides[1])
    return (
        rows.groupBy("season", "team_id")
        .agg(
            F.first("from_gameweek").alias("from_gameweek"),
            F.array_sort(F.collect_list("fx")).alias("fixtures"),
            F.avg("fx.difficulty").cast("decimal(3,2)").alias("avg_difficulty"),
        )
        .withColumn("fixture_count", F.size("fixtures"))
        .withColumn(
            "ticker",
            F.concat_ws(
                " ",
                F.transform(
                    "fixtures",
                    lambda x: F.concat(x["opponent"], F.lit("("), x["venue"], F.lit(")")),
                ),
            ),
        )
        .select(
            "season",
            "team_id",
            "from_gameweek",
            "fixture_count",
            "avg_difficulty",
            "ticker",
            "fixtures",
        )
    )


GOLD_TABLES = (
    "dim_team",
    "dim_gameweek",
    "dim_player",
    "fct_fixture",
    "fct_player_gameweek",
    "mart_league_table",
    "mart_player_form",
    "mart_team_gameweek",
    "mart_fixture_ticker",
)


def run_gold(
    spark: SparkSession, settings: Settings, seasons: Sequence[str] | None = None
) -> list[str]:
    """Rebuild gold for ``seasons`` (default: every season in silver). Returns the seasons."""

    def silver(name: str) -> DataFrame:
        df = delta_io.read(spark, settings.table_path("silver", name))
        return df if seasons is None else df.filter(F.col("season").isin(list(seasons)))

    target = sorted(
        seasons or {r["season"] for r in silver("gameweeks").select("season").distinct().collect()}
    )
    if not target:
        log.warning("gold skipped: silver has no seasons yet")
        return []

    teams = silver("teams").cache()
    gameweeks = silver("gameweeks").cache()
    players = silver("players").cache()
    fixtures = silver("fixtures").cache()
    history_path = settings.table_path("silver", "player_fixture_history")
    history = (
        silver("player_fixture_history") if delta_io.table_exists(spark, history_path) else None
    )

    built: dict[str, DataFrame] = {}
    built["dim_team"] = dim_team(teams)
    built["dim_gameweek"] = dim_gameweek(gameweeks, silver("gameweek_chip_plays"))
    built["dim_player"] = dim_player(
        silver("player_snapshots"), players, silver("positions")
    ).cache()
    built["fct_fixture"] = fct_fixture(fixtures, teams).cache()
    built["mart_league_table"] = mart_league_table(built["fct_fixture"], teams, gameweeks)
    built["mart_fixture_ticker"] = mart_fixture_ticker(built["fct_fixture"], gameweeks)
    # Before gameweek 1 kicks off there are no live stats yet; the player facts wait for them.
    if delta_io.table_exists(spark, settings.table_path("silver", "player_gameweek_stats")):
        pgw = fct_player_gameweek(
            silver("player_gameweek_stats"),
            built["dim_player"],
            gameweeks,
            fixtures,
            history=history,
            players=players,
        ).cache()
        built["fct_player_gameweek"] = pgw
        built["mart_player_form"] = mart_player_form(pgw, gameweeks, players)
        built["mart_team_gameweek"] = mart_team_gameweek(built["fct_fixture"], pgw)
    else:
        log.warning("no player gameweek stats yet; player facts and marts skipped")

    predicate = delta_io.sql_in("season", target)
    for name in GOLD_TABLES:
        if name in built:
            delta_io.replace_where(spark, built[name], settings.table_path("gold", name), predicate)
            log.info("gold table written", extra={"table": name, "seasons": target})
    spark.catalog.clearCache()
    return target

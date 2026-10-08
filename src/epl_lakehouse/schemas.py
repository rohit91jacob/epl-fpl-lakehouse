"""Explicit schemas for parsing raw FPL payloads (schema-on-read in the silver layer).

Only the fields the models use are declared. Unknown upstream fields are ignored, so
additive API changes never break the pipeline; removed or retyped fields surface as nulls
that the data-quality suite catches.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_SCHEMA = StructType(
    [
        StructField("endpoint", StringType(), False),
        StructField("resource_id", StringType(), True),
        StructField("season", StringType(), False),
        StructField("ingest_date", DateType(), False),
        StructField("run_id", StringType(), False),
        StructField("fetched_at", TimestampType(), False),
        StructField("source_url", StringType(), False),
        StructField("sha256", StringType(), False),
        StructField("size_bytes", LongType(), False),
        StructField("is_final", BooleanType(), True),
        StructField("raw_path", StringType(), False),
        StructField("payload", StringType(), False),
        StructField("loaded_at", TimestampType(), False),
    ]
)

_PLAYER_MATCH_STATS = """
    minutes INT, goals_scored INT, assists INT, clean_sheets INT, goals_conceded INT,
    own_goals INT, penalties_saved INT, penalties_missed INT, yellow_cards INT,
    red_cards INT, saves INT, bonus INT, bps INT, influence STRING, creativity STRING,
    threat STRING, ict_index STRING, starts INT, expected_goals STRING,
    expected_assists STRING, expected_goal_involvements STRING,
    expected_goals_conceded STRING, defensive_contribution INT
"""


def _struct(fields: str, *extra: str) -> str:
    """Turn ``a INT, b STRING`` into ``STRUCT<`a`: INT, `b`: STRING>``.

    ``fields`` must be flat ``name TYPE`` pairs; nested members go in ``extra`` verbatim.
    """
    parts = [" ".join(p.split()).split(" ", 1) for p in fields.split(",") if p.strip()]
    members = [f"`{name}`: {dtype}" for name, dtype in parts] + list(extra)
    return "STRUCT<" + ", ".join(members) + ">"


_EVENT = _struct(
    """id INT, name STRING, deadline_time STRING, finished BOOLEAN, data_checked BOOLEAN,
    is_previous BOOLEAN, is_current BOOLEAN, is_next BOOLEAN, average_entry_score INT,
    highest_score INT, most_selected INT, most_transferred_in INT, most_captained INT,
    most_vice_captained INT, top_element INT, transfers_made BIGINT""",
    "`chip_plays`: ARRAY<STRUCT<chip_name: STRING, num_played: BIGINT>>",
)

_TEAM = _struct(
    """id INT, code INT, name STRING, short_name STRING, strength INT, position INT,
    strength_overall_home INT, strength_overall_away INT, strength_attack_home INT,
    strength_attack_away INT, strength_defence_home INT, strength_defence_away INT,
    pulse_id INT"""
)

_ELEMENT_TYPE = _struct(
    """id INT, singular_name STRING, singular_name_short STRING, plural_name STRING,
    squad_select INT, squad_min_play INT, squad_max_play INT"""
)

_ELEMENT = _struct(
    """id INT, code INT, first_name STRING, second_name STRING, web_name STRING,
    team INT, team_code INT, element_type INT, status STRING, now_cost INT,
    cost_change_start INT, selected_by_percent STRING, form STRING, points_per_game STRING,
    total_points INT, event_points INT, transfers_in BIGINT, transfers_out BIGINT,
    transfers_in_event BIGINT, transfers_out_event BIGINT, chance_of_playing_next_round INT,
    news STRING, news_added STRING, birth_date STRING, team_join_date STRING,
    opta_code STRING, removed BOOLEAN, can_select BOOLEAN,"""
    + _PLAYER_MATCH_STATS
)

BOOTSTRAP_SCHEMA = (
    f"events ARRAY<{_EVENT}>, teams ARRAY<{_TEAM}>, "
    f"element_types ARRAY<{_ELEMENT_TYPE}>, elements ARRAY<{_ELEMENT}>"
)

_STAT_ENTRY = "ARRAY<STRUCT<value: INT, element: INT>>"
_FIXTURE = _struct(
    """id INT, code INT, event INT, kickoff_time STRING, finished BOOLEAN,
    finished_provisional BOOLEAN, started BOOLEAN, minutes INT, provisional_start_time BOOLEAN,
    team_h INT, team_a INT, team_h_score INT, team_a_score INT, team_h_difficulty INT,
    team_a_difficulty INT, pulse_id INT""",
    f"`stats`: ARRAY<STRUCT<identifier: STRING, h: {_STAT_ENTRY}, a: {_STAT_ENTRY}>>",
)
FIXTURES_SCHEMA = f"ARRAY<{_FIXTURE}>"

_LIVE_STATS = _struct(_PLAYER_MATCH_STATS + ", total_points INT, in_dreamteam BOOLEAN")
_EXPLAIN = (
    "ARRAY<STRUCT<fixture: INT, stats: ARRAY<STRUCT<identifier: STRING, points: INT, "
    "value: INT, points_modification: INT>>>>"
)
EVENT_LIVE_SCHEMA = f"elements ARRAY<STRUCT<id: INT, stats: {_LIVE_STATS}, explain: {_EXPLAIN}>>"

_HISTORY = _struct(
    """element INT, fixture INT, opponent_team INT, total_points INT, was_home BOOLEAN,
    kickoff_time STRING, round INT, value INT, transfers_balance BIGINT, selected BIGINT,
    transfers_in BIGINT, transfers_out BIGINT,"""
    + _PLAYER_MATCH_STATS
)
_HISTORY_PAST = _struct(
    """season_name STRING, element_code INT, start_cost INT, end_cost INT, total_points INT,"""
    + _PLAYER_MATCH_STATS
)
ELEMENT_SUMMARY_SCHEMA = f"history ARRAY<{_HISTORY}>, history_past ARRAY<{_HISTORY_PAST}>"

# Numeric fields the API ships as strings, with the decimal type they are cast to.
DECIMAL_STATS = {
    "influence": "decimal(7,1)",
    "creativity": "decimal(7,1)",
    "threat": "decimal(7,1)",
    "ict_index": "decimal(7,1)",
    "expected_goals": "decimal(7,2)",
    "expected_assists": "decimal(7,2)",
    "expected_goal_involvements": "decimal(7,2)",
    "expected_goals_conceded": "decimal(7,2)",
}

INT_STATS = [
    "minutes",
    "goals_scored",
    "assists",
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
    "starts",
    "defensive_contribution",
]

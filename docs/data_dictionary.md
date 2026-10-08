# Data dictionary

Every table is a Delta table under `$EPL_DATA_ROOT/<layer>/<table>`. Apart from
`silver.player_past_seasons` and the `ops` tables, every table is partitioned by
`season` (for example `2026-27`). FPL ids are reused every season, so `season` is part
of every key.

## Raw landing zone (`raw/`)

The API responses exactly as received, byte for byte, and never modified afterwards.

| Path | Content |
|---|---|
| `fpl/<endpoint>/season=<s>/ingest_date=<d>/run_id=<r>/<name>.json` | One API response |
| `_manifests/run_id=<r>.json` | Lists what a run landed: path, source URL, fetch time, SHA-256, size, finality, status (`succeeded` / `failed`) |
| `_state/season=<s>.json` | Incremental state: the final gameweeks, and the last checksum per resource |

Endpoints: `bootstrap-static`, `fixtures`, `event-live` (`event/<gw>/live`),
`element-summary` (`element-summary/<player>`).

## Bronze

### `bronze.fpl_responses`
One row per landed response. Partitioned by `endpoint` and `season`.

| Column | Type | Notes |
|---|---|---|
| endpoint | string | `bootstrap-static`, `fixtures`, `event-live`, `element-summary` |
| resource_id | string | gameweek for `event-live`, player id for `element-summary`, else null |
| season | string | derived from the gameweek 1 deadline |
| ingest_date | date | UTC date of the fetch |
| run_id | string | orchestrator run id (lineage) |
| fetched_at | timestamp | when the response was received |
| source_url | string | |
| sha256, size_bytes | string, bigint | integrity |
| is_final | boolean | `event-live` only: gameweek was `finished` and `data_checked` at fetch time |
| raw_path | string | path relative to `raw/` |
| payload | string | the JSON body, verbatim |
| loaded_at | timestamp | |

Key: `(endpoint, resource_id, season, run_id)`. Inserts only.

## Silver

| Table | Grain / key | Write mode | Source |
|---|---|---|---|
| `teams` | season, team_id | MERGE, newest wins | bootstrap `teams` |
| `positions` | season, position_id | MERGE | bootstrap `element_types` |
| `gameweeks` | season, gameweek_id | MERGE | bootstrap `events` |
| `gameweek_chip_plays` | season, gameweek_id, chip_name | MERGE | bootstrap `events[].chip_plays` |
| `players` | season, player_id | MERGE | bootstrap `elements` (current state) |
| `player_snapshots` | season, player_id, snapshot_date | MERGE (latest per day) | every bootstrap payload |
| `fixtures` | season, fixture_id | MERGE | `fixtures` |
| `fixture_player_stats` | season, fixture_id, stat, side, player_id | replace per season | `fixtures[].stats` |
| `player_gameweek_stats` | season, gameweek_id, player_id | replace per gameweek | `event/<gw>/live` |
| `player_gameweek_explain` | season, gameweek_id, player_id, fixture_id, stat | replace per gameweek | `event/<gw>/live` `explain` |
| `player_fixture_history` | season, player_id, fixture_id | MERGE | `element-summary` `history` |
| `player_past_seasons` | player_code, season_name | MERGE | `element-summary` `history_past` |

Conventions:
- Prices: `now_cost` / `value` are integers in tenths of £m (`61` is £6.1m). `price_m` is
  `decimal(4,1)` in £m.
- Numbers the API sends as strings (ICT, xG, form, ownership) are cast to decimals with
  `try_cast`, so a malformed value becomes null and is caught by data quality instead of
  failing the job.
- `player_code` / `team_code` are stable across seasons. `player_id` / `team_id` are not.
- `fetched_at` on each row says which response it came from.

## Gold

### `dim_team` — season, team_id
Names, short names and FPL strength ratings (home/away, overall/attack/defence).

### `dim_gameweek` — season, gameweek_id
Deadline, `finished`, `data_checked`, `is_current`, `is_next`, average and highest
manager scores, most-captained and top player, transfers, chips played (total and per
chip type).

### `dim_player` — player_sk (SCD type 2)
One row per version of a player's tracked attributes: `team_id`, `position_id`,
`now_cost`, `status`. `valid_from` is inclusive and `valid_to` is exclusive (null for
the current version). `is_current` marks the current version. Versions come from the
daily snapshots, so the history starts at the first ingestion.

### `fct_fixture` — season, fixture_id
Teams (ids and names), score, `result` (`H`/`D`/`A`), total goals, FPL difficulty for
each side, and `status` (`scheduled`, `in_play`, `finished`, `unscheduled`).

### `fct_player_gameweek` — season, gameweek_id, player_id
All per-match stats summed over the player's fixtures in that gameweek, plus:
- `team_id` and `price_m` **as of the gameweek**: taken from per-fixture history when it
  was ingested, otherwise from `dim_player` at the deadline, otherwise from the current
  record.
- `fixtures_played`: 0 for a blank gameweek, 2 for a double.
- `goal_involvements`, and `points_per_million` (`total_points / price_m`).

### `mart_league_table` — season, as_of_gameweek, team_id
The standings after every started gameweek: played, won, drawn, lost, goals for and
against, goal difference, points, points per game, and `form_last5` (oldest to newest,
e.g. `WWDLW`). Teams are ranked by points, then goal difference, then goals scored, then
name.

### `mart_player_form` — season, gameweek_id, player_id
Rolling 3- and 5-gameweek totals over calendar gameweeks (a missed gameweek counts as
zero): points, minutes, appearances, goal involvements, xGI, points per £m, and the rank
within the player's position.

### `mart_team_gameweek` — season, gameweek_id, team_id
Fixtures played, goals for and against, points, clean sheets, and `xg_for` (the sum of
the players' xG). `xg_against` is approximated by the largest `expected_goals_conceded`
among the team's players. Also `goals_minus_xg` and total FPL points.

### `mart_fixture_ticker` — season, team_id
The next 5 gameweeks of fixtures from `is_next`: an array of
`(gameweek, kickoff, opponent, venue, difficulty)`, the fixture count, average
difficulty, and a readable `ticker` such as `PEM(A) OAK(H) MAR(H)`.

## Ops

| Table | Content |
|---|---|
| `ops.bronze_loaded_runs` | Ingestion runs already loaded into bronze |
| `ops.watermarks` | Highest bronze `fetched_at` processed per (stage, endpoint) |
| `ops.dq_results` | Every data-quality check result: run, layer, table, check, severity, passed, violation count, time |

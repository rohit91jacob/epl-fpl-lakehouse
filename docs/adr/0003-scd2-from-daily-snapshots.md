# ADR 0003: Player SCD2 rebuilt from daily snapshots

**Status:** accepted

## Context
The API only shows a player's *current* price, team and status. History therefore has to
be captured by us.

## Decision
Every bootstrap payload adds a row per player to `silver.player_snapshots` (the latest
per day wins). `gold.dim_player` is recomputed from the snapshots on every run: a new
version starts whenever `(team, position, price, status)` changes, including A → B → A.

## Consequences
- The recompute is deterministic and trivially correct: about 700 players × about 300
  days is roughly 200k rows a season.
- History begins at the first ingestion. For earlier gameweeks, `fct_player_gameweek`
  uses the exact per-fixture price from `element-summary` history when that was ingested.
- Data-quality checks enforce exactly one current version per player, and no overlapping
  versions.

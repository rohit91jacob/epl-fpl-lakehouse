# Runbook

Every stage is idempotent, so the fix for most failures is to re-run the stage, or the
whole DAG run. Prefer that over editing data by hand.

## Exit codes

| Code | Meaning | First step |
|---|---|---|
| 0 | success | — |
| 1 | the stage failed (API down, bug, missing table) | read the task log; re-run |
| 2 | an `error`-severity data-quality check failed | query `ops.dq_results` (below) |

## Triage a data-quality failure

```bash
epl show ops.dq_results --where "NOT passed" --order-by checked_at \
  --columns run_id,layer,table,check,severity,violations
```

- Silver checks gate gold. If `dq_silver` fails, gold is not rebuilt, and dashboards keep
  showing the last good gold snapshot.
- A failure on a freshly finished gameweek is often a provisional-data issue, for example
  bonus points not yet confirmed. Re-run after FPL sets `data_checked`.
- Look at the raw data for the run: `raw/_manifests/run_id=<run>.json` lists every file
  and its checksum.

## Common failures

| Symptom | Cause | Action |
|---|---|---|
| `ContractError: missing required keys` | FPL changed the response shape | Inspect the raw file; update `schemas.py` and `REQUIRED_KEYS`; add a test; deploy; re-run |
| `HTTP 403/429` on ingest | rate limited or blocked | Lower `EPL_MAX_REQUESTS_PER_SECOND` / `EPL_MAX_WORKERS`; retries already back off and honour `Retry-After` |
| `HTTP 503` during the game update | FPL is offline between seasons or gameweeks | Wait; the DAG retries with backoff |
| Ingest status `failed` for some `element-summary` | partial outage | Re-run: failed runs don't advance state, so everything is retried |
| `ConcurrentAppendException` | two runs wrote the same table at once | Shouldn't happen with `max_active_runs=1`; re-run the stage |
| Spark `OutOfMemoryError` | driver too small | Raise `EPL_SPARK_DRIVER_MEMORY` |

## Backfill and reprocessing

- **Rebuild silver and gold from bronze** after a transform change. Nothing is refetched:
  ```bash
  epl silver --full-refresh && epl quality --layer silver && epl gold && epl quality --layer gold
  ```
- **Refetch everything** (corrupt raw data, or a new season's history): trigger the DAG
  with `{"full_refresh": true}`, or run `epl run --full-refresh`. This ignores the
  incremental state, including final gameweeks.
- **Rebuild one season of gold:** `epl gold --season 2026-27`.
- **Load a specific raw run into bronze:** `epl bronze --run-id <run> --only-run`.
- **Restore a table to an earlier version:** Delta time travel, for example
  `RESTORE TABLE delta.\`<path>\` TO VERSION AS OF <n>`. History is kept for
  `EPL_VACUUM_RETENTION_HOURS`.

## New season

FPL resets in July: the ids restart and the `season` derived from the gameweek 1
deadline changes. No action is needed, because every table is keyed by season and the
incremental state is per season. Before gameweek 1, gold builds only the dimensions,
league table and ticker, and the player facts appear once live stats exist.

## Maintenance

`epl_fpl_maintenance` runs weekly: `OPTIMIZE` (compaction) and then `VACUUM` with
`EPL_VACUUM_RETENTION_HOURS` (default 168). Don't set the retention below the longest
query or the longest time-travel need.

## Local inspection

```bash
epl show gold.mart_league_table --where "as_of_gameweek = 5" --order-by position
epl show gold.mart_player_form --where "gameweek_id = 5 AND form_rank_in_position <= 3" \
  --order-by position_id,form_rank_in_position
```

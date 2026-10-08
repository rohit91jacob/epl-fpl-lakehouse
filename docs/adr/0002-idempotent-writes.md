# ADR 0002: Every stage is idempotent

**Status:** accepted

## Decision
| Stage | Mechanism |
|---|---|
| ingest | A run id names its landing paths; failed runs don't advance state; byte-identical responses are skipped |
| bronze | Insert-only MERGE on `(endpoint, resource_id, season, run_id)`, plus `ops.bronze_loaded_runs` |
| silver entities | MERGE that updates only when the source `fetched_at` is strictly newer, so out-of-order loads converge |
| silver facts | `replaceWhere` per season or gameweek, guarded so an older payload never replaces a newer one |
| silver | A watermark per endpoint means a no-op rerun commits nothing (verified by test) |
| gold | Deterministic full rebuild per season with `replaceWhere season IN (...)` |

## Why replace instead of MERGE for facts
A newer `event/<gw>/live` or `fixtures` payload can *remove* rows, for example when
provisional bonus points are withdrawn. A key-based MERGE would leave those stale rows in
place. Replacing everything derived from the resource cannot.

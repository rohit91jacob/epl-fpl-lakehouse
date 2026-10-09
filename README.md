# EPL FPL Lakehouse

[![CI](https://github.com/rohit91jacob/epl-fpl-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/rohit91jacob/epl-fpl-lakehouse/actions/workflows/ci.yml)
[![Refresh](https://github.com/rohit91jacob/epl-fpl-lakehouse/actions/workflows/refresh.yml/badge.svg)](https://github.com/rohit91jacob/epl-fpl-lakehouse/actions/workflows/refresh.yml)
[![Live results](https://img.shields.io/badge/live%20results-GitHub%20Pages-2a78d6.svg)](https://rohit91jacob.github.io/epl-fpl-lakehouse/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-3.12-blue.svg)
![Spark](https://img.shields.io/badge/spark-4.1-orange.svg)
![Delta Lake](https://img.shields.io/badge/delta%20lake-4.3-00ADD4.svg)
![Airflow](https://img.shields.io/badge/airflow-3.3-017CEE.svg)

An end-to-end data pipeline for **English Premier League** data from the
[Fantasy Premier League](https://fantasy.premierleague.com) API.

- Python ingestion lands every API response unmodified in a checksummed raw zone.
- **PySpark + Delta Lake** turn it into bronze, silver and gold layers.
- Data-quality gates sit between the layers, and every stage is idempotent and
  incremental.
- **Apache Airflow** orchestrates the stages. CI runs the full stack in Docker.

**Live results, rebuilt every morning:** https://rohit91jacob.github.io/epl-fpl-lakehouse/

It produces a dimensional model and analytics marts: league standings after every
gameweek, player form and value, team xG, a fixture-difficulty ticker, and a type-2
player dimension that tracks price and transfer history.

> Verified against live data (2026-27, after gameweek 5): the computed league table
> matches the positions FPL publishes for **20/20 clubs**, and per-player season totals
> (points, goals, minutes) match FPL's figures for **all 667 players**. All 46
> data-quality checks pass.

---

## Architecture

```mermaid
flowchart LR
    API[(FPL API<br/>bootstrap-static · fixtures<br/>event/gw/live · element-summary)]
    subgraph Ingest["Ingest · pure Python"]
        C[HTTP client<br/>retries · rate limit · contracts]
    end
    subgraph Lake["Delta Lake · $EPL_DATA_ROOT"]
        R[(raw/<br/>immutable JSON + manifests)]
        B[(bronze<br/>fpl_responses)]
        S[(silver<br/>12 typed tables)]
        G[(gold<br/>dims · facts · marts)]
        O[(ops<br/>watermarks · dq_results)]
    end
    DQ1{{DQ gate}}
    DQ2{{DQ gate}}
    AF[[Airflow 3<br/>epl_fpl_daily · epl_fpl_maintenance]]

    API --> C --> R --> B --> S --> DQ1 --> G --> DQ2
    DQ1 -.results.-> O
    DQ2 -.results.-> O
    AF -. "epl ingest / bronze / silver / quality / gold" .-> C
```

| Component | Responsibility | Code |
|---|---|---|
| FPL client | Retries with exponential backoff (honours `Retry-After`), rate limiting, response contracts, and an offline `file://` transport | `ingestion/client.py` |
| Raw landing zone | Atomic writes of the exact response bytes, per-run manifests with SHA-256, and incremental state | `ingestion/landing.py`, `ingestion/pipeline.py` |
| Bronze | Append-only Delta table, one row per response, with full lineage | `bronze.py` |
| Silver | Schema-on-read parsing, typing, MERGE or replace semantics, and watermarks | `silver.py`, `schemas.py` |
| Gold | SCD2 player dimension, facts, and marts rebuilt per season | `gold.py` |
| Data quality | Declarative checks; results persisted; `error` checks fail the run | `quality/` |
| Orchestration | Airflow DAGs that call the `epl` CLI | `dags/` |
| Maintenance | `OPTIMIZE` + `VACUUM` | `maintenance.py` |
| Results site | Static, script-free HTML + `data.json` from gold, published to GitHub Pages | `report.py` |

## Tech stack

| | Version | Why |
|---|---|---|
| Python | 3.12 | |
| Apache Spark (PySpark) | 4.1.1 | Runs locally by default; point `EPL_SPARK_MASTER` at a cluster to scale |
| Delta Lake | 4.3.1 | ACID, MERGE, `replaceWhere`, time travel; pinned as a pair with Spark |
| Java | 17 or 21 | Required by Spark 4 |
| Apache Airflow | 3.3.2 | LocalExecutor + Postgres 16 in Docker Compose |
| requests / urllib3 | 2.x | HTTP with retry adapters |
| uv, ruff, mypy, pytest | latest | Locked dev toolchain (`uv.lock`) |

## Data source

| Endpoint | What | Fetched |
|---|---|---|
| `bootstrap-static/` | teams, players (price, status, totals), gameweeks, positions | every run |
| `fixtures/` | all 380 fixtures, scores, per-player match stats | every run |
| `event/{gw}/live/` | every player's stats and points breakdown for a gameweek | each started gameweek, until FPL marks it final (`data_checked`) |
| `element-summary/{id}/` | per-fixture player history (exact price at the time) and past seasons | every run, unchanged responses skipped (`EPL_INGEST_PLAYER_HISTORY`) |

The FPL API is public but undocumented, and the data belongs to the Premier League. This
project redistributes **no FPL data**: you fetch it yourself, for personal and
educational use, in line with the
[FPL terms](https://fantasy.premierleague.com/help/terms). The client identifies itself
and rate-limits to 4 requests/second by default. The bundled sample data
(`epl sample-data`) is **synthetic** with fictional clubs and players, and is used for
tests, CI and offline demos. This project is not affiliated with the Premier League.

## Data model

```mermaid
erDiagram
    dim_team ||--o{ fct_fixture : "home / away"
    dim_gameweek ||--o{ fct_fixture : contains
    dim_player ||--o{ fct_player_gameweek : "as-of version"
    dim_gameweek ||--o{ fct_player_gameweek : ""
    dim_team ||--o{ mart_league_table : ""
    fct_player_gameweek ||--o{ mart_player_form : "rolling windows"
    fct_fixture ||--o{ mart_team_gameweek : ""
    fct_fixture ||--o{ mart_fixture_ticker : "next 5 GWs"
```

| Layer | Tables |
|---|---|
| bronze | `fpl_responses` |
| silver | `teams`, `positions`, `gameweeks`, `gameweek_chip_plays`, `players`, `player_snapshots`, `fixtures`, `fixture_player_stats`, `player_gameweek_stats`, `player_gameweek_explain`, `player_fixture_history`, `player_past_seasons` |
| gold | `dim_team`, `dim_gameweek`, `dim_player` (SCD2), `fct_fixture`, `fct_player_gameweek`, `mart_league_table`, `mart_player_form`, `mart_team_gameweek`, `mart_fixture_ticker` |
| ops | `bronze_loaded_runs`, `watermarks`, `dq_results` |

The grain, keys, write mode and every column are in
**[docs/data_dictionary.md](docs/data_dictionary.md)**.

## Quickstart

**Prerequisites:** [uv](https://docs.astral.sh/uv/), and Java 17+ on `PATH` (for
Spark). Docker is needed only for the Airflow stack. Linux, macOS or WSL2.

```bash
git clone https://github.com/rohit91jacob/epl-fpl-lakehouse.git
cd epl-fpl-lakehouse
uv sync                      # creates .venv with the pinned toolchain
```

### Offline: synthetic data, no network needed after `uv sync`

```bash
uv run epl sample-data --out ./data/sample-api        # prints file:///.../sample-api
export EPL_FPL_BASE_URL=file://$PWD/data/sample-api
uv run epl run                                        # ingest → bronze → silver → DQ → gold → DQ
```

The first Spark start downloads the Delta Lake jars and their dependencies into `~/.ivy2.5.2`. This happens once; the Docker images bake them in.

### Live: the real FPL API

```bash
unset EPL_FPL_BASE_URL
uv run epl run          # about 10 minutes including ~670 polite API calls
uv run epl show gold.mart_league_table --where "as_of_gameweek = 5" --order-by position \
  --columns position,team_name,played,won,drawn,lost,goal_difference,points,form_last5 --limit 5
```

```text
+--------+---------+------+---+-----+----+---------------+------+----------+
|position|team_name|played|won|drawn|lost|goal_difference|points|form_last5|
+--------+---------+------+---+-----+----+---------------+------+----------+
|1       |Man City |5     |5  |0    |0   |8              |15    |WWWWW     |
|2       |Arsenal  |5     |4  |0    |1   |4              |12    |WWWWL     |
|3       |Brighton |5     |3  |1    |1   |11             |10    |WLDWW     |
|4       |Brentford|5     |2  |3    |0   |6              |9     |WDDDW     |
|5       |Leeds    |5     |2  |3    |0   |4              |9     |WDDWD     |
+--------+---------+------+---+-----+----+---------------+------+----------+
```

More examples:

```bash
uv run epl show gold.mart_player_form --where "gameweek_id = 5" --order-by "points_last5 DESC" \
  --columns web_name,price_m,points_last5,xgi_last5,points_per_million_last5 --limit 5
uv run epl show gold.mart_fixture_ticker --order-by avg_difficulty --columns team_id,avg_difficulty,ticker
uv run epl show ops.dq_results --where "NOT passed"      # empty when all checks pass
uv run epl report --out site                             # the results site: site/index.html
```

Each stage can also run on its own: `epl ingest`, `epl bronze`, `epl silver`,
`epl quality --layer silver|gold|all`, `epl gold`, `epl maintenance`. See `epl --help`.

### Airflow with Docker Compose *(verified in CI: `compose-e2e` job)*

```bash
cp .env.example .env          # set AIRFLOW_JWT_SECRET / AIRFLOW_FERNET_KEY for anything shared
docker compose up -d --build  # http://localhost:8080  (admin / admin)
```

Unpause `epl_fpl_daily`, which runs daily at 06:00 UTC, or trigger it with
`{"full_refresh": true}` to rebuild everything. The lake lives in the `lake` volume. To
run once without Airflow:

```bash
docker compose --profile standalone run --rm pipeline run
```

## Configuration

Every setting is an environment variable. Copy [.env.example](.env.example) to start.

| Variable | Default | Purpose |
|---|---|---|
| `EPL_DATA_ROOT` | `./data` | Root of `raw/`, `bronze/`, `silver/`, `gold/`, `ops/` |
| `EPL_FPL_BASE_URL` | `https://fantasy.premierleague.com/api` | API base URL, or `file:///dir` for captured or synthetic responses |
| `EPL_INGEST_PLAYER_HISTORY` | `true` | Fetch `element-summary` for every player (exact historical prices) |
| `EPL_HTTP_TIMEOUT_SECONDS` | `30` | Per-request timeout |
| `EPL_HTTP_MAX_RETRIES` / `EPL_HTTP_BACKOFF_SECONDS` | `5` / `1.0` | Retries on 429/5xx with exponential backoff |
| `EPL_MAX_REQUESTS_PER_SECOND` / `EPL_MAX_WORKERS` | `4` / `4` | Politeness limits |
| `EPL_SPARK_MASTER` | `local[*]` | Spark master URL |
| `EPL_SPARK_DRIVER_MEMORY` | `2g` | |
| `EPL_SPARK_SHUFFLE_PARTITIONS` | `8` | Also sets Delta snapshot parallelism |
| `EPL_SPARK_JARS` | *(empty)* | Pre-provisioned jars (the Docker images set this; no Maven at runtime) |
| `EPL_LOG_LEVEL` / `EPL_LOG_FORMAT` | `INFO` / `json` | JSON lines for log shippers, or `text` |
| `EPL_EXPECTED_TEAM_COUNT` | `20` | Used by the data-quality checks |
| `EPL_VACUUM_RETENTION_HOURS` | `168` | Time-travel window kept by `VACUUM` |

## Testing and CI

```bash
uv run pytest -m "not spark and not airflow"   # fast: client, ingestion, sample data, CLI
uv run pytest -m "not airflow"                 # + Spark end-to-end (needs Java 17+)
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

The Spark suite builds a lake from the synthetic API and checks it against **an
independent pure-Python oracle**. It replays a realistic sequence:
1. a run after gameweek 5;
2. a no-op rerun, asserting zero new Delta commits and identical gold;
3. a run after gameweek 6, asserting history is unchanged and new SCD2 versions appear for
   exactly the players whose price moved.

The synthetic season includes a blank gameweek, a double gameweek, a postponed fixture
and own goals. The HTTP tests use a real local server, so the urllib3 retry logic
actually runs.

| CI job | What it proves |
|---|---|
| `lint` | ruff lint + format, mypy on `src/` |
| `test` | every non-Airflow test on Java 17, with coverage |
| `airflow` | DAGs import cleanly on Airflow 3.3.2 (official constraints); task order and retry policy |
| `docker` | builds the pipeline image and runs the full pipeline inside it on synthetic data |
| `compose-e2e` | boots Airflow + Postgres with Docker Compose, runs `airflow dags test epl_fpl_daily`, then re-runs every data-quality check inside the stack |

Two more workflows keep the data current:

| Workflow | Schedule | What it does |
|---|---|---|
| `refresh.yml` | daily 06:30 UTC, or manual (with an optional full refresh) | Restores the lake from the Actions cache, runs `epl run` against the live API (incremental), runs maintenance on Mondays, saves the lake, builds the results site and deploys it to GitHub Pages. On a scheduled failure it opens a `Scheduled refresh is failing` issue, or comments on the open one. |
| `keepalive.yml` | 1st of each month | GitHub disables scheduled workflows in public repos after 60 days without activity; this re-enables them through the API, with no dummy commits. |

Dependabot updates uv, GitHub Actions and Docker base images weekly. Spark and Delta are
pinned and bumped together by hand. Pre-commit runs ruff and basic hygiene hooks.

## Operations

- **Schedule:** the hosted refresh is the GitHub Actions `refresh.yml` workflow (daily 06:30 UTC),
  which publishes the live results site. For self-hosted deployments the same stages run as the
  Airflow DAG `epl_fpl_daily` (06:00 UTC) plus `epl_fpl_maintenance` (Mondays 03:00 UTC).
- **Credentials: none to maintain.** The FPL API is public, and publishing uses GitHub's
  built-in per-run `GITHUB_TOKEN`, so nothing expires or needs rotating.
- **Where the lake lives on GitHub:** the Actions cache, restored at the start of each run and
  saved at the end. If the cache is evicted (7 days unused, or the 10 GB repo limit), the next
  run rebuilds the lake from the API. Standings and facts come back exactly; `dim_player`
  price history restarts from that day.
- **Incremental:** final gameweeks are never refetched, unchanged responses are not
  stored again, silver processes only bronze rows newer than its watermark, and gold
  rebuilds only the seasons present.
- **Idempotent:** every stage can be retried or re-run safely
  ([ADR 0002](docs/adr/0002-idempotent-writes.md)).
- **Data-quality gates:** silver and gold suites (46 checks) cover keys, nulls,
  referential integrity, ranges, scorelines reconciling with goal scorers, league-wide
  balance (goals for = goals against, wins = losses), SCD2 validity, and a comparison
  with FPL's published table. Results go to `ops.dq_results`. Exit code `2` means an
  `error` check failed.
- **Backfill, reprocessing, failure triage and new-season handling:**
  **[docs/runbook.md](docs/runbook.md)**.

## Project structure

```text
├── src/epl_lakehouse/
│   ├── cli.py                 # `epl` command: one subcommand per stage
│   ├── config.py              # EPL_* settings
│   ├── ingestion/             # client.py · landing.py · pipeline.py (no Spark needed)
│   ├── bronze.py · silver.py · gold.py · schemas.py · delta_io.py · ops.py
│   ├── quality/               # framework.py · suites.py · results.py
│   ├── maintenance.py         # OPTIMIZE + VACUUM
│   ├── report.py              # static results site (GitHub Pages)
│   ├── spark_session.py
│   └── sample.py              # deterministic synthetic FPL API
├── dags/                      # epl_fpl_daily.py · epl_fpl_maintenance.py
├── tests/                     # unit, HTTP, ingestion, Spark e2e vs oracle, DQ, CLI, DAGs
├── docs/                      # data_dictionary.md · runbook.md · adr/
├── Dockerfile                 # standalone pipeline image (Delta jars baked in)
├── docker/airflow/Dockerfile  # Airflow + Java + epl
├── docker-compose.yml         # Airflow 3 + Postgres (+ standalone profile)
├── .github/                   # CI, daily refresh + Pages, keep-alive, Dependabot
└── pyproject.toml · uv.lock · Makefile · .env.example
```

## Design decisions and trade-offs

- **[ADR 0001](docs/adr/0001-medallion-on-delta-lake.md): medallion on Delta Lake.** The
  raw bytes are always replayable, and every layer can be rebuilt from the one below.
  Spark is more than this data volume needs, but it gives ACID semantics and a scale-out
  path.
- **[ADR 0002](docs/adr/0002-idempotent-writes.md): MERGE for entities, `replaceWhere`
  for facts.** A newer payload can remove rows, for example withdrawn provisional bonus
  points.
- **[ADR 0003](docs/adr/0003-scd2-from-daily-snapshots.md): SCD2 rebuilt from daily
  snapshots.** This is deterministic and cheap at about 200k rows a season.
- **[ADR 0004](docs/adr/0004-airflow-runs-the-cli.md): Airflow runs the CLI.** That
  gives one code path, one JVM per task, and an orchestrator that can be swapped.

**Known limitations**
- `dim_player` history starts at the first ingestion, because the API exposes only
  current state. Per-fixture prices from `element-summary` cover earlier gameweeks.
- League ties are broken by points, goal difference, goals scored, then name. The Premier
  League's head-to-head tie-breakers are not implemented.
- `mart_team_gameweek.xg_against` is an approximation: it takes the largest
  `expected_goals_conceded` among the club's players.
- On GitHub, the lake persists in the Actions cache, which is best-effort storage. An evicted
  cache means a rebuild, and price history restarts.
- The raw zone is a local filesystem. Delta paths could be `s3a://`, but raw landing
  would need an object-store writer first.

**Roadmap:** an object-store lake (S3/GCS) so history survives cache eviction, a BI-friendly
SQL endpoint (Spark Thrift or DuckDB over Delta), and multi-season backfill from the
`history_past` codes.

## License

[MIT](LICENSE) for the code. FPL data is the property of the Premier League and is not
included in this repository.

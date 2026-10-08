# ADR 0004: Airflow orchestrates the `epl` CLI

**Status:** accepted

## Decision
Each Airflow task is a `BashOperator` that runs one `epl` subcommand. DAGs hold no
business logic.

## Rationale
- A task behaves exactly as it does in a terminal, a cron job or a container: one code
  path to test.
- Each task gets its own Spark JVM, which is freed when the task ends. That prevents
  memory creep in long-lived workers.
- Exit codes (1 for failure, 2 for data quality) make failures distinguishable in Airflow.
- Swapping the orchestrator (Dagster, Kubernetes CronJobs, Step Functions) only means
  calling the same CLI.

`max_active_runs=1` and `max_active_tasks=1` keep a single writer per Delta table, and
one local JVM per worker.

"""
### EPL FPL lakehouse — daily pipeline

`ingest → bronze → silver → dq_silver → gold → dq_gold`

Each task shells out to the `epl` CLI so the Spark JVM lives only for the task's
duration and tasks behave exactly as they do from a terminal. Every stage is idempotent,
so retries and manual re-runs are safe. Trigger with `{"full_refresh": true}` to refetch
and reprocess everything (backfills, schema changes).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

FULL_REFRESH = "{{ '--full-refresh' if params.full_refresh else '' }}"
RUN_ID = "--run-id '{{ run_id }}'"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="epl_fpl_daily",
    description="FPL API -> Delta Lake medallion (bronze/silver/gold) with data-quality gates",
    schedule="0 6 * * *",  # after overnight bonus-point confirmation
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # one local Spark JVM at a time per worker
    default_args=default_args,
    params={"full_refresh": Param(False, type="boolean", description="refetch and rebuild all")},
    tags=["epl", "fpl", "lakehouse", "spark"],
    doc_md=__doc__,
) as dag:
    ingest = BashOperator(task_id="ingest", bash_command=f"epl ingest {RUN_ID} {FULL_REFRESH}")
    bronze = BashOperator(task_id="bronze", bash_command=f"epl bronze {RUN_ID}")
    silver = BashOperator(task_id="silver", bash_command=f"epl silver {FULL_REFRESH}")
    dq_silver = BashOperator(
        task_id="dq_silver", bash_command=f"epl quality --layer silver {RUN_ID}", retries=0
    )
    gold = BashOperator(task_id="gold", bash_command="epl gold")
    dq_gold = BashOperator(
        task_id="dq_gold", bash_command=f"epl quality --layer gold {RUN_ID}", retries=0
    )

    ingest >> bronze >> silver >> dq_silver >> gold >> dq_gold

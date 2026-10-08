"""
### EPL FPL lakehouse — weekly Delta maintenance

Compacts small files (`OPTIMIZE`) and removes files no longer referenced by any table
version older than the retention window (`VACUUM`, default 168 hours).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="epl_fpl_maintenance",
    description="OPTIMIZE + VACUUM every Delta table in the lakehouse",
    schedule="0 3 * * 1",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["epl", "lakehouse", "maintenance"],
    doc_md=__doc__,
) as dag:
    BashOperator(task_id="optimize_and_vacuum", bash_command="epl maintenance")

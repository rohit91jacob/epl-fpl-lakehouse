"""DAG integrity tests. Run where apache-airflow is installed (the CI `airflow` job)."""

from __future__ import annotations

from itertools import pairwise
from pathlib import Path

import pytest

pytestmark = pytest.mark.airflow
airflow = pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():  # type: ignore[no-untyped-def]
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS))


def test_dags_import_cleanly(dagbag) -> None:  # type: ignore[no-untyped-def]
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"epl_fpl_daily", "epl_fpl_maintenance"}


def test_daily_dag_runs_stages_in_order(dagbag) -> None:  # type: ignore[no-untyped-def]
    dag = dagbag.get_dag("epl_fpl_daily")
    order = ["ingest", "bronze", "silver", "dq_silver", "gold", "dq_gold"]
    assert [t.task_id for t in dag.topological_sort()] == order
    for upstream, downstream in pairwise(order):
        assert downstream in dag.get_task(upstream).downstream_task_ids
    assert dag.max_active_runs == 1
    assert not dag.catchup


def test_quality_gates_do_not_retry(dagbag) -> None:  # type: ignore[no-untyped-def]
    dag = dagbag.get_dag("epl_fpl_daily")
    assert dag.get_task("dq_silver").retries == 0
    assert dag.get_task("ingest").retries == 2


def test_full_refresh_param_is_templated(dagbag) -> None:  # type: ignore[no-untyped-def]
    dag = dagbag.get_dag("epl_fpl_daily")
    assert "params.full_refresh" in dag.get_task("ingest").bash_command
    assert "run_id" in dag.get_task("bronze").bash_command

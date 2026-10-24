"""Checks that the DAG file loads and is wired the way the README says.

Needs Airflow installed, which the main test job does not have, so the whole
module is skipped there and run in its own CI job.
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS_FOLDER = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_FOLDER))


@pytest.fixture(scope="module")
def dag(dagbag):
    # The in-memory dict, not get_dag(), which would need a metadata database.
    return dagbag.dags.get("air_quality_daily")


def test_dag_file_imports_cleanly(dagbag):
    assert dagbag.import_errors == {}


def test_dag_exists_with_the_expected_tasks(dag):
    assert dag is not None
    assert [t.task_id for t in dag.topological_sort()] == [
        "resolve_window",
        "extract",
        "load",
        "dbt_source_freshness",
        "dbt_build",
    ]


def test_tasks_run_strictly_in_order(dag):
    assert dag.get_task("extract").upstream_task_ids == {"resolve_window"}
    assert dag.get_task("load").upstream_task_ids == {"extract"}
    assert dag.get_task("dbt_source_freshness").upstream_task_ids == {"load"}
    assert dag.get_task("dbt_build").upstream_task_ids == {"dbt_source_freshness"}


def test_schedule_and_concurrency(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == "0 12 * * *"


def test_network_tasks_retry_and_every_task_alerts(dag):
    for task_id in ("extract", "load", "dbt_source_freshness"):
        task = dag.get_task(task_id)
        assert task.retries == 3
        assert task.retry_exponential_backoff
    assert dag.get_task("dbt_build").retries == 1
    assert dag.get_task("resolve_window").retries == 0
    for task in dag.tasks:
        assert task.on_failure_callback, f"{task.task_id} has no failure alert"
        assert task.execution_timeout is not None


def test_dates_are_passed_as_environment_variables_not_pasted_into_the_command(dag):
    for task_id in ("extract", "load"):
        task = dag.get_task(task_id)
        assert "{{" not in task.bash_command
        assert set(task.env) == {"WINDOW_START", "WINDOW_END", "SNAPSHOT_DATE"}
        assert task.append_env is True

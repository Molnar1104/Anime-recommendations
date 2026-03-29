"""
tests/test_dag.py

Unit tests for the Airflow DAG definition.
Validates structure, task order, and default args.

Requires Airflow to be installed — tests are skipped otherwise
(Airflow is heavy and often installed via Docker rather than pip).
"""

import sys
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow", reason="Airflow not installed — skipping DAG tests")


# ---------------------------------------------------------------------------
# Fixture: import the DAG module
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dag_module():
    dag_path = Path(__file__).resolve().parents[1] / "dags"
    if str(dag_path) not in sys.path:
        sys.path.insert(0, str(dag_path))

    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    import anime_pipeline_dag as mod
    return mod


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_dag_exists(dag_module):
    assert hasattr(dag_module, "dag"), "DAG object not found in module"


def test_dag_id(dag_module):
    assert dag_module.dag.dag_id == "anime_pipeline"


def test_dag_schedule(dag_module):
    schedule = dag_module.dag.schedule_interval
    assert schedule is not None, "DAG should have a schedule"


def test_dag_has_four_tasks(dag_module):
    task_ids = [t.task_id for t in dag_module.dag.tasks]
    assert len(task_ids) == 4, f"Expected 4 tasks, got {task_ids}"


def test_task_ids_match(dag_module):
    task_ids = {t.task_id for t in dag_module.dag.tasks}
    expected = {"fetch_anime", "load_raw", "dbt_run", "dbt_test"}
    assert task_ids == expected, f"Expected {expected}, got {task_ids}"


def test_task_order(dag_module):
    """Verify the linear dependency chain: fetch → load → dbt_run → dbt_test."""
    tasks = {t.task_id: t for t in dag_module.dag.tasks}

    fetch_downstream = {t.task_id for t in tasks["fetch_anime"].downstream_list}
    load_downstream = {t.task_id for t in tasks["load_raw"].downstream_list}
    dbt_run_downstream = {t.task_id for t in tasks["dbt_run"].downstream_list}

    assert "load_raw" in fetch_downstream
    assert "dbt_run" in load_downstream
    assert "dbt_test" in dbt_run_downstream


def test_default_retries(dag_module):
    assert dag_module.dag.default_args.get("retries") == 3


def test_catchup_disabled(dag_module):
    assert dag_module.dag.catchup is False, "catchup should be False to avoid backfill storms"

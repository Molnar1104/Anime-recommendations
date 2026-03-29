"""
dags/anime_pipeline_dag.py

Airflow DAG that orchestrates the full anime data pipeline:
  1. fetch_anime_task  — extract data from AniList API → raw JSON files
  2. load_raw_task     — load raw JSON into PostgreSQL raw schema
  3. dbt_run_task      — run dbt models (staging → marts)
  4. dbt_test_task     — run dbt data-quality tests

Scheduled daily. Each task has 3 retries with exponential backoff.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_DIR / "dbt"

# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------

def _on_failure(context):
    """Log task failure details so they surface in Airflow logs."""
    task_id = context.get("task_instance").task_id
    exception = context.get("exception")
    logger.error("Task '%s' failed: %s", task_id, exception)

# ---------------------------------------------------------------------------
# Python callables
# ---------------------------------------------------------------------------

def _run_extraction(**kwargs):
    """Run the AniList extraction pipeline."""
    from extract.fetch_anime import (
        fetch_anime,
        fetch_reviews,
        fetch_users,
    )

    anime_records = fetch_anime()
    review_records = fetch_reviews(anime_records)
    fetch_users(review_records)


def _run_load(**kwargs):
    """Load raw JSON files into PostgreSQL raw schema."""
    from load.load_to_postgres import run as load_run

    load_run()

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "anime-pipeline",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="anime_pipeline",
    default_args=default_args,
    description="End-to-end anime data pipeline: extract → load → dbt transform → test",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["anime", "elt", "dbt"],
) as dag:

    fetch_anime_task = PythonOperator(
        task_id="fetch_anime",
        python_callable=_run_extraction,
    )

    load_raw_task = PythonOperator(
        task_id="load_raw",
        python_callable=_run_load,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
    )

    # Task dependencies — linear chain
    fetch_anime_task >> load_raw_task >> dbt_run_task >> dbt_test_task

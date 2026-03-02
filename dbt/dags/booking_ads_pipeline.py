"""
Airflow DAG: Booking Ads dbt Pipeline

Orchestrates the dbt project to transform raw booking and ad data
into analytics-ready marts.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/booking_analytics"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="booking_ads_dbt_pipeline",
    default_args=default_args,
    description="Transform raw booking & ad data into reporting marts using dbt",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "booking-ads", "elt"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir .",
        env={
            "DBT_HOST": "warehouse",
            "DBT_PORT": "5432",
        },
        append_env=True,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir .",
        env={
            "DBT_HOST": "warehouse",
            "DBT_PORT": "5432",
        },
        append_env=True,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select staging --profiles-dir .",
        env={
            "DBT_HOST": "warehouse",
            "DBT_PORT": "5432",
        },
        append_env=True,
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select intermediate marts --profiles-dir .",
        env={
            "DBT_HOST": "warehouse",
            "DBT_PORT": "5432",
        },
        append_env=True,
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select marts --profiles-dir .",
        env={
            "DBT_HOST": "warehouse",
            "DBT_PORT": "5432",
        },
        append_env=True,
    )

    # Task dependencies: staged approach with quality gates
    dbt_deps >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts

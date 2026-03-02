"""
Airflow DAG: Booking Ads dbt Pipeline (TaskFlow API)

Orchestrates the dbt project to transform raw booking and ad data
into analytics-ready marts.
"""

import subprocess

import pendulum

from airflow.sdk import dag, task

DBT_PROJECT_DIR = "/opt/airflow/dbt/booking_analytics"
DBT_ENV = {
    "DBT_HOST": "warehouse",
    "DBT_PORT": "5432",
}


def run_dbt(command: str) -> str:
    """Run a dbt command and return its output."""
    result = subprocess.run(
        f"cd {DBT_PROJECT_DIR} && dbt {command} --profiles-dir .",
        shell=True,
        capture_output=True,
        text=True,
        env={**DBT_ENV},
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt {command} failed:\n{result.stderr}")
    return result.stdout


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "booking-ads", "elt"],
    description="Transform raw booking & ad data into reporting marts using dbt",
)
def booking_ads_dbt_pipeline():
    """
    ### Booking Ads dbt Pipeline

    ELT pipeline that transforms raw booking and ad bidding data into
    analytics-ready reporting tables using dbt.

    **Layers**: staging → intermediate → marts

    Each layer runs its models then executes data quality tests
    before proceeding to the next layer.
    """

    @task()
    def dbt_deps():
        """Install dbt packages (e.g. dbt_utils)."""
        return run_dbt("deps")

    @task()
    def dbt_run_staging():
        """Build staging models: clean and type raw sources."""
        return run_dbt("run --select staging")

    @task()
    def dbt_test_staging():
        """Quality gate: test staging models before building marts."""
        return run_dbt("test --select staging")

    @task()
    def dbt_run_marts():
        """Build intermediate and mart models: joins, aggregations, star schema."""
        return run_dbt("run --select intermediate marts")

    @task()
    def dbt_test_marts():
        """Quality gate: test mart models (CTR range, FK integrity, etc.)."""
        return run_dbt("test --select marts")

    # Task dependencies: staged approach with quality gates
    deps = dbt_deps()
    staging = dbt_run_staging()
    staging_tests = dbt_test_staging()
    marts = dbt_run_marts()
    marts_tests = dbt_test_marts()

    deps >> staging >> staging_tests >> marts >> marts_tests


booking_ads_dbt_pipeline()

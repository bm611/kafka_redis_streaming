"""
Airflow DAG: Booking Ads dbt Pipeline (TaskFlow API)

Orchestrates the dbt project to transform raw booking and ad data
into analytics-ready marts.
"""

import os
import subprocess

import pendulum

from airflow.sdk import dag, task

DBT_PROJECT_DIR = "/opt/airflow/dbt/booking_analytics"
DBT_ENV = {
    "DBT_HOST": "warehouse",
    "DBT_PORT": "5432",
}
DBT_TIMEOUT_SECONDS = 30 * 60


def run_dbt(args: list[str]) -> str:
    """Run a dbt command and return its output."""
    project_file = os.path.join(DBT_PROJECT_DIR, "dbt_project.yml")
    if not os.path.exists(project_file):
        raise RuntimeError(f"dbt project file missing: {project_file}")

    result = subprocess.run(
        ["dbt", *args, "--profiles-dir", "."],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=DBT_TIMEOUT_SECONDS,
        env={**os.environ, **DBT_ENV},
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout


def should_run_dbt_deps() -> bool:
    """Run deps only when packages are missing or lockfile is stale."""
    packages_file = os.path.join(DBT_PROJECT_DIR, "packages.yml")
    lock_file = os.path.join(DBT_PROJECT_DIR, "package-lock.yml")
    packages_dir = os.path.join(DBT_PROJECT_DIR, "dbt_packages")

    if not os.path.exists(packages_file):
        return False
    if not os.path.isdir(packages_dir):
        return True
    if not os.path.exists(lock_file):
        return True
    return os.path.getmtime(packages_file) > os.path.getmtime(lock_file)


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
    def dbt_debug():
        """Fail fast if dbt project/profile/connection is invalid."""
        return run_dbt(["debug"])

    @task()
    def dbt_deps():
        """Install dbt packages (e.g. dbt_utils)."""
        if not should_run_dbt_deps():
            return "Skipping dbt deps: dbt_packages is up to date"
        return run_dbt(["deps"])

    @task()
    def dbt_run_staging():
        """Build staging models: clean and type raw sources."""
        return run_dbt(["run", "--select", "staging"])

    @task()
    def dbt_test_staging():
        """Quality gate: test staging models before building marts."""
        return run_dbt(["test", "--select", "staging"])

    @task()
    def dbt_run_marts():
        """Build intermediate and mart models: joins, aggregations, star schema."""
        return run_dbt(["run", "--select", "intermediate", "marts"])

    @task()
    def dbt_test_marts():
        """Quality gate: test mart models (CTR range, FK integrity, etc.)."""
        return run_dbt(["test", "--select", "marts"])

    # Task dependencies: staged approach with quality gates
    debug = dbt_debug()
    deps = dbt_deps()
    staging = dbt_run_staging()
    staging_tests = dbt_test_staging()
    marts = dbt_run_marts()
    marts_tests = dbt_test_marts()

    debug >> deps >> staging >> staging_tests >> marts >> marts_tests


booking_ads_dbt_pipeline()

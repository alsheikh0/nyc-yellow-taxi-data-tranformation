"""
nyc_taxi_pipeline.py
─────────────────────────────────────────────────────────────
Full monthly orchestration DAG for the NYC Taxi pipeline.

Flow:
  1. trigger_adf_ingest        → ADF pipeline copies new monthly Parquet from TLC API → Bronze (ADLS)
  2. wait_for_adf              → polls ADF run until succeeded / failed / cancelled
  3. run_bronze_to_silver      → submits PySpark notebook to Databricks
  4. wait_for_databricks       → polls Databricks run until terminal state
  5. run_dbt_staging           → dbt run --select staging
  6. run_dbt_gold_dims         → dbt run --select gold.dimensions
  7. run_dbt_gold_fact         → dbt run --select fact_trips  (depends on dims)
  8. run_dbt_gold_aggs         → dbt run --select gold.aggregates  (depends on fact)
  9. run_dbt_tests             → dbt test --select gold
 10. notify_success / notify_failure

Schedule: monthly, runs on the 3rd of every month
(TLC publishes previous-month data with ~2-month lag;
 this DAG targets the most recent available month.)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.utils.trigger_rule import TriggerRule


# ── DAG-level defaults ────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            [Variable.get("alert_email", default_var="de-team@company.com")],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
}

# Pulled from Airflow Variables — never hardcode credentials in DAG files
ADF_CONN_ID          = "azure_data_factory_default"
DATABRICKS_CONN_ID   = "databricks_default"
ADF_RESOURCE_GROUP   = Variable.get("adf_resource_group",   default_var="rg-nyc-taxi")
ADF_FACTORY_NAME     = Variable.get("adf_factory_name",     default_var="adf-nyc-taxi")
ADF_PIPELINE_NAME    = Variable.get("adf_pipeline_name",    default_var="pl_ingest_yellow_taxi")
DATABRICKS_JOB_ID    = Variable.get("databricks_job_id",    default_var="123456")
DBT_PROJECT_DIR      = Variable.get("dbt_project_dir",      default_var="/opt/airflow/dbt/nyc_taxi")
DBT_PROFILES_DIR     = Variable.get("dbt_profiles_dir",     default_var="/opt/airflow/dbt/profiles")


# ── Helper functions ──────────────────────────────────────────────────────────

def trigger_adf_pipeline(**context) -> str:
    """
    Triggers the ADF pipeline that copies the latest monthly Parquet
    from the TLC HTTP endpoint into Bronze ADLS.
    Pushes the ADF run_id to XCom so the poller can track it.
    """
    logical_date  = context["logical_date"]
    # TLC data lags by ~2 months; target the month 2 months prior
    target_year   = (logical_date - timedelta(days=60)).strftime("%Y")
    target_month  = (logical_date - timedelta(days=60)).strftime("%m")

    hook = AzureDataFactoryHook(azure_data_factory_conn_id=ADF_CONN_ID)
    run_response = hook.run_pipeline(
        pipeline_name    = ADF_PIPELINE_NAME,
        resource_group_name = ADF_RESOURCE_GROUP,
        factory_name     = ADF_FACTORY_NAME,
        parameters       = {
            "target_year":  target_year,
            "target_month": target_month,
        },
    )
    run_id = run_response.run_id
    context["ti"].xcom_push(key="adf_run_id", value=run_id)
    print(f"ADF pipeline triggered — run_id: {run_id} | target: {target_year}-{target_month}")
    return run_id


def check_adf_status(**context) -> str:
    """
    Polls ADF run status. Returns branch task_id based on result.
    Raises AirflowException on terminal failure so retry logic kicks in.
    """
    run_id = context["ti"].xcom_pull(key="adf_run_id")
    hook   = AzureDataFactoryHook(azure_data_factory_conn_id=ADF_CONN_ID)
    status = hook.get_pipeline_run(
        run_id              = run_id,
        resource_group_name = ADF_RESOURCE_GROUP,
        factory_name        = ADF_FACTORY_NAME,
    ).status

    print(f"ADF run {run_id} status: {status}")

    if status == "Succeeded":
        return "run_bronze_to_silver"
    elif status in ("Failed", "Cancelled"):
        raise RuntimeError(f"ADF pipeline run {run_id} ended with status: {status}")
    else:
        # Still running — this task will be retried by Airflow
        raise RuntimeError(f"ADF pipeline still running (status={status}). Will retry.")


def run_dbt_command(command: str, select: str = "", **kwargs) -> str:
    """
    Runs a dbt command inside a subprocess and returns stdout.
    Raises on non-zero exit code so Airflow marks the task failed.
    """
    import subprocess
    full_cmd = (
        f"dbt {command} "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--target prod "
        f"{('--select ' + select) if select else ''}"
    )
    print(f"Running: {full_cmd}")
    result = subprocess.run(
        full_cmd, shell=True, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt command failed:\n{result.stderr}")
    return result.stdout


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id              = "nyc_taxi_monthly_pipeline",
    description         = "NYC Yellow Taxi — Bronze → Silver → Gold (monthly)",
    default_args        = DEFAULT_ARGS,
    start_date          = datetime(2024, 1, 1),
    schedule_interval   = "0 6 3 * *",   # 06:00 UTC on the 3rd of every month
    catchup             = False,
    max_active_runs     = 1,
    tags                = ["nyc-taxi", "medallion", "monthly"],
    doc_md              = __doc__,
) as dag:

    # ── 0. Start sentinel ─────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── 1. Trigger ADF ingest pipeline ───────────────────────────────────────
    trigger_adf = PythonOperator(
        task_id         = "trigger_adf_ingest",
        python_callable = trigger_adf_pipeline,
    )

    # ── 2. Poll ADF until complete ────────────────────────────────────────────
    poll_adf = PythonOperator(
        task_id         = "wait_for_adf",
        python_callable = check_adf_status,
        retries         = 30,            # poll up to 30 times
        retry_delay     = timedelta(minutes=5),
    )

    # ── 3. Trigger Databricks PySpark job (Bronze → Silver) ──────────────────
    run_pyspark = DatabricksRunNowOperator(
        task_id         = "run_bronze_to_silver",
        databricks_conn_id = DATABRICKS_CONN_ID,
        job_id          = DATABRICKS_JOB_ID,
        notebook_params = {
            "target_month": "{{ (logical_date - macros.timedelta(days=60)).strftime('%Y-%m') }}",
            "mode":         "incremental",
        },
        polling_period_seconds = 30,
    )

    # ── 4. dbt staging layer ──────────────────────────────────────────────────
    dbt_staging = PythonOperator(
        task_id         = "run_dbt_staging",
        python_callable = run_dbt_command,
        op_kwargs       = {"command": "run", "select": "staging"},
    )

    # ── 5. dbt Gold — dimensions (no dependencies on fact) ───────────────────
    dbt_dims = PythonOperator(
        task_id         = "run_dbt_gold_dims",
        python_callable = run_dbt_command,
        op_kwargs       = {"command": "run", "select": "gold.dimensions"},
    )

    # ── 6. dbt Gold — fact_trips (depends on dims) ────────────────────────────
    dbt_fact = PythonOperator(
        task_id         = "run_dbt_gold_fact",
        python_callable = run_dbt_command,
        op_kwargs       = {"command": "run", "select": "fact_trips"},
    )

    # ── 7. dbt Gold — aggregates (depend on fact_trips) ──────────────────────
    dbt_aggs = PythonOperator(
        task_id         = "run_dbt_gold_aggs",
        python_callable = run_dbt_command,
        op_kwargs       = {"command": "run", "select": "gold.aggregates"},
    )

    # ── 8. dbt tests — run against full Gold layer ────────────────────────────
    dbt_test = PythonOperator(
        task_id         = "run_dbt_tests",
        python_callable = run_dbt_command,
        op_kwargs       = {"command": "test", "select": "gold"},
    )

    # ── 9. Success / failure notifications ───────────────────────────────────
    notify_success = BashOperator(
        task_id       = "notify_success",
        bash_command  = (
            'echo "NYC Taxi pipeline succeeded for '
            '{{ (logical_date - macros.timedelta(days=60)).strftime(\"%Y-%m\") }}" '
            '| mail -s "[SUCCESS] nyc_taxi_pipeline" '
            '"{{ var.value.alert_email }}" || true'
        ),
        trigger_rule  = TriggerRule.ALL_SUCCESS,
    )

    notify_failure = BashOperator(
        task_id       = "notify_failure",
        bash_command  = (
            'echo "NYC Taxi pipeline FAILED. Check Airflow logs." '
            '| mail -s "[FAILURE] nyc_taxi_pipeline" '
            '"{{ var.value.alert_email }}" || true'
        ),
        trigger_rule  = TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id      = "end",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    (
        start
        >> trigger_adf
        >> poll_adf
        >> run_pyspark
        >> dbt_staging
        >> dbt_dims
        >> dbt_fact
        >> dbt_aggs
        >> dbt_test
        >> [notify_success, notify_failure]
        >> end
    )

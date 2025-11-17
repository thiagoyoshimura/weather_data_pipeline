from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import os

# --- Paths ---
BASE_DIR = "/Users/thiagoyoshimura/Projects/weather_data_pipeline"
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
PYTHON = os.path.join(BASE_DIR, "venv", "bin", "python")
DBT_BIN = "/Users/thiagoyoshimura/Projects/weather_data_pipeline/venv/bin/dbt"

default_args = {
    "owner": "thiago",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_data_pipeline",
    default_args=default_args,
    description="Daily Weather Data Pipeline",
    schedule="0 13 * * *",  # every day at 1PM
    start_date=datetime(2025, 11, 13),
    catchup=False,
    tags=["weather"],
) as dag:

    # --- Python scripts ---
    def run_script(script_name, **kwargs):
        script_path = os.path.join(SCRIPTS_DIR, script_name)
        subprocess.run([PYTHON, script_path], check=True)

    ingest = PythonOperator(
        task_id="ingest_weather_api",
        python_callable=run_script,
        op_args=["ingest_weather_api.py"],
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=run_script,
        op_args=["transform_data.py"],
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_script,
        op_args=["load_data.py"],
    )

    # --- dbt tasks ---
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {BASE_DIR}/weather_dbt && {DBT_BIN} run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {BASE_DIR}/weather_dbt && {DBT_BIN} test",
    )

    # --- Task flow ---
    ingest >> transform >> load >> dbt_run >> dbt_test

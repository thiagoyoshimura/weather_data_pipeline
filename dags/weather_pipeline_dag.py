from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

# --- Paths ---
BASE_DIR = "/Users/thiagoyoshimura/Projects/weather_data_pipeline"
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
PYTHON = os.path.join(BASE_DIR, "venv", "bin", "python")

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
    tags=["weather"],  # optional, helps organize DAGs in UI
) as dag:

    def run_script(script_name, **kwargs):
        """Run a Python script using subprocess"""
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

    # Define task order
    ingest >> transform

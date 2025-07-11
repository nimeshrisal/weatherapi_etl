from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add parent directory to sys.path for main.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for weather data that runs every 5 minutes',
    schedule_interval='*/5 * * * *',  # Corrected parameter
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['weather', 'etl', 'pipeline'],
) as dag:
    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=lambda: __import__('main').main(),  # Import main.py and call main function
    )

    run_etl
from airflow import DAG
from airflow.operators.python import PythonOperator  
from datetime import datetime, timedelta
import os
import sys

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

dag = DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,  
    description='A simple ETL pipeline for weather data that runs every 5 minutes',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2023, 10, 1),  # Start date for the DAG
    catchup=False,  # Do not backfill
    tags=['weather', 'etl', 'pipeline']  # Tags for better organization in Airflow UI
)

run_etl = PythonOperator(
    task_id='run_etl',
    python_callable=lambda: __import__('main').main(),  # Import main.py and call main function
    dag=dag,
)

run_etl
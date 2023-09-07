from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from appscripts import weather_api_script

def funcion():
    print('hola mmundo')

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_api_DAG',
    default_args=default_args,
    start_date=datetime(2023, 9, 6),
    schedule_interval='0 0 * * *',
) as dag:
    task1 = PythonOperator(
        task_id='weather_api_test',
        python_callable=weather_api_script.etl_weather_data,
    )

task1
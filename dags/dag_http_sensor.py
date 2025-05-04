from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime


dag = DAG(
    dag_id='dag_http_sensor',
    description='Dag testando httpsensors',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False
)


def query_api():
    import requests
    import json

    url = 'https://pokeapi.co/api/v2/pokemon/pikachu'
    response = requests.get(url)
    data = json.loads(response.text)
    print(data)


check_api = HttpSensor(
    task_id='check_api',
    http_conn_id='connection',
    endpoint='pikachu',
    poke_interval=5,
    timeout=20,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=query_api,
    dag=dag,
)

check_api >> process_data
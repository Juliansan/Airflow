from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id='dag_xcom',
    description='Dag testando o xcom',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False
)


def task_write(**kwargs):
    kwargs['ti'].xcom_push(key='valor_xcom1', value=10200)


task_1 = PythonOperator(
    task_id='task_1',
    dag=dag,
    python_callable=task_write,
)


def task_read(**kwargs):
    valor = kwargs['ti'].xcom_pull(task_ids='task_1', key='valor_xcom1')
    print(f'Valor lido do xcom: {valor}')


task_2 = PythonOperator(
    task_id='task_2',
    dag=dag,
    python_callable=task_read,
)

task_1 >> task_2 

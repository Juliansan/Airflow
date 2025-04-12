from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models import Variable

dag = DAG(
    dag_id='dag_pools',
    description='Dag testando as pools',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False
)

task1 = BashOperator(
    task_id='task1',
    bash_command='sleep 5',
    pool='pool_1',
    dag=dag
)


task2 = BashOperator(
    task_id='task2',
    bash_command='sleep 5',
    pool='pool_1',
    priority_weight=5,
    dag=dag
)


task3 = BashOperator(
    task_id='task3',
    bash_command='sleep 5',
    pool='pool_1',
    dag=dag
)


task4 = BashOperator(
    task_id='task4',
    bash_command='sleep 5',
    pool='pool_1', priority_weight=10,
    dag=dag
)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG(
    dag_id='dag_dummy',
    description='Dag testando dummy tasks',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False,
    tags=['dummy', 'test']
)

task1 = BashOperator(
    task_id='task_1',
    bash_command='sleep 5',
    dag=dag
)

task2 = BashOperator(
    task_id='task_2',
    bash_command='sleep 5',
    dag=dag
)

task3 = BashOperator(
    task_id='task_3',
    bash_command='sleep 5',
    dag=dag
)

dummy = EmptyOperator(
    task_id='dummy',
    dag=dag
)

task4 = BashOperator(
    task_id='task_4',
    bash_command='sleep 5',
    dag=dag
)

task5 = BashOperator(
    task_id='task_5',
    bash_command='sleep 5',
    dag=dag
)

[task1, task2, task3] >> dummy >> [task4, task5]

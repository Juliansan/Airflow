from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


dag = DAG(
    dag_id='dag_run_dag2',
    description='testando dags com grupos',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False
    )

task_1 = BashOperator(
    task_id='tsk1',
    bash_command='sleep 5',
    dag=dag
)

task_2 = BashOperator(
    task_id='tsk2',
    bash_command='sleep 5',
    dag=dag
)

task_1 >> task_2
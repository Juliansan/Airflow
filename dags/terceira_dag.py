from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    dag_id='terceira_dag',
    description='Minha terceira dag.',
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

task_3 = BashOperator(
    task_id='tsk3',
    bash_command='sleep 5',
    dag=dag
)

[task_1, task_2] >> task_3
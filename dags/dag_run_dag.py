from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator



dag = DAG(
    dag_id='dag_run_dag',
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

task_2 = TriggerDagRunOperator(
    task_id='tsk2',
    trigger_dag_id='dag_run_dag2',
    dag=dag
)

task_1 >> task_2
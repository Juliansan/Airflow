from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


dag = DAG(
    dag_id='dag_group',
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

task_3 = BashOperator(
    task_id='tsk3',
    bash_command='sleep 5',
    dag=dag
)

task_4 = BashOperator(
    task_id='tsk4',
    bash_command='sleep 5',
    dag=dag
)

task_5 = BashOperator(
    task_id='tsk5',
    bash_command='sleep 5',
    dag=dag
)

task_6 = BashOperator(
    task_id='tsk6',
    bash_command='sleep 5',
    dag=dag
)

tsk_group_1 = TaskGroup(
    group_id='group1',
    dag=dag
)

task_7 = BashOperator(
    task_id='tsk7',
    bash_command='sleep 5',
    task_group=tsk_group_1,
    dag=dag
)

task_8 = BashOperator(
    task_id='tsk8',
    bash_command='sleep 5',
    task_group=tsk_group_1,
    dag=dag
)

task_9 = BashOperator(
    task_id='tsk9',
    bash_command='sleep 5',
    trigger_rule='one_failed',
    task_group=tsk_group_1,
    dag=dag
)

task_1 >> task_2
task_3 >> task_4
[task_2, task_4] >> task_5 >> task_6
task_6 >> tsk_group_1

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
import random


dag = DAG(
    dag_id='dag_branchs',
    description='Dag testando as branchs',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False
)


def branch_func():
    return random.randint(0, 100)


gera_numero_aleatorio = PythonOperator(
    task_id='gera_numero_aleatorio',
    python_callable=branch_func,
    dag=dag
)


def avalia_numero_aleatorio(ti):
    numero_aleatorio = ti.xcom_pull(task_ids='gera_numero_aleatorio')
    if numero_aleatorio % 2 == 0:
        return 'par_task'
    else:
        return 'impar_task'


branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=avalia_numero_aleatorio,
    provide_context=True,
    dag=dag
)

par_task = BashOperator(
    task_id='par_task',
    bash_command='echo "Numero par"',
    dag=dag
)


impar_task = BashOperator(
    task_id='impar_task',
    bash_command='echo "Numero impar"',
    dag=dag
)


gera_numero_aleatorio >> branch_task >> [par_task, impar_task]

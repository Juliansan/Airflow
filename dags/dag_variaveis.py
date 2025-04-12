from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

dag = DAG(
    dag_id='dag_variables',
    description='Dag testando variaveis',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False
)


def print_variable(**kwargs):
    minha_var = Variable.get("minhavar")
    print(f"A variável é: {minha_var}")


task_1 = PythonOperator(
    task_id='print_variable',
    python_callable=print_variable,
    provide_context=True,
    dag=dag
)


task_1

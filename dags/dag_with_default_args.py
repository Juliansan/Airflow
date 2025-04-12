from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


defautl_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 2),
    'email': ['test@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), # Tempo de espera entre as tentativas
}

dag = DAG(
    dag_id='defautl_args_dag',
    description='Dag com argumentos padrão.',
    default_args=defautl_args,
    schedule_interval='@hourly', 
    catchup=False, 
    default_view='graph', # Muda a visualização padrão para graph
    tags=['example', 'default_args'], # Tags para facilitar a busca
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

task_1 >> task_2 >> task_3

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta


defautl_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 2),
    'email': ['julian.sanm@protonmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),  # Tempo de espera entre as tentativas
}

dag = DAG(
    dag_id='dag_email_test',
    description='Dag testando email',
    default_args=defautl_args,
    schedule_interval=None,
    catchup=False,
    default_view='graph',  # Muda a visualização padrão para graph
    tags=['email_test', 'default_args'],  # Tags para facilitar a busca
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
    bash_command='exit 1;',
    dag=dag
)

task_5 = BashOperator(
    task_id='tsk5',
    bash_command='sleep 5',
    trigger_rule='none_failed',
    dag=dag
)

task_6 = BashOperator(
    task_id='tsk6',
    bash_command='sleep 5',
    trigger_rule='none_failed',
    dag=dag
)

send_email = EmailOperator(
    task_id='send_email',
    to='julian.sanm@protonmail.com',
    subject='Airflow Alert',
    html_content="""
    <h3>Airflow Alert</h3>
    <p>Task {{ task.task_id }} failed.</p>
    """,
    dag=dag,
    trigger_rule='one_failed',  # Envia o email se qualquer tarefa falhar

)

[task_1, task_2] >> task_3 >> task_4
task_4 >> [task_5, task_6, send_email]

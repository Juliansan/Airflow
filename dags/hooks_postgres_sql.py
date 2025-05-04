from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


dag = DAG(
    dag_id='hook_postgresql_db',
    description='Dag testando hooks postgresql',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False
)


def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS test_table_2 (
            id integer
        );
    """, autocommit=True)


def insert_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("""
        INSERT INTO test_table_2 (id) VALUES (1);
    """, autocommit=True)


def query_data(ti):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    result = pg_hook.get_records("""
        SELECT * FROM test_table_2;
    """)
    ti.xcom_push(key='query_result', value=result)


def print_result(ti):
    task_instance = ti.xcom_pull(task_ids='query_data', key='query_result')
    print("Resultado da consulta:")
    for row in task_instance:
        print(row)


create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag,
)

query_data_task = PythonOperator(
    task_id='query_data',
    python_callable=query_data,
    provide_context=True,
    dag=dag,
)

print_result_task = PythonOperator(
    task_id='print_result',
    python_callable=print_result,
    provide_context=True,
    dag=dag,
)

create_table_task >> insert_data_task >> query_data_task >> print_result_task

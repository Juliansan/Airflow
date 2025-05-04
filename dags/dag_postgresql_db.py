from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


def print_result(ti):
    task_instance = ti.xcom_pull(task_ids='query_data')
    print("Resultado da consulta:")
    for row in task_instance:
        print(row)


dag = DAG(
    dag_id='dag_postgresql_db',
    description='Dag testando banco de dados postgresql',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False
)

create_table = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='postgres',
    sql="""
        CREATE TABLE IF NOT EXISTS test_table (
            id integer
        );
    """,
    dag=dag,
)

insert_data = SQLExecuteQueryOperator(
    task_id='insert_data',
    conn_id='postgres',
    sql="""
        INSERT INTO test_table (id) VALUES (1);
    """,
    dag=dag,
)

query_data = SQLExecuteQueryOperator(
    task_id='query_data',
    conn_id='postgres',
    sql="""
        SELECT * FROM test_table;
    """,
    dag=dag,
)

print_result_task = PythonOperator(
    task_id='print_result',
    python_callable=print_result,
    provide_context=True,
    dag=dag,
)


create_table >> insert_data >> query_data >> print_result_task

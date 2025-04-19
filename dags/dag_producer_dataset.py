from airflow import DAG
from airflow import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


dag = DAG(
    dag_id='dag_producer_dataset',
    description='DAG to produce a dataset',
    schedule_interval=None,
    start_date=datetime(2023, 10, 1),
    catchup=False)

mydataset = Dataset("/opt/airflow/data/Churn_new.csv")


def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=';')
    dataset.to_csv("/opt/airflow/data/Churn_new.csv", sep=';')


t1 = PythonOperator(
    task_id='my_file',
    python_callable=my_file,
    dag=dag,
    outlets=[mydataset]
)

t1

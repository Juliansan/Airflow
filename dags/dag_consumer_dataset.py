from airflow import DAG
from airflow import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

dag = DAG(
    dag_id='dag_consumer_dataset',
    description='DAG to consume a dataset',
    schedule=[mydataset],
    start_date=datetime(2023, 10, 1),
    catchup=False)


def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=';')
    dataset.to_csv("/opt/airflow/data/Churn_new2.csv", sep=';')


t1 = PythonOperator(
    task_id='my_file',
    python_callable=my_file,
    dag=dag,
    provide_context=True,
)

t1

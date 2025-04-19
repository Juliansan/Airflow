from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import statistics as sts


dag = DAG(
    dag_id='dag_dataset',
    description='Dag testando datasets',
    schedule_interval=None,
    start_date=datetime(2025, 4, 2),
    catchup=False
)


def data_cleansing():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.columns = ["ID", "Score", "Estado", "Genero", "Idade", "Patrimonio",
                       "Saldo", "Produtos", "TemCartCredito", "Ativo", "Salario", "Saiu"]

    mediana = sts.median(dataset["Salario"])
    dataset["Salario"].fillna(mediana, inplace=True)

    dataset["Genero"].fillna("Masculino", inplace=True)
    mediana = sts.median(dataset["Idade"])
    dataset.loc[(dataset["Idade"] < 0) | (
        dataset["Idade"] > 120), "Idade"] = mediana

    dataset.drop_duplicates(subset=["ID"], keep="first", inplace=True)

    dataset.to_csv('/opt/airflow/data/Churn_clean.csv', sep=';', index=False)


data_set = PythonOperator(
    task_id='data_set',
    python_callable=data_cleansing,
    dag=dag,
)

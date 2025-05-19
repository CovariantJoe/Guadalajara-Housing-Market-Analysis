"""
@author: Covariant Joe

Apache Airflow pipeline to perform the ETL process and populate an IBM db2 database with historic and new housing market data in Guadalajara automatically.
Using Airflow allows the user to automatically perform this process every certain time, and monitor pipeline latency and throughput in order to optimize it.

Credentials need to be provided in Credentials.txt to connect remotely to a IBM db2 database instance. In order to run this program ibm_db needs to be installed.

"""

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from ETL import *

# Define from which URLs to extract data. Default is from Url1 to Url68 ( range(1,69) ) inside ETL.py
Urls = [ globals()[f"Url{i}"] for i in range(1,69) ]
# Path to IBM db2 credentials. Default uses the value in the variable Credentials defined inside ETL.py
Credentials = Credentials

default_args = {
    'owner': 'CovariantJoe',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Guadalajara-Market-ETL',
    default_args=default_args,
    description='ETL Pipeline to populate an IBM db2 database with historic housing data in Guadalajara',
    schedule_interval=timedelta(days=1),
)

def AirflowTransformer(**kwargs):
    ti = kwargs['ti']  # TaskInstance object
    data = ti.xcom_pull(task_ids='extract')
    return Transformer(data)

def AirflowLoader(**kwargs):
    ti = kwargs['ti']  # TaskInstance object
    data = ti.xcom_pull(task_ids='transform')
    return Loader(data, Credentials)

# Define the task to perform the data extraction
execute_extract = PythonOperator(
    task_id='extract',
    python_callable=Extractor,
    op_args = [Urls],
    dag=dag,
)

# Define the task to perform the data transformation
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=AirflowTransformer,
    dag=dag,
)

# Define the task to load the data to IBM db2
execute_load = PythonOperator(
    task_id='load',
    python_callable=AirflowLoader,
    dag=dag,
)

log("[Info] - Beginning Airflow pipeline")
# Pipeline order
execute_extract >> execute_transform >> execute_load
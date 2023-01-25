from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCLoudStorageToBigQueryOperator

from airflow.utils.dates import days_ago
default_arguments={'owner':"Bob",'start_date': days_ago(1)}

with DAG('bigquery_data_load',schedule_interval='@hourly',catchup=False,default_args=default_arguments) as dag:
    load_data=GoogleCLoudStorageToBigQueryOperator(
        task_id='load_data',
        bucket='aa'
    )
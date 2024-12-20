from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def upload_to_s3():
    hook = S3Hook(aws_conn_id='aws_default')  # Airflow의 AWS Connection ID 사용
    data = "This is a test data"
    hook.load_string(string_data=data, key='test/data.txt', bucket_name='team5-s3')

with DAG(
    dag_id='s3_upload_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )

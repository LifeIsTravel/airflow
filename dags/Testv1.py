from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 기본 설정 st
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_start():
    print("Start")

def wait_five_seconds():
    import time
    time.sleep(5)
    print("Waited 5 seconds")

def print_end():
    print("Workflow Completed")

# DAG 정의
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),  # 매일 실행
    start_date=datetime(2024, 12, 20),
    catchup=False,  # 과거 실행 건 무시
)

# Task 정의
start_task = PythonOperator(
    task_id='start_task',
    python_callable=print_start,
    dag=dag,
)

wait_task = PythonOperator(
    task_id='wait_task',
    python_callable=wait_five_seconds,
    dag=dag,
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=print_end,
    dag=dag,
)

# Task 의존성 설정
start_task >> wait_task >> end_task

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_paths():
    print("=" * 50)
    print("System Path (sys.path):")
    for path in sys.path:
        print(f"- {path}")
    print("\nEnvironment Variables:")
    print(f"AIRFLOW_HOME: {os.getenv('AIRFLOW_HOME')}")
    print(f"PYTHONPATH: {os.getenv('PYTHONPATH')}")
    print("=" * 50)


with DAG(
        dag_id="path_test_DAG",
        description="경로 확인용 테스트 DAG",
        start_date=datetime(2023, 12, 1),
        schedule_interval="0 * * * *",  # 매시간마다 실행
        catchup=False,
) as dag:
    check_paths_task = PythonOperator(
        task_id='check_paths',
        python_callable=print_paths,
    )

    check_paths_task

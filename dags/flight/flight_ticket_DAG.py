from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common.logger import get_logger
from flight.utils.flight_ticket_etl import extract_task, transform, load_to_rds, load_to_snowflake

logger = get_logger(__name__)

# 목적지 공항 코드 정의
airports = {
    "FUK": "후쿠오카",
    # "KIX": "오사카/간사이",
    # "NRT": "도쿄/나리타",
    # "CTS": "삿포로",
}

with DAG(
        dag_id="flight_ticket_collection",
        description="항공권 데이터 수집 DAG",
        start_date=datetime(2023, 12, 1),
        schedule_interval="0 14 * * *",  # 매일 오후 2시마다 실행
        catchup=False,
) as dag:
    with TaskGroup(group_id="extract") as extract_group:
        extract_task_results = []
        for airport_code, airport_name in airports.items():
            task = PythonOperator(
                task_id=f'extract_{airport_code}',
                python_callable=extract_task,
                op_kwargs={'airport_code': airport_code, 'airport_name': airport_name}
            )
            extract_task_results.append(task)

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_kwargs={'extract_data': extract_task_results}
    )

    with TaskGroup(group_id="load") as load_group:
        load_rds_task = PythonOperator(
            task_id='load_to_rds',
            python_callable=load_to_rds,
            op_kwargs={
                's3_bucket': "team5-s3",
                'folder_path': "{{ task_instance.xcom_pull(task_ids='transform') }}"
            }
        )

        load_snowflake_task = PythonOperator(
            task_id='load_to_snowflake',
            python_callable=load_to_snowflake,
            op_kwargs={
                's3_bucket': "team5-s3",
                'folder_path': "{{ task_instance.xcom_pull(task_ids='transform') }}"
            }
        )

    extract_task_results >> transform_task >> load_group

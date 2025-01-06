import logging
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.flight_etl import create_airport_task, transform, load

# 로깅설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# 목적지 공항 코드 정의
airports = {
    "FUK": "후쿠오카",
    # "KIX": "오사카/간사이",
    # "NRT": "도쿄/나리타",
    # "CTS": "삿포로",
}

with DAG(
        dag_id="async_flight_data_collection",
        description="비동기로 일본 항공권 데이터 수집 DAG",
        start_date=datetime(2023, 12, 1),
        schedule_interval="0 * * * *",  # 매시간마다 실행
        catchup=False,
) as dag:
    execution_time = '{{ ts }}'  # Airflow에서 제공하는 execution_time 템플릿 변수로 사용

    with TaskGroup(group_id="extract") as airport_group:
        task_results = []
        for airport_code, airport_name in airports.items():
            task_results.append(create_airport_task(airport_code, airport_name)(execution_time))

    transform_data = transform(execution_time, task_results)
    load(execution_time, transform_data)

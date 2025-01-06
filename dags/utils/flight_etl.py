import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta

import pytz
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from .fetch_flight_data import fetch_flight_data

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from common.read_parquet_files_from_s3 import read_parquet_files_from_s3
from common.bulk_copy_to_snowflake import bulk_copy_to_snowflake

# 로깅설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_airport_task(airport_code, airport_name):
    @task(task_id=f"process_{airport_code}")
    def process_airport(execution_time):
        async def airport_main():
            execution_datetime = trans_to_kst(execution_time)

            # 날짜 계산 range(n) -> 앞으로 n일 계산
            dates = [(execution_datetime + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(2)]

            tasks = []
            for date in dates:
                # ICN -> Target 및 Target -> ICN 항공편 모두 추가
                tasks.append(fetch_flight_data(date, "ICN", airport_code, execution_datetime))
                # tasks.append(fetch_flight_data(date, airport_code, "ICN", execution_datetime))

            # 모든 비동기 작업 실행
            await asyncio.gather(*tasks)

        asyncio.run(airport_main())
        logging.info(f"{airport_name} ({airport_code}) 처리 완료")
        return f"{airport_name} ({airport_code}) 처리 완료"

    return process_airport


@task
def transform(execution_time, extract_data):
    logging.info(f"{extract_data} Transform 태스크 시작... (실행 시간: {execution_time})")

    execution_datetime = trans_to_kst(execution_time)

    # 날짜와 시간을 원하는 형식으로 추출
    year_str = f"{execution_datetime.year}"  # '2024' 형태
    month_str = f"{execution_datetime.month:02d}"  # '12' 형태
    day_str = f"{execution_datetime.day:02d}"  # '20' 형태
    time_str = execution_datetime.strftime("%H-%M")  # 14-00 형태

    # 폴더 경로 생성 (형식: raw_data/flights/년/월/일/시-분/)
    folder_path = f"raw_data/flights/{year_str}/{month_str}/{day_str}/{time_str}/"
    logging.info(f"폴더 경로: {folder_path}")

    # AWS Glue 작업 실행
    try:
        # Glue 작업에 전달할 인수 설정
        arguments = {
            '--folder_path': folder_path  # Glue 작업에 folder_path 인수 전달
        }

        glue_job = GlueJobOperator(
            task_id='run_glue_job',
            job_name='team5-glue-flight',  # Glue 작업 이름
            region_name='ap-northeast-2',
            # script_location='s3://your-bucket/your-script.py',  # Glue 스크립트 경로
            # aws_conn_id='aws_default',  # AWS 연결 ID (Airflow 연결 설정에 맞게 수정)
            script_args=arguments  # 인수 전달
        )

        # Glue 작업 실행
        glue_job.execute(context={})
        logging.info(f"AWS Glue Job 실행 시작.")
    except Exception as e:
        logging.error(f"AWS Glue 작업 실행 중 오류 발생: {e}")

    return "transform 완료!"


@task
def load(execution_time, transform_data):
    logging.info(f"{transform_data} Load 태스크 시작... (실행 시간: {execution_time})")

    execution_datetime = trans_to_kst(execution_time)

    # 날짜와 시간을 원하는 형식으로 추출
    year_str = f"{execution_datetime.year}"  # '2024' 형태
    month_str = f"{execution_datetime.month:02d}"  # '12' 형태
    day_str = f"{execution_datetime.day:02d}"  # '20' 형태
    time_str = execution_datetime.strftime("%H-%M")  # 14-00 형태

    # 폴더 경로 생성 (형식: raw_data/flights/년/월/일/시-분/)
    folder_path = f"transform_data/flights/{year_str}/{month_str}/{day_str}/{time_str}/"
    logging.info(f"폴더 경로: {folder_path}")

    s3_bucket = "team5-s3"  # S3 버킷 이름
    parquet_file = read_parquet_files_from_s3(s3_bucket, folder_path)

    # Snowflake 테이블에 데이터 BULK COPY (Upsert 방식)
    if parquet_file:
        query_path = 'dags/sql/flight.sql'  # SQL 파일 경로
        bulk_copy_to_snowflake(parquet_file, query_path)
        logging.info(f"Copied {len(parquet_file)} files into Snowflake.")
    else:
        logging.info("No parquet files found to process.")


def trans_to_kst(execution_time):
    # Airflow가 돌린 경우 -> '%Y-%m-%dT%H:%M:%S%z' / 직접 돌린 경우 -> '%Y-%m-%dT%H:%M:%S.%f%z'
    if '.' in execution_time:
        start_date = datetime.strptime(execution_time, '%Y-%m-%dT%H:%M:%S.%f%z')
    else:
        start_date = datetime.strptime(execution_time, '%Y-%m-%dT%H:%M:%S%z')
        start_date += timedelta(hours=1)
    kst = pytz.timezone('Asia/Seoul')
    start_date_kst = start_date.astimezone(kst)
    logging.info(f"한국 시간: {start_date_kst}")
    return start_date_kst

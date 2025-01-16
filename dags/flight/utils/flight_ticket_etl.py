import asyncio

from common.aws_utils import start_glue_job, read_files_from_s3
from common.logger import get_logger
from flight.utils.bulk_copy import bulk_copy_to_snowflake, bulk_copy_to_rds
from flight.utils.date_calculator import trans_to_kst, get_dates_in_range, extract_date_time
from flight.utils.fetch_flight_data import fetch_flight_data

logger = get_logger(__name__)


def extract_task(airport_code: str, airport_name: str, **context):
    """
    Creates a task to extract flight data for a specific airport.

    Args:
        airport_code (str): The airport code (e.g., "ICN").
        airport_name (str): The name of the airport.
        context: Airflow context containing execution information.
    """

    async def airport_main():
        execution_datetime = trans_to_kst(execution_time)
        dates = get_dates_in_range(execution_datetime, num_days=30)  # 날짜 계산: 앞으로의 num_days 계산

        tasks = []  # ICN -> Target 및 Target -> ICN 항공편 모두 추가
        for date in dates:
            tasks.append(fetch_flight_data(date, "ICN", airport_code, execution_datetime))
            tasks.append(fetch_flight_data(date, airport_code, "ICN", execution_datetime))

        await asyncio.gather(*tasks)  # 모든 비동기 작업 실행

    execution_time = context['ts']
    asyncio.run(airport_main())
    logger.info(f"{airport_name} ({airport_code}) 처리 완료")
    return f"{airport_name} ({airport_code}) 처리 완료"


def transform(extract_data: str, **context) -> str:
    """
    Transforms the extracted data by running an AWS Glue job.

    Args:
        extract_data (str): The data extracted from the previous task.
        context: Airflow context containing execution information.
    """
    execution_time = context['ts']
    logger.info(f"{extract_data} Transform 태스크 시작... (실행 시간: {execution_time})")

    execution_datetime = trans_to_kst(execution_time)
    path_date = extract_date_time(execution_datetime)

    # 폴더 경로 생성 (형식: raw_data/flights/년/월/일/시-분/)
    folder_path = f"raw_data/flights/{path_date}/"
    arguments = {'--folder_path': folder_path}
    logger.info(f"폴더 경로: {folder_path}, Glue 전달할 파라미터: {arguments}")

    # AWS Glue 작업 실행
    try:
        start_glue_job(
            task_id='run_glue_job',  # Airflow task ID
            job_name='team5-glue-flight-naver',  # Glue 작업 이름
            arguments=arguments  # 전달할 인수
        )
        logger.info(f"AWS Glue Job 실행 시작.")
    except Exception as e:
        logger.error(f"AWS Glue 작업 실행 중 오류 발생: {e}")

    return "transform 완료!"


def load_to_rds(**context) -> None:
    """
    Loads data into RDS from S3 CSV files.

    Args:
        context: Airflow context containing execution information.
    """
    execution_time = context['ts']
    execution_datetime = trans_to_kst(execution_time)
    path_date = extract_date_time(execution_datetime)

    s3_bucket = "team5-s3"
    folder_path = f"transform_data/flights/{path_date}/"

    csv_files = read_files_from_s3(s3_bucket, folder_path, 'csv')
    if csv_files:
        bulk_copy_to_rds(csv_files)
        logger.info(f"CSV 파일 {len(csv_files)}개가 RDS에 성공적으로 로드되었습니다.")
    else:
        logger.info("처리할 CSV 파일이 없습니다.")


def load_to_snowflake(**context) -> None:
    """
    Loads data into Snowflake from S3 Parquet files.

    Args:
        context: Airflow context containing execution information.
    """
    execution_time = context['ts']
    execution_datetime = trans_to_kst(execution_time)
    path_date = extract_date_time(execution_datetime)

    s3_bucket = "team5-s3"
    folder_path = f"transform_data/flights/{path_date}/"

    parquet_files = read_files_from_s3(s3_bucket, folder_path, 'parquet')
    if parquet_files:
        bulk_copy_to_snowflake(parquet_files)
        logger.info(f"Parquet 파일 {len(parquet_files)}개가 Snowflake에 성공적으로 로드되었습니다.")
    else:
        logger.info("처리할 Parquet 파일이 없습니다.")

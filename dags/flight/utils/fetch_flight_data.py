import asyncio
from datetime import datetime

from common.api_utils import get_flight_data_naver
from common.aws_utils import upload_json_to_s3, start_lambda_function
from common.logger import get_logger
from flight.utils.date_calculator import extract_date_time

logger = get_logger(__name__)


async def fetch_flight_data(date: str, origin: str, target: str, execution_datetime: datetime) -> None:
    """
    Sends an asynchronous request to the Apify Actor for flight data and returns the results.

    Args:
        date (str): The date of the flight search in 'YYYYMMDD' format.
        origin (str): The origin airport code.
        target (str): The destination airport code.
        execution_datetime (datetime): The execution date and time used for path calculation.
    """
    semaphore = asyncio.Semaphore(6)  # 동시에 실행되는 비동기 작업을 6개로 제한
    async with semaphore:
        logger.info(f"{origin} -> {target}의 {date} 비행기 데이터를 가져오는 중...")

        path_date = extract_date_time(execution_datetime)

        # results = await get_flight_data_apify(date, origin, target) # Apify에서 비행기 데이터를 가져옴
        results = await get_flight_data_naver(date, origin, target)  # Naver에서 비행기 데이터를 가져옴
        s3_bucket = "team5-s3"

        # 결과가 있으면 JSON으로 저장
        if results["data"]["internationalList"]["resCnt"] > 0:
            logger.info(f"{origin} -> {target}의 {date} 데이터가 있으면 JSON 파일로 저장 중...")
            s3_key = f"raw_data/flights/{path_date}/{date}_{origin}_to_{target}.json"
            upload_json_to_s3(results, s3_bucket, s3_key)

        else:
            logger.warning(f"{origin} -> {target}의 {date} 비행기 데이터가 없으므로 빈 JSON 파일을 S3에 업로드합니다.")
            s3_key = f"raw_data/flights/{path_date}/{date}_{origin}_to_{target}_empty.json"
            upload_json_to_s3([], s3_bucket, s3_key)


def fetch_flight_date_lambda(date: str, origin: str, target: str, execution_datetime: datetime) -> None:
    """
    Prepares and sends payload with flight search parameters to Lambda function.

    Args:
        date (str): The date of the flight search in 'YYYYMMDD' format.
        origin (str): The origin airport code.
        target (str): The destination airport code.
        execution_datetime (datetime): The execution date and time used for path calculation.
    """
    logger.info(f"{origin} -> {target}의 {date} 비행기 데이터를 가져오는 중...")
    path_date = extract_date_time(execution_datetime)
    s3_key = f"raw_data/flights/{path_date}/{date}_{origin}_to_{target}.json"

    payload = f'''{{
        "s3_key": "{s3_key}",
        "date": "{date}",
        "origin": "{origin}",
        "target": "{target}"
    }}'''

    start_lambda_function(
        function_name="Team5-flight-naver",
        payload=payload
    )

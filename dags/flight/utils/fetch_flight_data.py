import asyncio
from datetime import datetime

from common.api_utils import get_flight_data
from common.aws_utils import upload_json_to_s3
from common.logger import get_logger
from flight.utils.date_calculator import extract_date_time

logger = get_logger(__name__)

semaphore = asyncio.Semaphore(6)  # 동시에 실행되는 비동기 작업을 6개로 제한


async def fetch_flight_data(date: str, origin: str, target: str, execution_datetime: datetime) -> None:
    """
    Sends an asynchronous request to the Apify Actor for flight data and returns the results.

    Args:
        date (str): The date of the flight search.
        origin (str): The origin airport code.
        target (str): The destination airport code.
        execution_datetime (datetime): The execution date and time used for path calculation.
    """
    async with semaphore:
        logger.info(f"{origin} -> {target}의 {date} 비행기 데이터를 가져오는 중...")

        path_date = extract_date_time(execution_datetime)

        # Apify에서 비행기 데이터를 가져옴
        results = await get_flight_data(date, origin, target)
        s3_bucket = "team5-s3"

        # 결과가 있으면 JSON으로 저장
        if results:
            logger.info(f"{origin} -> {target}의 {date} 데이터가 있으면 JSON 파일로 저장 중...")
            s3_key = f"raw_data/flights/{path_date}/{date}_{origin}_to_{target}.json"
            upload_json_to_s3(results, s3_bucket, s3_key)

        else:
            logger.warning(f"{origin} -> {target}의 {date} 비행기 데이터가 없으므로 빈 JSON 파일을 S3에 업로드합니다.")
            s3_key = f"raw_data/flights/{path_date}/{date}_{origin}_to_{target}_empty.json"
            upload_json_to_s3([], s3_bucket, s3_key)

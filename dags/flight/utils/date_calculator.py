from datetime import datetime, timedelta
from typing import List

import pytz
from common.logger import get_logger

logger = get_logger(__name__)


def trans_to_kst(execution_time: str) -> datetime:
    """
    Converts the given execution time to Korea Standard Time (KST).

    Args:
        execution_time: A string representing the execution time in ISO 8601 format.

    Returns:
        datetime: The execution time converted to Korea Standard Time (KST).
    """

    # if: 직접 돌린 경우 -> '%Y-%m-%dT%H:%M:%S.%f%z' / else: Airflow가 돌린 경우 -> '%Y-%m-%dT%H:%M:%S%z'
    if '.' in execution_time:
        start_date = datetime.strptime(execution_time, '%Y-%m-%dT%H:%M:%S.%f%z')
    else:
        start_date = datetime.strptime(execution_time, '%Y-%m-%dT%H:%M:%S%z')
        start_date += timedelta(days=1)

    kst = pytz.timezone('Asia/Seoul')
    start_date_kst = start_date.astimezone(kst)

    logger.info(f"한국 시간: {start_date_kst}")  # 로그: 한국 시간 출력
    return start_date_kst


def get_dates_in_range(start_date: datetime, num_days: int = 2) -> List[str]:
    """
    Returns a list of dates starting from the given start date for a specified number of days.

    Args:
        start_date: The starting date for the range.
        num_days: The number of days for which to generate dates (default is 2).

    Returns:
        List: A list of dates in the range.
    """
    logger.info(f"{num_days}일 간의 날짜 범위를 계산 중입니다.")  # 로그: 날짜 범위 계산 시작
    # dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(num_days)]  # apify
    dates = [(start_date + timedelta(days=i)).strftime("%Y%m%d") for i in range(num_days)]  # naver
    logger.info(f"계산된 날짜 범위: {dates[0]} ~ {dates[-1]}")  # 로그: 계산된 날짜 범위 출력
    return dates


def extract_date_time(execution_datetime: datetime) -> str:
    """
    Extracts year, month, day, and time from the given datetime object and formats them into strings.

    Args:
        execution_datetime: A datetime object representing the execution time.

    Returns:
        str: A formatted string combining year, month, day, and time.
    """
    # 날짜와 시간을 원하는 형식으로 추출
    year_str = f"{execution_datetime.year}"  # '2024' 형태
    month_str = f"{execution_datetime.month:02d}"  # '12' 형태
    day_str = f"{execution_datetime.day:02d}"  # '20' 형태
    time_str = execution_datetime.strftime("%H-%M")  # 14-00 형태

    formatted_str = f"{year_str}/{month_str}/{day_str}/{time_str}"

    logger.info(f"형식화된 날짜 및 시간: {formatted_str}")  # 로그: 형식화된 날짜 및 시간 출력
    return formatted_str

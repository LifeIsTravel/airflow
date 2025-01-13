from typing import Dict

from airflow.models import Variable
from apify_client.client import ApifyClientAsync
from common.logger import get_logger

logger = get_logger(__name__)

client = ApifyClientAsync(Variable.get("flight_api2_secret"))


async def get_flight_data(
        date: str,
        origin: str,
        target: str
) -> Dict:
    """
    Sends an asynchronous request to the Apify Actor for flight data and returns the results.

    Args:
        date: The date of the flight search.
        origin: The origin airport code.
        target: The destination airport code.

    Returns:
        Dict: The flight data results from the Apify Actor.
    """
    run_input = {
        "market": "KR",
        "currency": "KRW",
        "depart.0": date,
        "origin.0": origin,
        "target.0": target,
        "cabin_class": "economy",
        "alternate_origin": False,
        "alternate_target": False,
        "dev_dataset_clear": False,
        "dev_no_strip": False,
        "non_stop": False,
        "one_stop": False,
        "two_stop": False,
        "adults": 1,
    }

    logger.info(f"{origin} -> {target}의 {date} Apify Actor 실행 중...")

    try:
        # Actor 실행
        run = await client.actor("jupri/skyscanner-flight").call(run_input=run_input)

        # Actor 결과 가져오기
        results = []
        async for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)

        logger.info(f"{origin} -> {target}의 {date} Apify Actor 결과 가져오기 완료.")
        return results
    except Exception as e:
        logger.error(f"Apify Actor 실행 중 오류 발생: {str(e)}")
        raise

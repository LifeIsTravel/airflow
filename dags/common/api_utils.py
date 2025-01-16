import asyncio
import random
import time
from typing import Dict

import requests
from airflow.models import Variable
from apify_client.client import ApifyClientAsync
from common.logger import get_logger

logger = get_logger(__name__)

client = ApifyClientAsync(Variable.get("flight_api2_secret"))


async def get_flight_data_apify(
        date: str,
        origin: str,
        target: str
) -> Dict:
    """
    Sends an asynchronous request to the Apify Actor for flight data and returns the results.

    Args:
        date: The date of the flight search. (ex: %Y-%m-%d)
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


async def get_flight_data_naver(
        date: str,
        origin: str,
        target: str,
        retry_count=3
) -> Dict:
    """
        Sends an asynchronous request to the Naver Flight for flight data and returns the specified data.
        1. Initial request to generate required keys (galileoKey and travelBizKey).
        2. Second request with the keys to fetch the detailed flight data.

        Args:
            date: The date of the flight search. (ex: %Y%m%d)
            origin: The origin airport code.
            target: The destination airport code.
            retry_count (int, optional): The number of retries allowed if no results are found. Default is 3.

        Returns:
            Dict: The flight data results from the Naver Flight.
        """
    url = "https://airline-api.naver.com/graphql"
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ko,en-US;q=0.9,en;q=0.8,ko-KR;q=0.7",
        "content-type": "application/json",
        "origin": "https://flight.naver.com",
        "referer": f"https://flight.naver.com/flights/international/{origin}-{target}-{date}?adult=1&fareType=Y",
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
    }

    payload1 = {
        "operationName": "getInternationalList",
        "query": "query getInternationalList($trip: InternationalList_TripType!, $itinerary: [InternationalList_itinerary]!, $adult: Int = 1, $child: Int = 0, $infant: Int = 0, $fareType: InternationalList_CabinClass!, $where: InternationalList_DeviceType = pc, $isDirect: Boolean = false, $stayLength: String, $galileoKey: String, $galileoFlag: Boolean = true, $travelBizKey: String, $travelBizFlag: Boolean = true) {\n  internationalList(\n    input: {trip: $trip, itinerary: $itinerary, person: {adult: $adult, child: $child, infant: $infant}, fareType: $fareType, where: $where, isDirect: $isDirect, stayLength: $stayLength, galileoKey: $galileoKey, galileoFlag: $galileoFlag, travelBizKey: $travelBizKey, travelBizFlag: $travelBizFlag}\n  ) {\n    galileoKey\n    galileoFlag\n    travelBizKey\n    travelBizFlag\n    totalResCnt\n    resCnt\n    results {\n      airlines\n      airports\n      fareTypes\n      schedules\n      fares\n      errors\n      carbonEmissionAverage {\n        directFlightCarbonEmissionItineraryAverage\n        directFlightCarbonEmissionAverage\n      }\n    }\n  }\n}",
        "variables": {
            "adult": 1,
            "child": 0,
            "fareType": "Y",
            "galileoFlag": True,
            "galileoKey": "",
            "infant": 0,
            "isDirect": True,
            "itinerary": [
                {
                    "arrivalAirport": f"{target}",
                    "departureAirport": f"{origin}",
                    "departureDate": f"{date}",
                }
            ],
            "stayLength": "",
            "travelBizFlag": True,
            "travelBizKey": "",
            "trip": "OW",
            "where": "pc",
        },
    }

    payload2 = {
        "operationName": "getInternationalList",
        "query": "query getInternationalList($trip: InternationalList_TripType!, $itinerary: [InternationalList_itinerary]!, $adult: Int = 1, $child: Int = 0, $infant: Int = 0, $fareType: InternationalList_CabinClass!, $where: InternationalList_DeviceType = pc, $isDirect: Boolean = false, $stayLength: String, $galileoKey: String, $galileoFlag: Boolean = true, $travelBizKey: String, $travelBizFlag: Boolean = true) {\n  internationalList(\n    input: {trip: $trip, itinerary: $itinerary, person: {adult: $adult, child: $child, infant: $infant}, fareType: $fareType, where: $where, isDirect: $isDirect, stayLength: $stayLength, galileoKey: $galileoKey, galileoFlag: $galileoFlag, travelBizKey: $travelBizKey, travelBizFlag: $travelBizFlag}\n  ) {\n    galileoKey\n    galileoFlag\n    travelBizKey\n    travelBizFlag\n    totalResCnt\n    resCnt\n    results {\n      airlines\n      airports\n      fareTypes\n      schedules\n      fares\n      errors\n      carbonEmissionAverage {\n        directFlightCarbonEmissionItineraryAverage\n        directFlightCarbonEmissionAverage\n      }\n    }\n  }\n}",
        "variables": {
            "adult": 1,
            "child": 0,
            "fareType": "Y",
            "galileoFlag": False,
            "galileoKey": "",
            "infant": 0,
            "isDirect": True,
            "itinerary": [
                {
                    "arrivalAirport": f"{target}",
                    "departureAirport": f"{origin}",
                    "departureDate": f"{date}",
                }
            ],
            "stayLength": "",
            "travelBizFlag": False,
            "travelBizKey": "",
            "trip": "OW",
            "where": "pc",
        },
    }

    try:
        time.sleep(random.uniform(4, 5))
        logger.info(f"{origin} -> {target}의 {date} Naver Flight 1번째 요청 보내는 중...")
        response = requests.post(url, json=payload1, headers=headers)
        logger.info(f"{origin} -> {target}의 {date} 의 1번째 status code: {response.status_code}")
        response.raise_for_status()
        response_data = response.json()

        travel_biz_key = response_data["data"]["internationalList"]["travelBizKey"]
        galileo_key = response_data["data"]["internationalList"]["galileoKey"]

        await asyncio.sleep(random.uniform(10, 13))

        payload2["variables"].update({
            "galileoFlag": bool(galileo_key),
            "galileoKey": galileo_key,
            "travelBizFlag": bool(travel_biz_key),
            "travelBizKey": travel_biz_key,
        })

        time.sleep(random.uniform(3, 4))
        logger.info(f"{origin} -> {target}의 {date} Naver Flight 2번째 요청 보내는 중...")
        response2 = requests.post(url, json=payload2, headers=headers)
        logger.info(f"{origin} -> {target}의 {date} 의 2번째 status code: {response2.status_code}")
        response2.raise_for_status()
        response_data2 = response2.json()

        if response_data2["data"]["internationalList"]["resCnt"] == 0 and retry_count > 0:
            return await get_flight_data_naver(origin, target, date, retry_count - 1)

        logger.info(f"{origin} -> {target}의 {date} Naver Flight 결과 가져오기 완료.")
        return response_data2

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request error occurred: {req_err}")
        raise
    except Exception as e:
        logger.error(f"Naver Flight 실행 중 오류 발생: {str(e)}")
        raise

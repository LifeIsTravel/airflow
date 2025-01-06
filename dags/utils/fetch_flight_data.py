import asyncio
import logging
import os
import sys

from airflow.models import Variable
from apify_client.client import ApifyClientAsync

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from common.upload_json_to_s3 import upload_json_to_s3

# 로깅설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ApifyClient 초기화
api_key = Variable.get("flight_api2_secret")
client = ApifyClientAsync(api_key)

semaphore = asyncio.Semaphore(6)  # 동시에 실행되는 비동기 작업을 6개로 제한


# 비행기 데이터를 비동기적으로 가져오는 함수
async def fetch_flight_data(date, origin, target, execution_datetime):
    async with semaphore:  # Semaphore로 제한된 구간
        logging.info(f"{origin} -> {target}의 {date} 비행기 데이터를 가져오는 중...")

        # 날짜와 시간을 원하는 형식으로 추출
        year_str = f"{execution_datetime.year}"  # '2024' 형태
        month_str = f"{execution_datetime.month:02d}"  # '12' 형태
        day_str = f"{execution_datetime.day:02d}"  # '20' 형태
        time_str = execution_datetime.strftime("%H-%M")  # 14-00 형태

        # 출발지와 목적지가 같으면 처리하지 않음
        if origin == target:
            return

        # 현재 날짜와 공항에 대한 Actor 입력 준비
        run_input = {
            "market": "KR",
            "currency": "KRW",
            "depart.0": date,
            "origin.0": origin,  # 출발 공항
            "target.0": target,  # 목적지 공항
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

        logging.info(f"{origin} -> {target}의 {date} Apify Actor 실행 중...")
        # Actor를 실행하고 완료될 때까지 기다림
        run = await client.actor("jupri/skyscanner-flight").call(run_input=run_input)

        # Actor의 결과 가져오기
        results = []
        async for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)

        logging.info(f"{origin} -> {target}의 {date} Apify Actor 결과 가져오기 완료.")

        # 비동기 스레드에서 동기 API 호출 실행
        # results = await asyncio.to_thread(
        #     fetch_flight_data_api, run_input, date, origin, target
        # )
        # results = await fetch_flight_data_api(run_input, date, origin, target)  # 직접 호출

        # 결과가 있으면 JSON으로 저장
        if results:
            logging.info(f"{origin} -> {target}의 {date} 데이터가 있으면 JSON 파일로 저장 중...")
            # S3에 JSON 데이터 업로드
            s3_bucket = "team5-s3"  # S3 버킷 이름
            s3_key = f"raw_data/flights/{year_str}/{month_str}/{day_str}/{time_str}/{date}_{origin}_to_{target}.json"  # S3 객체 키
            upload_json_to_s3(results, s3_bucket, s3_key)

        else:
            # 빈 JSON 리스트 업로드
            logging.warning(f"{origin} -> {target}의 {date} 비행기 데이터가 없으므로 빈 JSON 파일을 S3에 업로드합니다.")
            s3_bucket = "team5-s3"  # S3 버킷 이름
            s3_key = f"raw_data/flights/{year_str}/{month_str}/{day_str}/{time_str}/{date}_{origin}_to_{target}_empty.json"
            upload_json_to_s3([], s3_bucket, s3_key)

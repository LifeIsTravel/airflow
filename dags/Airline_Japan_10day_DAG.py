import asyncio
import json
import logging
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from apify_client import ApifyClient

# 로깅설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
api_key = Variable.get("flight_api2_secret")
# ApifyClient 초기화
client = ApifyClient(api_key)

# 목적지 공항 코드 정의
airports = {
    "FUK": "후쿠오카",
    # "HKG": "홍콩",
    # "KIX": "오사카/간사이",
    # "PVG": "상하이/푸동",
    # "NRT": "도쿄/나리타",
    # "BKK": "방콕/수완나품",
    # "SEA": "시애틀",
    # "NGO": "나고야",
    # "TAO": "칭다오",
    # "SIN": "싱가포르",
    # "CTS": "삿포로",
}


# S3에 JSON 데이터를 문자열로 업로드하는 함수
def upload_json_to_s3(json_data, s3_bucket, s3_key):
    # S3Hook을 사용하여 S3와 연결
    s3_hook = S3Hook(aws_conn_id='aws_default')  # aws_default 연결 아이디

    try:
        # JSON 데이터를 문자열로 변환
        json_string = json.dumps(json_data, ensure_ascii=False, indent=4)

        # S3에 문자열 업로드
        s3_hook.load_string(
            string_data=json_string,
            bucket_name=s3_bucket,
            key=s3_key,
            replace=True  # 이미 존재하는 파일을 덮어쓸지 여부
        )
        logging.info(f"JSON 파일이 S3 버킷 {s3_bucket}에 {s3_key}로 업로드되었습니다.")
    except Exception as e:
        logging.error(f"S3에 JSON 파일 업로드 실패: {e}")


# Apify에서 비행기 데이터를 동기적으로 가져오는 함수
def fetch_flight_data_sync(run_input, date, origin, target):
    logging.info(f"{origin} -> {target}의 {date} Apify Actor 실행 중...")
    # Actor를 실행하고 완료될 때까지 기다림
    run = client.actor("jupri/skyscanner-flight").call(run_input=run_input)

    # Actor의 결과 가져오기
    results = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append(item)

    logging.info(f"{origin} -> {target}의 {date} Apify Actor 결과 가져오기 완료.")
    return results


# 비행기 데이터를 비동기적으로 가져오는 함수
async def fetch_flight_data(date, origin, target, execution_datetime):
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

    # 비동기 스레드에서 동기 API 호출 실행
    results = await asyncio.to_thread(
        fetch_flight_data_sync, run_input, date, origin, target
    )

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


# execution_time 이미 datetime 객체일 경우
async def main(execution_time):
    logging.info(f"비행기 데이터 처리 시작... (실행 시간: {execution_time})")

    execution_datetime = trans_to_kst(execution_time)

    # 날짜 계산
    dates = [(execution_datetime + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1)]

    # 각 날짜에 대해 작업을 수행
    tasks = []
    for date in dates:
        logging.info(f"{date}에 대해 데이터 수집 시작...")
        # (ICN -> 목적지)에 대해 처리
        for target, _ in airports.items():
            tasks.append(fetch_flight_data(date, "ICN", target, execution_datetime))

        # (출발지 -> ICN)에 대해 처리
        for origin, _ in airports.items():
            tasks.append(fetch_flight_data(date, origin, "ICN", execution_datetime))

    # 모든 작업을 동시에 실행
    await asyncio.gather(*tasks)
    logging.info("모든 비행기 데이터 수집 작업 완료!")


# @task를 사용하여 Airflow 태스크로 등록
@task
def extract(execution_time):
    asyncio.run(main(execution_time))  # execution_time main 함수로 전달
    return "extract 완료!"


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

    # Glue 작업에 전달할 인수 설정
    arguments = {
        '--folder_path': folder_path  # Glue 작업에 folder_path 인수 전달
    }

    # AWS Glue 작업 실행
    try:
        glue_job = GlueJobOperator(
            task_id='run_glue_job',
            job_name='team5-glue-test',  # Glue 작업 이름
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


with DAG(
        dag_id="async_flight_data_collection",
        description="비동기로 일본 항공권 데이터 수집 DAG",
        start_date=datetime(2023, 12, 1),
        schedule_interval="0 * * * *",  # 매시간마다 실행
        catchup=False,
) as dag:
    execution_time = '{{ ts }}'  # Airflow에서 제공하는 execution_time 템플릿 변수로 사용
    extract_data = extract(execution_time)  # extract 태스크 실행
    transform(execution_time, extract_data)  # transform 태스크 실행

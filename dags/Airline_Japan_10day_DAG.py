import asyncio
import io
import json
import logging
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from apify_client import ApifyClient

# 로깅 설정
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
    "KIX": "오사카/간사이",
    # "PVG": "상하이/푸동",
    "NRT": "도쿄/나리타",
    # "BKK": "방콕/수완나품",
    # "SEA": "시애틀",
    # "NGO": "나고야",
    # "TAO": "칭다오",
    # "SIN": "싱가포르",
    "CTS": "삿포로",
}


# S3에 바로 업로드하는 함수
def upload_to_s3(df, s3_bucket, s3_key):
    # S3Hook을 사용하여 S3와 연결
    s3_hook = S3Hook(aws_conn_id='aws_default')  # aws_default 연결 아이디

    try:
        # S3에 직접 업로드하기 위해 DataFrame을 in-memory Parquet 파일로 변환
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        parquet_buffer.seek(0)  # 버퍼의 처음으로 이동

        # S3에 업로드
        s3_hook.load_file_obj(
            file_obj=parquet_buffer,
            bucket_name=s3_bucket,
            key=s3_key,
            replace=True  # 이미 존재하는 파일을 덮어쓸지 여부
        )
        logging.info(f"파일이 S3 버킷 {s3_bucket}에 {s3_key}로 업로드되었습니다.")
    except Exception as e:
        logging.error(f"S3에 파일 업로드 실패: {e}")


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

    # 결과가 있으면 Parquet 파일로 저장
    if results:
        logging.info(f"{origin} -> {target}의 {date} 데이터가 있으면 파일로 저장 중...")
        # 결과를 DataFrame으로 변환
        df = pd.DataFrame(results)

        # 복잡한 유형(예: dict, list)의 컬럼을 JSON 문자열로 변환
        for col in df.columns:
            if isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)

        # 추출한 날짜와 시간을 하나의 컬럼으로 추가
        extracted_at_str = f"{year_str}{month_str}{day_str}{execution_datetime.strftime('%H%M')}"
        df['extracted_at'] = extracted_at_str

        # S3에 바로 업로드
        s3_bucket = "team5-s3"  # S3 버킷 이름
        s3_key = f"flights/{year_str}/{month_str}/{day_str}/{time_str}/{date}_{origin}_to_{target}.parquet"  # S3 객체 키
        upload_to_s3(df, s3_bucket, s3_key)

    else:
        # 빈 DataFrame 생성
        empty_df = pd.DataFrame()

        # S3에 바로 빈 파일 업로드
        s3_bucket = "team5-s3"  # S3 버킷 이름
        s3_key = f"flights/{year_str}/{month_str}/{day_str}/{time_str}/{date}_{origin}_to_{target}_empty.parquet"
        upload_to_s3(empty_df, s3_bucket, s3_key)
        logging.warning(f"{origin} -> {target}의 {date} 비행기 데이터가 없으므로 빈 파일이 S3에 업로드되었습니다.")


# execution_date는 이미 datetime 객체일 경우
async def main(execution_date):
    logging.info(f"비행기 데이터 처리 시작... (실행 시간: {execution_date})")

    # execution_date를 한국 시간(KST)으로 변환
    start_date = datetime.strptime(execution_date, '%Y-%m-%dT%H:%M:%S.%f%z')  # '%Y-%m-%dT%H:%M:%S%z' 스케쥴러로 돌릴때는 .%f가 필요없음
    kst = pytz.timezone('Asia/Seoul')
    start_date_kst = start_date.astimezone(kst)

    logging.info(f"한국 시간: {start_date_kst}")

    # 날짜 계산
    dates = [(start_date_kst + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(10)]

    # 각 날짜에 대해 작업을 수행
    tasks = []
    for date in dates:
        logging.info(f"{date}에 대해 데이터 수집 시작...")
        # (ICN -> 목적지)에 대해 처리
        for target, _ in airports.items():
            tasks.append(fetch_flight_data(date, "ICN", target, start_date_kst))

        # (출발지 -> ICN)에 대해 처리
        for origin, _ in airports.items():
            tasks.append(fetch_flight_data(date, origin, "ICN", start_date_kst))

    # 모든 작업을 동시에 실행
    await asyncio.gather(*tasks)
    logging.info("모든 비행기 데이터 수집 작업 완료!")


# PythonOperator에서 호출할 함수
def run_async_tasks(execution_date):
    asyncio.run(main(execution_date))  # execution_date를 main 함수로 전달


with DAG(
        dag_id="async_flight_data_collection",
        description="비동기로 일본 항공권 데이터 수집 DAG",
        start_date=datetime(2023, 12, 1),
        schedule_interval="0 * * * *",  # 매시간마다 실행
        catchup=False,
) as dag:
    task = PythonOperator(
        task_id="collect_flight_data_task",
        python_callable=run_async_tasks,
        op_kwargs={'execution_date': '{{ ts }}'},  # Airflow에서 제공하는 execution_date를 전달
    )

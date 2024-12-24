import logging
import io
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pandas as pd
from datetime import datetime, timedelta

import openmeteo_requests
import requests_cache
from retry_requests import retry

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

airport = [
    {
        'name': "ICN",
        'latitude': 37.4602,
        'longitude': 126.4407,
        'timezone': "Asia/Seoul"
    },
    {
        'name': "NRT",
        'latitude': 35.7720,
        'longitude': 140.3929,
        'timezone': "Asia/Tokyo"
    },
    {
        'name': "KIX",
        'latitude': 34.4349,
        'longitude': 135.2448,
        'timezone': "Asia/Tokyo"
    },
    {
        'name': "FUK",
        'latitude': 33.5869,
        'longitude': 130.4517,
        'timezone': "Asia/Tokyo"
    },
    {
        'name': "CTS",
        'latitude': 42.7752,
        'longitude': 141.6923,
        'timezone': "Asia/Tokyo"
    }
]

# 과거 데이터
def fetch_weather_data(execution_date, airport):
    execution_datetime = datetime.fromisoformat(execution_date)
    year_str = f"{execution_datetime.year}"  # '2024' 형태
    month_str = f"{execution_datetime.month:02d}"  # '12' 형태
    day_str = f"{execution_datetime.day:02d}"  # '20' 형태
    time_str = execution_datetime.strftime("%H")  # '14' 형태
    extracted_at_str = f"{year_str}{month_str}{day_str}{time_str}"
    
    session = requests.Session()
    retry_session = retry(session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    hourly_params = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "rain", "snowfall", "snow_depth", "cloud_cover", "wind_speed_10m", "wind_speed_100m", "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m"]
    params = {
        "latitude": airport['latitude'],
        "longitude": airport['longitude'],
        "start_date": (execution_datetime - timedelta(days=2)).strftime("%Y-%m-%d"),
        "end_date": (execution_datetime - timedelta(days=2)).strftime("%Y-%m-%d"),
        "hourly": hourly_params,
        "timezone": airport['timezone']
    }

    response = openmeteo.weather_api(url, params=params)[0]
    hourly = response.Hourly()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "timestamp": [datetime.strptime(extracted_at_str, "%Y%m%d%H")] * len(hourly.Variables(0).ValuesAsNumpy())  # 동일한 실행 시간을 모든 행에 저장
    }

    for i in range(len(hourly_params)):
        hourly_data[hourly_params[i]] = hourly.Variables(i).ValuesAsNumpy()
    df = pd.DataFrame(hourly_data)

    s3_bucket = "team5-s3"  # S3 버킷 이름
    s3_key = f"raw_data/weather/{year_str}/{month_str}/{day_str}/{airport['name']}_past_{time_str.split('-')[0]}.csv"  # S3 객체 키
    upload_to_s3(df, s3_bucket, s3_key)

    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    retry_session.timeout = 30


# 미래 데이터
def fetch_forecast_data(execution_date, airport):
    execution_datetime = datetime.fromisoformat(execution_date)
    year_str = f"{execution_datetime.year}"  # '2024' 형태
    month_str = f"{execution_datetime.month:02d}"  # '12' 형태
    day_str = f"{execution_datetime.day:02d}"  # '20' 형태
    time_str = execution_datetime.strftime("%H") # '14' 형태
    extracted_at_str = f"{year_str}{month_str}{day_str}{time_str}"

    session = requests.Session()
    retry_session = retry(session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    hourly_params = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "rain", "snowfall", "snow_depth", "cloud_cover", "visibility", "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m"]
    params = {
        "latitude": airport['latitude'],
        "longitude": airport['longitude'],
        "forecast_days": 16,
        "past_days": 1,
        "hourly": hourly_params,
        "timezone": airport['timezone']
    }
    
    response = openmeteo.weather_api(url, params=params)[0]
    hourly = response.Hourly()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True).tz_convert(airport['timezone']),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True).tz_convert(airport['timezone']),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "timestamp": [datetime.strptime(extracted_at_str, "%Y%m%d%H")] * len(hourly.Variables(0).ValuesAsNumpy())
    }

    for i in range(len(hourly_params)):
        hourly_data[hourly_params[i]] = hourly.Variables(i).ValuesAsNumpy()
    
    df = pd.DataFrame(hourly_data)
    
    s3_bucket = "team5-s3"  # S3 버킷 이름
    s3_key = f"raw_data/weather/{year_str}/{month_str}/{day_str}/{airport['name']}_future_{time_str.split('-')[0]}.csv"  # S3 객체 키
    upload_to_s3(df, s3_bucket, s3_key)


# S3에 바로 업로드하는 함수
def upload_to_s3(df, s3_bucket, s3_key):
    # S3Hook을 사용하여 S3와 연결
    s3_hook = S3Hook(aws_conn_id='aws_default')  # aws_default 연결 아이디

    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # S3에 업로드
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),  # StringIO 내용을 string 형태로 가져옴
            bucket_name=s3_bucket,
            key=s3_key,
            replace=True  # 이미 존재하는 파일을 덮어쓸지 여부
        )
        logging.info(f"파일이 S3 버킷 {s3_bucket}에 {s3_key}로 업로드되었습니다.")
    except Exception as e:
        logging.error(f"S3에 파일 업로드 실패: {e}")


with DAG(
    dag_id="weather_past_data_collection",
    description="과거 날씨 데이터 수집 DAG",
    start_date=datetime(2023, 12, 1),
    schedule_interval="0 18 * * *",  # 매일 18:00에 실행
    catchup=False,
) as past_dag:
    for a in airport:
        weather_past_data = PythonOperator(
            task_id=f"weather_past_data_{a['name']}",
            python_callable=fetch_weather_data,
            op_kwargs={'execution_date': '{{ ts }}', 'airport': a},
        )

# 미래 날씨 데이터 수집 DAG
with DAG(
    dag_id="weather_future_data_collection",
    description="미래 날씨 데이터 수집 DAG",
    start_date=datetime(2023, 12, 1),
    schedule_interval="0 * * * *",  # 매 정각마다 실행
    catchup=False,
) as future_dag:
    for a in airport:
        weather_future_data = PythonOperator(
            task_id=f"weather_future_data_{a['name']}",
            python_callable=fetch_forecast_data,
            op_kwargs={'execution_date': '{{ ts }}', 'airport': a},
        )
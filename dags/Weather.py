import logging
import io
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup

import pandas as pd
from datetime import datetime, timedelta

import openmeteo_requests
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

BUCKET_NAME = 'team5-s3'

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
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True).tz_convert(airport['timezone']),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True).tz_convert(airport['timezone']),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "timestamp": [datetime.strptime(extracted_at_str, "%Y%m%d%H")] * len(hourly.Variables(0).ValuesAsNumpy())  # 동일한 실행 시간을 모든 행에 저장
    }

    for i in range(len(hourly_params)):
        hourly_data[hourly_params[i]] = hourly.Variables(i).ValuesAsNumpy()
    df = pd.DataFrame(hourly_data)

    s3_bucket = "team5-s3"  # S3 버킷 이름
    s3_key = f"raw_data/weather/{year_str}/{month_str}/{day_str}/{airport['name']}_past_{time_str}.csv"  # S3 객체 키
    upload_to_s3(df, s3_bucket, s3_key)


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
    s3_key = f"raw_data/weather/{year_str}/{month_str}/{day_str}/{airport['name']}_future_{time_str}.csv"  # S3 객체 키
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


def bulk_copy_to_snowlake(parquet_files, table_name):
    # Snowflake Hook 인스턴스 생성
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    logging.info("Snowflake hook created")

    try:
        # s3 데이터가 snowflake stage에 바로 업데이트 되지 않아서 stage를 내렸다가 다시 업로드
        delete_query = f"""
                DROP STAGE IF EXISTS TEAM5.raw_data.team5_stage;
            """
        snowflake_hook.run(delete_query)
        logging.info("DROP STAGE Complete")

        create_query = f"""
                CREATE STAGE TEAM5.raw_data.team5_stage
                STORAGE_INTEGRATION = TEAM5_S3_INTEGRATION
                URL = 's3://team5-s3/transform_data/'
            """
        snowflake_hook.run(create_query)
        logging.info("CREATE STAGE Complete")

        files_list = "', '".join(parquet_files)  # 파일 목록을 '파일1', '파일2', ... 형태로 변환
        if table_name == 'PAST_WEATHER':
            copy_query = f"""
                COPY INTO "TEAM5"."RAW_DATA"."{table_name}"
                FROM (
                    SELECT 
                        $1:DATE::TIMESTAMP_TZ(9) AS DATE,
                        $1:TIMESTAMP::TIMESTAMP_TZ(9) AS TIMESTAMP,
                        $1:TEMPERATURE_2M::FLOAT AS TEMPERATURE_2M,
                        $1:RELATIVE_HUMIDITY_2M::FLOAT AS RELATIVE_HUMIDITY_2M,
                        $1:DEW_POINT_2M::FLOAT AS DEW_POINT_2M,
                        $1:PRECIPITATION::FLOAT AS PRECIPITATION,
                        $1:RAIN::FLOAT AS RAIN,
                        $1:SNOWFALL::FLOAT AS SNOWFALL,
                        $1:SNOW_DEPTH::FLOAT AS SNOW_DEPTH,
                        $1:CLOUD_COVER::FLOAT AS CLOUD_COVER,
                        $1:WIND_SPEED_10M::FLOAT AS WIND_SPEED_10M,
                        $1:WIND_SPEED_100M::FLOAT AS WIND_SPEED_100M,
                        $1:WIND_DIRECTION_10M::FLOAT AS WIND_DIRECTION_10M,
                        $1:WIND_DIRECTION_100M::FLOAT AS WIND_DIRECTION_100M,
                        $1:WIND_GUSTS_10M::FLOAT AS WIND_GUSTS_10M,
                        $1:AIRPORT_CODE::VARCHAR(10) AS AIRPORT_CODE
                    FROM '@"TEAM5"."RAW_DATA"."TEAM5_STAGE"'
                )
                FILES = ('{files_list}')
                FILE_FORMAT = (
                    TYPE = PARQUET,
                    REPLACE_INVALID_CHARACTERS = TRUE,
                    BINARY_AS_TEXT = FALSE
                )
                ON_ERROR = ABORT_STATEMENT;
            """
        else:
            copy_query = f"""
                COPY INTO "TEAM5"."RAW_DATA"."{table_name}"
                FROM (
                    SELECT 
                        $1:DATE::TIMESTAMP_TZ(9) AS DATE,
                        $1:TIMESTAMP::TIMESTAMP_TZ(9) AS TIMESTAMP,
                        $1:TEMPERATURE_2M::FLOAT AS TEMPERATURE_2M,
                        $1:RELATIVE_HUMIDITY_2M::FLOAT AS RELATIVE_HUMIDITY_2M,
                        $1:DEW_POINT_2M::FLOAT AS DEW_POINT_2M,
                        $1:PRECIPITATION::FLOAT AS PRECIPITATION,
                        $1:RAIN::FLOAT AS RAIN,
                        $1:SNOWFALL::FLOAT AS SNOWFALL,
                        $1:SNOW_DEPTH::FLOAT AS SNOW_DEPTH,
                        $1:CLOUD_COVER::FLOAT AS CLOUD_COVER,
                        $1:VISIBILITY::FLOAT AS VISIBILITY,
                        $1:WIND_SPEED_10M::FLOAT AS WIND_SPEED_10M,
                        $1:WIND_SPEED_80M::FLOAT AS WIND_SPEED_80M,
                        $1:WIND_SPEED_120M::FLOAT AS WIND_SPEED_120M,
                        $1:WIND_SPEED_180M::FLOAT AS WIND_SPEED_180M,
                        $1:WIND_DIRECTION_10M::FLOAT AS WIND_DIRECTION_10M,
                        $1:WIND_DIRECTION_80M::FLOAT AS WIND_DIRECTION_80M,
                        $1:WIND_DIRECTION_120M::FLOAT AS WIND_DIRECTION_120M,
                        $1:WIND_DIRECTION_180M::FLOAT AS WIND_DIRECTION_180M,
                        $1:WIND_GUSTS_10M::FLOAT AS WIND_GUSTS_10M,
                        $1:AIRPORT_CODE::VARCHAR(10) AS AIRPORT_CODE
                    FROM '@"TEAM5"."RAW_DATA"."TEAM5_STAGE"'
                )
                FILES = ('{files_list}')
                FILE_FORMAT = (
                    TYPE = PARQUET,
                    REPLACE_INVALID_CHARACTERS = TRUE,
                    BINARY_AS_TEXT = FALSE
                )
                ON_ERROR = ABORT_STATEMENT;
            """

        result = snowflake_hook.run(copy_query)
        logging.info(f"COPY INTO Complete. Loaded {len(parquet_files)} files.")
        return result
    except Exception as e:
        logging.error(f"Error in bulk copy to Snowflake: {str(e)}")
        raise

def read_parquet_files_from_s3(bucket_name, prefix):
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # S3 버킷에서 Parquet 파일 목록 가져오기
    keys = s3_hook.list_keys(bucket_name, prefix=prefix)
    logging.info(f"Found keys: {keys}")

    parquet_files = []
    for key in keys:
        if key.endswith('.parquet'):
            try:
                # S3 객체가 비어 있는지 확인
                size = s3_hook.get_key(key, bucket_name).content_length
                if size > 0:
                    parquet_files.append(key)
                else:
                    logging.warning(f"Empty Parquet file found: {key}")
            except Exception as e:
                logging.error(f"Error checking file size for {key}: {e}")

    # 필터링된 .parquet 파일 목록 반환
    return parquet_files


def snowflake_load(folder_name, table_name):
    folder_path = f"transform_data/weather/{folder_name}"

    parquet_files = read_parquet_files_from_s3(BUCKET_NAME, folder_path)

    if parquet_files:
        bulk_copy_to_snowlake(parquet_files, table_name)
        logging.info(f"Copied {len(parquet_files)} files into Snowflake.")
    else:
        logging.info("No parquet files found to process.")


# 과거 날씨 데이터 수집 DAG
with DAG(
    dag_id="weather_past_data_collection",
    description="과거 날씨 데이터 수집 DAG",
    start_date=datetime(2023, 12, 1),
    schedule_interval="0 18 * * *",  # 매일 18:00에 실행
    catchup=False,
) as past_dag:
    with TaskGroup("weather_past_data_group") as weather_past_data_group:
        for a in airport:
            PythonOperator(
                task_id=f"weather_past_data_{a['name']}",
                python_callable=fetch_weather_data,
                op_kwargs={'execution_date': '{{ ts }}', 'airport': a},
            )

    glue_job_past = GlueJobOperator(
        task_id='transform_weather_past_data',
        job_name='team5-glue-past-weather',
        region_name='ap-northeast-2',
        aws_conn_id='aws_default'
    )

    snowflake_task_past = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=snowflake_load,
        op_kwargs={
            'folder_name': 'past',
            'table_name': 'PAST_WEATHER'
        }
    )

    weather_past_data_group >> glue_job_past >> snowflake_task_past

# 미래 날씨 데이터 수집 DAG
with DAG(
    dag_id="weather_future_data_collection",
    description="미래 날씨 데이터 수집 DAG",
    start_date=datetime(2023, 12, 1),
    schedule_interval="0 * * * *",  # 매 정각마다 실행
    catchup=False,
) as future_dag:
    with TaskGroup("weather_future_data_group") as weather_future_data_group:
        for a in airport:
            PythonOperator(
                task_id=f"weather_future_data_{a['name']}",
                python_callable=fetch_forecast_data,
                op_kwargs={'execution_date': '{{ ts }}', 'airport': a},
            )

    glue_job_future = GlueJobOperator(
        task_id='transform_weather_future_data',
        job_name='team5-glue-future-weather',
        region_name='ap-northeast-2',
        aws_conn_id='aws_default'
    )

    snowflake_task_future = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=snowflake_load,
        op_kwargs={
            'folder_name': 'future',
            'table_name': 'FUTURE_WEATHER'
        }
    )

    weather_future_data_group >> glue_job_future >> snowflake_task_future
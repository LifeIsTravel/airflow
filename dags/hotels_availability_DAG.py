import json
import requests
import pandas as pd
import time
import traceback
from datetime import datetime, timedelta
from io import BytesIO
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.rds import RdsBaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


BUCKET_NAME = 'team5-s3'

default_args = {
    'owner': 'nykim',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def collect_hotel_availability(**context):
    """호텔 가용성 데이터 수집"""
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    logical_date = context['logical_date']
    date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')
    
    # S3에서 호텔 리스트 최신 parquet 파일 찾기
    prefix = "transform_data/hotels_search"
    all_files = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
    parquet_files = [f for f in all_files if f.endswith('.parquet')]
    
    if not parquet_files:
        raise AirflowException("No parquet files found")
    
    # 최신 파일 선택 (파일명으로 정렬)
    latest_file = sorted(parquet_files)[-1]
    logging.info(f"Reading from file: {latest_file}")

    # Parquet 파일을 바이너리로 읽기
    parquet_data = s3_hook.get_key(latest_file, BUCKET_NAME).get()['Body'].read()
    table = pq.read_table(pa.py_buffer(parquet_data))
    df = table.to_pandas()
    
    # 엔드포인트 getAvailablility API 호출 설정
    api_key = Variable.get('booking_com_api_key')
    url = "https://booking-com15.p.rapidapi.com/api/v1/hotels/getAvailability"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "booking-com15.p.rapidapi.com"
    }
    
    # 날짜 설정 (오늘부터 30일)
    today = datetime.now()
    min_date = today.strftime('%Y-%m-%d')
    max_date = (today + timedelta(days=30)).strftime('%Y-%m-%d')
    
    # 기본 쿼리 파라미터
    querystring = {
        "min_date": min_date,
        "max_date": max_date,
        "currency_code": "KRW"
    }
    
    processed_hotels = 0
    failed_hotels = 0
    
    # 호텔 가용성 체크시에는 unique한 hotel_id만 사용
    unique_hotel_ids = df['hotel_id'].unique()
    logging.info(f"Processing availability for {len(unique_hotel_ids)} unique hotels")
    # 중복된 데이터 있음.
    for hotel_id in unique_hotel_ids:
        try:
            # API 호출
            params = {**querystring, "hotel_id": hotel_id}
            logging.info(f"Requesting availability for hotel_id: {hotel_id}")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            # API 응답에 날짜 정보 추가, transform에서 필요함.
            response_data = response.json()
            response_data['search_params'] = {
                'min_date': min_date,
                'max_date': max_date
            }
            # 결과 저장 (시간별 디렉토리 구조)
            output_key = f"raw_data/hotels_availability/{date_str}/{hotel_id}_availability.json"

            # 날짜 정보 추가한 json으로 s3에 저장
            s3_hook.load_string(
                json.dumps(response_data, ensure_ascii=False),
                key=output_key,
                bucket_name=BUCKET_NAME
            )
            
            processed_hotels += 1
            if processed_hotels % 10 == 0:
                logging.info(f"Processed {processed_hotels} hotels")
                
        except Exception as e:
            failed_hotels += 1
            logging.error(f"Error processing hotel {hotel_id}: {str(e)}")
            logging.error(f"Error details: {traceback.format_exc()}")
            continue
            
        # API 호출 간격 조절
        time.sleep(1)
    
    logging.info(f"Processing completed. Success: {processed_hotels}, Failed: {failed_hotels}")
    
    if processed_hotels == 0:
        raise AirflowException("No hotels were successfully processed")

def transform_hotel_availability(**context):
    """호텔 가용성 데이터 변환"""
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    logical_date = context['logical_date']
    date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')
    #hour_str = logical_date.strftime('%H')
    
    transformed_data = []
    
    # 해당 시간의 모든 호텔 데이터 조회
    prefix = f"raw_data/hotels_availability/{date_str}"
    availability_files = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
    
    for file_key in availability_files:
        try:
            # 파일 읽기
            content = s3_hook.read_key(file_key, BUCKET_NAME)
            hotel_data = json.loads(content)
            
            # hotel_id 추출
            hotel_id = file_key.split('/')[-1].replace('_availability.json', '')
            
            # 검색 기간 가져오기
            min_date = hotel_data['search_params']['min_date']
            max_date = hotel_data['search_params']['max_date']
            
            # avDates에서 가능한 날짜와 가격 정보 추출
            available_dates = {}
            for date_price in hotel_data['data']['avDates']:
                for date, price in date_price.items():  # 각 딕셔너리에는 키-값 쌍이 하나만 있음
                    available_dates[date] = price
            
            # min_date부터 max_date까지의 모든 날짜에 대해 처리
            # 대소비교를 위해 striptime으로 변환
            current_date = datetime.strptime(min_date, '%Y-%m-%d')
            end_date = datetime.strptime(max_date, '%Y-%m-%d')
            
            while current_date <= end_date:
                current_date_str = current_date.strftime('%Y-%m-%d')
                
                transformed_record = {
                    'hotel_id': hotel_id,
                    'checkin_date': current_date_str,
                    'is_available': current_date_str in available_dates, # checkin 날짜가 데이터에 있으면 가능, 없으면 불가능
                    'price': available_dates.get(current_date_str, None), # 예약 불가능한 날에는 None으로 처리리
                    'currency': hotel_data['data']['currency'],
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                transformed_data.append(transformed_record)
                
                current_date += timedelta(days=1)
                
        except Exception as e:
            logging.error(f"{file_key} 처리 중 에러 발생: {str(e)}")
            logging.error(f"에러 상세정보: {traceback.format_exc()}")
            continue
    
    if transformed_data:
        # DataFrame 생성 및 Parquet 변환
        df = pd.DataFrame(transformed_data)
        
        # 데이터 타입 변환
        #df['checkin_date'] = pd.to_datetime(df['checkin_date']).dt.strftime('%Y-%m-%d')  # datetime으로 변환 후 다시 문자열로
        df['updated_at'] = pd.to_datetime(df['updated_at'])
        df['price'] = df['price'].astype('float')
        
        table = pa.Table.from_pandas(df)
        
        # Parquet 파일 생성 및 저장
        output_key = f"transform_data/hotels_availability/availability_{date_str}.parquet"
        parquet_buffer = pa.BufferOutputStream()
        pq.write_table(table, parquet_buffer)
        
        s3_hook.load_bytes(
            parquet_buffer.getvalue().to_pybytes(),
            key=output_key,
            bucket_name=BUCKET_NAME
        )
        
        logging.info(f"가용성 데이터 변환 완료: {len(transformed_data)}개 처리됨")
    else:
        raise AirflowException("변환할 데이터가 없습니다.")
    
def load_to_snowflake(**context):
    """snowflake에 적재"""
    
    try:
        logical_date = context['logical_date']
        date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')

        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        # Table은 snowflake에서 생성해놨음.    
        # COPY INTO 명령어로 데이터 로드
        copy_data = f"""
        COPY INTO TEAM5.RAW_DATA.HOTELS_AVAILABILITY
        FROM 's3://team5-s3/transform_data/hotels_availability/availability_{date_str}.parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        STORAGE_INTEGRATION = TEAM5_S3_INTEGRATION
        FILE_FORMAT = (
            TYPE = PARQUET,
            REPLACE_INVALID_CHARACTERS = TRUE,
            BINARY_AS_TEXT = FALSE
        )
        ON_ERROR = ABORT_STATEMENT
        FORCE = FALSE;  
        """ # 특정 파일만 로드, 이미 로드된 파일은 스킵

        snowflake_result = snow_hook.run(copy_data)
        
        if snowflake_result:
            logging.info(f"snowflake에 적재된 데이터 수: {len(snowflake_result)}")

    except Exception as e:
        logging.error(f"snoflake에 데이터 적재 실패: {str(e)}")
        raise
    
def load_to_rds(**context):
    """RDS에 최신 데이터만 업데이트"""
    try:
        logical_date = context['logical_date']
        date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')

        s3_hook = S3Hook(aws_conn_id='aws_default')
        parquet_data = s3_hook.get_key(
            key=f"transform_data/hotels_availablility/availablity_{date_str}.parquet",
            bucket_name=BUCKET_NAME
        ).get()['Body'].read()

        table = pq.read_table(pa.py_buffer(parquet_data))
        df = table.to_pandas()

        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        # 테이블이 없으면 생성
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS hotels_availability(
                hotel_id INTEGER NOT NULL,
                checkin_date DATE NOT NULL,
                is_available BOOLEAN NOT NULL,
                price NUMERIC,
                currency VARCHAR(3) NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (hotel_id, checkin_date)
            );
        """

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)

                # 기존 데이터 삭제
                cur.execute("TRUNCATE TABLE hotels_availability;")
                # 새로운 데이터 적재
                df.to_sql(
                    'hotels_availability',
                    pg_hook.get_sqlalchemy_engine(),
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
        logging.info(f"RDS에 적재된 데이터 수: {len(df)}")
    except Exception as e:
        logging.error(f"RDS 데이터 적재 실패: {str(e)}")
        raise

def load_to_databases(**context):
    """데이터베이스 적재를 위한 TaskGroup (Snowflake & RDS)"""
    with TaskGroup(group_id = "load_to_db") as load_group:
        snowflake_task = PythonOperator(
            task_id = 'load_to_snowflake',
            python_callable=load_to_snowflake
        )

        rds_task = PythonOperator(
            task_id = 'load_to_rds',
            python_callable = load_to_rds
        )

        [snowflake_task, rds_task]

        return load_group
# DAG 정의
with DAG(
    'hotels_availability_collection',
    default_args=default_args,
    description='일 1회 호텔 예약 가능 여부 및 가격 정보 수집',
    schedule_interval= '0 4 * * *',  # 매시간 -> 하루 한번으로 변경, 한국시간 13시시
    catchup=False
) as dag:
    
    extract_availability = PythonOperator(
        task_id='collect_hotel_availability',
        python_callable=collect_hotel_availability,
    )

    transform_availability = PythonOperator(
        task_id = 'transform_hotel_availability',
        python_callable = transform_hotel_availability,
    )

    load_availability = load_to_databases()

    extract_availability >> transform_availability >> load_availability

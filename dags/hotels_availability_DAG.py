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
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.exceptions import AirflowException

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
    
    # S3에서 최신 parquet 파일 찾기
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
    
    # API 호출 설정
    api_key = Variable.get('booking_com_api_key')
    url = "https://booking-com15.p.rapidapi.com/api/v1/hotels/getAvailability"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "booking-com15.p.rapidapi.com"
    }
    
    # 날짜 설정 (오늘부터 30일)
    min_date = logical_date.strftime('%Y-%m-%d')
    max_date = (logical_date + timedelta(days=30)).strftime('%Y-%m-%d')
    
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
        df['checkin_date'] = pd.to_datetime(df['checkin_date'])
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
# DAG 정의
with DAG(
    'hotels_availability_collection',
    default_args=default_args,
    description='일 1회회 호텔 예약 가능 여부 및 가격 정보 수집',
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

    extract_availability >> transform_availability

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import requests
import json

default_args = {
    'owner': 'nykim',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

BUCKET_NAME = 'team5-s3'

def get_places_data(city: dict, api_key: str):
    """도시별 장소 데이터 수집"""
    url = "https://places.googleapis.com/v1/places:searchText"
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.rating,places.userRatingCount,places.location,places.photos"
    }
    
    try:
        data = {
            "textQuery": city['search_query'],
            "languageCode": "ko"
        }
        
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
            
    except requests.exceptions.RequestException as e:
        logging.error(f"API 요청 중 에러 발생: {e}")
        return None

def collect_and_save_places(**context):
    """도시별 장소 데이터 수집 및 저장"""
    # 실행 날짜 당일
    logical_date = context['logical_date']
    date_str = logical_date.strftime('%Y%m%d')
    
    # S3 hook 초기화
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Google Places API 키
    api_key = Variable.get("google_places_api_key")
    
    # 수집할 도시 설정
    cities = [
        {
            "name": "Tokyo",
            "name_ko": "도쿄",
            "search_query": "popular places in Tokyo"
        },
        {
            "name": "Osaka",
            "name_ko": "오사카",
            "search_query": "popular places in Osaka"
        },
        {
            "name": "Fukuoka",
            "name_ko": "후쿠오카",
            "search_query": "popular places in Fukuoka"
        },
        {
            "name": "Sapporo",
            "name_ko": "삿포로",
            "search_query": "popular places in Sapporo"
        }
    ]
    
    for city in cities:
        try:
            # 데이터 수집
            response_data = get_places_data(city, api_key)
            
            if response_data:
                # S3에 JSON 응답 그대로 저장
                file_key = f"raw_data/google_places/{city['name']}_places_{date_str}.json"
                
                s3_hook.load_string(
                    json.dumps(response_data, ensure_ascii=False, indent=2),
                    key=file_key,
                    bucket_name=BUCKET_NAME
                )
                
                logging.info(f"{city['name_ko']} 데이터 저장 완료: {file_key}")
            else:
                raise Exception(f"{city['name_ko']} 데이터 수집 실패")
                
        except Exception as e:
            logging.error(f"{city['name_ko']} 처리 중 에러 발생: {str(e)}")
            raise

def get_city_airport_code(city_name: str):
    """도시별 공항 코드 반환"""
    airport_codes = {
        'Tokyo': 'NRT',     # 나리타 공항
        'Osaka': 'KIX',     # 간사이 국제공항
        'Fukuoka': 'FUK',   # 후쿠오카 공항
        'Sapporo': 'CTS'    # 신치토세 공항
    }
    return airport_codes.get(city_name)

def process_places_data(raw_data, city_info):
    """Places API 응답 데이터를 정형화된 형태로 변환"""
    processed_places = []
        
    for place in raw_data:
        # 위도/경도를 조합하여 고유 ID 생성
        lat = place['location']['latitude']
        lon = place['location']['longitude']
        place_id = f"{city_info['name']}_{lat:.6f}_{lon:.6f}".replace('.', '_')

        processed_place = {
            'place_id': place_id,
            'city_name': city_info['name'],
            'city_name_ko': city_info['name_ko'],
            'airport_code': get_city_airport_code(city_info['name']),
            'place_name': place.get('displayName', {}).get('text', ''),
            'address': place.get('formattedAddress', ''),
            'latitude': place.get('location', {}).get('latitude', 0.0),
            'longitude': place.get('location', {}).get('longitude', 0.0),
            'rating': place.get('rating', 0.0),
            'rating_count': place.get('userRatingCount', 0),
            'photo_url': place.get('photos', [{}])[0].get('name', '') if place.get('photos') else '',
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        processed_places.append(processed_place)
    
    return processed_places

def transform_and_save_places(**context):
    """수집된 장소 데이터를 변환하여 Parquet 형식으로 저장"""
    logical_date = context['logical_date']
    date_str = logical_date.strftime('%Y%m%d')
    
    # S3 hook 초기화
    s3_hook = S3Hook(aws_conn_id='aws_default')    
    # 처리할 도시 정보
    cities = [
        {"name": "Tokyo", "name_ko": "도쿄"},
        {"name": "Osaka", "name_ko": "오사카"},
        {"name": "Fukuoka", "name_ko": "후쿠오카"},
        {"name": "Sapporo", "name_ko": "삿포로"}
    ]
    
    all_places_data = []
    
    # 각 도시별 데이터 처리
    for city in cities:
        try:
            # Raw 데이터 파일 읽기
            raw_file_key = f"raw_data/google_places/{city['name']}_places_{date_str}.json"
            raw_data = json.loads(s3_hook.read_key(raw_file_key, BUCKET_NAME))
            
            # 데이터 변환
            processed_places = process_places_data(raw_data, city)
            all_places_data.extend(processed_places)
            
            logging.info(f"{city['name_ko']} 데이터 변환 완료")
            
        except Exception as e:
            logging.error(f"{city['name_ko']} 데이터 처리 중 에러 발생: {str(e)}")
            raise
    
    if all_places_data:
        try:
            # DataFrame 생성
            df = pd.DataFrame(all_places_data)
            
            # PyArrow 테이블로 변환
            table = pa.Table.from_pandas(df)
            
            # Parquet 파일로 변환하여 S3에 저장
            parquet_buffer = pa.BufferOutputStream()
            pq.write_table(table, parquet_buffer)
            
            transform_file_key = f"transform_data/places/places_{date_str}.parquet"
            s3_hook.load_bytes(
                parquet_buffer.getvalue().to_pybytes(),
                key=transform_file_key,
                bucket_name=BUCKET_NAME
            )
            
            logging.info(f"Parquet 파일 저장 완료: {transform_file_key}")
            
        except Exception as e:
            logging.error(f"Parquet 파일 저장 중 에러 발생: {str(e)}")
            raise
    else:
        raise ValueError("변환할 데이터가 없습니다.")

def load_to_snowflake(**context):
    """Parquet 파일을 Snowflake에 로드"""
    logical_date = context['logical_date']
    date_str = logical_date.strftime('%Y%m%d')  
    try:
        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # 타겟 테이블 생성
        create_table = """
        CREATE TABLE IF NOT EXISTS TEAM5.RAW_DATA.PLACES (
            place_id VARCHAR(100),
            city_name VARCHAR(50),
            city_name_ko VARCHAR(50),
            airport_code VARCHAR(10),
            place_name VARCHAR(200),
            address VARCHAR(500),
            latitude FLOAT,
            longitude FLOAT,
            rating FLOAT,
            rating_count INTEGER,
            photo_url VARCHAR(500),
            updated_at TIMESTAMP_NTZ
        );
        """
        
        # COPY INTO 명령어로 데이터 로드
        copy_data = f"""
        COPY INTO TEAM5.RAW_DATA.PLACES 
        FROM 's3://team5-s3/transform_data/places/places_{date_str}.parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        STORAGE_INTEGRATION = TEAM5_S3_INTEGRATION
        FILE_FORMAT = (
            TYPE = PARQUET,
            REPLACE_INVALID_CHARACTERS = TRUE,
            BINARY_AS_TEXT = FALSE
        )
        ON_ERROR = ABORT_STATEMENT;
        """
        
        # 최신 데이터 조회를 위한 View 생성
        create_view = """
        CREATE OR REPLACE VIEW TEAM5.RAW_DATA.VW_LATEST_PLACES AS
        WITH latest_updates AS (
            SELECT 
                place_id,
                MAX(updated_at) as max_updated_at
            FROM TEAM5.RAW_DATA.PLACES
            GROUP BY place_id
        )
        SELECT p.*
        FROM TEAM5.RAW_DATA.PLACES p
        JOIN latest_updates l
            ON p.place_id = l.place_id 
            AND p.updated_at = l.max_updated_at;
        """
        
        # SQL 실행
        with snow_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table)
                cur.execute(copy_data)
                cur.execute(create_view)
        
        logging.info("Snowflake 데이터 로드 및 View 생성 완료")
        
    except Exception as e:
        logging.error(f"Snowflake 데이터 로드 중 에러 발생: {str(e)}")
        raise

with DAG(
    'collect_and_transform_places',
    default_args=default_args,
    description='월 1회 일본 주요 도시의 인기 장소 정보 수집 및 변환',
    schedule_interval='0 0 1 * *',  # 매월 1일 00:00에 실행
    catchup=False
) as dag:
    
    extract = PythonOperator(
        task_id='collect_places_data',
        python_callable=collect_and_save_places,
    )
    
    transform = PythonOperator(
        task_id='transform_places_data',
        python_callable=transform_and_save_places,
    )
    
    load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )
    # Task 순서 정의
    extract >> transform >> load
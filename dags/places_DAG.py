from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
from io import StringIO
import pyarrow.parquet as pq
import logging
import requests
import json

default_args = {
    'owner': 'nykim',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

BUCKET_NAME = 'team5-s3'
# 실행 날짜 당일


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
    logical_date = context['logical_date']
    date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')
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
                file_key = f"raw_data/places/{city['name']}_places_{date_str}.json"
                
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
        
    # raw_data에서 places 배열 가져오기
    places_data = raw_data.get('places', [])
        
    for place in places_data:
        try:
            # 위도/경도를 조합하여 고유 ID 생성
            lat = place['location']['latitude']
            lon = place['location']['longitude']
            place_id = f"{city_info['name']}_{lat:.6f}_{lon:.6f}".replace('.', '_')

            processed_place = {
                'place_id': place_id,
                'city_name': city_info['name'],
                'city_name_ko': city_info['name_ko'],
                'airport_code': get_city_airport_code(city_info['name']),
                'place_name': place['displayName']['text'],
                'address': place.get('formattedAddress', ''),
                'latitude': lat,
                'longitude': lon,
                'rating': place.get('rating', 0.0),
                'rating_count': place.get('userRatingCount', 0),
                'photo_url': place.get('photos', [{}])[0].get('name', '') if place.get('photos') else '',
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            processed_places.append(processed_place)
            
        except Exception as e:
            logging.error(f"장소 데이터 처리 중 에러 발생: {str(e)}")
            continue
    return processed_places


def transform_and_save_places(**context):
    """수집된 장소 데이터를 변환하여 Parquet 형식으로 저장"""
    logical_date = context['logical_date']
    date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')
    
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
            raw_file_key = f"raw_data/places/{city['name']}_places_{date_str}.json"
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
            # 모든 데이터를 수집한 후 DataFrame 생성
            df = pd.DataFrame(all_places_data)
            df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0)
            df['rating_count'] = pd.to_numeric(df['rating_count'], errors='coerce').fillna(0)
            
            # 중복 제거
            df = df.drop_duplicates(subset=['place_id'], keep='first')
            
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
                            
            logging.info(f"전체 처리된 장소 수: {len(df)}")
            logging.info("\n도시별 장소 수:")
            logging.info(df['city_name_ko'].value_counts())
            
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
        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
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
        
        # 임시 테이블 생성 및 데이터 로드
        create_temp_table = """
        CREATE OR REPLACE TEMPORARY TABLE TEAM5.RAW_DATA.TEMP_PLACES (
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
        
        # 새로운 데이터를 임시 테이블에 로드
        copy_to_temp = f"""
        COPY INTO TEAM5.RAW_DATA.TEMP_PLACES
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
        
        # MERGE 문으로 데이터 업데이트
        merge_data = """
        MERGE INTO TEAM5.RAW_DATA.PLACES target
        USING TEAM5.RAW_DATA.TEMP_PLACES source
        ON target.place_id = source.place_id
        WHEN MATCHED THEN
            UPDATE SET 
                city_name = source.city_name,
                city_name_ko = source.city_name_ko,
                airport_code = source.airport_code,
                place_name = source.place_name,
                address = source.address,
                latitude = source.latitude,
                longitude = source.longitude,
                rating = source.rating,
                rating_count = source.rating_count,
                photo_url = source.photo_url,
                updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (
                place_id, city_name, city_name_ko, airport_code, 
                place_name, address, latitude, longitude, 
                rating, rating_count, photo_url, updated_at
            )
            VALUES (
                source.place_id, source.city_name, source.city_name_ko, source.airport_code,
                source.place_name, source.address, source.latitude, source.longitude,
                source.rating, source.rating_count, source.photo_url, source.updated_at
            );
        """
        
        # SQL 실행
        with snow_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table)
                cur.execute(create_temp_table)
                cur.execute(copy_to_temp)
                cur.execute(merge_data)
        
        logging.info("Snowflake 데이터 업데이트 완료")
        
    except Exception as e:
        logging.error(f"Snowflake 데이터 로드 중 에러 발생: {str(e)}")
        raise

def load_to_rds(**context):
    """RDS에 최신 데이터만 업데이트"""
    logical_date = context['logical_date']
    date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')
    csv_key = None
    try:
        #logical_date = context['logical_date']
        #date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')

        s3_hook = S3Hook(aws_conn_id='aws_default')
        parquet_data = s3_hook.get_key(
            key=f"transform_data/places/places_{date_str}.parquet",
            bucket_name=BUCKET_NAME
        ).get()['Body'].read()

        # Parquet을 CSV로 변환
        table = pq.read_table(pa.py_buffer(parquet_data))
        csv_buffer = StringIO()
        table.to_pandas().to_csv(csv_buffer, index=False)
        
        # CSV 파일을 S3에 임시 저장
        csv_key = f"temp/places_{date_str}.csv"
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=csv_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        logging.info(f"임시 CSV 파일 생성 완료: {csv_key}")

        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        # 테이블이 없으면 생성
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS places(
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
                updated_at TIMESTAMP
            );
        """

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                logging.info(f"테이블 생성 또는 확인 완료")

                # 기존 데이터 삭제
                cur.execute("SELECT COUNT(*) FROM places")
                before_count = cur.fetchone()[0]
                logging.info(f"삭제 전 기존 데이터 수: {before_count}")
                cur.execute("TRUNCATE TABLE places;")
                logging.info("기존 데이터 삭제 완료")
                
                # 새로운 데이터 적재
                logging.info(f"S3 경로 {BUCKET_NAME}/{csv_key}에서 데이터 적재 시작")
                insert_query = f"""
                    SELECT aws_s3.table_import_from_s3(
                            'places',
                            '', --모든 컬럼
                            '(format csv, header true)',
                            aws_commons.create_s3_uri(
                                '{BUCKET_NAME}',
                                '{csv_key}',
                                'ap-northeast-2'
                            )
                    );
                """
                cur.execute(insert_query)
                # 적재 후 데이터 수 확인
                cur.execute("SELECT COUNT(*) FROM places")
                after_count = cur.fetchone()[0]
                logging.info(f"데이터 적재 완료: {after_count}행 적재됨")

    except Exception as e:
        logging.error(f"RDS 데이터 처리 중 오류 발생: {str(e)}")
        raise

    finally:
        # 성공/실패 여부와 관계없이 임시 파일 삭제 시도
        if csv_key:
            try:
                s3_hook.delete_objects(
                    bucket=BUCKET_NAME,
                    keys=[csv_key]
                )
                logging.info(f"임시 CSV 파일 삭제 완료: {csv_key}")
            except Exception as delete_error:
                logging.error(f"임시 파일 삭제 실패: {str(delete_error)}")

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
with DAG(
    'google_popular_places_collection',
    default_args=default_args,
    description='일본 주요 도시의 인기 장소 정보 ETL',
    schedule_interval='@once', 
    catchup=False
) as dag:
    
    extract_places = PythonOperator(
        task_id='collect_places_data',
        python_callable=collect_and_save_places,
    )
    
    transform_places = PythonOperator(
        task_id='transform_places_data',
        python_callable=transform_and_save_places,
    )
    
    load_places = load_to_databases()
    # Task 순서 정의
    extract_places >> transform_places >> load_places
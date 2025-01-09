from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import traceback
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO
import logging
import requests
import json
from haversine import haversine, Unit
from typing import List, Dict

default_args = {
    'owner': 'nykim',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 8),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

BUCKET_NAME = 'team5-s3'

def get_top_places_coordinates() -> List[Dict]:
    """각 도시별 상위 10개 인기 장소의 좌표 정보를 가져옴"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # 최신 places parquet 파일 찾기
    prefix = "transform_data/places/"
    files = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
    latest_file = max(files)  # 가장 최근 파일
    
    # Parquet 파일을 바이너리로 읽기
    parquet_data = s3_hook.get_key(latest_file, BUCKET_NAME).get()['Body'].read()
    table = pq.read_table(pa.py_buffer(parquet_data))
    df = table.to_pandas()
    
    top_places = []
    # 각 도시별로 리뷰 수 기준 상위 10개 장소 선택
    for city in df['city_name'].unique():
        city_places = df[df['city_name'] == city].nlargest(10, 'rating_count')
        top_places.extend(city_places[['city_name', 'place_id', 'place_name', 'latitude', 'longitude']].to_dict('records'))
    
    return top_places

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """두 지점 간의 거리를 km 단위로 계산"""
    return round(haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS), 2)

def search_hotels_near_place(place: Dict, arrival_date: str, departure_date: str) -> Dict:
    """특정 장소 주변의 호텔 검색"""
    url = "https://booking-com15.p.rapidapi.com/api/v1/hotels/searchHotelsByCoordinates"
    
    querystring = {
        "latitude": place['latitude'],
        "longitude": place['longitude'],
        "arrival_date": arrival_date,
        "departure_date": departure_date,
        "radius": 10, # 10km 근방 (최소값값)
        "page_count": "1",  # 첫 페이지만 가져옴, 20개
        "languagecode": 'ko',
        "currency_code": 'KRW'
    }
    
    headers = {
        "x-rapidapi-host": "booking-com15.p.rapidapi.com",
        "x-rapidapi-key": Variable.get("booking_com_api_key")
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API 요청 중 에러 발생: {e}")
        return None
    
def collect_and_save_hotels(**context):
    """인기 장소 주변 호텔 데이터 수집 및 저장"""
    # 실행 날짜 기준 일주일 뒤 1박을 선택했을 때 나오는 호텔 리스트트
    logical_date = context['logical_date']
    date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')
    arrival_date = (logical_date + timedelta(days=7)).strftime('%Y-%m-%d')
    departure_date = (logical_date + timedelta(days=8)).strftime('%Y-%m-%d')
    
    # S3 hook 초기화
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # 인기 장소 목록 가져오기
    top_places = get_top_places_coordinates()
    
    for place in top_places:
        try:
            # 호텔 데이터 수집, api call
            hotels_data = search_hotels_near_place(place, arrival_date, departure_date)
            
            if hotels_data:
                # 파일명에 장소 정보와 날짜 포함
                
                # 도시별 디렉토리 구조로 파일 저장
                file_key = f"raw_data/hotels_search/{date_str}/{place['city_name']}/{place['place_id']}_hotels_{date_str}.json"
                
                # S3에 JSON 저장
                s3_hook.load_string(
                    json.dumps(hotels_data, ensure_ascii=False, indent=2),
                    key=file_key,
                    bucket_name=BUCKET_NAME
                )
                
                logging.info(f"{place['city_name']} - {place['place_name']} 주변 호텔 데이터 저장 완료: {file_key}")
            else:
                logging.warning(f"{place['place_name']} 주변 호텔 데이터 수집 실패")
                
        except Exception as e:
            logging.error(f"{place['place_name']} 처리 중 에러 발생: {str(e)}")
            continue

def transform_hotels_data(**context):
    # 도시 정보 매핑
    city_mapping = {
        'Tokyo': {'airport_code': 'NRT', 'city_name': 'Tokyo', 'city_name_ko': '도쿄'},
        'Osaka': {'airport_code': 'KIX', 'city_name': 'Osaka', 'city_name_ko': '오사카'},
        'Fukuoka': {'airport_code': 'FUK', 'city_name': 'Fukuoka', 'city_name_ko': '후쿠오카'},
        'Sapporo': {'airport_code': 'CTS', 'city_name': 'Sapporo', 'city_name_ko': '삿포로'}
    }
    """호텔 데이터 변환 및 거리 계산"""
    
    logging.info("transform_hotels_data 시작")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    logical_date = context['logical_date']
    date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')
    
    transformed_hotels = []
    processed_files = 0
    failed_files = 0
    
    # S3에서 당일 수집된 모든 호텔 데이터 파일 조회
    prefix = f"raw_data/hotels_search/{date_str}"
    hotel_files = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
    
    if not hotel_files:
        raise AirflowException(f"No files found in {prefix}")
    
    for file_key in hotel_files:
        try:
            logging.info(f"파일 처리 시작: {file_key}")
            
            # 파일명에서 도시와 장소 정보 추출
            path_parts = file_key.split('/')
            if len(path_parts) < 2:
                raise ValueError(f"Invalid file path structure: {file_key}")
                
            city_name = path_parts[-2]
            filename = path_parts[-1]
            # 파일명에서 place_id 찾기
            place_id = '_'.join(filename.split('_')[:5])
            
            if city_name not in city_mapping:
                raise ValueError(f"Unknown city name: {city_name}")
            
            # 파일 읽기
            content = s3_hook.read_key(file_key, BUCKET_NAME)
            try:
                hotel_data = json.loads(content)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in file {file_key}: {str(e)}")
            
            if 'data' not in hotel_data or 'result' not in hotel_data['data']:
                raise ValueError(f"Invalid data structure in file {file_key}")
            
            # 장소 정보 가져오기
            place_info = None
            places_coordinates = get_top_places_coordinates()
            try:
                # 인기 장소 각 도시별 10개씩 딕셔너리 화
                place_info = next(p for p in places_coordinates if p['place_id'] == place_id)
            except StopIteration:
                raise ValueError(f"Place ID not found: {place_id}")
            
            # 숙소 - 인기 장소 간 거리 계산
            for hotel in hotel_data['data']['result']:
                try:
                    distance = calculate_distance(
                        float(hotel['latitude']),
                        float(hotel['longitude']),
                        float(place_info['latitude']),
                        float(place_info['longitude'])
                    )
                    
                    city_info = city_mapping[city_name]
                    
                    transformed_hotel = {
                        'hotel_id': str(hotel['hotel_id']),
                        'hotel_name': hotel['hotel_name'],
                        'airport_code': city_info['airport_code'],
                        'city_name': city_info['city_name'],
                        'city_name_ko': city_info['city_name_ko'],
                        'place_id': place_info['place_id'],
                        'place_name': place_info['place_name'],
                        'review_score': float(hotel.get('review_score', 0)),
                        'review_score_word': hotel.get('review_score_word', ''),
                        'review_nr': int(hotel.get('review_nr', 0)),
                        'checkin_time': hotel.get('checkin', {}).get('from', ''),
                        'checkout_time': hotel.get('checkout', {}).get('until', ''),
                        'hotel_class': str(hotel.get('class', '')),
                        'latitude': float(hotel['latitude']),
                        'longitude': float(hotel['longitude']),
                        'distance': distance,
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    transformed_hotels.append(transformed_hotel)
                    
                except (KeyError, ValueError) as e:
                    logging.error(f"호텔 데이터 처리 중 에러 발생: {str(e)}")
                    continue  # 개별 호텔 데이터 에러는 건너뛰기
            
            processed_files += 1
            logging.info(f"파일 처리 완료: {file_key}")
            
        except Exception as e:
            failed_files += 1
            logging.error(f"{file_key} 처리 중 에러 발생: {str(e)}")
            logging.error(f"에러 상세정보: {traceback.format_exc()}")
    
    # 모든 파일이 실패한 경우 에러 발생
    if failed_files == len(hotel_files):
        raise AirflowException(f"모든 파일 처리 실패 ({failed_files}/{len(hotel_files)})")
    
    # 변환된 데이터가 없는 경우 에러 발생
    if not transformed_hotels:
        raise AirflowException("변환된 호텔 데이터가 없습니다.")
    
    logging.info(f"처리된 파일: {processed_files}/{len(hotel_files)}, 실패한 파일: {failed_files}")
    
    # DataFrame 생성 및 Parquet 변환
    df = pd.DataFrame(transformed_hotels)
    table = pa.Table.from_pandas(df)
    
    # 메모리에서 Parquet 파일 생성
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)
    
    # S3에 저장
    output_key = f"transform_data/hotels_search/hotels_{date_str}.parquet"
    s3_hook.load_bytes(
        parquet_buffer.getvalue().to_pybytes(),
        key=output_key,
        bucket_name=BUCKET_NAME
    )
    
    logging.info(f"호텔 데이터 변환 완료: {len(transformed_hotels)}개 처리됨")

def load_to_snowflake(**context):
    """snowflake에 적재"""
    
    try:
        logical_date = context['logical_date']
        date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')

        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        # Table은 snowflake에서 생성해놨음.    
        # COPY INTO 명령어로 데이터 로드
        copy_data = f"""
        COPY INTO TEAM5.RAW_DATA.HOTELS_SEARCH
        FROM 's3://team5-s3/transform_data/hotels_search/hotels_{date_str}.parquet'
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
    csv_key = None
    try:
        logical_date = context['logical_date']
        date_str = (logical_date + timedelta(days=1)).strftime('%Y%m%d')

        s3_hook = S3Hook(aws_conn_id='aws_default')
        parquet_data = s3_hook.get_key(
            key=f"transform_data/hotels_search/hotels_{date_str}.parquet",
            bucket_name=BUCKET_NAME
        ).get()['Body'].read()

        # Parquet을 CSV로 변환
        table = pq.read_table(pa.py_buffer(parquet_data))
        csv_buffer = StringIO()
        table.to_pandas().to_csv(csv_buffer, index=False)
        
        # CSV 파일을 S3에 임시 저장
        csv_key = f"temp/hotels_{date_str}.csv"
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
            CREATE TABLE IF NOT EXISTS hotels_search(
                hotel_id VARCHAR(100),
                hotel_name VARCHAR(200),
                airport_code VARCHAR(3),
                city_name VARCHAR(50),
                city_name_ko VARCHAR(50),
                place_id VARCHAR(100),
                place_name VARCHAR(200),
                reviews_score FLOAT,
                review_score_word VARCHAR(50),
                review_nr INTEGER,
                checkin_time VARCHAR(50),
                checkout_time VARCHAR(50),
                hotel_class VARCHAR(10),
                latitude FLOAT,
                longitude FLOAT,
                distance FLOAT,
                updated_at TIMESTAMP
            );
        """

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                logging.info(f"테이블 생성 또는 확인 완료")

                # 기존 데이터 삭제
                cur.execute("SELECT COUNT(*) FROM hotels_search")
                before_count = cur.fetchone()[0]
                logging.info(f"삭제 전 기존 데이터 수: {before_count}")
                cur.execute("TRUNCATE TABLE hotels_search;")
                logging.info("기존 데이터 삭제 완료")
                
                # 새로운 데이터 적재
                logging.info(f"S3 경로 {BUCKET_NAME}/{csv_key}에서 데이터 적재 시작")
                insert_query = f"""
                    SELECT aws_s3.table_import_from_s3(
                            'hotels_search',
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
                cur.execute("SELECT COUNT(*) FROM hotels_search")
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
    'hotels_info_data_collection',
    default_args=default_args,
    description='인기 장소 주변 호텔 정보 수집',
    schedule_interval='@once', 
    catchup=False
) as dag:
    
    extract_hotels = PythonOperator(
        task_id='collect_hotels_data',
        python_callable=collect_and_save_hotels,
    )
    transform_hotels = PythonOperator(
    task_id='transform_hotels_data',
    python_callable=transform_hotels_data,
    )

    load_hotels = load_to_databases()

    extract_hotels >> transform_hotels >> load_hotels
    
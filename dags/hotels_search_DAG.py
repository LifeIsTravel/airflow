from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import requests
import json
from haversine import haversine, Unit
from typing import List, Dict

default_args = {
    'owner': 'nykim',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
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
    
    # Parquet 파일 읽기
    parquet_data = s3_hook.read_key(latest_file, BUCKET_NAME)
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
        "radius": 10,
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
    # 실행 날짜 기준 다음 달 1일부터 2일까지 (예시 기간)
    logical_date = context['logical_date']
    date_str = logical_date.strftime('%Y%m%d')
    arrival_date = (logical_date + timedelta(days=7)).strftime('%Y-%m-%d')
    departure_date = (logical_date + timedelta(days=8)).strftime('%Y-%m-%d')
    
    # S3 hook 초기화
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # 인기 장소 목록 가져오기
    top_places = get_top_places_coordinates()
    
    for place in top_places:
        try:
            # 호텔 데이터 수집
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
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    logical_date = context['logical_date']
    date_str = logical_date.strftime('%Y%m%d')
    
    transformed_hotels = []
    
    # S3에서 당일 수집된 모든 호텔 데이터 파일 조회
    prefix = f"raw_data/hotels_search/{date_str}"
    hotel_files = []
    cities = s3_hook.list_directories(bucket_name=BUCKET_NAME, prefix=prefix)
    
    for city_prefix in cities:
        city_files = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=city_prefix)
        hotel_files.extend([f for f in city_files if date_str in f and f.endswith('.json')])
    
    for file_key in hotel_files:
        try:
            # 파일명에서 도시와 장소 정보 추출
            path_parts = file_key.split('/')
            city_name = path_parts[-2]
            filename = path_parts[-1]  # tokyo_35_7147_139_7967_hotels_20250106.json
            place_id = '_'.join(filename.split('_')[:4])  # tokyo_35_7147_139_7967
            
            # 파일 읽기
            content = s3_hook.read_key(file_key, BUCKET_NAME)
            hotel_data = json.loads(content)
            
            # 장소 정보 가져오기
            place_info = next(p for p in get_top_places_coordinates() if p['place_id'] == place_id)
            
            # 숙소 - 인기 장소 거리
            for hotel in hotel_data['data']['result']:
                distance = calculate_distance(
                    float(hotel['latitude']),
                    float(hotel['longitude']),
                    float(place_info['latitude']),
                    float(place_info['longitude'])
                )
                
                # 도시 이름으로 매핑 정보 가져오기
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
                    'distance': distance
                }
                transformed_hotels.append(transformed_hotel)
                
        except Exception as e:
            logging.error(f"{file_key} 처리 중 에러 발생: {str(e)}")
            continue
    
    if transformed_hotels:
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
    else:
        logging.warning("변환할 호텔 데이터가 없습니다.")

with DAG(
    'collect_and_transform_hotels_near_places',
    default_args=default_args,
    description='주 1회 인기 장소 주변 호텔 정보 수집',
    schedule_interval='0 0 * * 1',  # 매주 월요일 00:00에 실행
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

    extract_hotels >> transform_hotels
    
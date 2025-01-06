import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 로깅설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# S3에서 Parquet 파일을 읽어오는 함수 (파일 목록만 확인)
def read_parquet_files_from_s3(bucket_name, prefix):
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # S3 버킷에서 Parquet 파일 목록 가져오기
    keys = s3_hook.list_keys(bucket_name, prefix=prefix)
    logging.info("keys:", keys)

    # .parquet으로 끝나는 파일만 필터링
    parquet_files = [key for key in keys if key.endswith('.parquet')]
    # 'transform_data/'를 경로에서 제거
    parquet_files = [key.replace('transform_data/', '') for key in parquet_files]

    # 필터링된 .parquet 파일 목록 반환
    return parquet_files

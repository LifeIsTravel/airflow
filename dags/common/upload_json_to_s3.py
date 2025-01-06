import json
import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 로깅설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# S3에 JSON 데이터를 문자열로 업로드하는 함수
def upload_json_to_s3(json_data, s3_bucket, s3_key):
    # S3Hook을 사용하여 S3와 연결
    s3_hook = S3Hook(aws_conn_id='aws_default')  # aws_default 연결 아이디

    try:
        # JSON 데이터를 문자열로 변환
        json_string = json.dumps(json_data, ensure_ascii=False, indent=4)

        # S3에 문자열 업로드
        s3_hook.load_string(
            string_data=json_string,
            bucket_name=s3_bucket,
            key=s3_key,
            replace=True  # 이미 존재하는 파일을 덮어쓸지 여부
        )
        logging.info(f"JSON 파일이 S3 버킷 {s3_bucket}에 {s3_key}로 업로드되었습니다.")
    except Exception as e:
        logging.error(f"S3에 JSON 파일 업로드 실패: {e}")

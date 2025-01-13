import json
from typing import Optional, Dict, List

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from common.logger import get_logger

logger = get_logger(__name__)


def upload_json_to_s3(json_data: Dict, bucket: str, key: str, aws_conn_id: str = 'aws_default'):
    """
    Converts a dictionary to JSON and uploads it to an S3 bucket using Airflow's S3Hook.

    Args:
        json_data (dict): The dictionary to convert and upload.
        bucket (str): The name of the S3 bucket.
        key (str): The S3 object key.
        aws_conn_id (str): The Airflow connection ID for AWS.
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        json_string = json.dumps(json_data, ensure_ascii=False, indent=4)
        s3_hook.load_string(
            string_data=json_string,
            bucket_name=bucket,
            key=key,
            replace=True
        )
        logger.info(f"JSON 파일이 S3 버킷 {bucket}에 {key}로 업로드되었습니다.")
    except Exception as e:
        logger.error(f"S3에 JSON 파일 업로드 실패: {str(e)}")
        raise


def upload_parquet_to_s3(parquet_data: bytes, bucket: str, key: str, aws_conn_id: str = 'aws_default'):
    """
    Uploads Parquet data to an S3 bucket using Airflow's S3Hook.

    Args:
        parquet_data (bytes): The Parquet data as bytes.
        bucket (str): The name of the S3 bucket.
        key (str): The S3 object key.
        aws_conn_id (str): The Airflow connection ID for AWS.
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_hook.load_bytes(
            bytes_data=parquet_data,
            bucket_name=bucket,
            key=key,
            replace=True
        )
        logger.info(f"Parquet 파일이 S3 버킷 {bucket}에 {key}로 업로드되었습니다.")
    except Exception as e:
        logger.error(f"S3에 Parquet 파일 업로드 실패: {str(e)}")
        raise


def read_files_from_s3(bucket_name: str, prefix: str, file_type: str, aws_conn_id: str = 'aws_default') -> List[str]:
    """
    Returns a list of files from an S3 bucket based on the file type (either 'csv' or 'parquet') and filtering by a given prefix.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix to filter the S3 keys.
        file_type (str): The type of the file to filter ('csv' or 'parquet').
        aws_conn_id (str): The Airflow connection ID for AWS (default: 'aws_default').

    Returns:
        List[str]: A list of file keys in the S3 bucket that match the specified file type.
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        # S3 버킷에서 파일 목록 가져오기
        keys = s3_hook.list_keys(bucket_name, prefix=prefix)
        logger.info(f"파일 목록: {keys}")

        # 파일 형식에 따라 필터링
        if file_type == 'parquet':
            files = [key for key in keys if key.endswith('.parquet')]
        elif file_type == 'csv':
            files = [key for key in keys if key.endswith('.csv')]
        else:
            raise ValueError(f"지원되지 않는 파일 형식: {file_type}")

        logger.info(f"필터링된 {file_type.upper()} 파일 목록: {files[0]}...")
        return files
    except Exception as e:
        logger.error(f"S3에서 {file_type.upper()} 파일 목록을 가져오는 데 실패했습니다: {str(e)}")
        raise


def start_glue_job(
        job_name: str,
        arguments: Optional[Dict] = None,
        task_id: str = 'default_task_id',
        aws_conn_id: str = 'aws_default',
        region_name: str = 'ap-northeast-2'
):
    """
    Starts a Glue job using Airflow's GlueJobOperator.

    Args:
        job_name (str): The name of the Glue job to run.
        arguments (dict): The arguments to pass to the Glue job.
        task_id (str): The Airflow task ID for the Glue job.
        aws_conn_id (str): The Airflow connection ID for AWS.
        region_name (str): The AWS region where the Glue job is located.
    """
    try:
        glue_job = GlueJobOperator(
            task_id=task_id,
            job_name=job_name,
            region_name=region_name,
            script_args=arguments or {},
            aws_conn_id=aws_conn_id
        )
        logger.info(f"Glue 작업 '{job_name}'을 실행 중입니다...")
        glue_job.execute(context={})
        logger.info(f"Glue 작업 '{job_name}'이 성공적으로 실행되었습니다.")
    except Exception as e:
        logger.error(f"Glue 작업 실행 실패: {str(e)}")
        raise

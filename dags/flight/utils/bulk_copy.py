from typing import List

from common.db_utils import execute_snowflake_query, execute_rds_query
from common.logger import get_logger
from common.sql_utils import get_sql

logger = get_logger(__name__)


def bulk_copy_to_snowflake(parquet_files: List[str]) -> None:
    """
    Loads data from a list of Parquet files into Snowflake using a formatted SQL query.

    Args:
        parquet_files (List[str]): A list of Parquet file paths to be loaded into Snowflake.
    """
    for parquet_file in parquet_files:
        query = get_sql(domain='flight', name='load_flight_to_snowflake')
        formatted_query = query.format(parquet_file=parquet_file)

        try:
            execute_snowflake_query(formatted_query)
            logger.info(f"Parquet 파일 {parquet_file}의 데이터가 Snowflake에 성공적으로 로드되었습니다.")
        except Exception as e:
            logger.error(f"Parquet 파일 {parquet_file}의 데이터 로드 중 오류 발생: {str(e)}")


def bulk_copy_to_rds(csv_files: List[str]) -> None:
    """
    Drops the existing table, creates a new table, and loads data from a list of CSV files into RDS using formatted SQL queries.

    Args:
        csv_files (List[str]): A list of CSV file paths to be loaded into RDS.
    """
    drop_query = get_sql(domain='flight', name='drop_flight_rds')
    try:
        execute_rds_query(drop_query)
        logger.info("기존 RDS 테이블이 성공적으로 삭제되었습니다.")
    except Exception as e:
        logger.error(f"RDS 테이블 삭제 중 오류 발생: {str(e)}")

    create_query = get_sql(domain='flight', name='create_flight_rds')
    try:
        execute_rds_query(create_query)
        logger.info("새 RDS 테이블이 성공적으로 생성되었습니다.")
    except Exception as e:
        logger.error(f"RDS 테이블 생성 중 오류 발생: {str(e)}")

    for csv_file in csv_files:
        query = get_sql(domain='flight', name='load_flight_to_rds')
        formatted_query = query.format(csv_file=csv_file)

        try:
            execute_rds_query(formatted_query)
            logger.info(f"CSV 파일 {csv_file}의 데이터가 RDS에 성공적으로 로드되었습니다.")
        except Exception as e:
            logger.error(f"CSV 파일 {csv_file}의 데이터 로드 중 오류 발생: {str(e)}")

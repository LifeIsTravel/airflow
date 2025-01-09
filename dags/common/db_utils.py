from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from common.logger import get_logger

logger = get_logger(__name__)


def execute_snowflake_query(query: str, conn_id: str = 'snowflake_conn'):
    """
    Executes a query on Snowflake using the specified connection and schema.

    Args:
        query (str): The SQL query to execute.
        conn_id (str): The Airflow connection ID for Snowflake (default is 'snowflake_conn').
        schema (str, optional): The schema to use for the query. If not provided, the default schema will be used.
    """
    try:
        snowflake_hook = SnowflakeHook(snowflake_conn_id=conn_id)
        snowflake_hook.run(query)
        logger.info(f"Snowflake 쿼리 실행됨: {query}")
    except Exception as e:
        logger.error(f"Snowflake 쿼리 실행 오류: {str(e)}")
        raise


def execute_rds_query(query: str, conn_id: str = 'postgres_conn', schema: str = 'lifeistravel'):
    """
    Executes a query on RDS using the specified connection.

    Args:
        query (str): The SQL query to execute.
        conn_id (str): The Airflow connection ID for RDS (default is 'postgres_conn').
    """
    try:
        rds_hook = PostgresHook(postgres_conn_id=conn_id, schema=schema)
        rds_hook.run(query)
        logger.info(f"RDS 쿼리 실행됨: {query} (스키마: {schema})")
    except Exception as e:
        logger.error(f"RDS 쿼리 실행 오류: {str(e)}")
        raise

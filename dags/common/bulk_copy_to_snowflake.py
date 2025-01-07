import logging
import os

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# 로깅설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def bulk_copy_to_snowflake(parquet_files, query_path):
    # Snowflake Hook 인스턴스 생성
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    logging.info("Snowflake hook created")

    delete_query = f"""
            DROP STAGE IF EXISTS TEAM5.raw_data.team5_stage;
        """
    snowflake_hook.run(delete_query)
    logging.info("DROP STAGE Complete")

    create_query = f"""
            CREATE STAGE TEAM5.raw_data.team5_stage
            STORAGE_INTEGRATION = TEAM5_S3_INTEGRATION
            URL = 's3://team5-s3/transform_data/'
        """
    snowflake_hook.run(create_query)
    logging.info("CREATE STAGE Complete")

    # 여러 파일 패턴을 지정하여 COPY
    files_list = "', '".join(parquet_files)  # 파일 목록을 '파일1', '파일2', ... 형태로 변환
    open_path = os.path.join(os.path.dirname(__file__), query_path)

    with open(open_path, 'r') as file:
        query = file.read()

    formatted_query = query.format(files_list=files_list)
    snowflake_hook.run(formatted_query)
    logging.info("COPY INTO Complete")

    logging.info("Loaded clear!")

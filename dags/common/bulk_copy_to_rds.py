import logging
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook


def bulk_copy_to_rds(csv_files, query_path):
    # RDS(PostgreSQL) 연결 설정
    rds_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='lifeistravel')

    # 현재 파일 기준으로 두 단계 상위 디렉토리로 이동
    base_dir = os.path.dirname(os.path.dirname(__file__))  # 'dags' 디렉토리
    load_open_path = os.path.join(base_dir, query_path)

    with open(load_open_path, 'r') as file:
        load_query = file.read()

    # 데이터베이스 연결
    connection = rds_hook.get_conn()
    cursor = connection.cursor()

    try:
        # 1. 기존 테이블 삭제
        logging.info("기존 테이블을 삭제(DROP TABLE) 중...")
        cursor.execute("DROP TABLE IF EXISTS your_table_name;")

        # 2. 새로운 테이블 생성
        logging.info("새로운 테이블을 생성(CREATE TABLE) 중...")
        create_open_path = os.path.join(base_dir, 'sql/create_table_flight_rds.sql')

        with open(create_open_path, 'r') as file:
            create_query = file.read()

        cursor.execute(create_query)

        if not csv_files:
            raise ValueError("CSV 파일이 없습니다.")
        logging.info(f"첫 번째 CSV 파일: {csv_files[0]}")

        # 3. 파일별로 COPY 명령 실행
        for s3_key in csv_files:
            logging.info(f"{s3_key} 파일 로드 중...")
            load_formatted_query = load_query.format(s3_key=s3_key)
            cursor.execute(load_formatted_query)

        # 작업 커밋
        connection.commit()
        logging.info("Full-refresh 완료!")

    except Exception as e:
        logging.error(f"Full-refresh 실패: {e}")
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()

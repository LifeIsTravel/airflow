import os
import sys

from common.logger import get_logger

logger = get_logger(__name__)

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))


def get_sql(domain: str, name: str) -> str:
    """
    Reads and returns the contents of an SQL file.

    Args:
        domain: The domain where the SQL file is located.
        name: The name of the SQL file (without the extension).

    Returns:
        str: The contents of the SQL file.
    """

    # 현재 파일의 절대 경로를 기준으로 상대 경로 설정
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dags_dir = os.path.dirname(current_dir) # dags 폴더까지 올라가기
    sql_path = os.path.join(dags_dir, 'sql', domain, f'{name}.sql')

    try:
        with open(sql_path, 'r') as f:
            return f.read()
    except FileNotFoundError as e:
        logger.error(f"SQL 파일을 찾을 수 없습니다: {sql_path}")
        raise

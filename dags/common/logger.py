import logging
from typing import Optional


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Creates and returns a logger.

    Args:
        name: The name of the logger (default is None).
            * Typically, __name__ is passed to distinguish loggers by module.

    Returns:
        logging.Logger: The configured logger object.

    Examples:
        >>> logger = get_logger(__name__)
        >>> logger.info("Starting the task")
        >>> logger.error("An error occurred")
    """
    logger = logging.getLogger(name)

    # 로거가 이미 핸들러를 가지고 있다면 그대로 반환
    if logger.handlers:
        return logger

    # 기본 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    return logger

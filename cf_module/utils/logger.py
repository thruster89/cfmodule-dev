"""
로깅 유틸리티

모듈 전체에서 사용할 통합 로거를 제공한다.
debug=True 시 파이프라인 흐름을 상세 추적할 수 있다.

로그 파일:
  enable_file_logging() 호출 시 logs/ 디렉토리에 실행 단위 로그 파일 생성.
  파일명: cf_batch_YYYYMMDD_HHMMSS.log
"""

import logging
import os
import sys
from datetime import datetime


_ROOT_NAME = "cf_module"
_CONSOLE_FORMATTER = logging.Formatter(
    "%(name)s:%(lineno)d - %(levelname)s - %(message)s",
)
_FILE_FORMATTER = logging.Formatter(
    "%(asctime)s | %(name)s:%(lineno)d | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_DEBUG_ENABLED = False
_FILE_HANDLER = None  # 전역 파일 핸들러 (실행 단위 1개)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """모듈별 로거를 생성한다."""
    logger = logging.getLogger(f"{_ROOT_NAME}.{name}")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_CONSOLE_FORMATTER)
        logger.addHandler(handler)

    # 파일 핸들러가 활성화되어 있으면 추가
    if _FILE_HANDLER and _FILE_HANDLER not in logger.handlers:
        logger.addHandler(_FILE_HANDLER)

    # enable_debug()가 이미 호출됐으면 DEBUG 유지
    effective_level = logging.DEBUG if _DEBUG_ENABLED else level
    logger.setLevel(effective_level)
    logger.propagate = False  # 부모 로거로 중복 전파 방지
    return logger


def enable_file_logging(
    log_dir: str = "logs",
    prefix: str = "cf_batch",
    level: int = logging.INFO,
) -> str:
    """실행 단위 로그 파일을 생성한다.

    Args:
        log_dir: 로그 디렉토리 (기본: logs/)
        prefix: 파일명 접두사
        level: 파일 로그 레벨

    Returns:
        생성된 로그 파일 경로
    """
    global _FILE_HANDLER

    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"{prefix}_{timestamp}.log")

    _FILE_HANDLER = logging.FileHandler(log_path, encoding="utf-8")
    _FILE_HANDLER.setFormatter(_FILE_FORMATTER)
    _FILE_HANDLER.setLevel(level)

    # 루트 로거에도 추가
    root = logging.getLogger(_ROOT_NAME)
    root.addHandler(_FILE_HANDLER)

    # 이미 생성된 하위 로거에도 추가
    for name in list(logging.Logger.manager.loggerDict):
        if name.startswith(_ROOT_NAME):
            lg = logging.getLogger(name)
            if _FILE_HANDLER not in lg.handlers:
                lg.addHandler(_FILE_HANDLER)

    return log_path


def enable_debug() -> None:
    """cf_module 전체 로거를 DEBUG 레벨로 전환한다.

    이후 get_logger()로 생성되는 로거도 자동으로 DEBUG가 된다.
    """
    global _DEBUG_ENABLED
    _DEBUG_ENABLED = True

    root = logging.getLogger(_ROOT_NAME)
    root.setLevel(logging.DEBUG)

    # 이미 등록된 하위 로거 모두 DEBUG 전환
    for name in list(logging.Logger.manager.loggerDict):
        if name.startswith(_ROOT_NAME):
            logging.getLogger(name).setLevel(logging.DEBUG)

    # 루트 핸들러가 없으면 추가
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_CONSOLE_FORMATTER)
        root.addHandler(handler)

    # 파일 핸들러도 DEBUG로
    if _FILE_HANDLER:
        _FILE_HANDLER.setLevel(logging.DEBUG)

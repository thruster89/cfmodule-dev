"""
로깅 유틸리티

모듈 전체에서 사용할 통합 로거를 제공한다.
debug=True 시 파이프라인 흐름을 상세 추적할 수 있다.
"""

import logging
import sys


_ROOT_NAME = "cf_module"
_FORMATTER = logging.Formatter(
    "%(name)s:%(lineno)d - %(levelname)s - %(message)s",
)
_DEBUG_ENABLED = False


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """모듈별 로거를 생성한다."""
    logger = logging.getLogger(f"{_ROOT_NAME}.{name}")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_FORMATTER)
        logger.addHandler(handler)

    # enable_debug()가 이미 호출됐으면 DEBUG 유지
    effective_level = logging.DEBUG if _DEBUG_ENABLED else level
    logger.setLevel(effective_level)
    logger.propagate = False  # 부모 로거로 중복 전파 방지
    return logger


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
        handler.setFormatter(_FORMATTER)
        root.addHandler(handler)

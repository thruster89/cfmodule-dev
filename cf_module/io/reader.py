"""
통합 데이터 리더

SQLite3, DuckDB, CSV 등 다양한 소스에서 데이터를 읽어 pandas DataFrame으로 반환한다.
queries/ 디렉토리(개별 .sql) 또는 queries.json 기반 SQL 쿼리 실행을 지원한다.
"""

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd

from cf_module.config import DBConfig
from cf_module.utils.logger import get_logger

logger = get_logger("reader")


# DuckDB :name → $name 변환 패턴
_NAMED_PARAM_RE = re.compile(r":([A-Za-z_]\w*)")

# DuckDB VARCHAR BETWEEN 보정: col BETWEEN $x AND $y → CAST 적용
_BETWEEN_RE = re.compile(
    r"(\w+)\s+BETWEEN\s+(\$\w+)\s+AND\s+(\$\w+)",
    re.IGNORECASE,
)


def _named_to_dollar(sql: str) -> str:
    """SQL 내 :name 파라미터를 $name 으로 변환한다 (DuckDB 호환)."""
    return _NAMED_PARAM_RE.sub(r"$\1", sql)


def _cast_between_for_duckdb(sql: str) -> str:
    """DuckDB 전체 VARCHAR 환경에서 BETWEEN을 CAST(BIGINT)으로 감싼다.

    VARCHAR BETWEEN은 사전순 비교이므로 숫자 범위 쿼리에서 오작동한다.
    예: '8830' BETWEEN '8830' AND '8840' → '88300'도 포함됨
    """
    return _BETWEEN_RE.sub(
        r"CAST(\1 AS BIGINT) BETWEEN CAST(\2 AS BIGINT) AND CAST(\3 AS BIGINT)",
        sql,
    )


class DataReader:
    """통합 데이터 리더"""

    def __init__(self, db_config: DBConfig):
        self.db_config = db_config
        self._queries: dict = {}

    def load_queries(self, queries_path: str) -> dict:
        """SQL 쿼리 정의를 로드한다.

        queries_path가 디렉토리이면 *.sql 파일을 개별 로딩하고,
        .json 파일이면 기존 queries.json 형식으로 로딩한다.
        """
        path = Path(queries_path)

        if path.is_dir():
            self._queries = {}
            for sql_file in sorted(path.glob("*.sql")):
                sql_text = sql_file.read_text(encoding="utf-8").strip()
                self._queries[sql_file.stem] = {"query": sql_text}
            logger.debug("[SQL] queries/ 디렉토리에서 %d개 쿼리 로딩", len(self._queries))
        elif path.suffix == ".json" and path.is_file():
            with open(path, "r", encoding="utf-8") as f:
                self._queries = json.load(f)
            logger.debug("[SQL] queries.json에서 %d개 쿼리 로딩", len(self._queries))
        else:
            raise FileNotFoundError(f"쿼리 경로를 찾을 수 없습니다: {queries_path}")

        return self._queries

    def execute_query(
        self,
        query: str,
        params: Union[dict, tuple, None] = None,
    ) -> pd.DataFrame:
        """SQL 쿼리를 실행하고 DataFrame을 반환한다."""
        if params is None:
            params = {}
        db_type = self.db_config.db_type

        if db_type == "sqlite":
            return self._execute_sqlite(query, params)
        elif db_type == "duckdb":
            return self._execute_duckdb(query, params)
        else:
            raise ValueError(f"지원하지 않는 DB 타입: {db_type}")

    def fetch_data(
        self,
        query_name: str,
        params: Union[dict, tuple, None] = None,
    ) -> Optional[pd.DataFrame]:
        """쿼리 이름으로 데이터를 조회한다. (기존 query_manager.fetch_data 역할)"""
        query_def = self._queries.get(query_name)
        if not query_def:
            raise KeyError(f"쿼리를 찾을 수 없습니다: {query_name}")

        sql = query_def.get("query", "")
        if not sql:
            raise ValueError(f"쿼리 SQL이 비어 있습니다: {query_name}")

        logger.debug("[SQL] %-30s params=%s", query_name, params)
        df = self.execute_query(sql, params)
        logger.debug("[SQL] %-30s → %d행 × %d컬럼", query_name, len(df), len(df.columns))
        return df

    def read_csv(
        self,
        file_path: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """CSV 파일을 읽어 DataFrame을 반환한다."""
        return pd.read_csv(file_path, **kwargs)

    def read_excel(
        self,
        file_path: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Excel 파일을 읽어 DataFrame을 반환한다."""
        return pd.read_excel(file_path, **kwargs)

    # -- private methods --

    def _execute_sqlite(
        self, query: str, params: Union[dict, tuple]
    ) -> pd.DataFrame:
        """SQLite3 쿼리 실행 — dict/tuple 모두 네이티브 지원"""
        with sqlite3.connect(self.db_config.path) as conn:
            return pd.read_sql_query(query, conn, params=params or None)

    def _execute_duckdb(
        self, query: str, params: Union[dict, tuple]
    ) -> pd.DataFrame:
        """DuckDB 쿼리 실행 — :name → $name 자동 변환

        DuckDB 전체 VARCHAR 환경에서는 파라미터를 문자열로 변환하여
        타입 불일치(Cannot mix VARCHAR and INTEGER) 에러를 방지한다.
        """
        try:
            import duckdb
        except ImportError:
            raise ImportError("DuckDB를 사용하려면 `pip install duckdb`를 실행하세요.")

        # dict params일 때 :name → $name 변환 + BETWEEN CAST + 값 str 통일
        if isinstance(params, dict):
            query = _named_to_dollar(query)
            query = _cast_between_for_duckdb(query)
            param_arg = {k: str(v) for k, v in params.items()}
        else:
            param_arg = [str(v) for v in params] if params else []

        con = duckdb.connect(self.db_config.path)
        try:
            result = con.execute(query, param_arg)
            return result.fetchdf()
        finally:
            con.close()

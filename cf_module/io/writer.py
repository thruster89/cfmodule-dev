"""
결과 출력 모듈

프로젝션 결과를 CSV, Excel, DB 등 다양한 형식으로 저장한다.
"""

import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from cf_module.config import DBConfig, OutputConfig


class DataWriter:
    """결과 데이터 출력"""

    def __init__(self, output_config: OutputConfig):
        self.config = output_config
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)

    def write(
        self,
        df: pd.DataFrame,
        name: str,
        fmt: Optional[str] = None,
    ) -> str:
        """DataFrame을 지정된 형식으로 저장한다.

        Args:
            df: 저장할 데이터
            name: 파일/테이블 이름 (확장자 제외)
            fmt: 출력 형식 ("csv", "excel", "db"). None이면 config 기본값 사용.

        Returns:
            저장된 파일 경로 또는 테이블 이름
        """
        fmt = fmt or self.config.output_format

        if fmt == "csv":
            return self._write_csv(df, name)
        elif fmt == "excel":
            return self._write_excel(df, name)
        elif fmt == "db":
            return self._write_db(df, name)
        else:
            raise ValueError(f"지원하지 않는 출력 형식: {fmt}")

    def _write_csv(self, df: pd.DataFrame, name: str) -> str:
        path = Path(self.config.output_dir) / f"{name}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return str(path)

    def _write_excel(self, df: pd.DataFrame, name: str) -> str:
        path = Path(self.config.output_dir) / f"{name}.xlsx"
        df.to_excel(path, index=False)
        return str(path)

    def _write_db(self, df: pd.DataFrame, name: str) -> str:
        db_config = self.config.output_db
        if db_config is None:
            raise ValueError("DB 출력을 위해 output_db 설정이 필요합니다.")

        if db_config.db_type == "sqlite":
            with sqlite3.connect(db_config.path) as conn:
                df.to_sql(name, conn, if_exists="replace", index=False)
        elif db_config.db_type == "postgresql":
            self._write_postgresql(df, name, db_config)
        else:
            raise ValueError(f"지원하지 않는 DB 타입: {db_config.db_type}")

        return name

    def _write_postgresql(
        self, df: pd.DataFrame, name: str, db_config: DBConfig
    ) -> None:
        try:
            import psycopg2
            from psycopg2.extras import execute_values
        except ImportError:
            raise ImportError(
                "PostgreSQL 출력을 위해 `pip install psycopg2-binary`를 실행하세요."
            )

        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password,
        )
        try:
            cur = conn.cursor()
            # 테이블 생성 (간이 DDL)
            cols = ", ".join(f'"{c}" TEXT' for c in df.columns)
            cur.execute(
                f'DROP TABLE IF EXISTS {db_config.schema}."{name}";'
            )
            cur.execute(
                f'CREATE TABLE {db_config.schema}."{name}" ({cols});'
            )
            # 데이터 삽입
            values = [tuple(row) for row in df.to_numpy()]
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_sql = (
                f'INSERT INTO {db_config.schema}."{name}" '
                f"VALUES ({placeholders})"
            )
            cur.executemany(insert_sql, values)
            conn.commit()
        finally:
            conn.close()

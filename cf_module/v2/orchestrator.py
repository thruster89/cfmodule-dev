"""
오케스트레이터: 그룹별 청크 처리 + 멀티프로세싱

처리 흐름:
1. meta_assm_profile에서 그룹 목록 조회
2. 그룹별로 가정 데이터 1회 로드
3. 그룹 내 계약을 chunk_size 단위로 분할
4. 각 청크를 project_group()으로 벡터 처리
5. 결과를 Parquet 스트리밍 출력

멀티프로세싱:
- 그룹 간 독립 → Pool.map()으로 병렬화
- 그룹 내 청크는 순차 (메모리 제어)
"""

import os
import time
from dataclasses import dataclass
from multiprocessing import Pool, cpu_count
from typing import Callable, List, Optional

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from cf_module.v2.engine import (
    GroupAssumptions,
    ProjectionResultV2,
    load_group_assumptions,
    project_group,
)


@dataclass
class OrchestratorConfig:
    """오케스트레이터 설정."""
    db_path: str                         # v2 DuckDB 경로
    output_dir: str = "./output/v2"      # Parquet 출력 디렉토리
    chunk_size: int = 50_000             # 청크 당 계약 수
    n_workers: int = 1                   # 병렬 워커 수 (1=단일)
    max_proj_months: int = 1200          # 최대 프로젝션 개월
    clos_ym: str = "202309"              # 결산년월


# Parquet 출력 스키마
RESULT_SCHEMA = pa.schema([
    ("contract_id", pa.int32()),
    ("month", pa.int16()),
    ("tpx", pa.float64()),
    ("pay_tpx", pa.float64()),
    ("qx_monthly", pa.float64()),
    ("wx_monthly", pa.float64()),
    ("d_death", pa.float64()),
    ("d_lapse", pa.float64()),
    ("d_rsvamt", pa.float64()),
    ("d_bnft", pa.float64()),
    ("d_pyexsp", pa.float64()),
])


def run_all(
    config: OrchestratorConfig,
    progress_callback: Optional[Callable] = None,
) -> str:
    """전체 프로젝션 실행.

    Args:
        config: 오케스트레이터 설정
        progress_callback: 진행률 콜백 (current, total, profile)

    Returns:
        출력 Parquet 디렉토리 경로
    """
    os.makedirs(config.output_dir, exist_ok=True)

    conn = duckdb.connect(config.db_path, read_only=True)

    # 1. 프로파일 목록 조회
    profiles = conn.execute("""
        SELECT assm_profile, n_contracts
        FROM meta_assm_profile
        WHERE n_contracts > 0
        ORDER BY n_contracts DESC
    """).fetchdf()

    conn.close()

    total_contracts = int(profiles["n_contracts"].sum())
    n_profiles = len(profiles)
    print(f"[v2] {n_profiles}개 프로파일, 총 {total_contracts:,}건 처리 시작")

    t0 = time.time()

    if config.n_workers <= 1:
        # 단일 프로세스
        for i, row in profiles.iterrows():
            _process_profile(
                config.db_path,
                row["assm_profile"],
                config.chunk_size,
                config.max_proj_months,
                config.clos_ym,
                config.output_dir,
            )
            if progress_callback:
                progress_callback(i + 1, n_profiles, row["assm_profile"])
    else:
        # 멀티프로세싱
        args_list = [
            (
                config.db_path,
                row["assm_profile"],
                config.chunk_size,
                config.max_proj_months,
                config.clos_ym,
                config.output_dir,
            )
            for _, row in profiles.iterrows()
        ]

        with Pool(min(config.n_workers, cpu_count())) as pool:
            for i, _ in enumerate(pool.starmap(_process_profile, args_list)):
                if progress_callback:
                    progress_callback(i + 1, n_profiles, "")

    elapsed = time.time() - t0
    rate = total_contracts / elapsed if elapsed > 0 else 0
    print(f"[v2] 완료: {total_contracts:,}건, {elapsed:.1f}초, {rate:,.0f}건/초")

    return config.output_dir


def _process_profile(
    db_path: str,
    assm_profile: str,
    chunk_size: int,
    max_proj_months: int,
    clos_ym: str,
    output_dir: str,
) -> None:
    """하나의 가정 프로파일에 속하는 모든 계약을 처리.

    가정 데이터 1회 로드 → 청크 단위 프로젝션 → Parquet 출력.
    """
    conn = duckdb.connect(db_path, read_only=True)

    # 그룹 내 첫 계약 ID (위험률 매핑 조회용)
    sample = conn.execute("""
        SELECT contract_id
        FROM dim_contract
        WHERE assm_profile = ?
        LIMIT 1
    """, [assm_profile]).fetchone()

    if sample is None:
        conn.close()
        return

    sample_id = sample[0]

    # 가정 데이터 로드 (이 그룹에서 1회만!)
    assm = load_group_assumptions(
        conn, assm_profile, sample_id, max_proj_months
    )

    # Parquet writer 열기
    safe_name = assm_profile.replace("|", "_").replace("/", "_")
    pq_path = os.path.join(output_dir, f"{safe_name}.parquet")
    writer = None

    # 청크 단위 처리
    offset = 0
    while True:
        chunk_df = conn.execute("""
            SELECT
                contract_id, entry_age, bterm, pterm, ctr_ym
            FROM dim_contract
            WHERE assm_profile = ?
            ORDER BY contract_id
            LIMIT ? OFFSET ?
        """, [assm_profile, chunk_size, offset]).fetchdf()

        if chunk_df.empty:
            break

        # 경과월 계산
        clos_year = int(clos_ym[:4])
        clos_month = int(clos_ym[4:6])
        ctr_yms = chunk_df["ctr_ym"].values.astype(str)
        elapsed = np.array([
            _calc_elapsed_months(ym, clos_year, clos_month)
            for ym in ctr_yms
        ], dtype=np.int32)

        # 벡터 프로젝션
        result = project_group(
            conn=conn,
            assm=assm,
            contract_ids=chunk_df["contract_id"].values,
            entry_ages=chunk_df["entry_age"].values.astype(int),
            bterms=chunk_df["bterm"].values.astype(int),
            pterms=chunk_df["pterm"].values.astype(int),
            elapsed_months=elapsed,
            clos_ym=clos_ym,
            max_proj_months=max_proj_months,
        )

        # 결과 → Parquet
        batch = _result_to_arrow_batch(result)
        if batch is not None:
            if writer is None:
                writer = pq.ParquetWriter(pq_path, RESULT_SCHEMA)
            writer.write_batch(batch)

        offset += chunk_size

    if writer is not None:
        writer.close()

    conn.close()


def _calc_elapsed_months(ctr_ym: str, clos_year: int, clos_month: int) -> int:
    """계약년월로부터 결산일 기준 경과월 계산."""
    try:
        ctr_year = int(str(ctr_ym)[:4])
        ctr_month = int(str(ctr_ym)[4:6])
        return (clos_year - ctr_year) * 12 + (clos_month - ctr_month) + 1
    except (ValueError, IndexError):
        return 1


def _result_to_arrow_batch(result: ProjectionResultV2) -> Optional[pa.RecordBatch]:
    """ProjectionResultV2 → PyArrow RecordBatch (flat 구조)."""
    n = len(result.contract_ids)
    max_t = result.tpx.shape[1] if result.tpx.shape[1] > 0 else 0

    if n == 0 or max_t == 0:
        return None

    total_rows = n * max_t

    return pa.RecordBatch.from_arrays(
        [
            pa.array(np.repeat(result.contract_ids, max_t).astype(np.int32)),
            pa.array(np.tile(np.arange(1, max_t + 1, dtype=np.int16), n)),
            pa.array(result.tpx.ravel()),
            pa.array(result.pay_tpx.ravel()),
            pa.array(result.qx_monthly.ravel()),
            pa.array(result.wx_monthly.ravel()),
            pa.array(result.d_death.ravel()),
            pa.array(result.d_lapse.ravel()),
            pa.array(result.d_rsvamt.ravel()),
            pa.array(result.d_bnft.ravel()),
            pa.array(result.d_pyexsp.ravel()),
        ],
        schema=RESULT_SCHEMA,
    )


# ============================================================
# 결과 조회 유틸리티
# ============================================================

def read_results(output_dir: str) -> "pa.Table":
    """출력 Parquet 디렉토리에서 전체 결과를 읽는다."""
    import pyarrow.parquet as pq
    return pq.read_table(output_dir)


def query_contract(output_dir: str, contract_id: int) -> dict:
    """특정 계약의 프로젝션 결과를 조회한다."""
    conn = duckdb.connect()
    df = conn.execute(f"""
        SELECT *
        FROM read_parquet('{output_dir}/*.parquet')
        WHERE contract_id = ?
        ORDER BY month
    """, [contract_id]).fetchdf()
    conn.close()
    return df.to_dict("list") if not df.empty else {}

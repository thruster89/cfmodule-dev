"""
CF 프로젝션 통합 파이프라인.

OD_TBL_MN (위험률/해지율/유지자수) → OD_TRAD_PV (준비금/보험료/환급금)
전체 흐름을 단일 모듈로 통합.

Usage:
    from cf_module.pipeline import run_trad_pv_pipeline

    con = duckdb.connect('duckdb_transform.duckdb', read_only=True)
    results, stats = run_trad_pv_pipeline(con, tpcd_filter=('0', '9'))
    con.close()
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import duckdb
import numpy as np

from cf_module.calc.trad_pv import (
    TradPVResult,
    apply_soff_af_netting,
    compute_trad_pv,
)
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached
from cf_module.utils.logger import get_logger

logger = get_logger("pipeline")


# ---------------------------------------------------------------------------
# 결과 컨테이너
# ---------------------------------------------------------------------------

@dataclass
class PipelineStats:
    """파이프라인 실행 통계."""
    total_contracts: int = 0
    computed: int = 0
    errors: int = 0
    n_groups: int = 0
    elapsed_cache: float = 0.0
    elapsed_mn_load: float = 0.0
    elapsed_compute: float = 0.0
    elapsed_total: float = 0.0
    group_stats: List[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# MN 데이터 로더
# ---------------------------------------------------------------------------

def load_mn_data(
    con: duckdb.DuckDBPyConnection,
    target_ids: Optional[Set[int]] = None,
) -> Dict[int, dict]:
    """OD_TBL_MN에서 프로젝션에 필요한 배열을 IDNO별로 로드.

    Returns:
        {idno: {"pay_trmo": np.ndarray, "ctr_trmo": np.ndarray,
                "ctr_trme": np.ndarray}}
    """
    df = con.execute("""
        SELECT INFRC_IDNO,
               CTR_TRMO_MTNPSN_CNT, PAY_TRMO_MTNPSN_CNT,
               CTR_TRME_MTNPSN_CNT
        FROM OD_TBL_MN
        WHERE INFRC_SEQ = 1
        ORDER BY INFRC_IDNO, SETL_AFT_PASS_MMCNT
    """).fetchdf()

    result = {}
    for idno, g in df.groupby('INFRC_IDNO'):
        if target_ids is not None and idno not in target_ids:
            continue
        result[idno] = {
            "pay_trmo": g['PAY_TRMO_MTNPSN_CNT'].values,
            "ctr_trmo": g['CTR_TRMO_MTNPSN_CNT'].values,
            "ctr_trme": g['CTR_TRME_MTNPSN_CNT'].values,
        }
    return result


def load_pv_step_counts(
    con: duckdb.DuckDBPyConnection,
    target_ids: Optional[Set[int]] = None,
) -> Dict[int, int]:
    """OD_TRAD_PV에서 IDNO별 시점 수를 로드.

    MN 데이터가 없는 경우의 n_steps 결정용.
    """
    df = con.execute("""
        SELECT INFRC_IDNO, COUNT(*) AS cnt
        FROM OD_TBL_MN
        WHERE INFRC_SEQ = 1
        GROUP BY INFRC_IDNO
    """).fetchdf()
    result = {}
    for _, row in df.iterrows():
        idno = int(row['INFRC_IDNO'])
        if target_ids is not None and idno not in target_ids:
            continue
        result[idno] = int(row['cnt'])
    return result


# ---------------------------------------------------------------------------
# 핵심 파이프라인
# ---------------------------------------------------------------------------

def run_trad_pv_pipeline(
    con: duckdb.DuckDBPyConnection,
    tpcd_filter: Tuple[str, ...] = ("0", "9"),
    cov_filter: Optional[str] = None,
    prod_cls_filter: Optional[Tuple[str, str]] = None,
    idno_filter: Optional[Set[int]] = None,
    progress_callback=None,
) -> Tuple[Dict[int, TradPVResult], PipelineStats]:
    """OD_TRAD_PV 통합 파이프라인.

    실행 흐름:
      1. 참조 데이터 캐시 (TradPVDataCache)
      2. 대상 계약 필터링 + (PROD_CD, CLS_CD) 그룹핑
      3. OD_TBL_MN 일괄 로드
      4. 그룹별 계산 + 인라인 CTR_POLNO netting
      5. 결과 반환

    계산 순서 (의존성):
      STEP 1: 보험료 (시간축, 납입여부, 원수/할인/적립보험료)
      STEP 2: 미경과보험료 PRPD (→ KICS에 선행)
      STEP 3: 이율 배열 pubano/lwst (→ 적립금 부리에 선행)
      STEP 4: 적립금 (BAS 보간 / NoBAS 이율부리)
      STEP 5: 환급금 (SOFF_BF, SOFF_AF, LTRMNAT)
      STEP 6: KICS = (SOFF_AF + PRPD_PREM) × CTR_TRME
      STEP 7: 약관대출

    Args:
        con: DuckDB 연결 (duckdb_transform.duckdb)
        tpcd_filter: CTR_TPCD 필터 (기본: '0','9')
        cov_filter: 특정 COV_CD만 계산 (None=전체)
        prod_cls_filter: 특정 (PROD_CD, CLS_CD)만 계산
        idno_filter: 특정 IDNO 집합만 계산
        progress_callback: fn(group_idx, n_groups, prod_cd, cls_cd, n_contracts)

    Returns:
        (results, stats)
        results: {idno: TradPVResult}
        stats: PipelineStats
    """
    stats = PipelineStats()
    t_start = time.time()

    # --- 1. 참조 데이터 캐시 ---
    t0 = time.time()
    cache = TradPVDataCache(con)
    stats.elapsed_cache = time.time() - t0
    logger.info(f"Cache loaded: {stats.elapsed_cache:.2f}s")

    # --- 2. 대상 계약 필터링 ---
    target = {}
    for idno, v in cache.infrc.items():
        if str(v["ctr_tpcd"]) not in tpcd_filter:
            continue
        if cov_filter and v["cov_cd"] != cov_filter:
            continue
        if prod_cls_filter and (v["prod_cd"], v["cls_cd"]) != prod_cls_filter:
            continue
        if idno_filter and idno not in idno_filter:
            continue
        target[idno] = v

    target_ids = set(target.keys())
    stats.total_contracts = len(target_ids)

    # (PROD_CD, CLS_CD) 기준 그룹핑
    # → 동일 CTR_POLNO가 반드시 같은 그룹에 속하므로 netting 인라인 가능
    prod_cls_groups: Dict[Tuple[str, str], List[int]] = {}
    for idno, v in target.items():
        key = (v["prod_cd"], v["cls_cd"])
        prod_cls_groups.setdefault(key, []).append(idno)

    stats.n_groups = len(prod_cls_groups)
    logger.info(f"Target: {stats.total_contracts:,} contracts, "
                f"{stats.n_groups} (PROD,CLS) groups")

    # --- 3. OD_TBL_MN 일괄 로드 ---
    t0 = time.time()
    mn_data = load_mn_data(con, target_ids)
    step_counts = load_pv_step_counts(con, target_ids)
    stats.elapsed_mn_load = time.time() - t0
    logger.info(f"MN loaded: {stats.elapsed_mn_load:.2f}s ({len(mn_data):,} contracts)")

    # --- 4. 그룹별 계산 + 인라인 netting ---
    t0 = time.time()
    all_results: Dict[int, TradPVResult] = {}
    idno_to_cov = {idno: v["cov_cd"] for idno, v in cache.infrc.items()}

    sorted_groups = sorted(prod_cls_groups.items(), key=lambda x: -len(x[1]))
    for gi, ((prod_cd, cls_cd), idnos) in enumerate(sorted_groups):
        t_grp = time.time()
        batch_results: Dict[int, TradPVResult] = {}
        ctr_trme_map: Dict[int, np.ndarray] = {}
        batch_ok = 0
        batch_err = 0

        for idno in idnos:
            info = build_contract_info_cached(cache, idno)
            if not info:
                batch_err += 1
                continue

            n_steps = step_counts.get(idno)
            if not n_steps:
                batch_err += 1
                continue

            mn = mn_data.get(idno)
            pay_trmo = mn["pay_trmo"] if mn else None
            ctr_trmo = mn["ctr_trmo"] if mn else None
            ctr_trme = mn["ctr_trme"] if mn else None

            try:
                result = compute_trad_pv(
                    info, n_steps,
                    pay_trmo=pay_trmo,
                    ctr_trmo=ctr_trmo,
                    ctr_trme=ctr_trme,
                )
            except Exception as e:
                batch_err += 1
                if batch_err <= 3:
                    logger.warning(f"ERR IDNO={idno}: {e}")
                continue

            batch_results[idno] = result
            if ctr_trme is not None:
                ctr_trme_map[idno] = ctr_trme
            batch_ok += 1

        # 인라인 netting: CTR_POLNO별 SOFF_AF 상계
        polno_sub = {}
        for idno in batch_results:
            polno = target[idno].get("ctr_polno", "")
            if polno:
                polno_sub.setdefault(polno, []).append(idno)
        if polno_sub:
            apply_soff_af_netting(batch_results, polno_sub, ctr_trme_map, idno_to_cov)

        all_results.update(batch_results)
        stats.errors += batch_err

        grp_elapsed = time.time() - t_grp
        stats.group_stats.append({
            "prod_cd": prod_cd, "cls_cd": cls_cd,
            "n_contracts": len(idnos), "ok": batch_ok,
            "err": batch_err, "elapsed": grp_elapsed,
        })

        if progress_callback:
            progress_callback(gi + 1, stats.n_groups, prod_cd, cls_cd, len(idnos))

    stats.computed = len(all_results)
    stats.elapsed_compute = time.time() - t0
    stats.elapsed_total = time.time() - t_start

    logger.info(f"Pipeline done: {stats.computed:,} computed, "
                f"{stats.errors} errors, {stats.elapsed_total:.1f}s total")

    return all_results, stats


# ---------------------------------------------------------------------------
# 단건 파이프라인 (디버그/검증용)
# ---------------------------------------------------------------------------

def compute_single(
    con: duckdb.DuckDBPyConnection,
    idno: int,
    cache: Optional[TradPVDataCache] = None,
) -> Optional[TradPVResult]:
    """단건 OD_TRAD_PV 계산 (디버그용).

    Args:
        con: DuckDB 연결
        idno: INFRC_IDNO
        cache: 기존 캐시 (없으면 새로 생성)

    Returns:
        TradPVResult or None
    """
    if cache is None:
        cache = TradPVDataCache(con)

    info = build_contract_info_cached(cache, idno)
    if not info:
        logger.warning(f"IDNO={idno}: build_contract_info failed")
        return None

    mn = con.execute(f"""
        SELECT CTR_TRMO_MTNPSN_CNT, PAY_TRMO_MTNPSN_CNT, CTR_TRME_MTNPSN_CNT
        FROM OD_TBL_MN
        WHERE INFRC_IDNO={idno} AND INFRC_SEQ=1
        ORDER BY SETL_AFT_PASS_MMCNT
    """).fetchdf()

    n_steps = len(mn) if len(mn) > 0 else 0
    if n_steps == 0:
        logger.warning(f"IDNO={idno}: no MN data")
        return None

    return compute_trad_pv(
        info, n_steps,
        pay_trmo=mn['PAY_TRMO_MTNPSN_CNT'].values,
        ctr_trmo=mn['CTR_TRMO_MTNPSN_CNT'].values,
        ctr_trme=mn['CTR_TRME_MTNPSN_CNT'].values,
    )

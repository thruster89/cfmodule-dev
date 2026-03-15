"""
CF 프로젝션 통합 파이프라인.

RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN 전체 체인.
OD_ 테이블 의존 없이 raw 테이블에서 자체 산출.

Usage:
    from cf_module.pipeline import run_pipeline, run_trad_pv_pipeline

    con = duckdb.connect('duckdb_transform.duckdb', read_only=True)

    # 전체 체인 (RSK→LAPSE→MN→PV→BN)
    results, stats = run_pipeline(con)

    # TRAD_PV만 (MN 자체 산출)
    pv_results, stats = run_trad_pv_pipeline(con)

    con.close()
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import duckdb
import numpy as np

from cf_module.calc.tbl_bn import BNResult, compute_bn
from cf_module.calc.tbl_lapse_rt import compute_lapse_rt
from cf_module.calc.tbl_mn import compute_tbl_mn
from cf_module.calc.tbl_rsk_rt import compute_rsk_rt
from cf_module.calc.trad_pv import (
    TradPVResult,
    apply_soff_af_netting,
    compute_trad_pv,
)
from cf_module.data.bn_loader import BNDataCache
from cf_module.data.rsk_lapse_loader import ContractInfo, RawAssumptionLoader
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached
from cf_module.run import compute_n_steps
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
    elapsed_mn_compute: float = 0.0
    elapsed_pv_compute: float = 0.0
    elapsed_bn_compute: float = 0.0
    elapsed_total: float = 0.0
    group_stats: List[dict] = field(default_factory=list)

    # 하위호환: elapsed_mn_load
    @property
    def elapsed_mn_load(self):
        return self.elapsed_mn_compute


# ---------------------------------------------------------------------------
# MN 체인 산출 (RSK_RT → LAPSE_RT → TBL_MN)
# ---------------------------------------------------------------------------

def _compute_mn_single(loader: RawAssumptionLoader, ctr: ContractInfo, n_steps: int):
    """단건 RSK_RT → LAPSE_RT → TBL_MN 산출.

    Returns:
        (rsk_rt, lapse_rt, tbl_mn)
    """
    risks = loader.load_risk_codes(ctr)
    exit_flags = loader.load_exit_flags(ctr, risks)
    existing_cds = {r.risk_cd for r in risks}
    extra_risks = loader.load_extra_risk_codes(ctr, existing_cds)
    all_risks = risks + extra_risks

    all_risk_cds = [r.risk_cd for r in all_risks]
    mortality = loader.load_mortality_rates(all_risks, ctr)
    beprd = loader.load_beprd(ctr, all_risk_cds)
    invld = loader.load_invld_months(ctr)
    rsk_rt = compute_rsk_rt(ctr, all_risks, mortality, beprd, invld, n_steps)

    lapse_paying, lapse_paidup = loader.load_lapse_rates(ctr)
    skew = loader.load_skew(ctr)
    lapse_rt = compute_lapse_rt(ctr, lapse_paying, lapse_paidup, skew, n_steps)

    qx_rates = {cd: cols["INVLD_TRMNAT_AF_APLY_RSK_RT"]
                for cd, cols in rsk_rt.items()}
    wx_monthly = lapse_rt["APLY_TRMNAT_RT"]

    tbl_mn = compute_tbl_mn(
        ctr, all_risks, qx_rates, wx_monthly, exit_flags, n_steps,
    )
    return rsk_rt, lapse_rt, tbl_mn


def compute_mn_batch(
    con: duckdb.DuckDBPyConnection,
    loader: RawAssumptionLoader,
    target_ids: List[int],
    progress_interval: int = 5000,
) -> Dict[int, dict]:
    """배치 MN 산출 (RSK_RT → LAPSE_RT → TBL_MN).

    Returns:
        {idno: {"ctr": ContractInfo, "n_steps": int,
                "rsk_rt": dict, "lapse_rt": dict, "tbl_mn": dict}}
    """
    results = {}
    n_err = 0
    for i, idno in enumerate(target_ids):
        try:
            ctr = loader.load_contract(idno)
            n_steps = compute_n_steps(ctr)
            rsk_rt, lapse_rt, tbl_mn = _compute_mn_single(loader, ctr, n_steps)
            results[idno] = {
                "ctr": ctr,
                "n_steps": n_steps,
                "rsk_rt": rsk_rt,
                "lapse_rt": lapse_rt,
                "tbl_mn": tbl_mn,
            }
        except Exception as e:
            n_err += 1
            if n_err <= 5:
                logger.warning(f"MN ERR IDNO={idno}: {e}")

        if progress_interval and (i + 1) % progress_interval == 0:
            logger.info(f"  MN [{i+1}/{len(target_ids)}] OK={len(results)} ERR={n_err}")

    logger.info(f"MN batch: {len(results)}/{len(target_ids)} OK, {n_err} errors")
    return results


# ---------------------------------------------------------------------------
# TRAD_PV 파이프라인 (MN 자체 산출 포함)
# ---------------------------------------------------------------------------

def run_trad_pv_pipeline(
    con: duckdb.DuckDBPyConnection,
    tpcd_filter: Tuple[str, ...] = ("0", "9"),
    cov_filter: Optional[str] = None,
    prod_cls_filter: Optional[Tuple[str, str]] = None,
    idno_filter: Optional[Set[int]] = None,
    progress_callback=None,
    fast_mode: bool = False,
    mn_results: Optional[Dict[int, dict]] = None,
) -> Tuple[Dict[int, TradPVResult], PipelineStats]:
    """TRAD_PV 파이프라인 (MN 자체 산출, OD_ 테이블 의존 없음).

    실행 흐름:
      1. 참조 데이터 캐시 (TradPVDataCache + RawAssumptionLoader)
      2. 대상 계약 필터링 + (PROD_CD, CLS_CD) 그룹핑
      3. MN 자체 산출 (RSK_RT → LAPSE_RT → TBL_MN)
      4. TRAD_PV 계산 + 인라인 CTR_POLNO netting
      5. 결과 반환

    Args:
        con: DuckDB 연결
        tpcd_filter: CTR_TPCD 필터
        cov_filter: 특정 COV_CD만
        prod_cls_filter: 특정 (PROD_CD, CLS_CD)만
        idno_filter: 특정 IDNO 집합만
        progress_callback: fn(group_idx, n_groups, prod_cd, cls_cd, n_contracts)
        fast_mode: 성능 우선 모드
        mn_results: 외부에서 미리 산출한 MN 결과 (없으면 자체 산출)

    Returns:
        (results, stats)
    """
    stats = PipelineStats()
    t_start = time.time()

    # --- 1. 캐시 ---
    t0 = time.time()
    cache = TradPVDataCache(con)
    loader = RawAssumptionLoader(con)
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
    prod_cls_groups: Dict[Tuple[str, str], List[int]] = {}
    for idno, v in target.items():
        key = (v["prod_cd"], v["cls_cd"])
        prod_cls_groups.setdefault(key, []).append(idno)

    stats.n_groups = len(prod_cls_groups)
    logger.info(f"Target: {stats.total_contracts:,} contracts, "
                f"{stats.n_groups} (PROD,CLS) groups")

    # --- 3. MN 산출 ---
    t0 = time.time()
    if mn_results is None:
        mn_results = compute_mn_batch(con, loader, list(target_ids))
    stats.elapsed_mn_compute = time.time() - t0
    logger.info(f"MN computed: {stats.elapsed_mn_compute:.2f}s ({len(mn_results):,} contracts)")

    # --- 4. TRAD_PV 계산 + netting ---
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
            mn = mn_results.get(idno)
            if mn is None:
                batch_err += 1
                continue

            info = build_contract_info_cached(cache, idno)
            if not info:
                batch_err += 1
                continue

            tbl_mn = mn["tbl_mn"]
            n_steps = mn["n_steps"]
            pay_trmo = tbl_mn.get("PAY_TRMO_MTNPSN_CNT")
            ctr_trmo = tbl_mn.get("CTR_TRMO_MTNPSN_CNT")
            ctr_trme = tbl_mn.get("CTR_TRME_MTNPSN_CNT")

            try:
                result = compute_trad_pv(
                    info, n_steps,
                    pay_trmo=pay_trmo,
                    ctr_trmo=ctr_trmo,
                    ctr_trme=ctr_trme,
                    fast_mode=fast_mode,
                )
            except Exception as e:
                batch_err += 1
                if batch_err <= 3:
                    logger.warning(f"PV ERR IDNO={idno}: {e}")
                continue

            batch_results[idno] = result
            if ctr_trme is not None:
                ctr_trme_map[idno] = np.asarray(ctr_trme, dtype=np.float64)
            batch_ok += 1

        # netting
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
    stats.elapsed_pv_compute = time.time() - t0
    stats.elapsed_total = time.time() - t_start

    logger.info(f"PV pipeline done: {stats.computed:,} computed, "
                f"{stats.errors} errors, {stats.elapsed_total:.1f}s total")

    return all_results, stats


# ---------------------------------------------------------------------------
# 전체 파이프라인 (RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN)
# ---------------------------------------------------------------------------

def run_pipeline(
    con: duckdb.DuckDBPyConnection,
    tpcd_filter: Tuple[str, ...] = ("0", "9"),
    idno_filter: Optional[Set[int]] = None,
    include_trad_pv: bool = True,
    include_bn: bool = True,
    fast_mode: bool = False,
    progress_interval: int = 5000,
) -> Tuple[Dict[int, dict], PipelineStats]:
    """전체 CF 파이프라인 (OD_ 테이블 의존 없음).

    RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN 순차 산출.

    Returns:
        ({idno: {"ctr", "n_steps", "rsk_rt", "lapse_rt", "tbl_mn",
                 "trad_pv", "tbl_bn"}}, stats)
    """
    stats = PipelineStats()
    t_start = time.time()

    # --- 1. 캐시 로드 ---
    t0 = time.time()
    loader = RawAssumptionLoader(con)
    trad_cache = TradPVDataCache(con) if include_trad_pv else None
    bn_cache = BNDataCache(con) if include_bn else None
    stats.elapsed_cache = time.time() - t0
    logger.info(f"Cache loaded: {stats.elapsed_cache:.2f}s")

    # --- 2. 대상 계약 ---
    if idno_filter:
        target_ids = list(idno_filter)
    else:
        # TPCD 필터 적용
        tpcd_clause = ""
        if tpcd_filter:
            tpcd_in = ",".join(f"'{t}'" for t in tpcd_filter)
            tpcd_clause = f" AND CAST(CTR_TPCD AS VARCHAR) IN ({tpcd_in})"
        target_ids = [r[0] for r in con.execute(
            f"SELECT DISTINCT INFRC_IDNO FROM II_INFRC "
            f"WHERE INFRC_SEQ = 1{tpcd_clause} ORDER BY INFRC_IDNO"
        ).fetchall()]

    stats.total_contracts = len(target_ids)
    logger.info(f"Target: {stats.total_contracts:,} contracts")

    # --- 3. MN 배치 산출 (RSK_RT → LAPSE_RT → TBL_MN) ---
    t0 = time.time()
    mn_results = compute_mn_batch(con, loader, target_ids, progress_interval)
    stats.elapsed_mn_compute = time.time() - t0

    # --- CTR_POLNO 일괄 로드 (PV netting용) ---
    polno_map = {}  # {idno: ctr_polno}
    polno_rows = con.execute(
        "SELECT INFRC_IDNO, CTR_POLNO FROM II_INFRC WHERE INFRC_SEQ=1"
    ).fetchall()
    for r in polno_rows:
        if r[1]:
            polno_map[r[0]] = r[1]

    # --- 4. TRAD_PV ---
    pv_results = {}
    if include_trad_pv and trad_cache:
        t0 = time.time()
        # (PROD_CD, CLS_CD) 그룹핑 for netting
        prod_cls_groups: Dict[Tuple[str, str], List[int]] = {}
        for idno, mn in mn_results.items():
            ctr = mn["ctr"]
            key = (ctr.prod_cd, ctr.cls_cd)
            prod_cls_groups.setdefault(key, []).append(idno)

        idno_to_cov = {}
        pv_err = 0
        for (prod_cd, cls_cd), idnos in sorted(prod_cls_groups.items()):
            batch_pv: Dict[int, TradPVResult] = {}
            ctr_trme_map: Dict[int, np.ndarray] = {}

            for idno in idnos:
                mn = mn_results[idno]
                info = build_contract_info_cached(trad_cache, idno)
                if not info:
                    continue

                tbl_mn = mn["tbl_mn"]
                n_steps = mn["n_steps"]
                ctr = mn["ctr"]
                idno_to_cov[idno] = ctr.cov_cd

                try:
                    pv = compute_trad_pv(
                        info, n_steps,
                        pay_trmo=tbl_mn.get("PAY_TRMO_MTNPSN_CNT"),
                        ctr_trmo=tbl_mn.get("CTR_TRMO_MTNPSN_CNT"),
                        ctr_trme=tbl_mn.get("CTR_TRME_MTNPSN_CNT"),
                        fast_mode=fast_mode,
                    )
                    batch_pv[idno] = pv
                    ctr_trme_v = tbl_mn.get("CTR_TRME_MTNPSN_CNT")
                    if ctr_trme_v is not None:
                        ctr_trme_map[idno] = np.asarray(ctr_trme_v, dtype=np.float64)
                except Exception as e:
                    pv_err += 1
                    if pv_err <= 5:
                        logger.warning(f"PV ERR IDNO={idno}: {e}")

            # netting
            polno_sub = {}
            for idno in batch_pv:
                polno = polno_map.get(idno, "")
                if polno:
                    polno_sub.setdefault(polno, []).append(idno)
            if polno_sub:
                apply_soff_af_netting(batch_pv, polno_sub, ctr_trme_map, idno_to_cov)

            pv_results.update(batch_pv)

        stats.elapsed_pv_compute = time.time() - t0
        logger.info(f"PV computed: {len(pv_results):,} contracts, "
                     f"{pv_err} errors, {stats.elapsed_pv_compute:.1f}s")

    # --- 5. TBL_BN ---
    bn_results: Dict[int, BNResult] = {}
    if include_bn and bn_cache:
        t0 = time.time()

        # join_amt 일괄 로드
        join_amt_map = {}
        ja_rows = con.execute(
            "SELECT INFRC_IDNO, GRNTPT_JOIN_AMT FROM II_INFRC WHERE INFRC_SEQ=1"
        ).fetchall()
        for r in ja_rows:
            join_amt_map[r[0]] = float(r[1] or 0)

        bn_err = 0
        for i, (idno, mn) in enumerate(mn_results.items()):
            ctr = mn["ctr"]
            rsk_rt = mn["rsk_rt"]
            lapse_rt = mn["lapse_rt"]
            n_steps = mn["n_steps"]
            p, cl, cv = ctr.prod_cd, ctr.cls_cd, ctr.cov_cd

            # CLS_CD fallback
            bn_cls = "01"
            bnft_mapping = bn_cache.get_bnft_risk_mapping(p, bn_cls, cv)
            if not bnft_mapping:
                bn_cls = cl
                bnft_mapping = bn_cache.get_bnft_risk_mapping(p, bn_cls, cv)
            if not bnft_mapping:
                continue

            join_amt = join_amt_map.get(idno, 0.0)

            qx_rates = {cd: cols["INVLD_TRMNAT_AF_APLY_RSK_RT"]
                        for cd, cols in rsk_rt.items()}
            wx_monthly = lapse_rt["APLY_TRMNAT_RT"]

            acum_bnft = None
            pv = pv_results.get(idno)
            if pv:
                acum_bnft = pv.to_dict().get("APLY_PREM_ACUMAMT_BNFT")

            try:
                bn = compute_bn(
                    idno=idno, n_steps=n_steps,
                    prod_cd=p, cls_cd=cl, cov_cd=cv,
                    pass_yy=ctr.pass_yy, pass_mm=ctr.pass_mm,
                    bterm_yy=ctr.bterm_yy, join_amt=join_amt,
                    qx_monthly_rates=qx_rates,
                    wx_monthly=wx_monthly,
                    risk_meta=bn_cache.get_risk_meta(p, cl, cv),
                    rsvamt_cds=bn_cache.get_rsvamt_cds(p, cl, cv),
                    bnft_mapping=bnft_mapping,
                    get_defry_rate_fn=lambda bno, yr, _p=p, _c=bn_cls, _v=cv: (
                        bn_cache.get_defry_rate(_p, _c, _v, bno, yr)),
                    get_prtt_rate_fn=lambda bno, yr, _p=p, _c=bn_cls, _v=cv: (
                        bn_cache.get_prtt_rate(_p, _c, _v, bno, yr)),
                    get_ncov_months_fn=lambda bno, _p=p, _c=bn_cls, _v=cv: (
                        bn_cache.get_ncov_months(_p, _c, _v, bno)),
                    aply_prem_acumamt_bnft=acum_bnft,
                )
                bn_results[idno] = bn
            except Exception as e:
                bn_err += 1
                if bn_err <= 5:
                    logger.warning(f"BN ERR IDNO={idno}: {e}")

            if progress_interval and (i + 1) % progress_interval == 0:
                logger.info(f"  BN [{i+1}/{len(mn_results)}] OK={len(bn_results)} ERR={bn_err}")

        stats.elapsed_bn_compute = time.time() - t0
        logger.info(f"BN computed: {len(bn_results):,} contracts, "
                     f"{bn_err} errors, {stats.elapsed_bn_compute:.1f}s")

    # --- 결과 조립 ---
    all_results = {}
    for idno, mn in mn_results.items():
        all_results[idno] = {
            "ctr": mn["ctr"],
            "n_steps": mn["n_steps"],
            "rsk_rt": mn["rsk_rt"],
            "lapse_rt": mn["lapse_rt"],
            "tbl_mn": mn["tbl_mn"],
            "trad_pv": pv_results.get(idno),
            "tbl_bn": bn_results.get(idno),
        }

    stats.computed = len(all_results)
    stats.elapsed_total = time.time() - t_start
    logger.info(f"Pipeline done: {stats.computed:,} contracts, "
                f"{stats.elapsed_total:.1f}s total")

    return all_results, stats

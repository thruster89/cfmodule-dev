"""단독 산출 파이프라인.

DB 기대값 비교 없이 RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN 순차 산출.

Usage:
    import duckdb
    from cf_module.run import run_single, run_batch

    con = duckdb.connect('duckdb_transform.duckdb', read_only=True)

    # 단건
    result = run_single(con, 760397)

    # 단건 (TRAD_PV/BN 제외)
    result = run_single(con, 760397, include_trad_pv=False, include_bn=False)

    # 다건 (캐시 공유)
    results = run_batch(con, [760397, 124, 1028627])

    con.close()
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

import duckdb
import numpy as np

from cf_module.calc.tbl_bn import BNResult, compute_bn
from cf_module.calc.tbl_lapse_rt import compute_lapse_rt
from cf_module.calc.tbl_mn import compute_tbl_mn
from cf_module.calc.tbl_rsk_rt import compute_rsk_rt
from cf_module.calc.trad_pv import TradPVResult, apply_soff_af_netting, compute_trad_pv
from cf_module.data.bn_loader import BNDataCache
from cf_module.data.rsk_lapse_loader import ContractInfo, RawAssumptionLoader
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached


@dataclass
class SingleResult:
    """계약 1건의 전체 산출 결과."""
    idno: int
    n_steps: int
    ctr: ContractInfo
    rsk_rt: Dict[str, dict]
    lapse_rt: dict
    tbl_mn: dict
    trad_pv: Optional[TradPVResult] = None
    tbl_bn: Optional[BNResult] = None


def compute_n_steps(ctr: ContractInfo) -> int:
    """계약의 프로젝션 스텝 수 계산 (bterm - elapsed + 1)."""
    elapsed = ctr.pass_yy * 12 + ctr.pass_mm
    bterm_months = ctr.bterm_yy * 12
    return max(bterm_months - elapsed + 1, 1)


# ---------------------------------------------------------------------------
# 핵심 체인: RSK_RT → LAPSE_RT → TBL_MN
# ---------------------------------------------------------------------------

def _compute_mn_chain(
    loader: RawAssumptionLoader,
    ctr: ContractInfo,
    n_steps: int,
    return_dedup: bool = False,
):
    """RSK_RT → LAPSE_RT → TBL_MN 체인 산출.

    Returns:
        (rsk_rt, lapse_rt, tbl_mn) or (rsk_rt, lapse_rt, tbl_mn, dedup)
    """
    risks = loader.load_risk_codes(ctr)
    exit_flags = loader.load_exit_flags(ctr, risks)

    # 추가 위험코드 (C1/C2 등)
    existing_cds = {r.risk_cd for r in risks}
    extra_risks = loader.load_extra_risk_codes(ctr, existing_cds)
    all_risks = risks + extra_risks

    # STEP 1: RSK_RT
    all_risk_cds = [r.risk_cd for r in all_risks]
    mortality = loader.load_mortality_rates(all_risks, ctr)
    beprd = loader.load_beprd(ctr, all_risk_cds)
    invld = loader.load_invld_months(ctr)
    rsk_rt = compute_rsk_rt(ctr, all_risks, mortality, beprd, invld, n_steps)

    # STEP 2: LAPSE_RT
    lapse_paying, lapse_paidup = loader.load_lapse_rates(ctr)
    skew = loader.load_skew(ctr)
    lapse_rt = compute_lapse_rt(ctr, lapse_paying, lapse_paidup, skew, n_steps)

    # STEP 3: TBL_MN
    qx_rates = {cd: cols["INVLD_TRMNAT_AF_APLY_RSK_RT"]
                for cd, cols in rsk_rt.items()}
    wx_monthly = lapse_rt["APLY_TRMNAT_RT"]

    if return_dedup:
        tbl_mn, dedup = compute_tbl_mn(
            ctr, all_risks, qx_rates, wx_monthly, exit_flags, n_steps,
            return_dedup=True,
        )
        return rsk_rt, lapse_rt, tbl_mn, dedup

    tbl_mn = compute_tbl_mn(
        ctr, all_risks, qx_rates, wx_monthly, exit_flags, n_steps,
    )
    return rsk_rt, lapse_rt, tbl_mn


# ---------------------------------------------------------------------------
# TRAD_PV (CTR_POLNO netting 포함)
# ---------------------------------------------------------------------------

def _compute_trad_pv_single(
    con: duckdb.DuckDBPyConnection,
    loader: RawAssumptionLoader,
    trad_cache: TradPVDataCache,
    idno: int,
    tbl_mn: dict,
    n_steps: int,
) -> Optional[TradPVResult]:
    """TRAD_PV 산출 (같은 CTR_POLNO 그룹 netting 포함)."""
    info = build_contract_info_cached(trad_cache, idno)
    if not info:
        return None

    pay_trmo = tbl_mn.get("PAY_TRMO_MTNPSN_CNT")
    ctr_trmo = tbl_mn.get("CTR_TRMO_MTNPSN_CNT")
    ctr_trme = tbl_mn.get("CTR_TRME_MTNPSN_CNT")

    # CTR_POLNO 그룹 조회
    polno_row = con.execute("""
        SELECT CTR_POLNO FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1
    """, [idno]).fetchone()
    polno = polno_row[0] if polno_row else None

    group_idnos = [idno]
    idno_to_cov = {}
    if polno:
        grp = con.execute("""
            SELECT INFRC_IDNO, COV_CD FROM II_INFRC
            WHERE CTR_POLNO = ? AND INFRC_SEQ = 1
            ORDER BY INFRC_IDNO
        """, [polno]).fetchall()
        group_idnos = [r[0] for r in grp]
        idno_to_cov = {r[0]: r[1] for r in grp}

    results = {}
    ctr_trme_map = {}

    for gid in group_idnos:
        g_info = build_contract_info_cached(trad_cache, gid)
        if not g_info:
            continue

        if gid == idno:
            g_pay_trmo, g_ctr_trmo, g_ctr_trme = pay_trmo, ctr_trmo, ctr_trme
            g_n_steps = n_steps
        else:
            # 같은 POLNO 내 다른 계약은 MN 체인 자체 산출
            g_ctr_obj = loader.load_contract(gid)
            g_n_steps = compute_n_steps(g_ctr_obj)
            _, _, g_mn = _compute_mn_chain(loader, g_ctr_obj, g_n_steps)
            g_pay_trmo = g_mn.get("PAY_TRMO_MTNPSN_CNT")
            g_ctr_trmo = g_mn.get("CTR_TRMO_MTNPSN_CNT")
            g_ctr_trme = g_mn.get("CTR_TRME_MTNPSN_CNT")

        try:
            r = compute_trad_pv(
                g_info, g_n_steps,
                pay_trmo=g_pay_trmo, ctr_trmo=g_ctr_trmo, ctr_trme=g_ctr_trme,
            )
        except Exception:
            continue

        results[gid] = r
        if g_ctr_trme is not None:
            ctr_trme_map[gid] = np.asarray(g_ctr_trme, dtype=np.float64)

    # Netting
    if len(group_idnos) > 1 and polno:
        polno_map = {polno: group_idnos}
        apply_soff_af_netting(results, polno_map, ctr_trme_map, idno_to_cov)

    return results.get(idno)


# ---------------------------------------------------------------------------
# TBL_BN
# ---------------------------------------------------------------------------

def _compute_bn_single(
    con: duckdb.DuckDBPyConnection,
    bn_cache: BNDataCache,
    ctr: ContractInfo,
    dedup: dict,
    n_steps: int,
    aply_prem_acumamt_bnft: Optional[np.ndarray] = None,
) -> Optional[BNResult]:
    """TBL_BN 산출 (MN dedup 결과 기반)."""
    bnft_mapping = bn_cache.get_bnft_risk_mapping(
        ctr.prod_cd, ctr.cls_cd, ctr.cov_cd)
    if not bnft_mapping:
        return None

    join_amt_row = con.execute("""
        SELECT GRNTPT_JOIN_AMT FROM II_INFRC
        WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1
    """, [ctr.idno]).fetchone()
    join_amt = float(join_amt_row[0] or 0) if join_amt_row else 0.0

    p, cl, cv = ctr.prod_cd, ctr.cls_cd, ctr.cov_cd
    return compute_bn(
        idno=ctr.idno,
        n_steps=n_steps,
        prod_cd=p, cls_cd=cl, cov_cd=cv,
        pass_yy=ctr.pass_yy, pass_mm=ctr.pass_mm,
        bterm_yy=ctr.bterm_yy,
        join_amt=join_amt,
        risk_cds=dedup["risk_cds"],
        qx_ctr_per_risk=dedup["qx_ctr_per_risk"],
        exit_idx_ctr=dedup["exit_idx_ctr"],
        is_exit_rsv=dedup["is_exit_rsv"],
        wx_ctr=dedup["wx_ctr"],
        bnft_mapping=bnft_mapping,
        get_defry_rate_fn=lambda bno, yr: bn_cache.get_defry_rate(p, cl, cv, bno, yr),
        get_prtt_rate_fn=lambda bno, yr: bn_cache.get_prtt_rate(p, cl, cv, bno, yr),
        get_ncov_months_fn=lambda bno: bn_cache.get_ncov_months(p, cl, cv, bno),
        aply_prem_acumamt_bnft=aply_prem_acumamt_bnft,
    )


# ---------------------------------------------------------------------------
# 공개 API
# ---------------------------------------------------------------------------

def run_single(
    con: duckdb.DuckDBPyConnection,
    idno: int,
    trad_cache: Optional[TradPVDataCache] = None,
    bn_cache: Optional[BNDataCache] = None,
    loader: Optional[RawAssumptionLoader] = None,
    include_trad_pv: bool = True,
    include_bn: bool = True,
) -> SingleResult:
    """단건 전체 파이프라인 (DB 비교 없음).

    계산 순서: RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN

    Args:
        con: DuckDB 연결
        idno: INFRC_IDNO
        trad_cache: TradPVDataCache (없으면 자동 생성, 다건 시 공유 권장)
        bn_cache: BNDataCache (없으면 자동 생성, 다건 시 공유 권장)
        loader: RawAssumptionLoader (없으면 자동 생성, 다건 시 공유 권장)
        include_trad_pv: TRAD_PV 산출 포함 여부
        include_bn: TBL_BN 산출 포함 여부

    Returns:
        SingleResult (rsk_rt, lapse_rt, tbl_mn, trad_pv, tbl_bn)
    """
    if loader is None:
        loader = RawAssumptionLoader(con)

    ctr = loader.load_contract(idno)
    n_steps = compute_n_steps(ctr)
    need_dedup = include_bn

    # STEP 1~3: RSK_RT → LAPSE_RT → TBL_MN
    if need_dedup:
        rsk_rt, lapse_rt, tbl_mn, dedup = _compute_mn_chain(
            loader, ctr, n_steps, return_dedup=True)
    else:
        rsk_rt, lapse_rt, tbl_mn = _compute_mn_chain(
            loader, ctr, n_steps)
        dedup = None

    result = SingleResult(
        idno=idno, n_steps=n_steps, ctr=ctr,
        rsk_rt=rsk_rt, lapse_rt=lapse_rt, tbl_mn=tbl_mn,
    )

    # STEP 4: TRAD_PV
    if include_trad_pv:
        if trad_cache is None:
            trad_cache = TradPVDataCache(con)
        result.trad_pv = _compute_trad_pv_single(
            con, loader, trad_cache, idno, tbl_mn, n_steps)

    # STEP 5: TBL_BN
    if include_bn and dedup is not None:
        if bn_cache is None:
            bn_cache = BNDataCache(con)
        acum_bnft = None
        if result.trad_pv:
            d = result.trad_pv.to_dict()
            acum_bnft = d.get("APLY_PREM_ACUMAMT_BNFT")
        result.tbl_bn = _compute_bn_single(
            con, bn_cache, ctr, dedup, n_steps,
            aply_prem_acumamt_bnft=acum_bnft,
        )

    return result


def run_batch(
    con: duckdb.DuckDBPyConnection,
    idnos: Optional[List[int]] = None,
    include_trad_pv: bool = True,
    include_bn: bool = True,
    progress_interval: int = 1000,
) -> Dict[int, SingleResult]:
    """다건 파이프라인 (캐시 공유).

    Args:
        con: DuckDB 연결
        idnos: 대상 IDNO 목록 (None=전체)
        include_trad_pv: TRAD_PV 포함 여부
        include_bn: TBL_BN 포함 여부
        progress_interval: 진행 로그 출력 간격

    Returns:
        {idno: SingleResult}
    """
    if idnos is None:
        idnos = [r[0] for r in con.execute(
            "SELECT DISTINCT INFRC_IDNO FROM II_INFRC "
            "WHERE INFRC_SEQ = 1 ORDER BY INFRC_IDNO"
        ).fetchall()]

    # 캐시 공유
    loader = RawAssumptionLoader(con)
    trad_cache = TradPVDataCache(con) if include_trad_pv else None
    bn_cache = BNDataCache(con) if include_bn else None

    results = {}
    errors = 0
    for i, idno in enumerate(idnos):
        if progress_interval and (i + 1) % progress_interval == 0:
            print(f"  {i+1}/{len(idnos)}...")
        try:
            results[idno] = run_single(
                con, idno,
                trad_cache=trad_cache,
                bn_cache=bn_cache,
                loader=loader,
                include_trad_pv=include_trad_pv,
                include_bn=include_bn,
            )
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  IDNO={idno}: ERROR: {e}")

    print(f"  완료: {len(results)}/{len(idnos)} (ERROR={errors})")
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_summary(result: SingleResult, tables: List[str]):
    """결과 요약 출력."""
    ctr = result.ctr
    print(f"\n  계약: PROD={ctr.prod_cd} CLS={ctr.cls_cd} COV={ctr.cov_cd}")
    print(f"  가입연령={ctr.entry_age} 보험기간={ctr.bterm_yy}Y "
          f"납입기간={ctr.pterm_yy}Y 경과={ctr.pass_yy}Y{ctr.pass_mm}M")
    print(f"  n_steps={result.n_steps}")

    if "RSK_RT" in tables:
        risk_cds = list(result.rsk_rt.keys())
        print(f"\n  [RSK_RT] {len(risk_cds)} risks: {risk_cds}")
        for rsk_cd, cols in result.rsk_rt.items():
            af = cols["INVLD_TRMNAT_AF_APLY_RSK_RT"]
            nz = (af != 0).sum()
            print(f"    {rsk_cd}: AF non-zero={nz}/{len(af)}"
                  f"  range=[{af.min():.6e}, {af.max():.6e}]")

    if "LAPSE_RT" in tables:
        aply = result.lapse_rt["APLY_TRMNAT_RT"]
        print(f"\n  [LAPSE_RT] APLY range=[{aply.min():.6e}, {aply.max():.6e}]")

    if "MN" in tables:
        mn = result.tbl_mn
        print(f"\n  [TBL_MN] 18 cols")
        print(f"    CTR_TRME[end]={mn['CTR_TRME_MTNPSN_CNT'][-1]:.8f}"
              f"  PAY_TRME[end]={mn['PAY_TRME_MTNPSN_CNT'][-1]:.8f}")

    if "TRAD_PV" in tables and result.trad_pv:
        d = result.trad_pv.to_dict()
        print(f"\n  [TRAD_PV] {len(d)} cols")
        for k in ["ORIG_PREM", "SOFF_BF_TMRFND", "SOFF_AF_TMRFND"]:
            if k in d:
                arr = d[k]
                print(f"    {k}: [{arr.min():.2f}, {arr.max():.2f}]")
    elif "TRAD_PV" in tables:
        print(f"\n  [TRAD_PV] 없음")

    if "BN" in tables and result.tbl_bn:
        nb = len(result.tbl_bn.bnft_results)
        print(f"\n  [TBL_BN] {nb} BNFTs")
        for bno, br in sorted(result.tbl_bn.bnft_results.items()):
            py_sum = br.pyamt.sum()
            print(f"    BNFT_NO={bno}: PYAMT_sum={py_sum:,.0f}")
    elif "BN" in tables:
        print(f"\n  [TBL_BN] 없음")


def main():
    import argparse
    import sys
    import time

    parser = argparse.ArgumentParser(
        description="CF 단독 산출 파이프라인 (RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN)")
    parser.add_argument("--idno", type=int, required=True, help="INFRC_IDNO")
    parser.add_argument("--table", type=str, default=None,
                        help="특정 테이블까지만 산출 (RSK_RT, LAPSE_RT, MN, TRAD_PV, BN)")
    parser.add_argument("--no-pv", action="store_true", help="TRAD_PV 제외")
    parser.add_argument("--no-bn", action="store_true", help="TBL_BN 제외")
    parser.add_argument("--db", type=str, default="duckdb_transform.duckdb",
                        help="DB 경로 (기본: duckdb_transform.duckdb)")
    args = parser.parse_args()

    ALL_TABLES = ["RSK_RT", "LAPSE_RT", "MN", "TRAD_PV", "BN"]

    # --table 처리: 해당 테이블까지만 포함
    if args.table:
        tbl = args.table.upper()
        if tbl not in ALL_TABLES:
            print(f"ERROR: --table은 {ALL_TABLES} 중 하나")
            return 1
        cut = ALL_TABLES.index(tbl) + 1
        tables = ALL_TABLES[:cut]
    else:
        tables = ALL_TABLES[:]

    if args.no_pv and "TRAD_PV" in tables:
        tables.remove("TRAD_PV")
    if args.no_bn and "BN" in tables:
        tables.remove("BN")

    include_pv = "TRAD_PV" in tables
    include_bn = "BN" in tables

    print(f"{'='*60}")
    print(f"  CF 단독 산출: IDNO={args.idno}")
    print(f"  테이블: {' → '.join(tables)}")
    print(f"{'='*60}")

    t0 = time.time()
    con = duckdb.connect(args.db, read_only=True)

    result = run_single(con, args.idno,
                        include_trad_pv=include_pv,
                        include_bn=include_bn)

    _print_summary(result, tables)

    elapsed = time.time() - t0
    print(f"\n  총 소요: {elapsed:.2f}s")
    print(f"{'='*60}")

    con.close()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

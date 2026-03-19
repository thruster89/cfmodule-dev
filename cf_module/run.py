"""단독 산출 파이프라인.

RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN → EXP → CF → DC_RT → PVCF → BEL

Usage:
    python -m cf_module.run --idno 760397               # BEL만 출력 (기본)
    python -m cf_module.run --idno 760397 --table MN     # 특정 단계까지
    python -m cf_module.run --idno 760397 --debug        # 전체 중간테이블 CSV 출력
    python -m cf_module.run --idno 760397 --debug --save RSK_RT,CF,BEL  # 선택 출력
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import duckdb
import numpy as np

from cf_module.calc.bel import BELResult, compute_bel
from cf_module.calc.cf import CFResult, compute_cf
from cf_module.calc.dc_rt import DCRTResult, compute_dc_rt
from cf_module.calc.exp import ExpResult, compute_exp
from cf_module.calc.pvcf import PVCFResult, compute_pvcf
from cf_module.calc.tbl_bn import BNResult, compute_bn
from cf_module.calc.tbl_lapse_rt import compute_lapse_rt
from cf_module.calc.tbl_mn import compute_tbl_mn
from cf_module.calc.tbl_rsk_rt import compute_rsk_rt
from cf_module.calc.trad_pv import TradPVResult, apply_soff_af_netting, compute_trad_pv
from cf_module.data.bn_loader import BNDataCache
from cf_module.data.exp_loader import ExpDataCache
from cf_module.data.rsk_lapse_loader import ContractInfo, RawAssumptionLoader
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached

# 전체 테이블 순서
ALL_TABLES = ["RSK_RT", "LAPSE_RT", "MN", "TRAD_PV", "BN", "EXP", "CF", "DC_RT", "PVCF", "BEL"]


# ---------------------------------------------------------------------------
# 결과 컨테이너
# ---------------------------------------------------------------------------

@dataclass
class SingleResult:
    """계약 1건의 전체 산출 결과."""
    idno: int
    n_steps: int
    ctr: ContractInfo
    rsk_rt: Dict[str, dict] = field(default_factory=dict)
    lapse_rt: dict = field(default_factory=dict)
    tbl_mn: dict = field(default_factory=dict)
    trad_pv: Optional[TradPVResult] = None
    tbl_bn: Optional[BNResult] = None
    exp_results: Optional[List[ExpResult]] = None
    exp_items: Optional[list] = None
    cf: Optional[CFResult] = None
    dc_rt: Optional[DCRTResult] = None
    pvcf: Optional[PVCFResult] = None
    bel: Optional[BELResult] = None


def compute_n_steps(ctr: ContractInfo) -> int:
    """프로젝션 스텝 수 (bterm - elapsed + 1)."""
    elapsed = ctr.pass_yy * 12 + ctr.pass_mm
    return max(ctr.bterm_yy * 12 - elapsed + 1, 1)


# ---------------------------------------------------------------------------
# STEP 1~3: RSK_RT → LAPSE_RT → TBL_MN
# ---------------------------------------------------------------------------

def _compute_mn_chain(loader, ctr, n_steps):
    """RSK_RT → LAPSE_RT → TBL_MN."""
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

    qx_rates = {cd: cols["INVLD_TRMNAT_AF_APLY_RSK_RT"] for cd, cols in rsk_rt.items()}
    wx_monthly = lapse_rt["APLY_TRMNAT_RT"]
    tbl_mn = compute_tbl_mn(ctr, all_risks, qx_rates, wx_monthly, exit_flags, n_steps)

    return rsk_rt, lapse_rt, tbl_mn


# ---------------------------------------------------------------------------
# STEP 4: TRAD_PV
# ---------------------------------------------------------------------------

def _compute_trad_pv_single(con, loader, trad_cache, idno, tbl_mn, n_steps,
                             polno_map=None, mn_cache=None):
    """TRAD_PV (CTR_POLNO netting 포함).

    Args:
        polno_map: {idno: (polno, [(idno, cov_cd), ...])} 사전 매핑.
                   제공 시 SQL 조회 생략.
        mn_cache:  {idno: (rsk_rt, lapse_rt, tbl_mn)} MN 결과 캐시.
                   제공 시 그룹 내 타 IDNO의 MN 중복계산 생략.
    """
    info = build_contract_info_cached(trad_cache, idno)
    if not info:
        return None

    pay_trmo = tbl_mn.get("PAY_TRMO_MTNPSN_CNT")
    ctr_trmo = tbl_mn.get("CTR_TRMO_MTNPSN_CNT")
    ctr_trme = tbl_mn.get("CTR_TRME_MTNPSN_CNT")

    # POLNO 그룹 조회 (사전 매핑 우선)
    if polno_map is not None and idno in polno_map:
        polno, grp_list = polno_map[idno]
        group_idnos = [gid for gid, _ in grp_list]
        idno_to_cov = {gid: cov for gid, cov in grp_list}
    else:
        polno_row = con.execute(
            "SELECT CTR_POLNO FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
            [idno]).fetchone()
        polno = polno_row[0] if polno_row else None

        group_idnos = [idno]
        idno_to_cov = {}
        if polno:
            grp = con.execute(
                "SELECT INFRC_IDNO, COV_CD FROM II_INFRC "
                "WHERE CTR_POLNO = ? AND INFRC_SEQ = 1 ORDER BY INFRC_IDNO",
                [polno]).fetchall()
            group_idnos = [r[0] for r in grp]
            idno_to_cov = {r[0]: r[1] for r in grp}

    results = {}
    ctr_trme_map = {}
    for gid in group_idnos:
        g_info = build_contract_info_cached(trad_cache, gid)
        if not g_info:
            continue
        if gid == idno:
            g_pay_trmo, g_ctr_trmo, g_ctr_trme, g_n = pay_trmo, ctr_trmo, ctr_trme, n_steps
        else:
            # mn_cache에서 먼저 조회
            if mn_cache is not None and gid in mn_cache:
                _, _, g_mn = mn_cache[gid]
                g_ctr_obj = loader.load_contract(gid)
                g_n = compute_n_steps(g_ctr_obj)
            else:
                g_ctr_obj = loader.load_contract(gid)
                g_n = compute_n_steps(g_ctr_obj)
                _, _, g_mn = _compute_mn_chain(loader, g_ctr_obj, g_n)
                # 캐시에 저장
                if mn_cache is not None:
                    mn_cache[gid] = (None, None, g_mn)
            g_pay_trmo = g_mn.get("PAY_TRMO_MTNPSN_CNT")
            g_ctr_trmo = g_mn.get("CTR_TRMO_MTNPSN_CNT")
            g_ctr_trme = g_mn.get("CTR_TRME_MTNPSN_CNT")
        try:
            r = compute_trad_pv(g_info, g_n, pay_trmo=g_pay_trmo, ctr_trmo=g_ctr_trmo, ctr_trme=g_ctr_trme)
        except Exception:
            continue
        results[gid] = r
        if g_ctr_trme is not None:
            ctr_trme_map[gid] = np.asarray(g_ctr_trme, dtype=np.float64)

    if len(group_idnos) > 1 and polno:
        apply_soff_af_netting(results, {polno: group_idnos}, ctr_trme_map, idno_to_cov)

    return results.get(idno)


# ---------------------------------------------------------------------------
# STEP 5: TBL_BN
# ---------------------------------------------------------------------------

def _compute_bn_single(con, bn_cache, ctr, rsk_rt, lapse_rt, n_steps, acum_bnft=None):
    """TBL_BN."""
    p, cl, cv = ctr.prod_cd, ctr.cls_cd, ctr.cov_cd
    bn_cls = "01"
    bnft_mapping = bn_cache.get_bnft_risk_mapping(p, bn_cls, cv)
    if not bnft_mapping:
        bn_cls = cl
        bnft_mapping = bn_cache.get_bnft_risk_mapping(p, bn_cls, cv)
    if not bnft_mapping:
        return None

    join_amt_row = con.execute(
        "SELECT GRNTPT_JOIN_AMT FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
        [ctr.idno]).fetchone()
    join_amt = float(join_amt_row[0] or 0) if join_amt_row else 0.0

    qx_rates = {cd: cols["INVLD_TRMNAT_AF_APLY_RSK_RT"] for cd, cols in rsk_rt.items()}
    wx_monthly = lapse_rt["APLY_TRMNAT_RT"]

    return compute_bn(
        idno=ctr.idno, n_steps=n_steps,
        prod_cd=p, cls_cd=cl, cov_cd=cv,
        pass_yy=ctr.pass_yy, pass_mm=ctr.pass_mm,
        bterm_yy=ctr.bterm_yy, join_amt=join_amt,
        qx_monthly_rates=qx_rates, wx_monthly=wx_monthly,
        risk_meta=bn_cache.get_risk_meta(p, cl, cv),
        rsvamt_cds=bn_cache.get_rsvamt_cds(p, cl, cv),
        bnft_mapping=bnft_mapping,
        get_defry_rate_fn=lambda bno, yr: bn_cache.get_defry_rate(p, bn_cls, cv, bno, yr),
        get_prtt_rate_fn=lambda bno, yr: bn_cache.get_prtt_rate(p, bn_cls, cv, bno, yr),
        get_ncov_months_fn=lambda bno: bn_cache.get_ncov_months(p, bn_cls, cv, bno),
        aply_prem_acumamt_bnft=acum_bnft,
    )


# ---------------------------------------------------------------------------
# STEP 6: EXP
# ---------------------------------------------------------------------------

def _compute_exp_single(exp_cache, ctr, n_steps, pv_d, gprem=None, val5=None):
    """EXP 사업비 산출 (SQL 없음, 캐시 기반)."""
    elapsed = ctr.pass_yy * 12 + ctr.pass_mm
    pterm_mm = ctr.pterm_yy * 12

    acqs_grp, mnt_grp = exp_cache.get_prod_grps(ctr.prod_cd, ctr.cls_cd)
    if acqs_grp is None and mnt_grp is None:
        return [], []

    if val5 is None:
        val5 = getattr(ctr, 'assm_div_val5', None)
    grp5 = exp_cache.get_grp_cd5(val5)

    items = exp_cache.get_exp_items(acqs_grp, mnt_grp, grp5)
    if not items:
        return [], []

    if gprem is None:
        gprem = getattr(ctr, 'gprem', 0) or 0

    cncttp = pv_d.get("CNCTTP_ACUMAMT_KICS", np.zeros(n_steps)) if pv_d else np.zeros(n_steps)
    loan = pv_d.get("LOAN_REMAMT", np.zeros(n_steps)) if pv_d else np.zeros(n_steps)

    results = compute_exp(n_steps, elapsed, gprem, items, exp_cache,
                          pterm_mm=pterm_mm, cncttp_kics=cncttp, loan_remamt=loan)
    return results, items


# ---------------------------------------------------------------------------
# 공개 API
# ---------------------------------------------------------------------------

def run_single(
    con: duckdb.DuckDBPyConnection,
    idno: int,
    trad_cache: Optional[TradPVDataCache] = None,
    bn_cache: Optional[BNDataCache] = None,
    exp_cache: Optional[ExpDataCache] = None,
    loader: Optional[RawAssumptionLoader] = None,
    include_trad_pv: bool = True,
    include_bn: bool = True,
) -> SingleResult:
    """단건 전체 파이프라인.

    RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN → EXP → CF → DC_RT → PVCF → BEL
    """
    if loader is None:
        loader = RawAssumptionLoader(con)

    ctr = loader.load_contract(idno)
    n_steps = compute_n_steps(ctr)

    # STEP 1~3: RSK_RT → LAPSE_RT → TBL_MN
    rsk_rt, lapse_rt, tbl_mn = _compute_mn_chain(loader, ctr, n_steps)

    result = SingleResult(idno=idno, n_steps=n_steps, ctr=ctr,
                          rsk_rt=rsk_rt, lapse_rt=lapse_rt, tbl_mn=tbl_mn)

    # STEP 4: TRAD_PV
    if include_trad_pv:
        if trad_cache is None:
            polno_row = con.execute(
                "SELECT CTR_POLNO FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
                [idno]).fetchone()
            polno = polno_row[0] if polno_row else None
            group_ids = {idno}
            if polno:
                grp = con.execute(
                    "SELECT INFRC_IDNO FROM II_INFRC WHERE CTR_POLNO = ? AND INFRC_SEQ = 1",
                    [polno]).fetchall()
                group_ids = {r[0] for r in grp}
            trad_cache = TradPVDataCache(con, idno_filter=group_ids)
        result.trad_pv = _compute_trad_pv_single(con, loader, trad_cache, idno, tbl_mn, n_steps)

    # STEP 5: TBL_BN
    if include_bn:
        if bn_cache is None:
            bn_cache = BNDataCache(con, pcv_filter=[(ctr.prod_cd, ctr.cov_cd)])
        acum_bnft = None
        if result.trad_pv:
            acum_bnft = result.trad_pv.to_dict().get("APLY_PREM_ACUMAMT_BNFT")
        result.tbl_bn = _compute_bn_single(con, bn_cache, ctr, rsk_rt, lapse_rt, n_steps, acum_bnft)

    # STEP 6~10: EXP → CF → DC_RT → PVCF → BEL
    if include_trad_pv and include_bn and result.trad_pv and result.tbl_bn:
        pv_d = result.trad_pv.to_dict()

        # STEP 6: EXP
        if exp_cache is None:
            exp_cache = ExpDataCache(con)
        # gprem/val5: TradPVDataCache에서 가져오기 (SQL 제거)
        infrc_raw = trad_cache.infrc.get(idno, {}) if trad_cache else {}
        gprem = infrc_raw.get("gprem") or infrc_raw.get("effective_gprem", 0)
        val5 = None
        # ContractInfo에 assm_div_vals가 있으면 사용
        if hasattr(ctr, 'assm_div_vals') and ctr.assm_div_vals:
            val5 = ctr.assm_div_vals.get(5)
        if val5 is None:
            # 단건 fallback: DB 조회
            row = con.execute(
                "SELECT ASSM_DIV_VAL5 FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
                [idno]).fetchone()
            val5 = str(row[0]) if row and row[0] else None
        result.exp_results, result.exp_items = _compute_exp_single(
            exp_cache, ctr, n_steps, pv_d, gprem=gprem, val5=val5)

        # BN INSUAMT 합산
        bn_insuamt = np.zeros(n_steps, dtype=np.float64)
        for br in result.tbl_bn.bnft_results.values():
            bn_insuamt += br.bnft_insuamt

        # LSVY rate
        lsvy_rate = 0.0
        if result.exp_items:
            for tp, kd, it in result.exp_items:
                if tp == "LSVY":
                    lsvy_rate = it.get("rate", 0)

        # STEP 7: CF
        if result.exp_results is not None:
            result.cf = compute_cf(n_steps, tbl_mn, pv_d, bn_insuamt,
                                   result.exp_results, result.exp_items or [], lsvy_rate)

        # STEP 8: DC_RT
        dc_curve = trad_cache.dc_rt_curve if trad_cache else np.array([])
        result.dc_rt = compute_dc_rt(n_steps, dc_curve)

        # STEP 9: PVCF
        if result.cf and result.dc_rt:
            result.pvcf = compute_pvcf(result.cf, result.dc_rt)

        # STEP 10: BEL
        if result.pvcf:
            result.bel = compute_bel(result.pvcf)

    return result


# ---------------------------------------------------------------------------
# CSV 출력
# ---------------------------------------------------------------------------

def _dict_to_csv(data: dict, n_steps: int, path: str, idno: int, elapsed: int = 0):
    """dict(col→array) → CSV."""
    import pandas as pd
    rows = []
    for s in range(n_steps):
        row = {"INFRC_IDNO": idno, "CTR_MM": elapsed + s}
        for col, arr in data.items():
            row[col] = arr[s]
        rows.append(row)
    pd.DataFrame(rows).to_csv(path, index=False)
    return len(rows)


def _save_csv(result: SingleResult, tables: List[str], output_dir: str):
    """결과를 CSV로 저장."""
    import os
    import pandas as pd

    os.makedirs(output_dir, exist_ok=True)
    idno = result.idno
    elapsed = result.ctr.pass_yy * 12 + result.ctr.pass_mm
    saved = []

    if "RSK_RT" in tables:
        rows = []
        for rsk_cd, cols in result.rsk_rt.items():
            n = len(cols["INVLD_TRMNAT_AF_APLY_RSK_RT"])
            for t in range(n):
                row = {"INFRC_IDNO": idno, "CTR_MM": elapsed + t, "RSK_CD": rsk_cd}
                for col_name, arr in cols.items():
                    row[col_name] = arr[t]
                rows.append(row)
        if rows:
            path = os.path.join(output_dir, f"{idno}_RSK_RT.csv")
            pd.DataFrame(rows).to_csv(path, index=False)
            saved.append(("RSK_RT", path, len(rows)))

    if "LAPSE_RT" in tables:
        path = os.path.join(output_dir, f"{idno}_LAPSE_RT.csv")
        n = _dict_to_csv(result.lapse_rt, result.n_steps, path, idno, elapsed)
        saved.append(("LAPSE_RT", path, n))

    if "MN" in tables:
        path = os.path.join(output_dir, f"{idno}_TBL_MN.csv")
        n = _dict_to_csv(result.tbl_mn, result.n_steps, path, idno, elapsed)
        saved.append(("TBL_MN", path, n))

    if "TRAD_PV" in tables and result.trad_pv:
        path = os.path.join(output_dir, f"{idno}_TRAD_PV.csv")
        n = _dict_to_csv(result.trad_pv.to_dict(), result.n_steps, path, idno, elapsed)
        saved.append(("TRAD_PV", path, n))

    if "BN" in tables and result.tbl_bn:
        _BN_COLS = [
            ("TRMNAT_RT", "trmnat_rt"), ("RSVAMT_DEFRY_DRPO_RSKRT", "rsvamt_defry_drpo_rskrt"),
            ("BNFT_DRPO_RSKRT", "bnft_drpo_rskrt"), ("BNFT_RSKRT", "bnft_rskrt"),
            ("TRMO_MTNPSN_CNT", "trmo_mtnpsn_cnt"), ("TRMPSN_CNT", "trmpsn_cnt"),
            ("RSVAMT_DEFRY_DRPSN_CNT", "rsvamt_defry_drpsn_cnt"),
            ("DEFRY_DRPSN_CNT", "defry_drpsn_cnt"), ("TRME_MTNPSN_CNT", "trme_mtnpsn_cnt"),
            ("BNFT_OCURPE_CNT", "bnft_ocurpe_cnt"), ("CRIT_AMT", "crit_amt"),
            ("DEFRY_RT", "defry_rt"), ("PRTT_RT", "prtt_rt"), ("GRADIN_RT", "gradin_rt"),
            ("PYAMT", "pyamt"), ("BNFT_INSUAMT", "bnft_insuamt"),
        ]
        rows = []
        for bno, br in sorted(result.tbl_bn.bnft_results.items()):
            for t in range(len(br.pyamt)):
                row = {"INFRC_IDNO": idno, "CTR_MM": elapsed + t, "BNFT_NO": bno}
                for col_name, attr in _BN_COLS:
                    arr = getattr(br, attr, None)
                    if arr is not None:
                        row[col_name] = arr[t]
                rows.append(row)
        if rows:
            path = os.path.join(output_dir, f"{idno}_TBL_BN.csv")
            pd.DataFrame(rows).to_csv(path, index=False)
            saved.append(("TBL_BN", path, len(rows)))

    if "EXP" in tables and result.exp_results:
        rows = []
        for res in result.exp_results:
            for s in range(result.n_steps):
                rows.append({
                    "INFRC_IDNO": idno, "CTR_MM": elapsed + s,
                    "EXP_TPCD": res.tpcd, "EXP_KDCD": res.kdcd,
                    "D_IND_EXP_DVCD": res.d_ind, "EXP_VAL": res.values[s],
                })
        if rows:
            path = os.path.join(output_dir, f"{idno}_EXP.csv")
            pd.DataFrame(rows).to_csv(path, index=False)
            saved.append(("EXP", path, len(rows)))

    if "CF" in tables and result.cf:
        path = os.path.join(output_dir, f"{idno}_CF.csv")
        n = _dict_to_csv(result.cf.to_dict(), result.n_steps, path, idno, elapsed)
        saved.append(("CF", path, n))

    if "DC_RT" in tables and result.dc_rt:
        path = os.path.join(output_dir, f"{idno}_DC_RT.csv")
        n = _dict_to_csv(result.dc_rt.to_dict(), result.n_steps, path, idno, elapsed)
        saved.append(("DC_RT", path, n))

    if "PVCF" in tables and result.pvcf:
        path = os.path.join(output_dir, f"{idno}_PVCF.csv")
        n = _dict_to_csv(result.pvcf.to_dict(), result.n_steps, path, idno, elapsed)
        saved.append(("PVCF", path, n))

    if "BEL" in tables and result.bel:
        path = os.path.join(output_dir, f"{idno}_BEL.csv")
        bel_row = {"INFRC_IDNO": idno}
        bel_row.update(result.bel.to_dict())
        pd.DataFrame([bel_row]).to_csv(path, index=False)
        saved.append(("BEL", path, 1))

    return saved


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
        print(f"\n  [RSK_RT] {len(result.rsk_rt)} risks")

    if "MN" in tables:
        mn = result.tbl_mn
        print(f"  [TBL_MN] CTR_TRME[end]={mn['CTR_TRME_MTNPSN_CNT'][-1]:.6f}")

    if "BN" in tables and result.tbl_bn:
        print(f"  [TBL_BN] {len(result.tbl_bn.bnft_results)} BNFTs")

    if "CF" in tables and result.cf:
        d = result.cf.to_dict()
        print(f"  [CF] PREM_BASE sum={d['PREM_BASE'].sum():,.0f}")

    if "BEL" in tables and result.bel:
        print(f"  [BEL] = {result.bel.bel:,.2f}")


def main():
    import argparse
    import time

    parser = argparse.ArgumentParser(
        description="CF 산출 파이프라인 (RSK_RT → ... → BEL)")
    parser.add_argument("--idno", type=int, required=True, help="INFRC_IDNO")
    parser.add_argument("--table", type=str, default=None,
                        help=f"특정 테이블까지만 산출 ({', '.join(ALL_TABLES)})")
    parser.add_argument("--no-pv", action="store_true", help="TRAD_PV 이후 제외")
    parser.add_argument("--debug", action="store_true",
                        help="중간 테이블 CSV 출력 + 요약 (미지정 시 BEL만 출력)")
    parser.add_argument("--save", type=str, default=None,
                        help="--debug 시 저장할 테이블 (콤마구분, 예: RSK_RT,CF,BEL)")
    parser.add_argument("--output", "-o", type=str, default="output",
                        help="CSV 출력 디렉토리 (기본: output)")
    parser.add_argument("--db", type=str, default="duckdb_transform.duckdb",
                        help="DB 경로")
    args = parser.parse_args()

    # --table 처리
    if args.table:
        tbl = args.table.upper()
        if tbl not in ALL_TABLES:
            print(f"ERROR: --table은 {ALL_TABLES} 중 하나")
            return 1
        tables = ALL_TABLES[:ALL_TABLES.index(tbl) + 1]
    else:
        tables = ALL_TABLES[:]

    include_pv = not args.no_pv and "TRAD_PV" in tables
    include_bn = not args.no_pv and "BN" in tables

    print(f"{'='*60}")
    print(f"  CF 산출: IDNO={args.idno}")
    print(f"  {' → '.join(tables)}")
    print(f"{'='*60}")

    t0 = time.time()
    con = duckdb.connect(args.db, read_only=True)

    result = run_single(con, args.idno, include_trad_pv=include_pv, include_bn=include_bn)

    if args.debug:
        # --debug: 요약 출력 + CSV 저장
        _print_summary(result, tables)
        if args.save:
            save_tables = [t.strip().upper() for t in args.save.split(",")]
            # 유효성 검증
            for t in save_tables:
                if t not in ALL_TABLES:
                    print(f"  WARNING: '{t}'는 유효한 테이블이 아닙니다 ({ALL_TABLES})")
            save_tables = [t for t in save_tables if t in tables]
        else:
            save_tables = tables  # --debug만 쓰면 전체 저장
        saved = _save_csv(result, save_tables, args.output)
        for tbl_name, path, n_rows in saved:
            print(f"  [{tbl_name}] {n_rows}행 → {path}")
    else:
        # 기본 모드: BEL만 출력
        if result.bel:
            print(f"\n  [BEL] = {result.bel.bel:,.2f}")
            saved = _save_csv(result, ["BEL"], args.output)
            for tbl_name, path, n_rows in saved:
                print(f"  [{tbl_name}] → {path}")
        else:
            # --table로 BEL 이전 단계까지만 실행한 경우
            last_table = tables[-1]
            saved = _save_csv(result, [last_table], args.output)
            for tbl_name, path, n_rows in saved:
                print(f"  [{tbl_name}] {n_rows}행 → {path}")

    elapsed = time.time() - t0
    print(f"\n  총 소요: {elapsed:.2f}s")
    print(f"{'='*60}")

    con.close()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

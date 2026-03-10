"""단건 전체 파이프라인 테스트.

계산 순서: OD_RSK_RT → OD_LAPSE_RT → OD_TBL_MN → OD_TRAD_PV → OD_TBL_BN
각 테이블 산출 후 duckdb_transform.duckdb 기대값과 비교.

Usage:
    python test_pipeline_single.py --idno 760397
    python test_pipeline_single.py --idno 760397 --table RSK_RT
    python test_pipeline_single.py --idno 760397 --table MN --detail
"""
import argparse
import sys
import time

import duckdb
import numpy as np

DB_PATH = "duckdb_transform.duckdb"
TOL = 1e-10


# ─────────────────────────────────────────────────────────
# 공통 비교 함수
# ─────────────────────────────────────────────────────────
def compare_columns(comp_dict, exp_df, cols, label, tol=TOL, detail=False, head=10):
    """comp_dict vs exp_df 컬럼별 비교. 결과 dict 반환."""
    results = {}
    n = len(exp_df)
    for col in cols:
        if col not in comp_dict or col not in exp_df.columns:
            results[col] = {"status": "SKIP", "max_diff": 0, "t": 0}
            continue
        comp = np.asarray(comp_dict[col][:n], dtype=np.float64)
        exp = exp_df[col].values.astype(np.float64)
        diff = np.abs(comp - exp)
        max_diff = diff.max()
        t_max = int(np.argmax(diff))
        ok = max_diff <= tol
        results[col] = {
            "status": "PASS" if ok else "FAIL",
            "max_diff": max_diff,
            "t": t_max,
            "comp": comp[t_max] if n > 0 else 0,
            "exp": exp[t_max] if n > 0 else 0,
        }
        if detail and not ok:
            fb = int(np.argmax(diff > tol))
            start = max(0, fb - 2)
            end = min(start + head, n)
            print(f"\n  [{col}] 상세 (t={start}~{end-1}):")
            print(f"    {'t':>4s} {'comp':>18s} {'exp':>18s} {'diff':>14s}")
            for t in range(start, end):
                flag = " *" if diff[t] > tol else ""
                print(f"    {t:>4d} {comp[t]:>18.12f} {exp[t]:>18.12f} {diff[t]:>14.2e}{flag}")
    return results


def print_results(table_name, results):
    """비교 결과 출력."""
    pass_cnt = sum(1 for r in results.values() if r["status"] == "PASS")
    fail_cnt = sum(1 for r in results.values() if r["status"] == "FAIL")
    skip_cnt = sum(1 for r in results.values() if r["status"] == "SKIP")

    status = "ALL PASS" if fail_cnt == 0 else "FAIL"
    print(f"\n  [{table_name}] {status}  (PASS={pass_cnt} FAIL={fail_cnt} SKIP={skip_cnt})")

    if fail_cnt > 0:
        print(f"  {'컬럼':<40s} {'max_diff':>12s} {'t':>5s} {'comp':>18s} {'exp':>18s}")
        for col, r in results.items():
            if r["status"] == "FAIL":
                print(f"  {col:<40s} {r['max_diff']:>12.2e} {r['t']:>5d} "
                      f"{r.get('comp',0):>18.12f} {r.get('exp',0):>18.12f}")
    return fail_cnt == 0


# ─────────────────────────────────────────────────────────
# STEP 1: OD_RSK_RT
# ─────────────────────────────────────────────────────────
RSK_RT_COLS = [
    "RSK_RT", "LOSS_RT", "MTH_EFECT_COEF", "BEPRD_DEFRY_RT",
    "TRD_COEF", "ARVL_AGE_COEF",
    "INVLD_TRMNAT_BF_YR_RSK_RT", "INVLD_TRMNAT_BF_MM_RSK_RT",
    "INVLD_TRMNAT_AF_APLY_RSK_RT",
]


def run_rsk_rt(con, loader, ctr, risks, detail=False):
    """OD_RSK_RT 산출 + 비교."""
    from cf_module.calc.tbl_rsk_rt import compute_rsk_rt

    risk_cds = [r.risk_cd for r in risks]
    mortality = loader.load_mortality_rates(risks, ctr)
    beprd = loader.load_beprd(ctr, risk_cds)
    invld = loader.load_invld_months(ctr)

    # 기대값 & n_steps
    exp_df = con.execute("""
        SELECT RSK_RT_CD, SETL_AFT_PASS_MMCNT,
               RSK_RT, LOSS_RT, MTH_EFECT_COEF, BEPRD_DEFRY_RT,
               TRD_COEF, ARVL_AGE_COEF,
               INVLD_TRMNAT_BF_YR_RSK_RT, INVLD_TRMNAT_BF_MM_RSK_RT,
               INVLD_TRMNAT_AF_APLY_RSK_RT
        FROM OD_RSK_RT
        WHERE INFRC_IDNO = ?
        ORDER BY RSK_RT_CD, SETL_AFT_PASS_MMCNT
    """, [ctr.idno]).fetchdf()

    if len(exp_df) == 0:
        print(f"  [RSK_RT] 기대값 없음")
        return True, {}

    # risk별 n_steps
    n_steps_map = {}
    for rsk_cd, grp in exp_df.groupby("RSK_RT_CD"):
        n_steps_map[str(rsk_cd)] = len(grp)

    max_n = max(n_steps_map.values()) if n_steps_map else 0
    result = compute_rsk_rt(ctr, risks, mortality, beprd, invld, max_n)

    # risk별 비교
    all_pass = True
    for rsk_cd, grp in exp_df.groupby("RSK_RT_CD"):
        rsk_cd = str(rsk_cd)
        if rsk_cd not in result:
            print(f"  RSK_RT {rsk_cd}: 산출 결과 없음")
            all_pass = False
            continue
        comp = result[rsk_cd]
        n = len(grp)
        grp = grp.sort_values("SETL_AFT_PASS_MMCNT").reset_index(drop=True)
        res = compare_columns(comp, grp, RSK_RT_COLS, f"RSK_RT/{rsk_cd}", detail=detail)
        ok = print_results(f"RSK_RT/{rsk_cd}", res)
        if not ok:
            all_pass = False

    return all_pass, result


# ─────────────────────────────────────────────────────────
# STEP 2: OD_LAPSE_RT
# ─────────────────────────────────────────────────────────
LAPSE_RT_COLS = ["TRMNAT_RT", "SKEW", "APLY_TRMNAT_RT"]


def run_lapse_rt(con, loader, ctr, detail=False):
    """OD_LAPSE_RT 산출 + 비교."""
    from cf_module.calc.tbl_lapse_rt import compute_lapse_rt

    lapse_paying, lapse_paidup = loader.load_lapse_rates(ctr)
    skew = loader.load_skew(ctr)

    exp_df = con.execute("""
        SELECT SETL_AFT_PASS_MMCNT, TRMNAT_RT, SKEW, APLY_TRMNAT_RT
        FROM OD_LAPSE_RT
        WHERE INFRC_IDNO = ?
        ORDER BY SETL_AFT_PASS_MMCNT
    """, [ctr.idno]).fetchdf()

    if len(exp_df) == 0:
        print(f"  [LAPSE_RT] 기대값 없음")
        return True, {}

    n_steps = len(exp_df)
    result = compute_lapse_rt(ctr, lapse_paying, lapse_paidup, skew, n_steps)
    res = compare_columns(result, exp_df, LAPSE_RT_COLS, "LAPSE_RT", detail=detail)
    ok = print_results("LAPSE_RT", res)
    return ok, result


# ─────────────────────────────────────────────────────────
# STEP 3: OD_TBL_MN
# ─────────────────────────────────────────────────────────
TBL_MN_COLS = [
    "CTR_TRMO_MTNPSN_CNT", "CTR_TRMNAT_RT",
    "CTR_RSVAMT_DEFRY_DRPO_RSKRT", "CTR_BNFT_DRPO_RSKRT",
    "CTR_TRMPSN_CNT", "CTR_RSVAMT_DEFRY_DRPSN_CNT",
    "CTR_BNFT_DEFRY_DRPSN_CNT", "CTR_TRME_MTNPSN_CNT",
    "PAY_TRMO_MTNPSN_CNT", "PAY_TRMNAT_RT",
    "PAY_RSVAMT_DEFRY_DRPO_RSKRT", "PAY_BNFT_DRPO_RSKRT",
    "PYEXSP_DRPO_RSKRT", "PAY_TRMPSN_CNT",
    "PAY_RSVAMT_DEFRY_DRPSN_CNT", "PAY_BNFT_DEFRY_DRPSN_CNT",
    "PYEXSP_DRPSN_CNT", "PAY_TRME_MTNPSN_CNT",
]


def run_tbl_mn(con, loader, ctr, risks, rsk_rt_result, lapse_result, detail=False):
    """OD_TBL_MN 산출 + 비교."""
    from cf_module.data.rsk_lapse_loader import RiskInfo
    from cf_module.calc.tbl_mn import compute_tbl_mn

    exit_flags = loader.load_exit_flags(ctr, risks)

    exp_df = con.execute("""
        SELECT * FROM OD_TBL_MN
        WHERE INFRC_IDNO = ?
        ORDER BY SETL_AFT_PASS_MMCNT
    """, [ctr.idno]).fetchdf()

    if len(exp_df) == 0:
        print(f"  [TBL_MN] 기대값 없음")
        return True, {}

    n_steps = len(exp_df)

    # qx_monthly_rates: OD_RSK_RT의 INVLD_TRMNAT_AF_APLY_RSK_RT
    qx_rates = {}
    rsk_df = con.execute("""
        SELECT RSK_RT_CD, INVLD_TRMNAT_AF_APLY_RSK_RT
        FROM OD_RSK_RT
        WHERE INFRC_IDNO = ?
        ORDER BY RSK_RT_CD, SETL_AFT_PASS_MMCNT
    """, [ctr.idno]).fetchdf()

    for rsk_cd, grp in rsk_df.groupby("RSK_RT_CD"):
        qx_rates[str(rsk_cd)] = grp["INVLD_TRMNAT_AF_APLY_RSK_RT"].values.astype(np.float64)

    # wx_monthly: OD_LAPSE_RT의 APLY_TRMNAT_RT
    lapse_df = con.execute("""
        SELECT APLY_TRMNAT_RT FROM OD_LAPSE_RT
        WHERE INFRC_IDNO = ?
        ORDER BY SETL_AFT_PASS_MMCNT
    """, [ctr.idno]).fetchdf()
    wx_monthly = lapse_df["APLY_TRMNAT_RT"].values.astype(np.float64) if len(lapse_df) > 0 else np.zeros(0)

    # C1/C2 가상코드 추가
    existing_cds = {r.risk_cd for r in risks}
    for rsk_cd in qx_rates:
        if rsk_cd not in existing_cds:
            if rsk_cd in exit_flags:
                risks.append(RiskInfo(
                    risk_cd=rsk_cd, chr_cd="X", mm_trf_way_cd=0,
                    dead_rt_dvcd=0, rsk_grp_no=f"__{rsk_cd}__",
                ))

    # wx 길이 맞추기
    if len(wx_monthly) < n_steps:
        wx_monthly = np.pad(wx_monthly, (0, n_steps - len(wx_monthly)))
    elif len(wx_monthly) > n_steps:
        wx_monthly = wx_monthly[:n_steps]

    result = compute_tbl_mn(
        ctr=ctr, risks=risks, qx_monthly_rates=qx_rates,
        wx_monthly=wx_monthly, exit_flags=exit_flags, n_steps=n_steps,
    )

    res = compare_columns(result, exp_df, TBL_MN_COLS, "TBL_MN", detail=detail)
    ok = print_results("TBL_MN", res)
    return ok, result


# ─────────────────────────────────────────────────────────
# STEP 4: OD_TRAD_PV
# ─────────────────────────────────────────────────────────
# test_trad_pv_all.py CHECK_COLS와 동일 (to_dict() 키 기준)
TRAD_PV_COLS = [
    "CTR_AFT_PASS_MMCNT", "PREM_PAY_YN", "ORIG_PREM", "DC_PREM",
    "ACUM_NPREM", "ACUM_NPREM_PRPD", "PRPD_MMCNT", "PRPD_PREM",
    "PAD_PREM", "ADD_ACCMPT_GPREM", "ADD_ACCMPT_NPREM",
    "ACQSEXP1_BIZEXP", "ACQSEXP2_BIZEXP",
    "AFPAY_MNTEXP", "LUMPAY_BIZEXP", "PAY_GRCPR_ACQSEXP",
    "YSTR_RSVAMT", "YYEND_RSVAMT", "YSTR_RSVAMT_TRM", "YYEND_RSVAMT_TRM",
    "PENS_INRT", "PENS_DEFRY_RT", "PENS_ANNUAL_SUM", "HAFWAY_WDAMT",
    "APLY_PUBANO_INRT",
    "APLY_ADINT_TGT_AMT",
    "APLY_PREM_ACUMAMT_BNFT", "APLY_PREM_ACUMAMT_EXP",
    "LWST_ADINT_TGT_AMT", "LWST_PREM_ACUMAMT",
    "SOFF_BF_TMRFND", "SOFF_AF_TMRFND", "LTRMNAT_TMRFND",
    "HAFWAY_WDAMT_ADD", "SOFF_BF_TMRFND_ADD", "SOFF_AF_TMRFND_ADD",
    "CNCTTP_ACUMAMT_KICS",
    "LOAN_INT", "LOAN_REMAMT", "LOAN_RPAY_HAFWAY",
    "LOAN_NEW", "LOAN_RPAY_MATU",
    "MATU_MAINT_BNS_ACUM_AMT",
]


def run_trad_pv(con, trad_cache, idno, mn_result, detail=False):
    """OD_TRAD_PV 산출 + 비교 (CTR_POLNO netting 포함)."""
    from cf_module.calc.trad_pv import compute_trad_pv, apply_soff_af_netting
    from cf_module.data.trad_pv_loader import build_contract_info_cached

    exp_df = con.execute("""
        SELECT * FROM OD_TRAD_PV
        WHERE INFRC_IDNO = ?
        ORDER BY SETL_AFT_PASS_MMCNT
    """, [idno]).fetchdf()

    if len(exp_df) == 0:
        print(f"  [TRAD_PV] 기대값 없음")
        return True, {}

    # CTR_POLNO 그룹 조회 (netting 대상)
    polno_row = con.execute("""
        SELECT CTR_POLNO FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1
    """, [idno]).fetchone()
    polno = polno_row[0] if polno_row else None

    # 같은 CTR_POLNO 내 모든 계약 조회
    group_idnos = [idno]
    if polno:
        grp = con.execute("""
            SELECT INFRC_IDNO, COV_CD FROM II_INFRC
            WHERE CTR_POLNO = ? AND INFRC_SEQ = 1
            ORDER BY INFRC_IDNO
        """, [polno]).fetchall()
        group_idnos = [r[0] for r in grp]
        idno_to_cov = {r[0]: r[1] for r in grp}
    else:
        idno_to_cov = {}

    # 그룹 내 모든 계약 산출
    results = {}
    ctr_trme_map = {}
    for gid in group_idnos:
        info = build_contract_info_cached(trad_cache, gid)
        if not info:
            continue

        # MN 데이터 (대상 계약은 mn_result 사용, 나머지는 DB에서)
        if gid == idno and mn_result:
            pt = mn_result.get("PAY_TRMO_MTNPSN_CNT")
            ct = mn_result.get("CTR_TRMO_MTNPSN_CNT")
            ce = mn_result.get("CTR_TRME_MTNPSN_CNT")
        else:
            mn_df = con.execute("""
                SELECT CTR_TRMO_MTNPSN_CNT, PAY_TRMO_MTNPSN_CNT, CTR_TRME_MTNPSN_CNT
                FROM OD_TBL_MN WHERE INFRC_IDNO = ?
                ORDER BY SETL_AFT_PASS_MMCNT
            """, [gid]).fetchdf()
            if len(mn_df) > 0:
                pt = mn_df["PAY_TRMO_MTNPSN_CNT"].values
                ct = mn_df["CTR_TRMO_MTNPSN_CNT"].values
                ce = mn_df["CTR_TRME_MTNPSN_CNT"].values
            else:
                pt = ct = ce = None

        gn = con.execute(
            "SELECT COUNT(*) FROM OD_TRAD_PV WHERE INFRC_IDNO = ?", [gid]
        ).fetchone()[0]
        if gn == 0:
            continue

        r = compute_trad_pv(info, gn, pay_trmo=pt, ctr_trmo=ct, ctr_trme=ce)
        results[gid] = r
        if ce is not None:
            ctr_trme_map[gid] = np.asarray(ce, dtype=np.float64)

    # Netting 적용
    if len(group_idnos) > 1 and polno:
        polno_map = {polno: group_idnos}
        apply_soff_af_netting(results, polno_map, ctr_trme_map, idno_to_cov)

    if idno not in results:
        print(f"  [TRAD_PV] build_contract_info 실패")
        return False, {}

    d = results[idno].to_dict()
    n_steps = len(exp_df)

    # 존재하는 컬럼만 비교
    check_cols = [c for c in TRAD_PV_COLS if c in d and c in exp_df.columns]
    res = compare_columns(d, exp_df, check_cols, "TRAD_PV", tol=1e-6, detail=detail)
    ok = print_results("TRAD_PV", res)
    return ok, d


# ─────────────────────────────────────────────────────────
# STEP 5: OD_TBL_BN
# ─────────────────────────────────────────────────────────
BN_DERIVED_COLS = [
    "TRMO_MTNPSN_CNT", "TRMPSN_CNT",
    "RSVAMT_DEFRY_DRPSN_CNT", "DEFRY_DRPSN_CNT",
    "TRME_MTNPSN_CNT", "BNFT_OCURPE_CNT",
    "CRIT_AMT", "DEFRY_RT", "PRTT_RT", "GRADIN_RT",
    "PYAMT", "BNFT_INSUAMT",
]
BN_RATE_COLS = [
    "TRMNAT_RT", "RSVAMT_DEFRY_DRPO_RSKRT",
    "BNFT_DRPO_RSKRT", "BNFT_RSKRT",
]


def run_tbl_bn(con, idno, detail=False):
    """OD_TBL_BN Phase 1 산출 + 비교 (rate 입력 → 파생 컬럼)."""
    from test_tbl_bn import compute_bn_from_rates

    # 계약 정보
    info = con.execute("""
        SELECT PROD_CD, COV_CD, CLS_CD, GRNTPT_JOIN_AMT
        FROM II_INFRC WHERE INFRC_SEQ = 1 AND INFRC_IDNO = ?
    """, [idno]).fetchone()
    if not info:
        print(f"  [TBL_BN] 계약 정보 없음")
        return True

    join_amt = float(info[3] or 0)

    bn_df = con.execute("""
        SELECT BNFT_NO, SETL_AFT_PASS_MMCNT,
               TRMNAT_RT, RSVAMT_DEFRY_DRPO_RSKRT,
               BNFT_DRPO_RSKRT, BNFT_RSKRT,
               TRMO_MTNPSN_CNT, TRMPSN_CNT,
               RSVAMT_DEFRY_DRPSN_CNT, DEFRY_DRPSN_CNT,
               TRME_MTNPSN_CNT, BNFT_OCURPE_CNT,
               CRIT_AMT, DEFRY_RT, PRTT_RT, GRADIN_RT,
               PYAMT, BNFT_INSUAMT
        FROM OD_TBL_BN
        WHERE INFRC_SEQ = 1 AND INFRC_IDNO = ?
        ORDER BY BNFT_NO, SETL_AFT_PASS_MMCNT
    """, [idno]).fetchdf()

    if len(bn_df) == 0:
        print(f"  [TBL_BN] 기대값 없음")
        return True

    all_pass = True
    n_bnft = 0
    n_bnft_pass = 0

    for bnft_no, grp in bn_df.groupby("BNFT_NO"):
        grp = grp.sort_values("SETL_AFT_PASS_MMCNT").reset_index(drop=True)
        if grp["TRMO_MTNPSN_CNT"].max() == 0:
            continue
        n_bnft += 1
        n = len(grp)

        trmnat_rt = grp["TRMNAT_RT"].values.astype(np.float64)
        rsvamt_drpo = grp["RSVAMT_DEFRY_DRPO_RSKRT"].values.astype(np.float64)
        bnft_drpo = grp["BNFT_DRPO_RSKRT"].values.astype(np.float64)
        bnft_rskrt = grp["BNFT_RSKRT"].values.astype(np.float64)
        defry_rt = grp["DEFRY_RT"].values.astype(np.float64)
        prtt_rt = grp["PRTT_RT"].values.astype(np.float64)
        gradin_rt = grp["GRADIN_RT"].values.astype(np.float64)

        comp = compute_bn_from_rates(
            n, trmnat_rt, rsvamt_drpo, bnft_drpo, bnft_rskrt,
            join_amt, defry_rt, prtt_rt, gradin_rt,
        )

        bnft_pass = True
        for col in BN_DERIVED_COLS:
            if col in comp and col in grp.columns:
                c = np.asarray(comp[col][:n], dtype=np.float64)
                e = grp[col].values.astype(np.float64)
                max_diff = np.abs(c - e).max()
                # PYAMT은 float precision 1e-6 허용
                tol = 1e-5 if col == "PYAMT" else TOL
                if max_diff > tol:
                    bnft_pass = False
                    if detail:
                        t = int(np.argmax(np.abs(c - e)))
                        print(f"    BNFT_NO={bnft_no} {col}: diff={max_diff:.2e} "
                              f"t={t} comp={c[t]:.12f} exp={e[t]:.12f}")

        if bnft_pass:
            n_bnft_pass += 1
        else:
            all_pass = False

    status = "ALL PASS" if all_pass else "FAIL"
    print(f"\n  [TBL_BN] {status}  (BNFT: {n_bnft_pass}/{n_bnft} PASS)")
    return all_pass


# ─────────────────────────────────────────────────────────
# 메인 파이프라인
# ─────────────────────────────────────────────────────────
TABLE_ORDER = ["RSK_RT", "LAPSE_RT", "MN", "TRAD_PV", "BN"]


def main():
    parser = argparse.ArgumentParser(description="단건 전체 파이프라인 테스트")
    parser.add_argument("--idno", type=int, required=True, help="INFRC_IDNO")
    parser.add_argument("--table", type=str, default=None,
                        help="특정 테이블만 실행 (RSK_RT, LAPSE_RT, MN, TRAD_PV, BN)")
    parser.add_argument("--detail", action="store_true", help="실패 시 상세 출력")
    args = parser.parse_args()

    idno = args.idno
    tables = [args.table.upper()] if args.table else TABLE_ORDER

    print(f"{'='*70}")
    print(f"  단건 파이프라인 테스트: IDNO={idno}")
    print(f"  테이블: {' → '.join(tables)}")
    print(f"{'='*70}")

    t0 = time.time()
    con = duckdb.connect(DB_PATH, read_only=True)

    # 로더 초기화
    from cf_module.data.rsk_lapse_loader import RawAssumptionLoader
    loader = RawAssumptionLoader(con)

    # 계약 정보
    ctr = loader.load_contract(idno)
    risks = loader.load_risk_codes(ctr)

    print(f"\n  계약: PROD={ctr.prod_cd} CLS={ctr.cls_cd} COV={ctr.cov_cd}")
    print(f"  가입연령={ctr.entry_age} 보험기간={ctr.bterm_yy}Y 납입기간={ctr.pterm_yy}Y")
    print(f"  경과={ctr.pass_yy}Y{ctr.pass_mm}M PAY_STCD={ctr.pay_stcd}")
    print(f"  위험코드: {[r.risk_cd for r in risks]}")

    results = {}
    rsk_rt_result = {}
    lapse_result = {}
    mn_result = {}
    trad_pv_result = {}

    # STEP 1: RSK_RT
    if "RSK_RT" in tables:
        print(f"\n{'─'*70}")
        print(f"  STEP 1: OD_RSK_RT")
        t1 = time.time()
        ok, rsk_rt_result = run_rsk_rt(con, loader, ctr, risks, detail=args.detail)
        results["RSK_RT"] = ok
        print(f"  ({time.time()-t1:.1f}s)")

    # STEP 2: LAPSE_RT
    if "LAPSE_RT" in tables:
        print(f"\n{'─'*70}")
        print(f"  STEP 2: OD_LAPSE_RT")
        t1 = time.time()
        ok, lapse_result = run_lapse_rt(con, loader, ctr, detail=args.detail)
        results["LAPSE_RT"] = ok
        print(f"  ({time.time()-t1:.1f}s)")

    # STEP 3: TBL_MN (depends on RSK_RT, LAPSE_RT)
    if "MN" in tables:
        print(f"\n{'─'*70}")
        print(f"  STEP 3: OD_TBL_MN")
        t1 = time.time()
        ok, mn_result = run_tbl_mn(con, loader, ctr, risks, rsk_rt_result, lapse_result,
                                    detail=args.detail)
        results["MN"] = ok
        print(f"  ({time.time()-t1:.1f}s)")

    # STEP 4: TRAD_PV (depends on MN)
    if "TRAD_PV" in tables:
        print(f"\n{'─'*70}")
        print(f"  STEP 4: OD_TRAD_PV")
        t1 = time.time()
        from cf_module.data.trad_pv_loader import TradPVDataCache
        trad_cache = TradPVDataCache(con)
        ok, trad_pv_result = run_trad_pv(con, trad_cache, idno, mn_result,
                                          detail=args.detail)
        results["TRAD_PV"] = ok
        print(f"  ({time.time()-t1:.1f}s)")

    # STEP 5: TBL_BN
    if "BN" in tables:
        print(f"\n{'─'*70}")
        print(f"  STEP 5: OD_TBL_BN (Phase 1)")
        t1 = time.time()
        ok = run_tbl_bn(con, idno, detail=args.detail)
        results["BN"] = ok
        print(f"  ({time.time()-t1:.1f}s)")

    # 최종 결과
    elapsed = time.time() - t0
    print(f"\n{'='*70}")
    print(f"  최종 결과 (IDNO={idno}, {elapsed:.1f}s)")
    print(f"{'='*70}")
    all_ok = True
    for tbl, ok in results.items():
        status = "PASS" if ok else "FAIL"
        mark = "  " if ok else "!!"
        print(f"  {mark} {tbl:<15s} {status}")
        if not ok:
            all_ok = False

    print(f"\n  {'ALL PASS' if all_ok else 'FAIL'}")
    con.close()
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())

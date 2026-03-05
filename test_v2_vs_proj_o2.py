"""
v2 엔진 vs PROJ_O2.vdb OD_TBL_MN 전체 비교 테스트

OD_TBL_MN 345행(월별 계산기수): 유지자수, 납입자수, 해약률, 탈퇴율, 탈퇴자수 등
v2 engine project_group() 결과와 전 시점 비교.

Usage:
    python test_v2_vs_proj_o2.py
    python test_v2_vs_proj_o2.py --keep-db
    python test_v2_vs_proj_o2.py --csv   # 비교 결과 CSV 저장
"""

import argparse
import os
import time

import numpy as np
import pandas as pd
import sqlite3

from cf_module.v2.etl import migrate_legacy_db
from cf_module.v2.engine import load_group_assumptions, project_group


# ── 설정 ──
LEGACY_DB = r"C:\Users\thrus\Downloads\VSOLN2\VSOLN2.vdb"
PROJ_O2_DB = r"C:\python\cf_module\PROJ_O2.vdb"
V2_DB_PATH = os.path.join(os.path.dirname(__file__), "v2_test_proj_o2.duckdb")

IDNO = 760397
CLOS_YM = "202309"


def load_expected():
    """PROJ_O2.vdb OD_TBL_MN 로드."""
    conn = sqlite3.connect(PROJ_O2_DB)
    df = pd.read_sql_query("""
        SELECT * FROM OD_TBL_MN ORDER BY SETL_AFT_PASS_MMCNT
    """, conn)
    conn.close()
    return df


def main():
    parser = argparse.ArgumentParser(description="v2 vs PROJ_O2.vdb OD_TBL_MN 비교")
    parser.add_argument("--keep-db", action="store_true", help="DuckDB 파일 보존")
    parser.add_argument("--csv", action="store_true", help="비교 결과 CSV 저장")
    args = parser.parse_args()

    print("=" * 80)
    print("v2 엔진 vs PROJ_O2.vdb OD_TBL_MN 전체 비교")
    print("=" * 80)

    # ── 1. 기대값 로드 ──
    print("\n[1] PROJ_O2.vdb OD_TBL_MN 로드")
    expected = load_expected()
    n_months = len(expected)
    print(f"    {n_months}개월 데이터")

    # ── 2. v2 ETL + 프로젝션 ──
    print("\n[2] v2 ETL + 프로젝션")
    if os.path.exists(V2_DB_PATH):
        os.remove(V2_DB_PATH)

    t0 = time.time()
    v2 = migrate_legacy_db(
        legacy_path=LEGACY_DB,
        v2_path=V2_DB_PATH,
        infrc_seq=1,
        assm_ym="202306",
        idno_start=IDNO,
        idno_end=IDNO,
    )

    c = v2.execute("SELECT * FROM dim_contract").fetchdf().iloc[0]
    assm = load_group_assumptions(v2, c["assm_profile"], IDNO, max_duration=1200)

    ctr_ym = str(c["ctr_ym"])
    elapsed = (int(CLOS_YM[:4]) - int(ctr_ym[:4])) * 12 + (int(CLOS_YM[4:6]) - int(ctr_ym[4:6])) + 1  # 계약월 포함

    result = project_group(
        conn=v2,
        assm=assm,
        contract_ids=np.array([int(c["contract_id"])]),
        entry_ages=np.array([int(c["entry_age"])]),
        bterms=np.array([int(c["bterm"])]),
        pterms=np.array([int(c["pterm"])]),
        elapsed_months=np.array([elapsed], dtype=np.int32),
        clos_ym=CLOS_YM,
        max_proj_months=1200,
    )
    print(f"    완료 ({time.time()-t0:.1f}s), 프로젝션 shape: {result.tpx.shape}")

    # ── 3. 비교 테이블 구축 ──
    # PROJ_O2 row 0 = 초기화 (SETL=0, TRMO=0, TRME=1.0)
    # PROJ_O2 row t+1 = v2 result index t
    # 비교 범위: SETL=1 ~ min(n_months-1, v2 max_t)
    max_t = result.tpx.shape[1]
    compare_n = min(n_months - 1, max_t)
    print(f"\n[3] 비교: {compare_n}개월 (SETL=1~{compare_n})")

    # v2 결과 추출 (1D, index 0 = first contract)
    v2_tpx = result.tpx[0, :compare_n]
    v2_pay_tpx = result.pay_tpx[0, :compare_n]
    v2_wx_ctr = result.wx_monthly[0, :compare_n]
    v2_wx_pay = result.wx_pay_monthly[0, :compare_n]
    v2_d_rsvamt = result.d_rsvamt[0, :compare_n]
    v2_d_bnft = result.d_bnft[0, :compare_n]
    v2_d_pyexsp = result.d_pyexsp[0, :compare_n]

    # tpx_bot (기시유지자): tpx_bot[0]=1, tpx_bot[t]=tpx[t-1]
    v2_tpx_bot = np.ones(compare_n)
    v2_tpx_bot[1:] = v2_tpx[:compare_n - 1]

    # pay_tpx_bot
    v2_pay_tpx_bot = np.ones(compare_n)
    v2_pay_tpx_bot[1:] = v2_pay_tpx[:compare_n - 1]

    # 탈퇴자수 = tpx_bot × rate
    v2_ctr_trmpsn = v2_tpx_bot * v2_wx_ctr      # CTR 해약자수
    v2_pay_trmpsn = v2_pay_tpx_bot * v2_wx_pay   # PAY 해약자수

    # CTR 탈퇴율 (위험률): RSVAMT, BNFT
    v2_ctr_rsvamt_rt = np.where(v2_tpx_bot > 0, v2_d_rsvamt / v2_tpx_bot, 0)
    v2_ctr_bnft_rt = np.where(v2_tpx_bot > 0, v2_d_bnft / v2_tpx_bot, 0)

    # PAY PYEXSP 탈퇴율 — d_pyexsp는 PAY tpx_bot 기준
    v2_pyexsp_rt = np.where(v2_pay_tpx_bot > 0, v2_d_pyexsp / v2_pay_tpx_bot, 0)

    # PROJ_O2 기대값 (SETL=1~compare_n)
    exp = expected.iloc[1:compare_n + 1].reset_index(drop=True)

    # ── 비교 항목 정의 ──
    comparisons = [
        # (이름, v2 값, 기대값 컬럼, 유형)
        ("CTR_TRME (tpx)",           v2_tpx,          "CTR_TRME_MTNPSN_CNT",          "amount"),
        ("CTR_TRMNAT_RT (wx_ctr)",   v2_wx_ctr,       "CTR_TRMNAT_RT",                "rate"),
        ("CTR_RSVAMT_DRPO_RSKRT",    v2_ctr_rsvamt_rt,"CTR_RSVAMT_DEFRY_DRPO_RSKRT",  "rate"),
        ("CTR_BNFT_DRPO_RSKRT",      v2_ctr_bnft_rt,  "CTR_BNFT_DRPO_RSKRT",          "rate"),
        ("CTR_TRMPSN (해약자수)",     v2_ctr_trmpsn,   "CTR_TRMPSN_CNT",               "amount"),
        ("CTR_RSVAMT_DRPSN",         v2_d_rsvamt,      "CTR_RSVAMT_DEFRY_DRPSN_CNT",   "amount"),
        ("CTR_BNFT_DRPSN",           v2_d_bnft,        "CTR_BNFT_DEFRY_DRPSN_CNT",     "amount"),
        ("PAY_TRME (pay_tpx)",       v2_pay_tpx,      "PAY_TRME_MTNPSN_CNT",          "amount"),
        ("PAY_TRMNAT_RT (wx_pay)",   v2_wx_pay,       "PAY_TRMNAT_RT",                "rate"),
        ("PYEXSP_DRPO_RSKRT",        v2_pyexsp_rt,    "PYEXSP_DRPO_RSKRT",            "rate"),
        ("PAY_TRMPSN (해약자수)",     v2_pay_trmpsn,   "PAY_TRMPSN_CNT",               "amount"),
        ("PYEXSP_DRPSN",             v2_d_pyexsp,      "PYEXSP_DRPSN_CNT",             "amount"),
    ]

    # ── 4. 비교 실행 ──
    print("\n" + "=" * 80)
    print(f"{'항목':<30} {'Max Diff':>12} {'Max RDiff%':>12} {'PASS 기준':>10} {'결과':>6}")
    print("-" * 80)

    tol_rate = 1e-8     # 율 비교: 1e-8
    tol_amount = 1e-8   # 금액/자수 비교: 1e-8
    all_pass = True
    results_detail = []

    for name, v2_arr, exp_col, ctype in comparisons:
        exp_arr = exp[exp_col].values.astype(np.float64)
        diff = np.abs(v2_arr - exp_arr)
        max_diff = diff.max()
        max_diff_idx = int(diff.argmax())

        # 상대 오차
        denom = np.maximum(np.abs(exp_arr), 1e-15)
        rdiff = diff / denom
        max_rdiff = rdiff.max()

        tol = tol_rate if ctype == "rate" else tol_amount
        passed = max_diff < tol
        if not passed:
            all_pass = False

        status = "PASS" if passed else "FAIL"
        print(f"  {name:<28} {max_diff:>12.2e} {max_rdiff*100:>11.6f}% {tol:>10.0e} [{status}]")

        if not passed:
            # 최대 오차 시점 상세
            print(f"    → 최대차이 t={max_diff_idx + 1}: "
                  f"v2={v2_arr[max_diff_idx]:.12f}, "
                  f"exp={exp_arr[max_diff_idx]:.12f}")

        # CSV용 상세 데이터 저장
        for t in range(compare_n):
            results_detail.append({
                "t": t + 1,
                "item": name,
                "v2": v2_arr[t],
                "expected": exp_arr[t],
                "diff": diff[t],
                "rdiff_pct": rdiff[t] * 100,
            })

    print("-" * 80)
    print(f"\n{'모든 항목 PASS!' if all_pass else '일부 항목 FAIL'}")
    print("=" * 80)

    # ── 5. 요약 통계 ──
    print("\n[요약] 시점별 최대 오차 (처음 12개월)")
    print(f"  {'t':>4} {'CTR_TRME':>14} {'PAY_TRME':>14} {'CTR_WX':>14} {'PAY_WX':>14} {'PYEXSP_RT':>14}")
    for t in range(min(12, compare_n)):
        ctr_d = abs(v2_tpx[t] - exp.iloc[t]["CTR_TRME_MTNPSN_CNT"])
        pay_d = abs(v2_pay_tpx[t] - exp.iloc[t]["PAY_TRME_MTNPSN_CNT"])
        cwx_d = abs(v2_wx_ctr[t] - exp.iloc[t]["CTR_TRMNAT_RT"])
        pwx_d = abs(v2_wx_pay[t] - exp.iloc[t]["PAY_TRMNAT_RT"])
        pyx_d = abs(v2_pyexsp_rt[t] - exp.iloc[t]["PYEXSP_DRPO_RSKRT"])
        print(f"  {t+1:>4} {ctr_d:>14.2e} {pay_d:>14.2e} {cwx_d:>14.2e} {pwx_d:>14.2e} {pyx_d:>14.2e}")

    # ── CSV 저장 ──
    if args.csv:
        csv_path = os.path.join(os.path.dirname(__file__), "v2_vs_proj_o2.csv")
        pd.DataFrame(results_detail).to_csv(csv_path, index=False)
        print(f"\n  CSV 저장: {csv_path}")

    # ── 정리 ──
    v2.close()
    if not args.keep_db and os.path.exists(V2_DB_PATH):
        os.remove(V2_DB_PATH)
    elif args.keep_db:
        print(f"\n  DuckDB 보존: {V2_DB_PATH}")


if __name__ == "__main__":
    main()

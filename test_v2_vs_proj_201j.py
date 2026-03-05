"""
v2 엔진 vs PROJ_O_201J20110004359.vdb OD_TBL_MN 비교 테스트

INFRC_IDNO 17, 50 (PROD=LA0201J) 두 건에 대해
v2 engine project_group() 결과와 전 시점 비교.

Usage:
    python test_v2_vs_proj_201j.py
    python test_v2_vs_proj_201j.py --keep-db
    python test_v2_vs_proj_201j.py --csv
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
PROJ_DB = os.path.join(os.path.dirname(__file__), "PROJ_O_201J20110004359.vdb")
V2_DB_PATH = os.path.join(os.path.dirname(__file__), "v2_test_201j.duckdb")

IDNOS = [17, 50]
CLOS_YM = "202309"
EXE_HIST_NO = "D000000001"


def load_expected(idno):
    """PROJ_O DB에서 특정 IDNO의 OD_TBL_MN 로드."""
    conn = sqlite3.connect(PROJ_DB)
    df = pd.read_sql_query("""
        SELECT * FROM OD_TBL_MN
        WHERE INFRC_IDNO = ? AND EXE_HIST_NO = ?
        ORDER BY SETL_AFT_PASS_MMCNT
    """, conn, params=[idno, EXE_HIST_NO])
    conn.close()
    return df


def compare_single(idno, result, ci, expected, args):
    """단일 계약 비교. ci = contract index in result arrays."""
    n_months = len(expected)
    max_t = result.tpx.shape[1]
    compare_n = min(n_months - 1, max_t)

    print(f"\n{'─' * 80}")
    print(f"  IDNO={idno}: {n_months}개월 데이터, 비교 {compare_n}개월 (SETL=1~{compare_n})")
    print(f"{'─' * 80}")

    # v2 결과 추출
    v2_tpx = result.tpx[ci, :compare_n]
    v2_pay_tpx = result.pay_tpx[ci, :compare_n]
    v2_wx_ctr = result.wx_monthly[ci, :compare_n]
    v2_wx_pay = result.wx_pay_monthly[ci, :compare_n]
    v2_d_rsvamt = result.d_rsvamt[ci, :compare_n]
    v2_d_bnft = result.d_bnft[ci, :compare_n]
    v2_d_pyexsp = result.d_pyexsp[ci, :compare_n]

    # tpx_bot (기시유지자)
    v2_tpx_bot = np.ones(compare_n)
    v2_tpx_bot[1:] = v2_tpx[:compare_n - 1]

    # pay_tpx_bot (기시납입자)
    v2_pay_tpx_bot = np.ones(compare_n)
    v2_pay_tpx_bot[1:] = v2_pay_tpx[:compare_n - 1]

    # 탈퇴자수
    v2_ctr_trmpsn = v2_tpx_bot * v2_wx_ctr
    v2_pay_trmpsn = v2_pay_tpx_bot * v2_wx_pay

    # 탈퇴율 역산
    v2_ctr_rsvamt_rt = np.where(v2_tpx_bot > 0, v2_d_rsvamt / v2_tpx_bot, 0)
    v2_ctr_bnft_rt = np.where(v2_tpx_bot > 0, v2_d_bnft / v2_tpx_bot, 0)
    v2_pyexsp_rt = np.where(v2_pay_tpx_bot > 0, v2_d_pyexsp / v2_pay_tpx_bot, 0)

    # 기대값 (SETL=1~compare_n)
    exp = expected.iloc[1:compare_n + 1].reset_index(drop=True)

    comparisons = [
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

    tol_rate = 1e-8
    tol_amount = 1e-8
    all_pass = True
    results_detail = []

    print(f"\n  {'항목':<28} {'Max Diff':>12} {'Max RDiff%':>12} {'기준':>10} {'결과':>6}")
    print(f"  {'-' * 74}")

    for name, v2_arr, exp_col, ctype in comparisons:
        exp_arr = exp[exp_col].values.astype(np.float64)
        diff = np.abs(v2_arr - exp_arr)
        max_diff = diff.max()
        max_diff_idx = int(diff.argmax())

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
            print(f"    -> 최대차이 t={max_diff_idx + 1}: "
                  f"v2={v2_arr[max_diff_idx]:.12f}, "
                  f"exp={exp_arr[max_diff_idx]:.12f}")

        if args.csv:
            for t in range(compare_n):
                results_detail.append({
                    "idno": idno, "t": t + 1, "item": name,
                    "v2": v2_arr[t], "expected": exp_arr[t],
                    "diff": diff[t], "rdiff_pct": rdiff[t] * 100,
                })

    print(f"\n  IDNO={idno}: {'모든 항목 PASS!' if all_pass else '일부 항목 FAIL'}")
    return all_pass, results_detail


def main():
    parser = argparse.ArgumentParser(description="v2 vs PROJ_O_201J OD_TBL_MN 비교")
    parser.add_argument("--keep-db", action="store_true", help="DuckDB 파일 보존")
    parser.add_argument("--csv", action="store_true", help="비교 결과 CSV 저장")
    args = parser.parse_args()

    print("=" * 80)
    print("v2 엔진 vs PROJ_O_201J20110004359.vdb OD_TBL_MN 비교")
    print(f"  대상: IDNO {IDNOS}")
    print("=" * 80)

    # ── 1. ETL ──
    print("\n[1] v2 ETL: VSOLN2.vdb → DuckDB")
    if os.path.exists(V2_DB_PATH):
        os.remove(V2_DB_PATH)

    t0 = time.time()
    v2 = migrate_legacy_db(
        legacy_path=LEGACY_DB,
        v2_path=V2_DB_PATH,
        infrc_seq=1,
        assm_ym="202306",
        idno_start=min(IDNOS),
        idno_end=max(IDNOS),
    )
    print(f"    ETL 완료 ({time.time()-t0:.1f}s)")

    # ETL 결과 확인
    contracts = v2.execute("SELECT * FROM dim_contract ORDER BY contract_id").fetchdf()
    print(f"    계약 {len(contracts)}건: {contracts['contract_id'].tolist()}")

    # ── 2. 계약별 프로젝션 + 비교 ──
    all_results = []
    grand_pass = True

    for idno in IDNOS:
        print(f"\n{'=' * 80}")
        print(f"[IDNO={idno}] 프로젝션 + 비교")

        # 기대값 로드
        expected = load_expected(idno)
        if len(expected) == 0:
            print(f"  경고: IDNO={idno} 기대값 없음, 건너뜀")
            continue

        # 계약 정보
        c = contracts[contracts["contract_id"] == idno]
        if len(c) == 0:
            print(f"  경고: IDNO={idno} ETL 결과 없음, 건너뜀")
            continue
        c = c.iloc[0]

        print(f"  prod={c['prod_cd']}, age={c['entry_age']}, bterm={c['bterm']}, pterm={c['pterm']}")
        print(f"  ctr_ym={c['ctr_ym']}, assm_profile={c['assm_profile']}")

        # GroupAssumptions 로드
        assm = load_group_assumptions(v2, c["assm_profile"], idno, max_duration=1200)
        print(f"  risks: {assm.risk_meta.risk_cds.tolist()}")
        print(f"  CTR exit: {assm.risk_meta.is_exit_ctr.sum()}개, PAY exit: {assm.risk_meta.is_exit_pay.sum()}개")

        # elapsed 계산
        ctr_ym = str(c["ctr_ym"])
        elapsed = (int(CLOS_YM[:4]) - int(ctr_ym[:4])) * 12 + (int(CLOS_YM[4:6]) - int(ctr_ym[4:6])) + 1

        # 프로젝션
        t1 = time.time()
        result = project_group(
            conn=v2,
            assm=assm,
            contract_ids=np.array([idno]),
            entry_ages=np.array([int(c["entry_age"])]),
            bterms=np.array([int(c["bterm"])]),
            pterms=np.array([int(c["pterm"])]),
            elapsed_months=np.array([elapsed], dtype=np.int32),
            clos_ym=CLOS_YM,
            max_proj_months=1200,
        )
        print(f"  프로젝션 완료 ({time.time()-t1:.1f}s), shape: {result.tpx.shape}")

        # 비교
        passed, detail = compare_single(idno, result, 0, expected, args)
        if not passed:
            grand_pass = False
        all_results.extend(detail)

    # ── 3. 최종 결과 ──
    print(f"\n{'=' * 80}")
    print(f"최종 결과: {'모든 계약 PASS!' if grand_pass else '일부 계약 FAIL'}")
    print("=" * 80)

    # CSV
    if args.csv and all_results:
        csv_path = os.path.join(os.path.dirname(__file__), "v2_vs_proj_201j.csv")
        pd.DataFrame(all_results).to_csv(csv_path, index=False)
        print(f"\n  CSV 저장: {csv_path}")

    # 정리
    v2.close()
    if not args.keep_db and os.path.exists(V2_DB_PATH):
        os.remove(V2_DB_PATH)
    elif args.keep_db:
        print(f"\n  DuckDB 보존: {V2_DB_PATH}")


if __name__ == "__main__":
    main()

"""
v2 엔진 vs PROJ_O_201J20110004359.vdb OD_TBL_MN 전체 비교 테스트

INFRC_IDNO 17 (주계약 CLA00500), 50 (특약 CLA10007) 검증.
Input: VSOLN.vdb, Expected: PROJ_O_201J20110004359.vdb

Usage:
    python test_v2_vs_proj_o_201j.py
    python test_v2_vs_proj_o_201j.py --idno 17
    python test_v2_vs_proj_o_201j.py --idno 50
    python test_v2_vs_proj_o_201j.py --csv
    python test_v2_vs_proj_o_201j.py --keep-db
"""

import argparse
import os
import time

import numpy as np
import pandas as pd
import sqlite3

from cf_module.v2.etl import migrate_legacy_db
from cf_module.v2.engine import load_group_assumptions, project_group


# -- 설정 --
LEGACY_DB = os.path.join(os.path.dirname(__file__), "VSOLN.vdb")
PROJ_DB = os.path.join(os.path.dirname(__file__), "PROJ_O_201J20110004359.vdb")
V2_DB_PATH = os.path.join(os.path.dirname(__file__), "v2_test_201j.duckdb")

CLOS_YM = "202309"
EXE_HIST_NO = "D000000001"  # 중복 제거용 (D000000001, D000000002 동일 데이터)


def load_expected(idno: int) -> pd.DataFrame:
    """PROJ_O_201J20110004359.vdb OD_TBL_MN 로드 (단일 EXE_HIST_NO)."""
    conn = sqlite3.connect(PROJ_DB)
    df = pd.read_sql_query(f"""
        SELECT * FROM OD_TBL_MN
        WHERE INFRC_IDNO = {idno}
          AND EXE_HIST_NO = '{EXE_HIST_NO}'
        ORDER BY SETL_AFT_PASS_MMCNT
    """, conn)
    conn.close()
    return df


def run_comparison(v2, idno: int, save_csv: bool = False):
    """단일 IDNO에 대해 v2 프로젝션 vs 기대값 비교."""
    print(f"\n{'=' * 80}")
    print(f"INFRC_IDNO = {idno}")
    print(f"{'=' * 80}")

    # -- 1. 기대값 로드 --
    print(f"\n[1] 기대값 로드 (EXE_HIST_NO={EXE_HIST_NO})")
    expected = load_expected(idno)
    n_months = len(expected)
    print(f"    {n_months}개월 데이터 (SETL 0~{expected['SETL_AFT_PASS_MMCNT'].max()})")

    # -- 2. dim_contract에서 계약 정보 조회 --
    print(f"\n[2] v2 dim_contract 조회")
    contracts = v2.execute("""
        SELECT * FROM dim_contract WHERE contract_id = ?
    """, [idno]).fetchdf()

    if contracts.empty:
        print(f"    ERROR: contract_id={idno} not found in dim_contract!")
        return False

    c = contracts.iloc[0]
    print(f"    contract_id={c['contract_id']}, entry_age={c['entry_age']}, "
          f"bterm={c['bterm']}, pterm={c['pterm']}, assm_profile={c['assm_profile']}")

    # -- 3. 가정 로드 + 프로젝션 --
    print(f"\n[3] 가정 로드 + 프로젝션")
    t0 = time.time()

    assm = load_group_assumptions(v2, c["assm_profile"], idno, max_duration=1200)

    ctr_ym = str(c["ctr_ym"])
    elapsed = (int(CLOS_YM[:4]) - int(ctr_ym[:4])) * 12 + (int(CLOS_YM[4:6]) - int(ctr_ym[4:6])) + 1

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
    print(f"    완료 ({time.time()-t0:.1f}s), shape: {result.tpx.shape}")

    # -- 4. 비교 --
    max_t = result.tpx.shape[1]
    compare_n = min(n_months - 1, max_t)
    print(f"\n[4] 비교: {compare_n}개월 (SETL=1~{compare_n})")

    # v2 결과 (contract index 0)
    v2_tpx = result.tpx[0, :compare_n]
    v2_pay_tpx = result.pay_tpx[0, :compare_n]
    v2_wx_ctr = result.wx_monthly[0, :compare_n]
    v2_wx_pay = result.wx_pay_monthly[0, :compare_n]
    v2_d_rsvamt = result.d_rsvamt[0, :compare_n]
    v2_d_bnft = result.d_bnft[0, :compare_n]
    v2_d_pyexsp = result.d_pyexsp[0, :compare_n]

    # tpx_bot
    v2_tpx_bot = np.ones(compare_n)
    v2_tpx_bot[1:] = v2_tpx[:compare_n - 1]

    v2_pay_tpx_bot = np.ones(compare_n)
    v2_pay_tpx_bot[1:] = v2_pay_tpx[:compare_n - 1]

    # 탈퇴자수
    v2_ctr_trmpsn = v2_tpx_bot * v2_wx_ctr
    v2_pay_trmpsn = v2_pay_tpx_bot * v2_wx_pay

    # 탈퇴율
    v2_ctr_rsvamt_rt = np.where(v2_tpx_bot > 0, v2_d_rsvamt / v2_tpx_bot, 0)
    v2_ctr_bnft_rt = np.where(v2_tpx_bot > 0, v2_d_bnft / v2_tpx_bot, 0)
    v2_pyexsp_rt = np.where(v2_pay_tpx_bot > 0, v2_d_pyexsp / v2_pay_tpx_bot, 0)

    # 기대값 (SETL=1~compare_n)
    exp = expected.iloc[1:compare_n + 1].reset_index(drop=True)

    # -- 비교 항목 --
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

    print(f"\n{'항목':<30} {'Max Diff':>12} {'Max RDiff%':>12} {'PASS 기준':>10} {'결과':>6}")
    print("-" * 80)

    tol_rate = 1e-8
    tol_amount = 1e-8
    all_pass = True
    results_detail = []

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
            print(f"    -> max diff t={max_diff_idx + 1}: "
                  f"v2={v2_arr[max_diff_idx]:.12f}, "
                  f"exp={exp_arr[max_diff_idx]:.12f}")

        for t in range(compare_n):
            results_detail.append({
                "idno": idno, "t": t + 1, "item": name,
                "v2": v2_arr[t], "expected": exp_arr[t],
                "diff": diff[t], "rdiff_pct": rdiff[t] * 100,
            })

    print("-" * 80)
    print(f"\n  IDNO={idno}: {'모든 항목 PASS!' if all_pass else '일부 항목 FAIL'}")

    # 처음 12개월 요약
    print(f"\n  [요약] 처음 12개월 최대 오차")
    print(f"  {'t':>4} {'CTR_TRME':>14} {'PAY_TRME':>14} {'CTR_WX':>14} {'PAY_WX':>14} {'PYEXSP_RT':>14}")
    for t in range(min(12, compare_n)):
        ctr_d = abs(v2_tpx[t] - exp.iloc[t]["CTR_TRME_MTNPSN_CNT"])
        pay_d = abs(v2_pay_tpx[t] - exp.iloc[t]["PAY_TRME_MTNPSN_CNT"])
        cwx_d = abs(v2_wx_ctr[t] - exp.iloc[t]["CTR_TRMNAT_RT"])
        pwx_d = abs(v2_wx_pay[t] - exp.iloc[t]["PAY_TRMNAT_RT"])
        pyx_d = abs(v2_pyexsp_rt[t] - exp.iloc[t]["PYEXSP_DRPO_RSKRT"])
        print(f"  {t+1:>4} {ctr_d:>14.2e} {pay_d:>14.2e} {cwx_d:>14.2e} {pwx_d:>14.2e} {pyx_d:>14.2e}")

    if save_csv:
        csv_path = os.path.join(os.path.dirname(__file__), f"v2_vs_201j_idno{idno}.csv")
        pd.DataFrame(results_detail).to_csv(csv_path, index=False)
        print(f"\n  CSV 저장: {csv_path}")

    return all_pass


def main():
    parser = argparse.ArgumentParser(description="v2 vs PROJ_O_201J20110004359.vdb 비교")
    parser.add_argument("--idno", type=int, default=None, help="특정 IDNO만 테스트 (17 or 50)")
    parser.add_argument("--keep-db", action="store_true", help="DuckDB 파일 보존")
    parser.add_argument("--csv", action="store_true", help="비교 결과 CSV 저장")
    args = parser.parse_args()

    idnos = [args.idno] if args.idno else [17, 50]

    print("=" * 80)
    print("v2 엔진 vs PROJ_O_201J20110004359.vdb OD_TBL_MN 비교")
    print(f"  Legacy DB: {LEGACY_DB}")
    print(f"  Expected:  {PROJ_DB}")
    print(f"  IDNOs:     {idnos}")
    print("=" * 80)

    # -- ETL --
    if os.path.exists(V2_DB_PATH):
        os.remove(V2_DB_PATH)

    idno_min = min(idnos)
    idno_max = max(idnos)

    print(f"\n[ETL] VSOLN.vdb -> DuckDB (IDNO {idno_min}~{idno_max})")
    t0 = time.time()
    v2 = migrate_legacy_db(
        legacy_path=LEGACY_DB,
        v2_path=V2_DB_PATH,
        infrc_seq=1,
        assm_ym="202306",
        idno_start=idno_min,
        idno_end=idno_max,
    )
    print(f"  완료 ({time.time()-t0:.1f}s)")

    # dim_contract 확인
    dc = v2.execute("SELECT contract_id, entry_age, bterm, pterm, assm_profile FROM dim_contract ORDER BY contract_id").fetchdf()
    print(f"  dim_contract: {len(dc)} rows")
    print(dc.to_string(index=False))

    # -- 비교 --
    all_pass = True
    for idno in idnos:
        passed = run_comparison(v2, idno, save_csv=args.csv)
        if not passed:
            all_pass = False

    # -- 최종 결과 --
    print(f"\n{'=' * 80}")
    print(f"최종 결과: {'ALL PASS!' if all_pass else 'SOME FAIL'}")
    print(f"{'=' * 80}")

    # -- 정리 --
    v2.close()
    if not args.keep_db and os.path.exists(V2_DB_PATH):
        os.remove(V2_DB_PATH)
    elif args.keep_db:
        print(f"\n  DuckDB 보존: {V2_DB_PATH}")


if __name__ == "__main__":
    main()

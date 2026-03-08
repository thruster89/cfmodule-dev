"""OD_RSK_RT / OD_LAPSE_RT 검증 테스트.

duckdb_transform.duckdb (raw 테이블) → RawAssumptionLoader → compute → PROJ_O2.vdb 비교

v2 ETL 불필요 — raw 테이블에서 직접 드라이버 매칭.
"""
import os
import sqlite3
import sys

import numpy as np

DB_PATH = "duckdb_transform.duckdb"
PROJ_O2_DB = r"C:\python\cf_module\PROJ_O2.vdb"
IDNO = 760397
TOL = 1e-12


def load_expected_rsk_rt(proj_conn, idno):
    """PROJ_O2.vdb에서 OD_RSK_RT 기대값 로드."""
    rows = proj_conn.execute("""
        SELECT SETL_AFT_PASS_MMCNT, CTR_AFT_PASS_MMCNT, RSK_RT_CD,
               RSK_RT, LOSS_RT, MTH_EFECT_COEF, BEPRD_DEFRY_RT,
               TRD_COEF, ARVL_AGE_COEF,
               INVLD_TRMNAT_BF_YR_RSK_RT, INVLD_TRMNAT_BF_MM_RSK_RT,
               INVLD_TRMNAT_AF_APLY_RSK_RT
        FROM OD_RSK_RT
        WHERE INFRC_IDNO = ?
        ORDER BY RSK_RT_CD, SETL_AFT_PASS_MMCNT
    """, (idno,)).fetchall()
    return rows


def load_expected_lapse_rt(proj_conn, idno):
    """PROJ_O2.vdb에서 OD_LAPSE_RT 기대값 로드."""
    rows = proj_conn.execute("""
        SELECT SETL_AFT_PASS_MMCNT, CTR_AFT_PASS_MMCNT,
               TRMNAT_RT, SKEW, APLY_TRMNAT_RT
        FROM OD_LAPSE_RT
        WHERE INFRC_IDNO = ?
        ORDER BY SETL_AFT_PASS_MMCNT
    """, (idno,)).fetchall()
    return rows


def main():
    import duckdb
    from cf_module.data.rsk_lapse_loader import RawAssumptionLoader
    from cf_module.calc.tbl_rsk_rt import compute_rsk_rt
    from cf_module.calc.tbl_lapse_rt import compute_lapse_rt

    print(f"OD_RSK_RT / OD_LAPSE_RT (IDNO={IDNO})")
    print("=" * 70)

    # -- 1. DB 연결 --
    print("\n[1] DB connect")
    con = duckdb.connect(DB_PATH, read_only=True)
    loader = RawAssumptionLoader(con)

    # -- 2. 계약 정보 + 가정 로드 --
    print("\n[2] load contract + assumptions")
    ctr = loader.load_contract(IDNO)
    risks = loader.load_risk_codes(ctr)
    risk_cds = [r.risk_cd for r in risks]

    mortality = loader.load_mortality_rates(risks, ctr)
    lapse_paying, lapse_paidup = loader.load_lapse_rates(ctr)
    skew = loader.load_skew(ctr)
    beprd = loader.load_beprd(ctr, risk_cds)
    invld = loader.load_invld_months(ctr)

    print(f"  PROD={ctr.prod_cd}, CLS={ctr.cls_cd}, COV={ctr.cov_cd}")
    print(f"  entry_age={ctr.entry_age}, bterm={ctr.bterm_yy}Y, pterm={ctr.pterm_yy}Y")
    print(f"  pass={ctr.pass_yy}Y{ctr.pass_mm}M")
    print(f"  risks: {risk_cds}")
    print(f"  invld: {invld}")
    print(f"  lapse_paying[:5]: {lapse_paying[:5]}")
    print(f"  skew[:5]: {skew[:5]}")

    # -- 3. PROJ_O2 기대값 --
    print("\n[3] PROJ_O2 load")
    proj_conn = sqlite3.connect(PROJ_O2_DB)
    rsk_rows = load_expected_rsk_rt(proj_conn, IDNO)
    lapse_rows = load_expected_lapse_rt(proj_conn, IDNO)
    proj_conn.close()

    n_steps = len(lapse_rows)
    print(f"  OD_RSK_RT: {len(rsk_rows)} rows, OD_LAPSE_RT: {n_steps} rows")

    # -- 4. OD_RSK_RT --
    print("\n[4] OD_RSK_RT compute")
    rsk_result = compute_rsk_rt(
        ctr=ctr, risks=risks, mortality_rates=mortality,
        beprd=beprd, invld_months=invld, n_steps=n_steps,
    )

    # 기대값을 risk별 분리
    exp_by_risk = {}
    for row in rsk_rows:
        rsk_cd = str(row[2])
        exp_by_risk.setdefault(rsk_cd, []).append(row)

    rsk_check_cols = [
        ("RSK_RT", 3), ("LOSS_RT", 4), ("MTH_EFECT_COEF", 5),
        ("BEPRD_DEFRY_RT", 6), ("TRD_COEF", 7), ("ARVL_AGE_COEF", 8),
        ("INVLD_TRMNAT_BF_YR_RSK_RT", 9),
        ("INVLD_TRMNAT_BF_MM_RSK_RT", 10),
        ("INVLD_TRMNAT_AF_APLY_RSK_RT", 11),
    ]

    total_pass = 0
    total_fail = 0

    for rsk_cd in sorted(exp_by_risk.keys()):
        exp_rows = exp_by_risk[rsk_cd]
        n = len(exp_rows)
        if rsk_cd not in rsk_result:
            print(f"  {rsk_cd}: SKIP (not computed)")
            continue

        comp = rsk_result[rsk_cd]
        risk_ok = True

        for col_name, col_idx in rsk_check_cols:
            exp_arr = np.array([r[col_idx] for r in exp_rows], dtype=np.float64)
            comp_arr = comp[col_name][:n]
            diff = np.abs(exp_arr - comp_arr)
            max_diff = diff.max()

            if max_diff > TOL:
                risk_ok = False
                total_fail += 1
                fb = np.argmax(diff > TOL)
                print(f"  {rsk_cd}.{col_name}: FAIL max={max_diff:.2e} "
                      f"t={fb} exp={exp_arr[fb]:.12f} comp={comp_arr[fb]:.12f}")
            else:
                total_pass += 1

        if risk_ok:
            print(f"  {rsk_cd}: ALL PASS ({n} x 9 cols)")

    print(f"\n  RSK_RT: PASS={total_pass}, FAIL={total_fail}")

    # -- 5. OD_LAPSE_RT --
    print("\n[5] OD_LAPSE_RT compute")
    lapse_result = compute_lapse_rt(
        ctr=ctr, lapse_paying=lapse_paying, lapse_paidup=lapse_paidup,
        skew=skew, n_steps=n_steps,
    )

    lapse_check = [("TRMNAT_RT", 2), ("SKEW", 3), ("APLY_TRMNAT_RT", 4)]
    lapse_pass = 0
    lapse_fail = 0

    for col_name, col_idx in lapse_check:
        exp_arr = np.array([r[col_idx] for r in lapse_rows], dtype=np.float64)
        comp_arr = lapse_result[col_name][:n_steps]
        diff = np.abs(exp_arr - comp_arr)
        max_diff = diff.max()

        if max_diff > TOL:
            lapse_fail += 1
            fb = np.argmax(diff > TOL)
            print(f"  {col_name}: FAIL max={max_diff:.2e} "
                  f"t={fb} exp={exp_arr[fb]:.16f} comp={comp_arr[fb]:.16f}")
            bad = np.where(diff > TOL)[0][:3]
            for bi in bad:
                print(f"    t={bi}: exp={exp_arr[bi]:.16f} comp={comp_arr[bi]:.16f}")
        else:
            lapse_pass += 1
            print(f"  {col_name}: PASS (max={max_diff:.2e})")

    print(f"\n  LAPSE_RT: PASS={lapse_pass}, FAIL={lapse_fail}")

    con.close()

    ok = (total_fail == 0) and (lapse_fail == 0)
    print("\n" + "=" * 70)
    print("ALL PASS" if ok else f"FAIL: RSK {total_fail}, LAPSE {lapse_fail}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

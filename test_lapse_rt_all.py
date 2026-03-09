"""OD_LAPSE_RT 전건 검증 테스트.

duckdb_transform.duckdb의 OD_LAPSE_RT 기대값 vs compute_lapse_rt 비교.
42,001건 전체 대상.
"""
import sys
import time

import duckdb
import numpy as np

DB_PATH = "duckdb_transform.duckdb"
TOL = 1e-12


def main():
    from cf_module.data.rsk_lapse_loader import RawAssumptionLoader
    from cf_module.calc.tbl_lapse_rt import compute_lapse_rt

    con = duckdb.connect(DB_PATH, read_only=True)
    loader = RawAssumptionLoader(con)

    # 계약 목록
    idnos = con.execute("""
        SELECT DISTINCT INFRC_IDNO FROM OD_LAPSE_RT ORDER BY INFRC_IDNO
    """).fetchall()
    idnos = [r[0] for r in idnos]
    print(f"OD_LAPSE_RT 검증: {len(idnos)}건")
    print("=" * 70)

    # 기대값 일괄 로드
    print("[1] Loading expected data...")
    t0 = time.time()
    exp_df = con.execute("""
        SELECT INFRC_IDNO, SETL_AFT_PASS_MMCNT,
               TRMNAT_RT, SKEW, APLY_TRMNAT_RT
        FROM OD_LAPSE_RT
        ORDER BY INFRC_IDNO, SETL_AFT_PASS_MMCNT
    """).fetchdf()
    print(f"  {len(exp_df)} rows loaded in {time.time()-t0:.1f}s")

    # IDNO별 그룹핑
    grouped = {}
    for idno in idnos:
        mask = exp_df["INFRC_IDNO"] == idno
        grouped[idno] = exp_df[mask]

    # 결과 집계
    total_pass = 0
    total_fail = 0
    fail_details = []
    skip_count = 0
    err_count = 0

    print(f"\n[2] Computing and comparing...")
    t1 = time.time()

    for i, idno in enumerate(idnos):
        if (i + 1) % 5000 == 0:
            print(f"  {i+1}/{len(idnos)}...")

        try:
            ctr = loader.load_contract(idno)
        except Exception as e:
            err_count += 1
            if err_count <= 5:
                print(f"  IDNO={idno}: load_contract ERROR: {e}")
            continue

        try:
            lapse_pay, lapse_paidup = loader.load_lapse_rates(ctr)
            skew = loader.load_skew(ctr)
        except Exception as e:
            err_count += 1
            if err_count <= 5:
                print(f"  IDNO={idno}: load_assumptions ERROR: {e}")
            continue

        exp = grouped[idno]
        n_steps = len(exp)

        try:
            result = compute_lapse_rt(ctr, lapse_pay, lapse_paidup, skew, n_steps)
        except Exception as e:
            err_count += 1
            if err_count <= 5:
                print(f"  IDNO={idno}: compute ERROR: {e}")
            continue

        # 비교
        exp_trmnat = exp["TRMNAT_RT"].values.astype(np.float64)
        exp_skew = exp["SKEW"].values.astype(np.float64)
        exp_aply = exp["APLY_TRMNAT_RT"].values.astype(np.float64)

        cols = [
            ("TRMNAT_RT", result["TRMNAT_RT"][:n_steps], exp_trmnat),
            ("SKEW", result["SKEW"][:n_steps], exp_skew),
            ("APLY_TRMNAT_RT", result["APLY_TRMNAT_RT"][:n_steps], exp_aply),
        ]

        contract_ok = True
        for col_name, comp_arr, exp_arr in cols:
            diff = np.abs(comp_arr - exp_arr)
            max_diff = diff.max()
            if max_diff > TOL:
                contract_ok = False
                total_fail += 1
                fb = int(np.argmax(diff > TOL))
                if len(fail_details) < 30:
                    fail_details.append(
                        f"  IDNO={idno} {col_name}: max={max_diff:.2e} "
                        f"t={fb} exp={exp_arr[fb]:.12f} comp={comp_arr[fb]:.12f}"
                    )
            else:
                total_pass += 1

        if not contract_ok and len(fail_details) <= 30:
            pass  # already added above

    elapsed = time.time() - t1
    print(f"\n  Computed {len(idnos)} contracts in {elapsed:.1f}s")

    # 결과 출력
    print(f"\n{'=' * 70}")
    print(f"PASS: {total_pass}  FAIL: {total_fail}  ERROR: {err_count}")
    print(f"Contracts: {len(idnos)}  Columns: 3  (TRMNAT_RT, SKEW, APLY_TRMNAT_RT)")

    if fail_details:
        print(f"\nFail details (first {len(fail_details)}):")
        for d in fail_details:
            print(d)

    ok = total_fail == 0 and err_count == 0
    print(f"\n{'ALL PASS' if ok else 'FAIL'}")

    con.close()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

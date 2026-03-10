"""OD_TBL_MN 검증 테스트.

duckdb_transform.duckdb의 OD_TBL_MN 기대값 vs compute_tbl_mn 비교.
입력: OD_RSK_RT (INVLD_TRMNAT_AF_APLY_RSK_RT), OD_LAPSE_RT (APLY_TRMNAT_RT)

Usage:
    python test_tbl_mn.py                    # 전건 (42,001건)
    python test_tbl_mn.py --idno 760397      # 단건
    python test_tbl_mn.py --n 100            # 처음 100건
"""
import argparse
import sys
import time

import duckdb
import numpy as np

DB_PATH = "duckdb_transform.duckdb"
TOL = 1e-10

# 비교 대상 컬럼 (OD_TBL_MN 컬럼명)
CHECK_COLS = [
    "CTR_TRMO_MTNPSN_CNT",
    "CTR_TRMNAT_RT",
    "CTR_RSVAMT_DEFRY_DRPO_RSKRT",
    "CTR_BNFT_DRPO_RSKRT",
    "CTR_TRMPSN_CNT",
    "CTR_RSVAMT_DEFRY_DRPSN_CNT",
    "CTR_BNFT_DEFRY_DRPSN_CNT",
    "CTR_TRME_MTNPSN_CNT",
    "PAY_TRMO_MTNPSN_CNT",
    "PAY_TRMNAT_RT",
    "PAY_RSVAMT_DEFRY_DRPO_RSKRT",
    "PAY_BNFT_DRPO_RSKRT",
    "PYEXSP_DRPO_RSKRT",
    "PAY_TRMPSN_CNT",
    "PAY_RSVAMT_DEFRY_DRPSN_CNT",
    "PAY_BNFT_DEFRY_DRPSN_CNT",
    "PYEXSP_DRPSN_CNT",
    "PAY_TRME_MTNPSN_CNT",
]


def load_rsk_rt_grouped(con, idnos):
    """OD_RSK_RT에서 INVLD_TRMNAT_AF_APLY_RSK_RT 일괄 로드."""
    df = con.execute("""
        SELECT INFRC_IDNO, RSK_RT_CD, INVLD_TRMNAT_AF_APLY_RSK_RT
        FROM OD_RSK_RT
        ORDER BY INFRC_IDNO, RSK_RT_CD, SETL_AFT_PASS_MMCNT
    """).fetchnumpy()

    idno_arr = df["INFRC_IDNO"]
    rsk_arr = df["RSK_RT_CD"]
    val_arr = df["INVLD_TRMNAT_AF_APLY_RSK_RT"].astype(np.float64)

    idno_set = set(idnos)
    grouped = {idno: {} for idno in idnos}

    # 연속된 (INFRC_IDNO, RSK_RT_CD) 그룹을 슬라이싱
    n = len(idno_arr)
    i = 0
    while i < n:
        cur_idno = int(idno_arr[i])
        cur_rsk = str(rsk_arr[i])
        j = i + 1
        while j < n and int(idno_arr[j]) == cur_idno and str(rsk_arr[j]) == cur_rsk:
            j += 1
        if cur_idno in idno_set:
            grouped[cur_idno][cur_rsk] = val_arr[i:j].copy()
        i = j

    return grouped


def load_lapse_rt_grouped(con, idnos):
    """OD_LAPSE_RT에서 APLY_TRMNAT_RT 일괄 로드."""
    df = con.execute("""
        SELECT INFRC_IDNO, APLY_TRMNAT_RT
        FROM OD_LAPSE_RT
        ORDER BY INFRC_IDNO, SETL_AFT_PASS_MMCNT
    """).fetchnumpy()

    idno_arr = df["INFRC_IDNO"]
    val_arr = df["APLY_TRMNAT_RT"].astype(np.float64)

    idno_set = set(idnos)
    grouped = {}

    n = len(idno_arr)
    i = 0
    while i < n:
        cur_idno = int(idno_arr[i])
        j = i + 1
        while j < n and int(idno_arr[j]) == cur_idno:
            j += 1
        if cur_idno in idno_set:
            grouped[cur_idno] = val_arr[i:j].copy()
        i = j

    return grouped


def main():
    from cf_module.data.rsk_lapse_loader import RawAssumptionLoader, RiskInfo
    from cf_module.calc.tbl_mn import compute_tbl_mn

    parser = argparse.ArgumentParser(description="OD_TBL_MN 검증")
    parser.add_argument("--idno", type=int, default=None, help="단건 IDNO")
    parser.add_argument("--n", type=int, default=None, help="처음 N건만")
    args = parser.parse_args()

    con = duckdb.connect(DB_PATH, read_only=True)
    loader = RawAssumptionLoader(con)

    # 계약 목록
    if args.idno:
        idnos = [args.idno]
    else:
        idnos = [r[0] for r in con.execute("""
            SELECT DISTINCT INFRC_IDNO FROM OD_TBL_MN ORDER BY INFRC_IDNO
        """).fetchall()]
        if args.n:
            idnos = idnos[:args.n]

    print(f"OD_TBL_MN 검증: {len(idnos)}건")
    print("=" * 70)

    # 기대값 일괄 로드
    print("[1] Loading expected data...")
    t0 = time.time()
    col_sql = ", ".join(["INFRC_IDNO", "SETL_AFT_PASS_MMCNT"] + CHECK_COLS)
    if args.idno:
        exp_df = con.execute(f"""
            SELECT {col_sql} FROM OD_TBL_MN
            WHERE INFRC_IDNO = ?
            ORDER BY INFRC_IDNO, SETL_AFT_PASS_MMCNT
        """, [args.idno]).fetchdf()
    else:
        exp_df = con.execute(f"""
            SELECT {col_sql} FROM OD_TBL_MN
            ORDER BY INFRC_IDNO, SETL_AFT_PASS_MMCNT
        """).fetchdf()
    print(f"  OD_TBL_MN: {len(exp_df)} rows in {time.time()-t0:.1f}s")

    # OD_RSK_RT / OD_LAPSE_RT 일괄 로드
    t0 = time.time()
    rsk_rt_grouped = load_rsk_rt_grouped(con, idnos)
    print(f"  OD_RSK_RT grouped in {time.time()-t0:.1f}s")

    t0 = time.time()
    lapse_rt_grouped = load_lapse_rt_grouped(con, idnos)
    print(f"  OD_LAPSE_RT grouped in {time.time()-t0:.1f}s")

    # IDNO별 그룹핑
    grouped = {}
    for idno in idnos:
        mask = exp_df["INFRC_IDNO"] == idno
        grouped[idno] = exp_df[mask]

    # 결과 집계
    col_pass = {c: 0 for c in CHECK_COLS}
    col_fail = {c: 0 for c in CHECK_COLS}
    err_count = 0
    fail_details = []

    print(f"\n[2] Computing and comparing...")
    t1 = time.time()

    for i, idno in enumerate(idnos):
        if (i + 1) % 5000 == 0:
            print(f"  {i+1}/{len(idnos)}...")

        try:
            ctr = loader.load_contract(idno)
            risks = loader.load_risk_codes(ctr)
            exit_flags = loader.load_exit_flags(ctr, risks)

            # OD_RSK_RT에 있는 risk_cd 중 risks에 없는 것 추가 (C1/C2 등)
            rsk_rates = rsk_rt_grouped.get(idno, {})
            existing_cds = {r.risk_cd for r in risks}
            for rsk_cd in rsk_rates:
                if rsk_cd not in existing_cds:
                    if rsk_cd in exit_flags:
                        risks.append(RiskInfo(
                            risk_cd=rsk_cd,
                            chr_cd="X",       # 가상 코드
                            mm_trf_way_cd=0,
                            dead_rt_dvcd=0,   # 사망위험 (C1/C2 등)
                            rsk_grp_no=f"__{rsk_cd}__",
                        ))

            wx_monthly = lapse_rt_grouped.get(idno, np.zeros(0))
        except Exception as e:
            err_count += 1
            if err_count <= 5:
                print(f"  IDNO={idno}: load ERROR: {e}")
            continue

        exp = grouped[idno]
        n_steps = len(exp)

        # wx_monthly 길이 맞추기
        if len(wx_monthly) < n_steps:
            wx_monthly = np.pad(wx_monthly, (0, n_steps - len(wx_monthly)))
        elif len(wx_monthly) > n_steps:
            wx_monthly = wx_monthly[:n_steps]

        try:
            result = compute_tbl_mn(
                ctr=ctr, risks=risks,
                qx_monthly_rates=rsk_rates,
                wx_monthly=wx_monthly,
                exit_flags=exit_flags, n_steps=n_steps,
            )
        except Exception as e:
            err_count += 1
            if err_count <= 5:
                print(f"  IDNO={idno}: compute ERROR: {e}")
            continue

        # 비교
        for col_name in CHECK_COLS:
            exp_arr = exp[col_name].values.astype(np.float64)
            comp_arr = result[col_name][:n_steps]
            diff = np.abs(comp_arr - exp_arr)
            max_diff = diff.max()

            if max_diff > TOL:
                col_fail[col_name] += 1
                fb = int(np.argmax(diff > TOL))
                if len(fail_details) < 50:
                    fail_details.append(
                        f"  IDNO={idno} {col_name}: max={max_diff:.2e} "
                        f"t={fb} exp={exp_arr[fb]:.12f} comp={comp_arr[fb]:.12f}"
                    )
            else:
                col_pass[col_name] += 1

    elapsed_sec = time.time() - t1
    print(f"\n  Computed {len(idnos)} contracts in {elapsed_sec:.1f}s")

    # 결과 출력
    total_pass = sum(col_pass.values())
    total_fail = sum(col_fail.values())

    print(f"\n{'=' * 70}")
    print(f"{'Column':<40} {'PASS':>8} {'FAIL':>8}")
    print("-" * 70)
    for col_name in CHECK_COLS:
        p, f = col_pass[col_name], col_fail[col_name]
        status = "OK" if f == 0 else "FAIL"
        print(f"  {col_name:<38} {p:>8} {f:>8}  [{status}]")
    print("-" * 70)
    print(f"  {'TOTAL':<38} {total_pass:>8} {total_fail:>8}  ERROR={err_count}")

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

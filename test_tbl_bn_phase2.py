"""OD_TBL_BN Phase 2 검증 테스트.

Per-BNFT 독립 dedup으로 Rate 4개 컬럼 + 파생 12개 컬럼 전부 자체 산출 후 기대값 비교.

Usage:
    python test_tbl_bn_phase2.py
    python test_tbl_bn_phase2.py --n 100
    python test_tbl_bn_phase2.py --idno 1061324
    python test_tbl_bn_phase2.py --all
"""
import argparse
import sys
import time

import duckdb
import numpy as np

DB_PATH = "duckdb_transform.duckdb"

ALL_COLS = [
    "TRMNAT_RT", "RSVAMT_DEFRY_DRPO_RSKRT",
    "BNFT_DRPO_RSKRT", "BNFT_RSKRT",
    "TRMO_MTNPSN_CNT", "TRMPSN_CNT",
    "RSVAMT_DEFRY_DRPSN_CNT", "DEFRY_DRPSN_CNT",
    "TRME_MTNPSN_CNT", "BNFT_OCURPE_CNT",
    "CRIT_AMT", "DEFRY_RT", "PRTT_RT", "GRADIN_RT",
    "PYAMT", "BNFT_INSUAMT",
]


def verify_bn_idno(con, idno, bn_cache, trad_pv_result=None):
    """단건 BN Phase 2 검증."""
    from cf_module.calc.tbl_bn import compute_bn

    # 계약 정보
    info = con.execute("""
        SELECT PROD_CD, COV_CD, CLS_CD,
               PASS_YYCNT, PASS_MMCNT, INSTRM_YYCNT,
               GRNTPT_JOIN_AMT
        FROM II_INFRC
        WHERE INFRC_SEQ = 1 AND INFRC_IDNO = ?
    """, [idno]).fetchone()
    if not info:
        return None

    prod_cd = info[0]
    cov_cd = info[1]
    cls_cd = str(info[2]).zfill(2) if info[2] else "01"
    pass_yy = int(info[3] or 0)
    pass_mm = int(info[4] or 0)
    bterm_yy = int(info[5] or 0)
    join_amt = float(info[6] or 0)

    # BN 기대값 로드
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

    if bn_df is None or len(bn_df) == 0:
        return None

    # Raw rates
    rsk_df = con.execute("""
        SELECT RSK_RT_CD, SETL_AFT_PASS_MMCNT, INVLD_TRMNAT_AF_APLY_RSK_RT
        FROM OD_RSK_RT WHERE INFRC_SEQ = 1 AND INFRC_IDNO = ?
        ORDER BY RSK_RT_CD, SETL_AFT_PASS_MMCNT
    """, [idno]).fetchdf()
    qx_rates = {}
    for rsk_cd, g in rsk_df.groupby("RSK_RT_CD"):
        qx_rates[str(rsk_cd)] = g.sort_values("SETL_AFT_PASS_MMCNT")[
            "INVLD_TRMNAT_AF_APLY_RSK_RT"
        ].values

    lapse_df = con.execute("""
        SELECT APLY_TRMNAT_RT FROM OD_LAPSE_RT
        WHERE INFRC_SEQ = 1 AND INFRC_IDNO = ?
        ORDER BY SETL_AFT_PASS_MMCNT
    """, [idno]).fetchdf()
    wx = lapse_df["APLY_TRMNAT_RT"].values
    n_steps = len(wx)

    # BN cache data — BNFT 관련: CLS=01 우선, 없으면 계약 CLS_CD 사용
    risk_meta = bn_cache.get_risk_meta(prod_cd, cls_cd, cov_cd)
    rsvamt_cds = bn_cache.get_rsvamt_cds(prod_cd, cls_cd, cov_cd)
    bn_cls = "01"
    bnft_mapping = bn_cache.get_bnft_risk_mapping(prod_cd, bn_cls, cov_cd)
    if not bnft_mapping:
        bn_cls = cls_cd
        bnft_mapping = bn_cache.get_bnft_risk_mapping(prod_cd, bn_cls, cov_cd)

    # TRAD_PV 적립금 (PRTT 산출용)
    acum = None
    if trad_pv_result is not None:
        acum = trad_pv_result.get("APLY_PREM_ACUMAMT_BNFT")

    # compute_bn (Phase 2)
    result = compute_bn(
        idno=idno,
        n_steps=n_steps,
        prod_cd=prod_cd,
        cls_cd=cls_cd,
        cov_cd=cov_cd,
        pass_yy=pass_yy,
        pass_mm=pass_mm,
        bterm_yy=bterm_yy,
        join_amt=join_amt,
        qx_monthly_rates=qx_rates,
        wx_monthly=wx,
        risk_meta=risk_meta,
        rsvamt_cds=rsvamt_cds,
        bnft_mapping=bnft_mapping,
        get_defry_rate_fn=lambda bno, dy: bn_cache.get_defry_rate(
            prod_cd, bn_cls, cov_cd, bno, dy
        ),
        get_prtt_rate_fn=lambda bno, dy: bn_cache.get_prtt_rate(
            prod_cd, bn_cls, cov_cd, bno, dy
        ),
        get_ncov_months_fn=lambda bno: bn_cache.get_ncov_months(
            prod_cd, bn_cls, cov_cd, bno
        ),
        aply_prem_acumamt_bnft=acum,
    )

    # 검증
    col_pass = {c: 0 for c in ALL_COLS}
    col_fail = {c: 0 for c in ALL_COLS}
    col_max_diff = {c: 0.0 for c in ALL_COLS}
    col_fail_examples = {c: [] for c in ALL_COLS}
    n_bnft_pass = 0
    n_bnft_fail = 0

    for bnft_no, bnft_df in bn_df.groupby("BNFT_NO"):
        bnft_df = bnft_df.sort_values("SETL_AFT_PASS_MMCNT").reset_index(drop=True)
        n = len(bnft_df)

        # All-zero TRMO -> skip
        if bnft_df["TRMO_MTNPSN_CNT"].max() == 0:
            continue

        bn_result = result.bnft_results.get(int(bnft_no))
        if bn_result is None:
            n_bnft_fail += 1
            continue

        computed = bn_result.to_dict()
        all_pass = True

        for col in ALL_COLS:
            if col not in computed or col not in bnft_df.columns:
                continue

            comp = computed[col][:n]
            exv = bnft_df[col].values[:n].astype(np.float64)
            diff = np.max(np.abs(comp - exv))

            if diff < 1e-6:
                col_pass[col] += 1
            else:
                col_fail[col] += 1
                all_pass = False
                if diff > col_max_diff[col]:
                    col_max_diff[col] = diff
                if len(col_fail_examples[col]) < 5:
                    idx = int(np.argmax(np.abs(comp - exv)))
                    col_fail_examples[col].append(
                        f"IDNO={idno} BNFT={bnft_no} t={idx} "
                        f"comp={comp[idx]:.10f} exp={exv[idx]:.10f} diff={diff:.10f}"
                    )

        if all_pass:
            n_bnft_pass += 1
        else:
            n_bnft_fail += 1

    return col_pass, col_fail, col_max_diff, col_fail_examples, n_bnft_pass, n_bnft_fail


def main():
    parser = argparse.ArgumentParser(description="OD_TBL_BN Phase 2")
    parser.add_argument("--n", type=int, default=50)
    parser.add_argument("--idno", type=int, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    t_start = time.time()
    con = duckdb.connect(DB_PATH, read_only=True)

    from cf_module.data.bn_loader import BNDataCache
    bn_cache = BNDataCache(con)
    print(f"BN cache: {time.time() - t_start:.2f}s")

    from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached
    from cf_module.calc.trad_pv import compute_trad_pv
    pv_cache = TradPVDataCache(con)
    print(f"PV cache: {time.time() - t_start:.2f}s")

    # target IDNOs
    if args.idno:
        target_ids = [args.idno]
    elif args.all:
        target_ids = con.execute(
            "SELECT DISTINCT INFRC_IDNO FROM OD_TBL_BN WHERE INFRC_SEQ = 1"
        ).fetchdf()["INFRC_IDNO"].values.tolist()
    else:
        all_ids = con.execute(
            "SELECT DISTINCT INFRC_IDNO FROM OD_TBL_BN WHERE INFRC_SEQ = 1"
        ).fetchdf()["INFRC_IDNO"].values
        rng = np.random.RandomState(args.seed)
        idx = rng.choice(len(all_ids), min(args.n, len(all_ids)), replace=False)
        target_ids = all_ids[idx].tolist()

    print(f"Target: {len(target_ids)}")

    # MN data for TRAD_PV
    mn_all = con.execute("""
        SELECT INFRC_IDNO, CTR_TRMO_MTNPSN_CNT, PAY_TRMO_MTNPSN_CNT, CTR_TRME_MTNPSN_CNT
        FROM OD_TBL_MN WHERE INFRC_SEQ = 1
        ORDER BY INFRC_IDNO, SETL_AFT_PASS_MMCNT
    """).fetchdf()
    mn_grouped = {i: g for i, g in mn_all.groupby("INFRC_IDNO")}
    del mn_all

    # compute & verify
    t_calc = time.time()
    total_pass = {c: 0 for c in ALL_COLS}
    total_fail = {c: 0 for c in ALL_COLS}
    total_max_diff = {c: 0.0 for c in ALL_COLS}
    total_fail_examples = {c: [] for c in ALL_COLS}
    n_ok = 0
    n_err = 0
    n_skip = 0
    total_bnft_pass = 0
    total_bnft_fail = 0

    for gi, idno in enumerate(target_ids):
        try:
            # TRAD_PV
            pv_info = build_contract_info_cached(pv_cache, idno)
            pv_result = None
            if pv_info:
                mn = mn_grouped.get(idno)
                if mn is not None and len(mn) > 0:
                    pv_r = compute_trad_pv(
                        pv_info, len(mn),
                        pay_trmo=mn["PAY_TRMO_MTNPSN_CNT"].values,
                        ctr_trmo=mn["CTR_TRMO_MTNPSN_CNT"].values,
                        ctr_trme=mn["CTR_TRME_MTNPSN_CNT"].values,
                        fast_mode=True,
                    )
                    pv_result = pv_r.to_dict()

            result = verify_bn_idno(con, idno, bn_cache, pv_result)
            if result is None:
                n_skip += 1
                continue

            cp, cf, cmd, cfe, bp, bf = result
            for c in ALL_COLS:
                total_pass[c] += cp[c]
                total_fail[c] += cf[c]
                if cmd[c] > total_max_diff[c]:
                    total_max_diff[c] = cmd[c]
                total_fail_examples[c].extend(cfe[c])
            total_bnft_pass += bp
            total_bnft_fail += bf

            if bf == 0:
                n_ok += 1
            else:
                n_err += 1

        except Exception as e:
            n_err += 1
            if n_err <= 3:
                print(f"  ERR IDNO={idno}: {e}")
                import traceback
                traceback.print_exc()

        if (gi + 1) % max(1, len(target_ids) // 10) == 0:
            print(f"  [{gi+1}/{len(target_ids)}] OK={n_ok} FAIL={n_err} SKIP={n_skip}")

    elapsed_calc = time.time() - t_calc
    total_time = time.time() - t_start

    # results
    print(f"\n{'='*70}")
    print(f"OD_TBL_BN Phase 2: {len(target_ids)}, "
          f"OK={n_ok}, FAIL={n_err}, SKIP={n_skip}")
    print(f"BNFT: PASS={total_bnft_pass}, FAIL={total_bnft_fail}")
    print(f"Calc: {elapsed_calc:.1f}s, Total: {total_time:.1f}s")
    print(f"{'='*70}")

    print(f"\n{'Col':<35s} {'PASS':>7s} {'FAIL':>7s} {'max_diff':>12s}")
    print("-" * 65)
    for col in ALL_COLS:
        ps = total_pass[col]
        fl = total_fail[col]
        md = total_max_diff[col]
        tag = "PASS" if fl == 0 else "FAIL"
        print(f"{col:<35s} {ps:>7d} {fl:>7d} {md:>12.6f}  {tag}")

    fail_cols = [c for c in ALL_COLS if total_fail[c] > 0]
    if fail_cols:
        print(f"\n{'='*70}")
        print("FAIL details (max 5 per col)")
        print(f"{'='*70}")
        for col in fail_cols:
            print(f"\n{col} (FAIL={total_fail[col]}, max_diff={total_max_diff[col]:.10f}):")
            for ex in total_fail_examples[col][:5]:
                print(f"  {ex}")

    con.close()


if __name__ == "__main__":
    main()

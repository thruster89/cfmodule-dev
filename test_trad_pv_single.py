"""OD_TRAD_PV 단건 검증 스크립트.

Usage:
    python test_trad_pv_single.py --idno 625683
    python test_trad_pv_single.py --idno 625683 --detail    # t별 상세 출력
    python test_trad_pv_single.py --idno 625683 --cols APLY_ADINT_TGT_AMT,LWST_PREM_ACUMAMT
"""
import argparse
import duckdb
import numpy as np
from cf_module.calc.trad_pv import compute_trad_pv
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached

DB_PATH = 'duckdb_transform.duckdb'


def main():
    parser = argparse.ArgumentParser(description="OD_TRAD_PV 단건 검증")
    parser.add_argument("--idno", type=int, required=True, help="INFRC_IDNO")
    parser.add_argument("--detail", action="store_true", help="t별 상세 출력")
    parser.add_argument("--cols", type=str, default=None,
                        help="검증할 컬럼 (쉼표 구분, 미지정 시 전체)")
    parser.add_argument("--head", type=int, default=20, help="detail 모드 출력 행수")
    args = parser.parse_args()

    con = duckdb.connect(DB_PATH, read_only=True)
    cache = TradPVDataCache(con)

    idno = args.idno
    info = build_contract_info_cached(cache, idno)
    if not info:
        print(f"IDNO={idno}: build_contract_info 실패")
        return

    n_steps = con.execute(
        f'SELECT COUNT(*) FROM OD_TRAD_PV WHERE INFRC_IDNO={idno}'
    ).fetchone()[0]
    if n_steps == 0:
        print(f"IDNO={idno}: OD_TRAD_PV 데이터 없음")
        return

    mn = con.execute(
        f'SELECT CTR_TRMO_MTNPSN_CNT, PAY_TRMO_MTNPSN_CNT, CTR_TRME_MTNPSN_CNT '
        f'FROM OD_TBL_MN WHERE INFRC_IDNO={idno} ORDER BY SETL_AFT_PASS_MMCNT'
    ).fetchdf()

    result = compute_trad_pv(
        info, n_steps,
        pay_trmo=mn['PAY_TRMO_MTNPSN_CNT'].values if len(mn) > 0 else None,
        ctr_trmo=mn['CTR_TRMO_MTNPSN_CNT'].values if len(mn) > 0 else None,
        ctr_trme=mn['CTR_TRME_MTNPSN_CNT'].values if len(mn) > 0 else None,
    )
    d = result.to_dict()
    exp = con.execute(
        f'SELECT * FROM OD_TRAD_PV WHERE INFRC_IDNO={idno} ORDER BY SETL_AFT_PASS_MMCNT'
    ).fetchdf()

    # --- 계약 정보 출력 ---
    print(f"{'='*70}")
    print(f"IDNO={idno}  prod={info.prod_cd}  cov={info.cov_cd}  cls={info.cls_cd}")
    print(f"tpcd={info.ctr_tpcd}  pass_yy={info.pass_yy}  pass_mm={info.pass_mm}"
          f"  pterm={info.pterm_yy}  bterm={info.bterm_yy}")
    print(f"gprem={info.gprem}  pay_stcd={info.pay_stcd}  paycyc={info.paycyc}")
    print(f"bas={'Y' if info.bas else 'N'}  accmpt_rspb={info.accmpt_rspb_rsvamt}")
    print(f"nprem_nobas={info.acum_nprem_nobas:.4f}  nprem_old={info.acum_nprem_old:.4f}"
          f"  amort_mm={info.amort_mm}")
    if info.acum_cov:
        ac = info.acum_cov
        print(f"acum_cov: inrt_cd={ac['aply_inrt_cd']}  lwst={ac['lwst_grnt_inrt']}"
              f"  chng_cd={ac.get('lwst_chng_crit_cd',0)}")
    print(f"n_steps={n_steps}  mn_rows={len(mn)}")
    print(f"{'='*70}")

    # --- 컬럼 비교 ---
    all_cols = [c for c in d if c in exp.columns]
    if args.cols:
        check_cols = [c.strip() for c in args.cols.split(",")]
    else:
        check_cols = all_cols

    pass_cnt = 0
    fail_cnt = 0
    fail_cols = []

    print(f"\n{'컬럼':<30s} {'PASS':>6s} {'max_diff':>14s} {'t':>4s} {'comp':>16s} {'exp':>16s}")
    print("-" * 90)

    for col in check_cols:
        if col not in d or col not in exp.columns:
            print(f"{col:<30s}  SKIP (미존재)")
            continue
        comp = np.array(d[col][:len(exp)], dtype=np.float64)
        exv = exp[col].values.astype(np.float64)
        diff = np.max(np.abs(comp - exv))
        idx = int(np.argmax(np.abs(comp - exv)))

        if diff < 1e-6:
            pass_cnt += 1
            print(f"{col:<30s}   PASS {diff:>14.6f}")
        else:
            fail_cnt += 1
            fail_cols.append(col)
            print(f"{col:<30s}   FAIL {diff:>14.4f} {idx:>4d} {comp[idx]:>16.4f} {exv[idx]:>16.4f}")

    print(f"\n총 {pass_cnt + fail_cnt}개 컬럼: PASS={pass_cnt} FAIL={fail_cnt}")

    # --- detail 모드 ---
    if args.detail and fail_cols:
        detail_cols = [c.strip() for c in args.cols.split(",")] if args.cols else fail_cols
        print(f"\n{'='*70}")
        print(f"상세 비교 (head={args.head})")
        print(f"{'='*70}")

        for col in detail_cols:
            if col not in d or col not in exp.columns:
                continue
            comp = np.array(d[col][:len(exp)], dtype=np.float64)
            exv = exp[col].values.astype(np.float64)
            diffs = np.abs(comp - exv)
            if np.max(diffs) < 1e-6 and not args.cols:
                continue

            print(f"\n--- {col} ---")
            print(f"{'t':>4s} {'CTR_MM':>7s} {'comp':>16s} {'exp':>16s} {'diff':>14s}")
            ctr_mm = d.get('CTR_AFT_PASS_MMCNT', np.arange(len(exp)))

            if np.max(diffs) > 1e-6:
                first_fail = next((t for t in range(len(exp)) if diffs[t] > 1e-6), 0)
                start = max(0, first_fail - 2)
                for t in range(start, min(start + args.head, len(exp))):
                    cm = int(ctr_mm[t]) if t < len(ctr_mm) else 0
                    flag = " *" if diffs[t] > 1e-6 else ""
                    print(f"{t:>4d} {cm:>7d} {comp[t]:>16.4f} {exv[t]:>16.4f} {diffs[t]:>14.6f}{flag}")
            else:
                for t in range(min(args.head, len(exp))):
                    cm = int(ctr_mm[t]) if t < len(ctr_mm) else 0
                    print(f"{t:>4d} {cm:>7d} {comp[t]:>16.4f} {exv[t]:>16.4f} {diffs[t]:>14.6f}")

    con.close()


if __name__ == "__main__":
    main()

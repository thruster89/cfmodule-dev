"""OD_TRAD_PV 전체 검증 — CTR_TPCD IN ('0','9'), CTR_POLNO netting 적용.

배치 구조: (PROD_CD, CLS_CD) 기준 → 동일 증번(CTR_POLNO) 자동 그룹핑
MN/PV 데이터: 전체 일괄 로드 (SQL 2회) → groupby 인메모리 인덱싱

Usage:
    python test_trad_pv_all.py
    python test_trad_pv_all.py --cov CLA10007       # 특정 COV_CD만
    python test_trad_pv_all.py --save                # 결과 파일 저장
"""
import argparse
import time
import duckdb
import numpy as np
from cf_module.calc.trad_pv import compute_trad_pv, apply_soff_af_netting
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached

DB_PATH = 'duckdb_transform.duckdb'

# 비교 대상 컬럼
CHECK_COLS = [
    'CTR_AFT_PASS_MMCNT', 'PREM_PAY_YN', 'ORIG_PREM', 'DC_PREM',
    'ACUM_NPREM', 'ACUM_NPREM_PRPD', 'PRPD_MMCNT', 'PRPD_PREM',
    'PAD_PREM', 'ADD_ACCMPT_GPREM', 'ADD_ACCMPT_NPREM',
    'ACQSEXP1_BIZEXP', 'ACQSEXP2_BIZEXP',
    'AFPAY_MNTEXP', 'LUMPAY_BIZEXP', 'PAY_GRCPR_ACQSEXP',
    'YSTR_RSVAMT', 'YYEND_RSVAMT', 'YSTR_RSVAMT_TRM', 'YYEND_RSVAMT_TRM',
    'PENS_INRT', 'PENS_DEFRY_RT', 'PENS_ANNUAL_SUM', 'HAFWAY_WDAMT',
    'APLY_PUBANO_INRT',
    'APLY_ADINT_TGT_AMT',
    'APLY_PREM_ACUMAMT_BNFT', 'APLY_PREM_ACUMAMT_EXP',
    'LWST_ADINT_TGT_AMT', 'LWST_PREM_ACUMAMT',
    'SOFF_BF_TMRFND', 'SOFF_AF_TMRFND', 'LTRMNAT_TMRFND',
    'HAFWAY_WDAMT_ADD', 'SOFF_BF_TMRFND_ADD', 'SOFF_AF_TMRFND_ADD',
    'CNCTTP_ACUMAMT_KICS',
    'LOAN_INT', 'LOAN_REMAMT', 'LOAN_RPAY_HAFWAY',
    'LOAN_NEW', 'LOAN_RPAY_MATU',
    'MATU_MAINT_BNS_ACUM_AMT',
]


def verify_results(results, pv_grouped):
    """결과 검증. Returns (col_pass, col_fail, col_max_diff, col_fail_examples, n_ok, n_err)."""
    col_pass = {c: 0 for c in CHECK_COLS}
    col_fail = {c: 0 for c in CHECK_COLS}
    col_max_diff = {c: 0.0 for c in CHECK_COLS}
    col_fail_examples = {c: [] for c in CHECK_COLS}
    n_ok = 0
    n_err = 0

    for idno, result in results.items():
        exp = pv_grouped.get(idno)
        if exp is None or len(exp) == 0:
            n_err += 1
            continue

        d = result.to_dict()
        n_steps = result.n_steps

        all_col_pass = True
        for col in CHECK_COLS:
            if col not in d or col not in exp.columns:
                continue
            comp = np.array(d[col][:n_steps], dtype=np.float64)
            exv = exp[col].values.astype(np.float64)
            diff = np.max(np.abs(comp - exv))
            if diff < 1e-6:
                col_pass[col] += 1
            else:
                col_fail[col] += 1
                all_col_pass = False
                if diff > col_max_diff[col]:
                    col_max_diff[col] = diff
                if len(col_fail_examples[col]) < 5:
                    idx = int(np.argmax(np.abs(comp - exv)))
                    col_fail_examples[col].append(
                        f"IDNO={idno} t={idx} comp={comp[idx]:.4f} exp={exv[idx]:.4f} diff={diff:.4f}"
                    )

        if all_col_pass:
            n_ok += 1

    return col_pass, col_fail, col_max_diff, col_fail_examples, n_ok, n_err


def main():
    parser = argparse.ArgumentParser(description="OD_TRAD_PV 전체 검증 (TPCD 0,9)")
    parser.add_argument("--cov", type=str, default=None, help="특정 COV_CD만 검증")
    parser.add_argument("--save", action="store_true", help="결과 파일 저장")
    args = parser.parse_args()

    t_start = time.time()
    con = duckdb.connect(DB_PATH, read_only=True)
    cache = TradPVDataCache(con)
    print(f"Cache: {time.time() - t_start:.2f}s")

    # TPCD (0,9) 대상 IDNO
    target = {idno: v for idno, v in cache.infrc.items()
              if str(v["ctr_tpcd"]) in ("0", "9")}

    if args.cov:
        target = {idno: v for idno, v in target.items() if v["cov_cd"] == args.cov}

    target_ids = set(target.keys())
    total_ids = len(target_ids)

    # === (PROD_CD, CLS_CD) 기준 그룹핑 ===
    # 동일 CTR_POLNO는 반드시 같은 (PROD, CLS)에 속하므로 netting 인라인 가능
    prod_cls_groups = {}  # (prod, cls) -> [idno, ...]
    for idno, v in target.items():
        key = (v["prod_cd"], v["cls_cd"])
        prod_cls_groups.setdefault(key, []).append(idno)

    print(f"대상: {total_ids:,}건, {len(prod_cls_groups)} (PROD,CLS) 그룹")

    # === MN / PV 전체 일괄 로드 (SQL 2회) ===
    t_load = time.time()
    mn_all = con.execute("""
        SELECT INFRC_IDNO, CTR_TRMO_MTNPSN_CNT, PAY_TRMO_MTNPSN_CNT, CTR_TRME_MTNPSN_CNT
        FROM OD_TBL_MN
        WHERE INFRC_SEQ = 1
        ORDER BY INFRC_IDNO, SETL_AFT_PASS_MMCNT
    """).fetchdf()
    pv_all = con.execute("""
        SELECT *
        FROM OD_TRAD_PV
        WHERE INFRC_SEQ = 1
        ORDER BY INFRC_IDNO, SETL_AFT_PASS_MMCNT
    """).fetchdf()
    mn_grouped = {i: g for i, g in mn_all.groupby('INFRC_IDNO')}
    pv_grouped = {i: g for i, g in pv_all.groupby('INFRC_IDNO')}
    print(f"MN/PV 일괄 로드: {time.time() - t_load:.2f}s "
          f"(MN={len(mn_all):,}, PV={len(pv_all):,})")
    del mn_all, pv_all  # 메모리 해제

    # === 계산 + 인라인 netting ===
    t_calc = time.time()
    all_results = {}
    idno_to_cov = {idno: v["cov_cd"] for idno, v in cache.infrc.items()}
    n_err = 0

    sorted_groups = sorted(prod_cls_groups.items(), key=lambda x: -len(x[1]))
    for gi, ((prod_cd, cls_cd), idnos) in enumerate(sorted_groups):
        t0 = time.time()
        batch_ok = 0
        batch_err = 0
        batch_results = {}    # {idno: TradPVResult}
        ctr_trme_map = {}     # {idno: np.ndarray}

        for idno in idnos:
            info = build_contract_info_cached(cache, idno)
            if not info:
                batch_err += 1
                continue

            exp = pv_grouped.get(idno)
            if exp is None or len(exp) == 0:
                batch_err += 1
                continue

            n_steps = len(exp)
            mn = mn_grouped.get(idno)
            pay_trmo = mn['PAY_TRMO_MTNPSN_CNT'].values if mn is not None else None
            ctr_trmo = mn['CTR_TRMO_MTNPSN_CNT'].values if mn is not None else None
            ctr_trme = mn['CTR_TRME_MTNPSN_CNT'].values if mn is not None else None

            try:
                result = compute_trad_pv(info, n_steps,
                                         pay_trmo=pay_trmo, ctr_trmo=ctr_trmo,
                                         ctr_trme=ctr_trme)
            except Exception as e:
                batch_err += 1
                if batch_err <= 3:
                    print(f"    ERR IDNO={idno}: {e}")
                continue

            batch_results[idno] = result
            if ctr_trme is not None:
                ctr_trme_map[idno] = ctr_trme
            batch_ok += 1

        # 인라인 netting: 이 (PROD,CLS) 그룹 내 CTR_POLNO별 상계
        # (동일 POLNO는 반드시 같은 PROD,CLS에 속하므로 그룹 내에서 완결)
        polno_sub = {}
        for idno in batch_results:
            polno = target[idno].get("ctr_polno", "")
            if polno:
                polno_sub.setdefault(polno, []).append(idno)
        if polno_sub:
            apply_soff_af_netting(batch_results, polno_sub, ctr_trme_map, idno_to_cov)

        all_results.update(batch_results)
        n_err += batch_err
        elapsed = time.time() - t0
        print(f"  [{gi+1:>2d}/{len(sorted_groups)}] {prod_cd}/{cls_cd}: "
              f"{len(idnos):>5d}건 {elapsed:>5.1f}s  계산={batch_ok} ERR={batch_err}")

    print(f"\n계산+netting: {time.time() - t_calc:.1f}s, {len(all_results):,}건 완료, ERR={n_err}")

    # === 검증 ===
    t_verify = time.time()
    tp, tf, tmd, tfe, tok, terr = verify_results(all_results, pv_grouped)
    print(f"검증: {time.time() - t_verify:.1f}s")

    total_time = time.time() - t_start

    lines = []
    def p(s=""):
        print(s)
        lines.append(s)

    p(f"\n{'='*70}")
    p(f"OD_TRAD_PV 전체 검증: {total_ids:,}건 (TPCD 0,9), "
      f"ALL_PASS={tok:,}, ERR={n_err}")
    p(f"총 소요: {total_time:.1f}s")
    p(f"{'='*70}")
    p(f"\n{'컬럼':<30s} {'PASS':>7s} {'FAIL':>7s} {'max_diff':>12s}")
    p("-" * 60)
    for col in CHECK_COLS:
        ps = tp[col]
        fl = tf[col]
        md = tmd[col]
        tag = "PASS" if fl == 0 else "FAIL"
        p(f"{col:<30s} {ps:>7d} {fl:>7d} {md:>12.4f}  {tag}")

    fail_cols = [c for c in CHECK_COLS if tf[c] > 0]
    if fail_cols:
        p(f"\n{'='*70}")
        p("FAIL 상세 (컬럼별 최대 5건)")
        p(f"{'='*70}")
        for col in fail_cols:
            p(f"\n{col} (FAIL={tf[col]}, max_diff={tmd[col]:.4f}):")
            for ex in tfe[col]:
                p(f"  {ex}")

    # (PROD_CD, CLS_CD) 요약
    p(f"\n{'='*70}")
    p("(PROD_CD, CLS_CD)별 요약")
    p(f"{'='*70}")
    grp_stats = {}
    for idno, result in all_results.items():
        v = target[idno]
        key = f"{v['prod_cd']}/{v['cls_cd']}"
        d = result.to_dict()
        exp = pv_grouped.get(idno)
        if exp is None:
            continue
        n = result.n_steps
        has_fail = False
        for col in CHECK_COLS:
            if col not in d or col not in exp.columns:
                continue
            comp = np.array(d[col][:n], dtype=np.float64)
            exv = exp[col].values.astype(np.float64)
            if np.max(np.abs(comp - exv)) >= 1e-6:
                has_fail = True
                break
        s = grp_stats.setdefault(key, {"ok": 0, "fail": 0})
        if has_fail:
            s["fail"] += 1
        else:
            s["ok"] += 1

    p(f"{'PROD/CLS':<18s} {'건수':>6s} {'OK':>6s} {'FAIL':>6s}")
    p("-" * 40)
    for key in sorted(grp_stats, key=lambda c: -(grp_stats[c]["ok"]+grp_stats[c]["fail"])):
        s = grp_stats[key]
        total_c = s["ok"] + s["fail"]
        tag = "OK" if s["fail"] == 0 else f"FAIL({s['fail']})"
        p(f"{key:<18s} {total_c:>6d} {s['ok']:>6d} {s['fail']:>6d}  {tag}")

    if args.save:
        fname = "test_results/trad_pv_all_tpcd09.txt"
        with open(fname, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"\n결과 저장: {fname}")

    con.close()


if __name__ == "__main__":
    main()

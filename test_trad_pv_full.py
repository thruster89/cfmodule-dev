"""OD_TRAD_PV 전체 컬럼 × 전 시점 검증 (CLA00500 한정).

DuckDB 단일 DB (duckdb_transform.duckdb) + 일괄 로드.
"""
import time
import duckdb
import numpy as np
from cf_module.calc.trad_pv import compute_trad_pv
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached

DB_PATH = 'duckdb_transform.duckdb'

t_start = time.time()
con = duckdb.connect(DB_PATH, read_only=True)

# 캐시 초기화
cache = TradPVDataCache(con)
print(f"Cache: {time.time() - t_start:.2f}s")

# CLA00500 일괄 로드
t0 = time.time()
cla500_ids = [idno for idno, v in cache.infrc.items() if v["cov_cd"] == "CLA00500"]
cla500_set = set(cla500_ids)

mn_all = con.execute("""
    SELECT m.INFRC_IDNO, m.CTR_TRMO_MTNPSN_CNT, m.PAY_TRMO_MTNPSN_CNT, m.CTR_TRME_MTNPSN_CNT
    FROM OD_TBL_MN m
    JOIN II_INFRC i ON m.INFRC_IDNO = i.INFRC_IDNO AND i.INFRC_SEQ = 1
    WHERE m.INFRC_SEQ = 1 AND i.COV_CD = 'CLA00500'
    ORDER BY m.INFRC_IDNO, m.SETL_AFT_PASS_MMCNT
""").fetchdf()

pv_all = con.execute("""
    SELECT p.*
    FROM OD_TRAD_PV p
    JOIN II_INFRC i ON p.INFRC_IDNO = i.INFRC_IDNO AND i.INFRC_SEQ = 1
    WHERE p.INFRC_SEQ = 1 AND i.COV_CD = 'CLA00500'
    ORDER BY p.INFRC_IDNO, p.SETL_AFT_PASS_MMCNT
""").fetchdf()

# groupby로 IDNO별 인덱스 사전 구축
mn_grouped = {idno: g for idno, g in mn_all.groupby('INFRC_IDNO')}
pv_grouped = {idno: g for idno, g in pv_all.groupby('INFRC_IDNO')}

print(f"CLA00500: {len(cla500_ids)}건, 일괄 로드: {time.time() - t0:.2f}s "
      f"(MN={len(mn_all):,}, PV={len(pv_all):,})")

# 비교 대상 컬럼
check_cols = [
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

col_pass = {c: 0 for c in check_cols}
col_fail = {c: 0 for c in check_cols}
col_max_diff = {c: 0.0 for c in check_cols}
col_fail_examples = {c: [] for c in check_cols}

n_ok = 0
n_err = 0

t_loop = time.time()
for i, idno in enumerate(cla500_ids):
    if (i + 1) % 1000 == 0:
        elapsed = time.time() - t_loop
        print(f"  progress: {i+1}/{len(cla500_ids)} ({elapsed:.1f}s)")

    info = build_contract_info_cached(cache, idno)
    if not info:
        n_err += 1
        continue

    mn_df = mn_grouped.get(idno)
    exp = pv_grouped.get(idno)
    if exp is None or len(exp) == 0:
        n_err += 1
        continue

    n_steps = len(exp)
    pay_trmo = mn_df['PAY_TRMO_MTNPSN_CNT'].values if mn_df is not None else None
    ctr_trmo = mn_df['CTR_TRMO_MTNPSN_CNT'].values if mn_df is not None else None
    ctr_trme = mn_df['CTR_TRME_MTNPSN_CNT'].values if mn_df is not None else None

    result = compute_trad_pv(info, n_steps,
                             pay_trmo=pay_trmo, ctr_trmo=ctr_trmo,
                             ctr_trme=ctr_trme)
    d = result.to_dict()

    all_col_pass = True
    for col in check_cols:
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
            if len(col_fail_examples[col]) < 3:
                idx = int(np.argmax(np.abs(comp - exv)))
                col_fail_examples[col].append(
                    f"IDNO={idno} t={idx} comp={comp[idx]:.4f} exp={exv[idx]:.4f} diff={diff:.4f}"
                )

    if all_col_pass:
        n_ok += 1

total_time = time.time() - t_start
loop_time = time.time() - t_loop

print(f"\n{'='*70}")
print(f"CLA00500 전체 점검: {len(cla500_ids)}건, ALL_PASS={n_ok}, ERR={n_err}")
print(f"총 소요: {total_time:.1f}s (캐시+로드: {t_loop - t_start:.1f}s, 계산+비교: {loop_time:.1f}s)")
print(f"{'='*70}")
print(f"\n{'컬럼':<30s} {'PASS':>6s} {'FAIL':>6s} {'max_diff':>12s}")
print("-" * 60)
for col in check_cols:
    p = col_pass[col]
    f = col_fail[col]
    md = col_max_diff[col]
    tag = "PASS" if f == 0 else "FAIL"
    print(f"{col:<30s} {p:>6d} {f:>6d} {md:>12.4f}  {tag}")

fail_cols = [c for c in check_cols if col_fail[c] > 0]
if fail_cols:
    print(f"\n{'='*70}")
    print("FAIL 상세 (컬럼별 최대 3건)")
    print(f"{'='*70}")
    for col in fail_cols:
        print(f"\n{col} (FAIL={col_fail[col]}, max_diff={col_max_diff[col]:.4f}):")
        for ex in col_fail_examples[col]:
            print(f"  {ex}")

con.close()

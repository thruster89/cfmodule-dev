"""OD_TRAD_PV 전체 컬럼 × 전 시점 검증 (CLA00500 한정).

DuckDB 단일 DB (duckdb_transform.duckdb) 사용.
"""
import time
import duckdb
import numpy as np
from cf_module.calc.trad_pv import compute_trad_pv
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached

DB_PATH = 'duckdb_transform.duckdb'

t_start = time.time()
con = duckdb.connect(DB_PATH, read_only=True)

# 캐시 초기화 (DuckDB에서 일괄 로드)
cache = TradPVDataCache(con)

# CLA00500 IDNOs (캐시에서 필터링)
all_ids = con.execute(
    'SELECT DISTINCT INFRC_IDNO FROM OD_TRAD_PV'
).fetchdf()['INFRC_IDNO'].tolist()

cla500_ids = [idno for idno in all_ids
              if idno in cache.infrc and cache.infrc[idno]["cov_cd"] == "CLA00500"]

print(f"CLA00500 contracts: {len(cla500_ids)}")
print(f"Cache load + filter: {time.time() - t_start:.2f}s")

# 비교 대상 컬럼 (to_dict 기준)
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

# 컬럼별 집계
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

    n_steps = con.execute(
        f'SELECT COUNT(*) FROM OD_TRAD_PV WHERE INFRC_SEQ = 1 AND INFRC_IDNO={idno}'
    ).fetchone()[0]

    # OD_TBL_MN에서 PAY_TRMO / CTR_TRMO / CTR_TRME 로드
    mn = con.execute(
        f'SELECT CTR_TRMO_MTNPSN_CNT, PAY_TRMO_MTNPSN_CNT, CTR_TRME_MTNPSN_CNT '
        f'FROM OD_TBL_MN WHERE INFRC_SEQ = 1 AND INFRC_IDNO={idno} ORDER BY SETL_AFT_PASS_MMCNT'
    ).fetchdf()
    pay_trmo = mn['PAY_TRMO_MTNPSN_CNT'].values if len(mn) > 0 else None
    ctr_trmo = mn['CTR_TRMO_MTNPSN_CNT'].values if len(mn) > 0 else None
    ctr_trme = mn['CTR_TRME_MTNPSN_CNT'].values if len(mn) > 0 else None

    result = compute_trad_pv(info, n_steps,
                             pay_trmo=pay_trmo, ctr_trmo=ctr_trmo,
                             ctr_trme=ctr_trme)
    d = result.to_dict()
    exp = con.execute(
        f'SELECT * FROM OD_TRAD_PV WHERE INFRC_SEQ = 1 AND INFRC_IDNO={idno} ORDER BY SETL_AFT_PASS_MMCNT'
    ).fetchdf()

    all_col_pass = True
    for col in check_cols:
        if col not in d or col not in exp.columns:
            continue
        comp = np.array(d[col][:len(exp)], dtype=np.float64)
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
print(f"총 소요: {total_time:.1f}s (캐시: {t_loop - t_start:.1f}s, 계산: {loop_time:.1f}s)")
print(f"{'='*70}")
print(f"\n{'컬럼':<30s} {'PASS':>6s} {'FAIL':>6s} {'max_diff':>12s}")
print("-" * 60)
for col in check_cols:
    p = col_pass[col]
    f = col_fail[col]
    md = col_max_diff[col]
    tag = "PASS" if f == 0 else "FAIL"
    print(f"{col:<30s} {p:>6d} {f:>6d} {md:>12.4f}  {tag}")

# FAIL 컬럼 상세
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

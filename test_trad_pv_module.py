"""trad_pv 모듈 검증 스크립트."""
import sqlite3
import duckdb
import numpy as np
from cf_module.calc.trad_pv import compute_trad_pv
from cf_module.data.trad_pv_loader import build_contract_info

legacy = sqlite3.connect('VSOLN.vdb')
proj = duckdb.connect('proj_o.duckdb', read_only=True)

test_ids = [20986, 23402, 21840, 1045116, 567223, 1306790,
            1, 16, 50, 51,        # BAS 추가
            17, 18, 19, 21, 22]   # NoBAS
cols_check = [
    'CTR_AFT_PASS_MMCNT', 'PREM_PAY_YN', 'ACUM_NPREM',
    'YSTR_RSVAMT', 'YYEND_RSVAMT', 'PAD_PREM', 'ACQSEXP1_BIZEXP',
    'APLY_PREM_ACUMAMT_BNFT', 'APLY_PREM_ACUMAMT_EXP',
    'SOFF_BF_TMRFND', 'SOFF_AF_TMRFND', 'LTRMNAT_TMRFND',
]

for idno in test_ids:
    info = build_contract_info(legacy, idno)
    if not info:
        print(f"IDNO={idno}: load failed")
        continue

    n_steps = proj.execute(
        f"SELECT COUNT(*) FROM OD_TRAD_PV WHERE INFRC_SEQ = 1 AND INFRC_IDNO={idno}"
    ).fetchone()[0]

    result = compute_trad_pv(info, n_steps)
    d = result.to_dict()
    exp = proj.execute(
        f"SELECT * FROM OD_TRAD_PV WHERE INFRC_SEQ = 1 AND INFRC_IDNO={idno} ORDER BY SETL_AFT_PASS_MMCNT"
    ).fetchdf()

    has_bas = info.bas is not None
    all_pass = True
    fail_cols = []
    for col in cols_check:
        if col not in d or col not in exp.columns:
            continue
        computed = d[col][:len(exp)]
        expected = exp[col].values
        diff = np.max(np.abs(computed - expected))
        if diff >= 1e-6:
            all_pass = False
            fail_cols.append(f"{col}({diff:.1e})")

    tag = "BAS" if has_bas else "NoBAS"
    status = "PASS" if all_pass else "FAIL"
    extra = "  " + ", ".join(fail_cols) if fail_cols else ""
    print(f"IDNO={idno:>8d} [{tag:>5s}] {info.prod_cd} {status}{extra}")

legacy.close()
proj.close()

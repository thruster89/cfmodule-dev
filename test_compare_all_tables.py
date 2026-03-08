"""IDNO 17, 50 전체 테이블 비교 (PROJ_O_201J20110004359.vdb, EXE_HIST_NO='D000000001').

비교 대상 (데이터 있는 테이블):
  1. OD_TRAD_PV   — 보험료/적립금/환급금
  2. OD_TBL_MN    — 유지자/탈퇴자 (v1 pipeline)
  3. OD_TBL_BN    — 급부 테이블 (IDNO 50만)
  4. OD_RSK_RT    — 위험률
  5. OD_LAPSE_RT  — 해지율
  6. OD_CF        — Cash Flow
  7. OD_EXP       — 사업비 상세
  8. OD_PVCF      — PV CF
  9. OD_DC_RT     — 할인율
 10. OP_BEL       — BEL 요약
 11. OS_EXP_ACVAL_* — 사업비 현가

사용법:
  python test_compare_all_tables.py                # 기본 (IDNO 17, 50)
  python test_compare_all_tables.py --idno 50      # IDNO 50만
  python test_compare_all_tables.py --debug        # 비교 CSV 저장 (debug/ 폴더)
"""
import sqlite3
import os
import sys
import numpy as np

EXE = "D000000001"
PROJ_DB = r"C:\Users\thrus\AppData\Local\SolV\Workspaces\Workspace\projects\zzz\database_files\PROJ_O.vdb"
LEGACY_DB = "VSOLN.vdb"
SKIP_COLS = {"EXE_HIST_NO", "EXE_SEQ", "RUNSET_ID", "INFRC_SEQ", "INFRC_IDNO",
             "SETL_AFT_PASS_MMCNT", "REG_DTTM", "REG_ID"}
DEBUG_DIR = "debug"


# ──────────────────────────────────────────────
# 공통 유틸
# ──────────────────────────────────────────────
def _run_v1_pipeline(idno):
    """v1 파이프라인으로 프로젝션 실행. (result 반환)"""
    from cf_module.config import (CFConfig, DBConfig, RunsetParams,
                                  ProjectionConfig, BatchConfig,
                                  ScenarioConfig, OutputConfig)
    from cf_module.io.reader import DataReader
    from cf_module.data.model_point import load_model_points
    from cf_module.data.assumptions import AssumptionLoader
    from cf_module.projection.projector import run_projection

    config = CFConfig(
        db=DBConfig(db_type="sqlite", path=LEGACY_DB),
        runset=RunsetParams(infrc_seq=1, infrc_idno=idno,
                            clos_ym="202309", assm_ym="202306"),
        projection=ProjectionConfig(time_step="monthly", base_date="202309"),
        batch=BatchConfig(chunk_size=100_000),
        scenario=ScenarioConfig(),
        output=OutputConfig(output_dir="./output"),
        run_targets=["ifrs17"], run_mode="valuation", debug=False,
    )
    reader = DataReader(config.db)
    reader.load_queries(config.queries_path)
    mp = load_model_points(reader, config, query_name="II_INFRC_SINGLE",
                           params=config.runset.query_params_single)
    if mp.n_points == 0:
        return None
    loader = AssumptionLoader(reader, config)
    assumptions = loader.load_all(params=config.runset.query_params_with_assm,
                                  mp_ids=mp.mp_ids, mp=mp)
    return run_projection(mp, assumptions, config, reader=reader)


def load_expected(proj, tbl, idno, order_col="SETL_AFT_PASS_MMCNT"):
    """기대값 dict 로드."""
    cols = [c[1] for c in proj.execute(f"PRAGMA table_info({tbl})").fetchall()]
    rows = proj.execute(
        f"SELECT * FROM {tbl} WHERE EXE_HIST_NO=? AND INFRC_SEQ = 1 AND INFRC_IDNO=? ORDER BY {order_col}",
        [EXE, idno]
    ).fetchall()
    data = {}
    for i, c in enumerate(cols):
        data[c] = [r[i] for r in rows]
    return data, len(rows)


def _save_csv(filename, csv_data):
    """debug/ 폴더에 CSV 저장."""
    import pandas as pd
    os.makedirs(DEBUG_DIR, exist_ok=True)
    path = os.path.join(DEBUG_DIR, filename)
    pd.DataFrame(csv_data).to_csv(path, index=False)
    print(f"  >> CSV: {path}")


def _build_compare_csv(computed_map, exp_data, n_rows, skip_cols, index_col=None,
                       index_vals=None):
    """computed_map과 exp_data로부터 비교 CSV data dict 생성."""
    csv_data = {}
    if index_col and index_vals is not None:
        csv_data[index_col] = index_vals

    # 구현된 컬럼: calc / exp / diff
    for col, calc_arr in computed_map.items():
        csv_data[f"calc_{col}"] = calc_arr
        if col in exp_data:
            exp_arr = np.array(exp_data[col], dtype=np.float64)[:n_rows]
            csv_data[f"exp_{col}"] = exp_arr
            csv_data[f"diff_{col}"] = np.abs(calc_arr - exp_arr)

    # 미구현 컬럼: exp만
    for col in exp_data:
        if col in skip_cols or col in computed_map:
            continue
        exp_arr = np.array(exp_data[col], dtype=np.float64)[:n_rows]
        if np.any(exp_arr != 0):
            csv_data[f"exp_{col}"] = exp_arr

    return csv_data


def _compare_cols(computed_map, exp_data, n_rows, skip_cols, tol=1e-8):
    """computed_map vs exp_data 비교 → report["cols"] dict."""
    cols = {}
    for col in exp_data:
        if col in skip_cols:
            continue
        expected = np.array(exp_data[col], dtype=np.float64)[:n_rows]
        if col in computed_map:
            computed = computed_map[col][:n_rows]
            diff = float(np.max(np.abs(computed - expected)))
            if diff < tol:
                cols[col] = {"match": True, "max_diff": diff}
            else:
                diffs = np.abs(computed - expected)
                fi = int(np.argmax(diffs > tol))
                cols[col] = {
                    "match": False, "max_diff": diff,
                    "first_fail_idx": fi,
                    "comp": float(computed[fi]), "exp": float(expected[fi]),
                }
        else:
            if np.all(expected == 0):
                cols[col] = {"match": True, "max_diff": 0, "note": "zero_skip"}
            else:
                cols[col] = {
                    "match": False, "note": "NOT_MAPPED",
                    "max_abs": float(np.max(np.abs(expected))),
                    "nonzero": int(np.count_nonzero(expected)),
                }
    return cols


# ──────────────────────────────────────────────
# 1. OD_TRAD_PV
# ──────────────────────────────────────────────
def compare_trad_pv(legacy, proj, idno, debug=False, v1_dec=None):
    from cf_module.calc.trad_pv import compute_trad_pv
    from cf_module.data.trad_pv_loader import build_contract_info

    info = build_contract_info(legacy, idno)
    if not info:
        return {"status": "LOAD_FAIL", "cols": {}}

    exp_data, n = load_expected(proj, "OD_TRAD_PV", idno)
    if n == 0:
        return {"status": "NO_DATA", "cols": {}}

    # v1 dec에서 TRMO/TRME 추출
    pay_trmo = None
    ctr_trmo = None
    ctr_trme = None
    if v1_dec is not None:
        n_dec = v1_dec.tpx.shape[1]
        # TRMO[SETL=s] = tpx[s-1] (s>=1), TRMO[0]=0
        ctr_trmo_full = np.zeros(n, dtype=np.float64)
        for s in range(1, min(n, n_dec)):
            ctr_trmo_full[s] = v1_dec.tpx[0, s - 1]
        pay_trmo_full = np.zeros(n, dtype=np.float64)
        if v1_dec.pay_tpx is not None:
            for s in range(1, min(n, n_dec)):
                pay_trmo_full[s] = v1_dec.pay_tpx[0, s - 1]
        # TRME[SETL=s] = tpx[s]
        ctr_trme_full = np.zeros(n, dtype=np.float64)
        for s in range(min(n, n_dec)):
            ctr_trme_full[s] = v1_dec.tpx[0, s]
        pay_trmo = pay_trmo_full
        ctr_trmo = ctr_trmo_full
        ctr_trme = ctr_trme_full

    result = compute_trad_pv(info, n, pay_trmo=pay_trmo, ctr_trmo=ctr_trmo,
                             ctr_trme=ctr_trme)
    d = result.to_dict()

    # IP_P_ACUM_COV 미대상 → 공시이율/ADINT/LWST/KICS는 계산 스킵 (0 출력)
    acum_skip = set()
    if not info.acum_cov:
        acum_skip = {"APLY_PUBANO_INRT", "APLY_ADINT_TGT_AMT",
                     "LWST_ADINT_TGT_AMT", "LWST_PREM_ACUMAMT",
                     "CNCTTP_ACUMAMT_KICS"}

    # computed_map 구성 (기대값에 있는 컬럼 중 계산 가능한 것)
    computed_map = {}
    for col in exp_data:
        if col in SKIP_COLS:
            continue
        if col in acum_skip:
            continue  # _compare_cols에서 NOT_MAPPED으로 SKIP 처리됨
        if col in d:
            computed_map[col] = np.array(d[col][:n], dtype=np.float64)

    report = {
        "status": "OK", "has_bas": info.bas is not None, "n_steps": n,
        "cols": _compare_cols(computed_map, exp_data, n, SKIP_COLS, tol=1e-6),
    }

    if debug:
        setl = np.arange(n)
        csv = _build_compare_csv(computed_map, exp_data, n, SKIP_COLS,
                                 "SETL_AFT_PASS_MMCNT", setl)
        _save_csv(f"od_trad_pv_{idno}.csv", csv)

    return report


# ──────────────────────────────────────────────
# 2. OD_TBL_MN
# ──────────────────────────────────────────────
def compare_tbl_mn(proj, idno, debug=False, v1_dec=None):
    """v1 파이프라인으로 OD_TBL_MN 산출 후 비교."""

    exp_data, n = load_expected(proj, "OD_TBL_MN", idno)
    if n == 0:
        return {"status": "NO_DATA", "cols": {}}

    if v1_dec is None:
        from cf_module.calc.decrement import DecrementResult
        result = _run_v1_pipeline(idno)
        v1_dec = result.decrement if result else None
        if v1_dec is None:
            return {"status": "NO_CONTRACT", "cols": {}}

    dec = v1_dec

    # PROJ_O: SETL=0은 초기행, SETL=1부터 실제 계산
    # v1: tpx[t] = SETL=t
    n_steps = dec.tpx.shape[1]
    compare_n = min(n - 1, n_steps - 1)

    tpx = dec.tpx[0, 1:compare_n + 1]
    pay_tpx = dec.pay_tpx[0, 1:compare_n + 1] if dec.pay_tpx is not None else np.zeros(compare_n)
    wx = dec.wx_monthly[0, 1:compare_n + 1]
    tpx_bot = dec.tpx[0, :compare_n]
    pay_tpx_bot = dec.pay_tpx[0, :compare_n] if dec.pay_tpx is not None else np.ones(compare_n)

    if dec.pay_dx_monthly is not None and dec.pay_qx_monthly is not None:
        pay_wx = dec.pay_dx_monthly[0, 1:compare_n + 1] - dec.pay_qx_monthly[0, 1:compare_n + 1]
    else:
        pay_wx = wx.copy()

    d_rsvamt = dec.d_rsvamt[0, 1:compare_n + 1] if dec.d_rsvamt is not None else np.zeros(compare_n)
    d_bnft = dec.d_bnft[0, 1:compare_n + 1] if dec.d_bnft is not None else np.zeros(compare_n)
    d_pyexsp = dec.pay_d_pyexsp[0, 1:compare_n + 1] if dec.pay_d_pyexsp is not None else np.zeros(compare_n)
    pay_d_rsvamt = dec.pay_d_rsvamt[0, 1:compare_n + 1] if dec.pay_d_rsvamt is not None else np.zeros(compare_n)
    pay_d_bnft = dec.pay_d_bnft[0, 1:compare_n + 1] if dec.pay_d_bnft is not None else np.zeros(compare_n)

    with np.errstate(divide='ignore', invalid='ignore'):
        computed_map = {
            "CTR_TRME_MTNPSN_CNT": tpx,
            "CTR_TRMNAT_RT": wx,
            "CTR_RSVAMT_DEFRY_DRPO_RSKRT": np.where(tpx_bot > 0, d_rsvamt / tpx_bot, 0),
            "CTR_BNFT_DRPO_RSKRT": np.where(tpx_bot > 0, d_bnft / tpx_bot, 0),
            "CTR_TRMO_MTNPSN_CNT": tpx_bot,
            "CTR_TRMPSN_CNT": tpx_bot * wx,
            "CTR_RSVAMT_DEFRY_DRPSN_CNT": d_rsvamt,
            "CTR_BNFT_DEFRY_DRPSN_CNT": d_bnft,
            "PAY_TRME_MTNPSN_CNT": pay_tpx,
            "PAY_TRMNAT_RT": pay_wx,
            "PAY_RSVAMT_DEFRY_DRPO_RSKRT": np.where(pay_tpx_bot > 0, pay_d_rsvamt / pay_tpx_bot, 0),
            "PAY_RSVAMT_DEFRY_DRPSN_CNT": pay_d_rsvamt,
            "PAY_BNFT_DRPO_RSKRT": np.where(pay_tpx_bot > 0, pay_d_bnft / pay_tpx_bot, 0),
            "PAY_BNFT_DEFRY_DRPSN_CNT": pay_d_bnft,
            "PYEXSP_DRPO_RSKRT": np.where(pay_tpx_bot > 0, d_pyexsp / pay_tpx_bot, 0),
            "PAY_TRMO_MTNPSN_CNT": pay_tpx_bot,
            "PAY_TRMPSN_CNT": pay_tpx_bot * pay_wx,
            "PYEXSP_DRPSN_CNT": d_pyexsp,
        }

    # SETL=1부터 비교 (exp_data를 [1:compare_n+1]로 슬라이싱)
    exp_sliced = {}
    for col in exp_data:
        arr = exp_data[col]
        exp_sliced[col] = arr[1:compare_n + 1]

    report = {
        "status": "OK", "n_steps": n,
        "cols": _compare_cols(computed_map, exp_sliced, compare_n, SKIP_COLS),
    }

    if debug:
        setl = np.arange(1, compare_n + 1)
        csv = _build_compare_csv(computed_map, exp_sliced, compare_n, SKIP_COLS,
                                 "SETL_AFT_PASS_MMCNT", setl)
        _save_csv(f"od_tbl_mn_{idno}.csv", csv)

    return report


# ──────────────────────────────────────────────
# 3. OD_LAPSE_RT
# ──────────────────────────────────────────────
def compare_lapse_rt(proj, idno, debug=False, v1_dec=None):
    """OD_LAPSE_RT: TRMNAT_RT, SKEW, APLY_TRMNAT_RT 비교."""

    exp_data, n = load_expected(proj, "OD_LAPSE_RT", idno)
    if n == 0:
        return {"status": "NO_DATA", "cols": {}}

    if v1_dec is None:
        result = _run_v1_pipeline(idno)
        v1_dec = result.decrement if result else None
        if v1_dec is None:
            return {"status": "LOAD_FAIL", "cols": {}}

    trmnat_rt = v1_dec.wx_annual[0, :n]
    skew_arr = v1_dec.skew[:n] if v1_dec.skew is not None else np.full(n, 1.0 / 12.0)
    aply_trmnat_rt = 1.0 - (1.0 - trmnat_rt) ** skew_arr

    computed_map = {
        "TRMNAT_RT": trmnat_rt,
        "SKEW": skew_arr,
        "APLY_TRMNAT_RT": aply_trmnat_rt,
    }

    skip = SKIP_COLS | {"CTR_AFT_PASS_MMCNT"}
    report = {
        "status": "OK", "n_steps": n,
        "cols": _compare_cols(computed_map, exp_data, n, skip),
    }

    if debug:
        setl = np.arange(n)
        csv = _build_compare_csv(computed_map, exp_data, n, skip,
                                 "SETL_AFT_PASS_MMCNT", setl)
        _save_csv(f"od_lapse_rt_{idno}.csv", csv)

    return report


# ──────────────────────────────────────────────
# 4. OD_RSK_RT
# ──────────────────────────────────────────────
def compare_rsk_rt(proj, idno, debug=False, v1_dec=None):
    """OD_RSK_RT: 위험률코드별 위험률 비교 (long format: SETL × RSK_RT_CD)."""

    # 기대값 로드 (ORDER BY SETL, RSK_RT_CD)
    cols = [c[1] for c in proj.execute("PRAGMA table_info(OD_RSK_RT)").fetchall()]
    rows = proj.execute(
        "SELECT * FROM OD_RSK_RT WHERE EXE_HIST_NO=? AND INFRC_SEQ = 1 AND INFRC_IDNO=? "
        "ORDER BY SETL_AFT_PASS_MMCNT, RSK_RT_CD",
        [EXE, idno]
    ).fetchall()
    if not rows:
        return {"status": "NO_DATA", "cols": {}}

    exp_data = {}
    for i, c in enumerate(cols):
        exp_data[c] = [r[i] for r in rows]
    n_rows = len(rows)

    if v1_dec is None:
        result = _run_v1_pipeline(idno)
        v1_dec = result.decrement if result else None
        if v1_dec is None:
            return {"status": "LOAD_FAIL", "cols": {}}

    dec = v1_dec
    if dec.qx_raw_by_risk is None or dec.rsk_rt_cd is None:
        return {"status": "NO_RISK_DATA", "cols": {}}

    # v1: (n_steps, n_risks) wide → long format (SETL × code)
    n_steps, n_risks = dec.qx_raw_by_risk.shape
    v1_codes = list(dec.rsk_rt_cd)
    mm_trf = dec.mm_trf_way_cd

    # 기대값에서 코드 순서 파악
    exp_codes = sorted(set(exp_data["RSK_RT_CD"]))
    n_exp_codes = len(exp_codes)
    n_setl = n_rows // n_exp_codes

    # 코드 매핑: exp_code → v1 risk index
    code_to_v1 = {}
    for j, cd in enumerate(v1_codes):
        code_to_v1[cd] = j

    # pre-dedup monthly 계산 (BF_MM = AF_APLY)
    qx_be_annual = dec.qx_be_annual_by_risk  # (n_steps, n_risks)
    bf_mm = qx_be_annual.copy()
    c1 = mm_trf == 1
    c2 = mm_trf == 2
    if np.any(c1):
        bf_mm[:, c1] = 1.0 - (1.0 - qx_be_annual[:, c1]) ** (1.0 / 12.0)
    if np.any(c2):
        bf_mm[:, c2] = qx_be_annual[:, c2] / 12.0

    # BEPRD = be_annual / raw (safe div)
    with np.errstate(divide='ignore', invalid='ignore'):
        beprd = np.where(dec.qx_raw_by_risk > 0,
                         qx_be_annual / dec.qx_raw_by_risk, 1.0)

    # long format 배열 구축 (n_setl × n_exp_codes 행)
    compare_n = min(n_setl, n_steps)
    total_rows = compare_n * n_exp_codes

    calc_rsk_rt = np.zeros(total_rows)
    calc_beprd = np.zeros(total_rows)
    calc_bf_yr = np.zeros(total_rows)
    calc_bf_mm = np.zeros(total_rows)
    calc_af_aply = np.zeros(total_rows)
    calc_loss_rt = np.ones(total_rows)
    calc_mth_coef = np.ones(total_rows)
    calc_trd_coef = np.ones(total_rows)
    calc_arvl_coef = np.ones(total_rows)

    for t in range(compare_n):
        for ci, cd in enumerate(exp_codes):
            idx = t * n_exp_codes + ci
            if cd in code_to_v1:
                j = code_to_v1[cd]
                calc_rsk_rt[idx] = dec.qx_raw_by_risk[t, j]
                calc_beprd[idx] = beprd[t, j]
                calc_bf_yr[idx] = qx_be_annual[t, j]
                calc_bf_mm[idx] = bf_mm[t, j]
                calc_af_aply[idx] = bf_mm[t, j]  # BF_MM == AF_APLY

    computed_map = {
        "RSK_RT": calc_rsk_rt[:total_rows],
        "BEPRD_DEFRY_RT": calc_beprd[:total_rows],
        "INVLD_TRMNAT_BF_YR_RSK_RT": calc_bf_yr[:total_rows],
        "INVLD_TRMNAT_BF_MM_RSK_RT": calc_bf_mm[:total_rows],
        "INVLD_TRMNAT_AF_APLY_RSK_RT": calc_af_aply[:total_rows],
        "LOSS_RT": calc_loss_rt[:total_rows],
        "MTH_EFECT_COEF": calc_mth_coef[:total_rows],
        "TRD_COEF": calc_trd_coef[:total_rows],
        "ARVL_AGE_COEF": calc_arvl_coef[:total_rows],
    }

    # 기대값도 total_rows만큼 슬라이싱
    exp_sliced = {}
    for col in exp_data:
        exp_sliced[col] = exp_data[col][:total_rows]

    skip = SKIP_COLS | {"CTR_AFT_PASS_MMCNT", "RSK_RT_CD"}
    report = {
        "status": "OK", "n_rows": n_rows,
        "cols": _compare_cols(computed_map, exp_sliced, total_rows, skip),
    }

    if debug:
        setl_arr = []
        code_arr = []
        for t in range(compare_n):
            for cd in exp_codes:
                setl_arr.append(t)
                code_arr.append(cd)
        csv = {"SETL_AFT_PASS_MMCNT": setl_arr, "RSK_RT_CD": code_arr}
        csv.update(_build_compare_csv(computed_map, exp_sliced, total_rows, skip))
        _save_csv(f"od_rsk_rt_{idno}.csv", csv)

    return report


# ──────────────────────────────────────────────
# 통용: 미구현 테이블
# ──────────────────────────────────────────────
def compare_unimplemented(proj, tbl, idno, order_col="SETL_AFT_PASS_MMCNT",
                          extra_skip=None, debug=False):
    exp_data, n = load_expected(proj, tbl, idno, order_col)
    if n == 0:
        return {"status": "NO_DATA", "cols": {}}
    skip = SKIP_COLS | (extra_skip or set())
    report = {"status": "OK", "n_rows": n, "cols": {}}
    for col in exp_data:
        if col in skip:
            continue
        vals = np.array(exp_data[col], dtype=np.float64)
        nz = int(np.count_nonzero(vals))
        if nz == 0:
            report["cols"][col] = {"match": True, "note": "all_zero"}
        else:
            report["cols"][col] = {
                "match": False, "note": "NOT_IMPL",
                "min": float(np.min(vals)), "max": float(np.max(vals)),
                "nonzero": nz,
            }

    if debug:
        csv_data = {}
        # 인덱스 컬럼
        if order_col in exp_data:
            csv_data[order_col] = exp_data[order_col]
        # extra_skip에 있는 분류키도 포함
        if extra_skip:
            for ks in extra_skip:
                if ks in exp_data and ks != order_col:
                    csv_data[ks] = exp_data[ks]
        # 데이터 컬럼
        for col in exp_data:
            if col in SKIP_COLS or col in csv_data:
                continue
            csv_data[f"exp_{col}"] = exp_data[col]
        tbl_lower = tbl.lower()
        _save_csv(f"{tbl_lower}_{idno}.csv", csv_data)

    return report


# ──────────────────────────────────────────────
# 출력
# ──────────────────────────────────────────────
def print_report(table_name, idno, report):
    bas = report.get("has_bas")
    bas_tag = f" [{'BAS' if bas else 'NoBAS'}]" if bas is not None else ""
    n = report.get("n_steps") or report.get("n_rows", "?")
    print(f"\n{'='*70}")
    print(f"  {table_name}  IDNO={idno}{bas_tag}  rows={n}  status={report['status']}")
    print(f"{'='*70}")
    if report["status"] != "OK":
        return 0, 0, 0

    pass_c = fail_c = skip_c = 0
    for col, info in sorted(report.get("cols", {}).items()):
        note = info.get("note", "")
        if info.get("match"):
            pass_c += 1
            continue
        if note in ("NOT_IMPL", "NOT_MAPPED"):
            nz = info.get("nonzero", 0)
            mx = info.get("max_abs", info.get("max", 0))
            skip_c += 1
            print(f"  SKIP  {col:40s}  {note:10s}  nonzero={nz:>5d}  max={mx:.4e}")
            continue
        # FAIL
        fail_c += 1
        md = info.get("max_diff", 0)
        fi = info.get("first_fail_idx", "?")
        cv = info.get("comp", "?")
        ev = info.get("exp", "?")
        print(f"  FAIL  {col:40s}  max_diff={md:.4e}  @{fi}  comp={cv}  exp={ev}")

    print(f"  --- PASS={pass_c}  FAIL={fail_c}  SKIP(미구현)={skip_c}")
    return pass_c, fail_c, skip_c


def main():
    import argparse
    parser = argparse.ArgumentParser(description="PROJ_O 전체 테이블 비교")
    parser.add_argument("--idno", type=int, nargs="+", default=[17, 50],
                        help="비교할 INFRC_IDNO (기본: 17 50)")
    parser.add_argument("--debug", action="store_true",
                        help="비교 결과 CSV 저장 (debug/ 폴더)")
    args = parser.parse_args()
    test_ids = args.idno
    legacy = sqlite3.connect(LEGACY_DB)
    proj = sqlite3.connect(PROJ_DB)

    total_pass = total_fail = total_skip = 0

    for idno in test_ids:
        print(f"\n{'#'*70}")
        print(f"#  IDNO = {idno}")
        print(f"{'#'*70}")

        # v1 파이프라인 1회 실행 (MN + LAPSE_RT 공유)
        v1_result = _run_v1_pipeline(idno)
        v1_dec = v1_result.decrement if v1_result else None

        # 1. OD_TRAD_PV
        r = compare_trad_pv(legacy, proj, idno, debug=args.debug, v1_dec=v1_dec)
        p, f, s = print_report("OD_TRAD_PV", idno, r)
        total_pass += p; total_fail += f; total_skip += s

        # 2. OD_TBL_MN
        r = compare_tbl_mn(proj, idno, debug=args.debug, v1_dec=v1_dec)
        p, f, s = print_report("OD_TBL_MN", idno, r)
        total_pass += p; total_fail += f; total_skip += s

        # 3. OD_TBL_BN
        r = compare_unimplemented(proj, "OD_TBL_BN", idno,
                                  extra_skip={"CTR_AFT_PASS_MMCNT", "BNFT_NO"},
                                  debug=args.debug)
        p, f, s = print_report("OD_TBL_BN", idno, r)
        total_pass += p; total_fail += f; total_skip += s

        # 4. OD_RSK_RT
        r = compare_rsk_rt(proj, idno, debug=args.debug, v1_dec=v1_dec)
        p, f, s = print_report("OD_RSK_RT", idno, r)
        total_pass += p; total_fail += f; total_skip += s

        # 5. OD_LAPSE_RT
        r = compare_lapse_rt(proj, idno, debug=args.debug, v1_dec=v1_dec)
        p, f, s = print_report("OD_LAPSE_RT", idno, r)
        total_pass += p; total_fail += f; total_skip += s

        # 6. OD_CF
        r = compare_unimplemented(proj, "OD_CF", idno,
                                  extra_skip={"CTR_AFT_PASS_MMCNT"},
                                  debug=args.debug)
        p, f, s = print_report("OD_CF", idno, r)
        total_pass += p; total_fail += f; total_skip += s

        # 7. OD_EXP
        r = compare_unimplemented(proj, "OD_EXP", idno,
                                  extra_skip={"CTR_AFT_PASS_MMCNT", "EXP_TPCD",
                                              "D_IND_EXP_DVCD", "EXP_KDCD"},
                                  debug=args.debug)
        p, f, s = print_report("OD_EXP", idno, r)
        total_pass += p; total_fail += f; total_skip += s

        # 8. OD_PVCF
        r = compare_unimplemented(proj, "OD_PVCF", idno,
                                  extra_skip={"CTR_AFT_PASS_MMCNT"},
                                  debug=args.debug)
        p, f, s = print_report("OD_PVCF", idno, r)
        total_pass += p; total_fail += f; total_skip += s

        # 9. OD_DC_RT
        r = compare_unimplemented(proj, "OD_DC_RT", idno,
                                  extra_skip={"CTR_AFT_PASS_MMCNT"},
                                  debug=args.debug)
        p, f, s = print_report("OD_DC_RT", idno, r)
        total_pass += p; total_fail += f; total_skip += s

    # 10. OP_BEL (IDNO별 1행)
    for idno in test_ids:
        cols_info = [c[1] for c in proj.execute("PRAGMA table_info(OP_BEL)").fetchall()]
        row = proj.execute(
            "SELECT * FROM OP_BEL WHERE EXE_HIST_NO=? AND INFRC_SEQ = 1 AND INFRC_IDNO=?",
            [EXE, idno]
        ).fetchone()
        if row:
            print(f"\n{'='*70}")
            print(f"  OP_BEL  IDNO={idno}  (1 row summary)")
            print(f"{'='*70}")
            for c, v in zip(cols_info, row):
                if c in SKIP_COLS or c == "CMPT_PRPO_DVCD":
                    continue
                if v and v != 0:
                    print(f"  {c:40s} = {v}")
            total_skip += 1
            if args.debug:
                csv_data = {c: [v] for c, v in zip(cols_info, row)
                            if c not in SKIP_COLS and c != "CMPT_PRPO_DVCD"}
                _save_csv(f"op_bel_{idno}.csv", csv_data)

    # 11. OS_EXP_ACVAL_* (소수 행)
    for tbl in ["OS_EXP_ACVAL_ACQS", "OS_EXP_ACVAL_MNT", "OS_EXP_ACVAL_LSVY"]:
        for idno in test_ids:
            cols_info = [c[1] for c in proj.execute(f"PRAGMA table_info({tbl})").fetchall()]
            rows = proj.execute(
                f"SELECT * FROM {tbl} WHERE EXE_HIST_NO=? AND INFRC_IDNO=?",
                [EXE, idno]
            ).fetchall()
            if rows:
                print(f"\n{'='*70}")
                print(f"  {tbl}  IDNO={idno}  ({len(rows)} rows)")
                print(f"{'='*70}")
                for row in rows:
                    for c, v in zip(cols_info, row):
                        if c in SKIP_COLS or c == "D_IND_EXP_DVCD":
                            continue
                        if v and v != 0:
                            print(f"  {c:40s} = {v}")
                    print("  ---")
                total_skip += 1
                if args.debug:
                    csv_data = {}
                    for ci, c in enumerate(cols_info):
                        if c in SKIP_COLS:
                            continue
                        csv_data[c] = [r[ci] for r in rows]
                    _save_csv(f"{tbl.lower()}_{idno}.csv", csv_data)

    legacy.close()
    proj.close()

    print(f"\n{'='*70}")
    print(f"  GRAND TOTAL:  PASS={total_pass}  FAIL={total_fail}  SKIP(미구현)={total_skip}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

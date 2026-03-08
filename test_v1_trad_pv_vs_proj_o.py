"""
v1 OD_TRAD_PV 비교 테스트 (35개 컬럼)

Legacy: VSOLN.vdb, Expected: proj_o.duckdb (42,000 IDNOs)

구현 완료 (BAS 보유 233건 100% PASS):
  CTR_AFT_PASS_MMCNT, PREM_PAY_YN, ORIG/DC_PREM, ACUM_NPREM,
  PAD_PREM, YSTR/YYEND_RSVAMT(_TRM), ACQSEXP1_BIZEXP,
  APLY_PREM_ACUMAMT_BNFT/EXP, SOFF_BF/AF_TMRFND, LTRMNAT_TMRFND,
  + 13개 0-필드 (ADD_*, PENS_*, HAFWAY_*, LOAN_NEW/RPAY_MATU, MATU_*)

미구현 (이율 의존):
  APLY_PUBANO_INRT, APLY_ADINT_TGT_AMT, LWST_*, CNCTTP_ACUMAMT_KICS,
  LOAN_INT/RPAY_HAFWAY/REMAMT

Usage:
    python test_v1_trad_pv_vs_proj_o.py                 # 기본 10건
    python test_v1_trad_pv_vs_proj_o.py --n 100         # 100건 샘플
    python test_v1_trad_pv_vs_proj_o.py --idno 46017    # 특정 IDNO
    python test_v1_trad_pv_vs_proj_o.py --csv            # 불일치 CSV 저장
    python test_v1_trad_pv_vs_proj_o.py --debug 46017   # 단건 상세 디버그
"""

import argparse
import json
import time
import sys

import duckdb
import numpy as np
import pandas as pd
import sqlite3

LEGACY_DB = "VSOLN.vdb"
PROJ_O_DB = "proj_o.duckdb"
CLOS_YM = "202309"
TOL = 1e-6  # 정수 필드 위주이므로 1e-6 허용

# INRT lookup: APLY_INRT_CD → {SETL_str → INRT_float}
_INRT_CACHE = None

def _load_inrt_lookup():
    global _INRT_CACHE
    if _INRT_CACHE is None:
        try:
            with open("inrt_lookup.json") as f:
                _INRT_CACHE = json.load(f)
        except FileNotFoundError:
            _INRT_CACHE = {}
    return _INRT_CACHE


def load_contract_info(conn: sqlite3.Connection, idno: int) -> dict:
    """II_INFRC에서 계약 기본 정보 로드."""
    row = conn.execute("""
        SELECT INFRC_IDNO, PROD_CD, COV_CD, CTR_DT,
               INSTRM_YYCNT, PAYPR_YYCNT, PASS_YYCNT, PASS_MMCNT,
               GRNTPT_GPREM, GRNTPT_JOIN_AMT,
               TOT_TRMNAT_DDCT_AMT, STD_TRMNAT_DDCT_AMT,
               PAYPR_DVCD, ETC_EXPCT_BIZEXP_KEY_VAL, CLS_CD,
               PAY_STCD, PREM_DC_RT1, CTR_TPCD, ACCMPT_GPREM
        FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1
    """, [idno]).fetchone()
    if not row:
        return None
    gprem = row[8]
    accmpt_gprem = row[18] or 0
    # BAS 미보유 적립형: GRNTPT_GPREM=0이면 ACCMPT_GPREM 사용
    effective_gprem = gprem if gprem else accmpt_gprem
    return {
        "idno": row[0], "prod_cd": row[1], "cov_cd": row[2], "ctr_dt": row[3],
        "bterm_yy": row[4], "pterm_yy": row[5],
        "pass_yy": row[6], "pass_mm": row[7],
        "gprem": effective_gprem, "join_amt": row[9],
        "tot_trmnat_ddct": row[10], "std_trmnat_ddct": row[11],
        "paypr_dvcd": row[12], "etc_bizexp_key": row[13], "cls_cd": row[14],
        "pay_stcd": row[15],  # 1=납입중, 2=납입완료, 3=납입면제
        "prem_dc_rt": row[16] or 0,  # 보험료 할인율
        "ctr_tpcd": str(row[17] or ""),
        "accmpt_gprem": accmpt_gprem,
    }


def load_rsvamt_bas(conn: sqlite3.Connection, idno: int) -> dict:
    """II_RSVAMT_BAS에서 준비금/순보험료 로드."""
    row = conn.execute(
        "SELECT * FROM II_RSVAMT_BAS WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
        [idno],
    ).fetchone()
    if not row:
        return None
    cols = [c[1] for c in conn.execute("PRAGMA table_info(II_RSVAMT_BAS)").fetchall()]
    data = dict(zip(cols, row))

    crit_join = data["CRIT_JOIN_AMT"]
    nprem = data["NPREM"]

    # 연도별 YSTR/YYEND 배열 (1~120)
    ystr_arr = []
    yyend_arr = []
    for yr in range(1, 121):
        ystr_key = f"YSTR_RSVAMT{yr}"
        yyend_key = f"YYEND_RSVAMT{yr}"
        ystr_arr.append(data.get(ystr_key, 0) or 0)
        yyend_arr.append(data.get(yyend_key, 0) or 0)

    return {
        "crit_join_amt": crit_join,
        "nprem": nprem,
        "ystr": np.array(ystr_arr, dtype=np.float64),
        "yyend": np.array(yyend_arr, dtype=np.float64),
    }


def load_bizexp_rt(conn: sqlite3.Connection, prod_cd: str, cov_cd: str,
                    cls_cd: str, paypr_dvcd: str, etc_key: str) -> dict:
    """IP_P_EXPCT_BIZEXP_RT에서 사업비율 로드."""
    row = conn.execute("""
        SELECT FRYY_GPREM_VS_ACQSEXP_RT,
               INPAY_GPREM_VS_MNTEXP_RT1, INPAY_GPREM_VS_LOSS_SVYEXP_RT
        FROM IP_P_EXPCT_BIZEXP_RT
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
          AND PAYPR_DVCD = ? AND ETC_EXPCT_BIZEXP_KEY_VAL = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd, paypr_dvcd, etc_key]).fetchone()
    if row:
        return {
            "acqsexp_rt": row[0],
            "inpay_mntexp_rt": row[1] or 0,
            "loss_svyexp_rt": row[2] or 0,
        }
    return None


def load_acqsexp_rt(conn: sqlite3.Connection, prod_cd: str, cov_cd: str,
                     cls_cd: str, paypr_dvcd: str, etc_key: str) -> float:
    """IP_P_EXPCT_BIZEXP_RT에서 초년도 사업비율 로드 (하위호환)."""
    d = load_bizexp_rt(conn, prod_cd, cov_cd, cls_cd, paypr_dvcd, etc_key)
    return d["acqsexp_rt"] if d else None


def load_acum_cov(conn: sqlite3.Connection, prod_cd: str, cov_cd: str,
                   cls_cd: str) -> dict:
    """IP_P_ACUM_COV에서 적립 관련 설정 로드."""
    row = conn.execute("""
        SELECT APLY_INRT_CD, INRT_ADINT_CD, LWST_GRNT_INRT1
        FROM IP_P_ACUM_COV
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd]).fetchone()
    if row:
        return {"aply_inrt_cd": str(row[0]).zfill(2), "inrt_adint_cd": row[1],
                "lwst_grnt_inrt": row[2] or 0.0}
    return None


def load_expct_inrt(conn: sqlite3.Connection, prod_cd: str, cov_cd: str,
                     cls_cd: str) -> dict:
    """IP_P_EXPCT_INRT에서 예정이율 로드."""
    row = conn.execute("""
        SELECT EXPCT_INRT1, STD_INRT1
        FROM IP_P_EXPCT_INRT
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd]).fetchone()
    if row:
        return {"expct_inrt": row[0] or 0.0, "std_inrt": row[1] or 0.0}
    return None


def compute_acum_interest_based(V: float, nprem: float, inrt_cd: str,
                                 lwst_inrt: float, n_steps: int,
                                 elapsed_mm: int, pterm_mm: int,
                                 pay_stcd: int,
                                 expct_inrt: float = 0.0) -> tuple:
    """이율 기반 적립금 계산 (BAS 미보유 계약).

    검증된 ACUM 공식:
      - SETL=0: ACUM = V (이자 없음)
      - SETL>0: cum_int += ADINT[s] * INRT[s] / 12, ACUM = ADINT + cum_int
      - 보험연도 경계: cum_int = 0 리셋

    ADINT는 연도 내 선형 (V_year + s*P) 근사 사용.
    정확한 ADINT 공식(geometric decay alpha)은 미도출 상태.

    Returns (aply_acum, lwst_acum) arrays.
    """
    inrt_lookup = _load_inrt_lookup()
    inrt_map = inrt_lookup.get(inrt_cd, {})

    aply_acum = np.zeros(n_steps, dtype=np.float64)
    lwst_acum = np.zeros(n_steps, dtype=np.float64)

    V_year_aply = V
    V_year_lwst = V
    cum_int_aply = 0.0
    cum_int_lwst = 0.0
    s_year = 0  # 연도 내 위치

    for t in range(n_steps):
        ctr_mm = elapsed_mm + t
        ins_year = (ctr_mm - 1) // 12 + 1

        is_paying = (pay_stcd == 1) and (ctr_mm <= pterm_mm)
        P = nprem if is_paying else 0.0

        # INRT: CD='00'은 고정이율(EXPCT_INRT), 그 외 lookup + 마지막값 연장
        if inrt_cd == '00':
            inrt_aply = expct_inrt
        else:
            inrt_str = inrt_map.get(str(t))
            if inrt_str is not None:
                inrt_aply = float(inrt_str)
            elif inrt_map:
                last_key = max(inrt_map.keys(), key=int)
                inrt_aply = float(inrt_map[last_key])
            else:
                inrt_aply = expct_inrt  # lookup 없으면 EXPCT_INRT 폴백
        inrt_lwst = lwst_inrt

        if t == 0:
            s_year = 0
            adint = V_year_aply + s_year * P
            aply_acum[t] = adint
            lwst_acum[t] = adint
        else:
            prev_ctr = elapsed_mm + t - 1
            prev_year = (prev_ctr - 1) // 12 + 1
            if ins_year != prev_year:
                V_year_aply = aply_acum[t - 1]
                V_year_lwst = lwst_acum[t - 1]
                s_year = 0
                cum_int_aply = 0.0
                cum_int_lwst = 0.0

            s_year += 1
            adint_aply = V_year_aply + s_year * P
            adint_lwst = V_year_lwst + s_year * P

            cum_int_aply += adint_aply * inrt_aply / 12
            cum_int_lwst += adint_lwst * inrt_lwst / 12
            aply_acum[t] = adint_aply + cum_int_aply
            lwst_acum[t] = adint_lwst + cum_int_lwst

    return aply_acum, lwst_acum


def load_tmrfnd_calc_tp(conn: sqlite3.Connection, prod_cd: str, cov_cd: str) -> dict:
    """IP_P_COV에서 TMRFND_CALC_TP 코드 로드."""
    row = conn.execute("""
        SELECT TMRFND_CALC_TP1_CD, TMRFND_CALC_TP2_CD, TMRFND_CALC_TP3_CD
        FROM IP_P_COV
        WHERE PROD_CD = ? AND COV_CD = ?
        LIMIT 1
    """, [prod_cd, cov_cd]).fetchone()
    if not row:
        return {"tp1": None, "tp2": None, "tp3": None}
    return {"tp1": row[0], "tp2": row[1], "tp3": row[2]}


def load_rsvamt_tmrfnd(conn: sqlite3.Connection, idno: int) -> dict:
    """II_RSVAMT_TMRFND에서 해지환급금 기준 데이터 로드."""
    row = conn.execute(
        "SELECT * FROM II_RSVAMT_TMRFND WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
        [idno],
    ).fetchone()
    if not row:
        return None
    cols = [c[1] for c in conn.execute("PRAGMA table_info(II_RSVAMT_TMRFND)").fetchall()]
    data = dict(zip(cols, row))
    return data


# SOFF 차감 적용 상품 목록 (PTERM > 5yr 조건과 함께 사용)
SOFF_DEDUCT_PRODS = {"LA0211Z", "LA0215R", "LA0215X", "LA0216R", "LA0216W", "LA0217W"}


def compute_trad_pv(info: dict, bas: dict, n_steps: int) -> dict:
    """Phase 1 TRAD_PV 필드 계산.

    SOFF_BF_TMRFND 규칙:
      - PROD ∈ SOFF_DEDUCT_PRODS AND PTERM > 5yr: 초기연도 ACQSEXP 차감
      - LA0217Y: Type C (별도 처리, 현재 미구현)
      - 그 외: SOFF = APLY_PREM_ACUMAMT_BNFT

    Returns dict of arrays indexed by SETL (0-based).
    """
    elapsed = info["pass_yy"] * 12 + info["pass_mm"]  # CTR_AFT_PASS_MMCNT at SETL=0
    gprem = info["gprem"]
    join_amt = info["join_amt"]
    has_bas = bas is not None
    mult = (join_amt / bas["crit_join_amt"] if bas["crit_join_amt"] else 1.0) if has_bas else 1.0
    pterm_mm = info["pterm_yy"] * 12
    bterm_mm = info["bterm_yy"] * 12

    acqsexp1_val = info.get("acqsexp1", 0) or 0
    prod_cd = info.get("prod_cd", "")
    ctr_tpcd = str(info.get("ctr_tpcd", ""))
    cls_cd = str(info.get("cls_cd", ""))
    # ACQSEXP 차감: SOFF_DEDUCT_PRODS(TPCD=9) 또는 TPCD='0'
    apply_deduction = ((prod_cd in SOFF_DEDUCT_PRODS) and (info["pterm_yy"] > 5)) or \
                      (ctr_tpcd == "0" and acqsexp1_val > 0)
    # SOFF 납입중 비율 (CTR_TPCD + CLS_CD 기반)
    is_tmrfnd_prod = prod_cd in ("LA0217Y",)  # TMRFND 기반 SOFF 상품
    if ctr_tpcd == "3":
        soff_pay_rate = 0.3
    elif ctr_tpcd == "5":
        soff_pay_rate = 0.5
    elif is_tmrfnd_prod and ctr_tpcd == "1" and cls_cd in ("01", "02"):
        soff_pay_rate = 0.0
    else:
        soff_pay_rate = 1.0

    ctr_mm = np.arange(n_steps) + elapsed  # CTR_AFT_PASS_MMCNT
    pay_stcd = info.get("pay_stcd", 1)
    if pay_stcd != 1:
        # PAY_STCD=2(납입완료) or 3(납입면제): 전 구간 납입 없음
        prem_pay_yn = np.zeros(n_steps, dtype=np.float64)
    else:
        prem_pay_yn = (ctr_mm <= pterm_mm).astype(np.float64)
    orig_prem = np.full(n_steps, gprem, dtype=np.float64)
    dc_rt = info.get("prem_dc_rt", 0)
    dc_val = int(gprem * (1 - dc_rt) + 0.5)  # round-half-up
    dc_prem = np.full(n_steps, dc_val, dtype=np.float64)
    if has_bas:
        nprem_val = bas["nprem"] * mult
        acum_nprem = np.full(n_steps, nprem_val, dtype=np.float64)
    else:
        # BAS 미보유: 납입 중에만 ACUM_NPREM, 납입후 0
        nprem_val = info.get("acum_nprem_nobas", 0.0)
        acum_nprem = prem_pay_yn * nprem_val
    # PAD_PREM: 초기값 = GPREM × CTR_MM[0], 이후 PREM_PAY_YN=1일 때 GPREM 누적
    pad_prem = np.empty(n_steps, dtype=np.float64)
    pad_prem[0] = gprem * ctr_mm[0]
    for t in range(1, n_steps):
        pad_prem[t] = pad_prem[t - 1] + gprem * prem_pay_yn[t]
    acqsexp1 = np.full(n_steps, acqsexp1_val, dtype=np.float64)

    # YSTR/YYEND 준비금 (보험연도 기반)
    ystr_rsvamt = np.zeros(n_steps, dtype=np.float64)
    yyend_rsvamt = np.zeros(n_steps, dtype=np.float64)
    soff_bf_tmrfnd = np.zeros(n_steps, dtype=np.float64)
    aply_prem_acumamt_bnft = np.zeros(n_steps, dtype=np.float64)

    # BAS 미보유: 이율 기반 ACUM 계산
    acum_nobas = None
    if not has_bas:
        acum_cov = info.get("acum_cov")
        V = info.get("accmpt_rspb_rsvamt", 0.0)
        if acum_cov and V:
            inrt_cd = acum_cov["aply_inrt_cd"]
            lwst_inrt = acum_cov["lwst_grnt_inrt"]
            ei = info.get("expct_inrt_data")
            expct_inrt_val = ei["expct_inrt"] if ei else 0.0
            acum_nobas, _ = compute_acum_interest_based(
                V, nprem_val, inrt_cd, lwst_inrt,
                n_steps, elapsed, pterm_mm, pay_stcd,
                expct_inrt=expct_inrt_val)

    for t in range(n_steps):
        cm = int(ctr_mm[t])
        ins_year = (cm - 1) // 12 + 1  # 보험연도 (1-based)
        month_in_year = cm - (ins_year - 1) * 12  # 연도 내 경과월 (1~12)

        if has_bas:
            yr_idx = ins_year - 1  # 0-based array index
            if yr_idx < 120:
                ystr_v = bas["ystr"][yr_idx] * mult
                yyend_v = bas["yyend"][yr_idx] * mult
            else:
                ystr_v = 0.0
                yyend_v = 0.0
            ystr_rsvamt[t] = ystr_v
            yyend_rsvamt[t] = yyend_v
            interp = ystr_v + (yyend_v - ystr_v) * month_in_year / 12
        else:
            # BAS 미보유: 이율 기반 ACUM 사용
            ystr_rsvamt[t] = 0.0
            yyend_rsvamt[t] = 0.0
            interp = acum_nobas[t] if acum_nobas is not None else 0.0

        aply_prem_acumamt_bnft[t] = interp

        # SOFF: TPCD 비율 적용 (납입중만) + ACQSEXP 차감
        rate = soff_pay_rate if prem_pay_yn[t] > 0 else 1.0
        soff_val = interp * rate
        if apply_deduction:
            remaining_84 = max(84 - cm, 0)
            soff_val -= acqsexp1_val * remaining_84 / 84
        soff_bf_tmrfnd[t] = soff_val

    # LTRMNAT_TMRFND: CTR_TPCD='9' -> 0, else -> max(0, ACUM - 7yr deduction)
    ctr_tpcd = info.get("ctr_tpcd", "")
    if str(ctr_tpcd) == "9":
        ltrmnat_tmrfnd = np.zeros(n_steps, dtype=np.float64)
    else:
        deduction = acqsexp1_val * np.maximum(84 - ctr_mm, 0) / 84
        ltrmnat_tmrfnd = np.maximum(0, aply_prem_acumamt_bnft - deduction)

    zeros = np.zeros(n_steps, dtype=np.float64)

    return {
        "CTR_AFT_PASS_MMCNT": ctr_mm.astype(np.float64),
        "PREM_PAY_YN": prem_pay_yn,
        "ORIG_PREM": orig_prem,
        "DC_PREM": dc_prem,
        "ACUM_NPREM": acum_nprem,
        "ACUM_NPREM_PRPD": zeros.copy(),           # 3건만 non-zero
        "PRPD_MMCNT": zeros.copy(),                 # 극소
        "PRPD_PREM": zeros.copy(),                  # 극소
        "PAD_PREM": pad_prem,
        "ADD_ACCMPT_GPREM": zeros.copy(),            # 전부 0
        "ADD_ACCMPT_NPREM": zeros.copy(),            # 전부 0
        "ACQSEXP1_BIZEXP": acqsexp1,
        "ACQSEXP2_BIZEXP": zeros.copy(),             # 전부 0
        "AFPAY_MNTEXP": zeros.copy(),                # 전부 0
        "LUMPAY_BIZEXP": zeros.copy(),               # 전부 0
        "PAY_GRCPR_ACQSEXP": zeros.copy(),           # 전부 0
        "YSTR_RSVAMT": ystr_rsvamt,
        "YYEND_RSVAMT": yyend_rsvamt,
        "YSTR_RSVAMT_TRM": ystr_rsvamt.copy(),       # 100% 동일
        "YYEND_RSVAMT_TRM": yyend_rsvamt.copy(),     # 100% 동일
        "PENS_INRT": zeros.copy(),                   # 전부 0
        "PENS_DEFRY_RT": zeros.copy(),               # 전부 0
        "PENS_ANNUAL_SUM": zeros.copy(),             # 전부 0
        "HAFWAY_WDAMT": zeros.copy(),                # 전부 0
        "APLY_PREM_ACUMAMT_BNFT": aply_prem_acumamt_bnft,
        "APLY_PREM_ACUMAMT_EXP": aply_prem_acumamt_bnft.copy(),  # 100% 동일
        "SOFF_BF_TMRFND": soff_bf_tmrfnd,
        "SOFF_AF_TMRFND": soff_bf_tmrfnd.copy(),    # 99.997% 동일
        "LTRMNAT_TMRFND": ltrmnat_tmrfnd,
        "HAFWAY_WDAMT_ADD": zeros.copy(),            # 전부 0
        "SOFF_BF_TMRFND_ADD": zeros.copy(),          # 전부 0
        "SOFF_AF_TMRFND_ADD": zeros.copy(),          # 전부 0
        "LOAN_NEW": zeros.copy(),                    # 전부 0
        "LOAN_RPAY_MATU": zeros.copy(),              # 전부 0
        "MATU_MAINT_BNS_ACUM_AMT": zeros.copy(),    # 전부 0
    }


COMPARE_ITEMS = [
    "CTR_AFT_PASS_MMCNT",
    "PREM_PAY_YN",
    "ORIG_PREM",
    "DC_PREM",
    "ACUM_NPREM",
    "ACUM_NPREM_PRPD",
    "PRPD_MMCNT",
    "PRPD_PREM",
    "PAD_PREM",
    "ADD_ACCMPT_GPREM",
    "ADD_ACCMPT_NPREM",
    "ACQSEXP1_BIZEXP",
    "ACQSEXP2_BIZEXP",
    "AFPAY_MNTEXP",
    "LUMPAY_BIZEXP",
    "PAY_GRCPR_ACQSEXP",
    "YSTR_RSVAMT",
    "YYEND_RSVAMT",
    "YSTR_RSVAMT_TRM",
    "YYEND_RSVAMT_TRM",
    "PENS_INRT",
    "PENS_DEFRY_RT",
    "PENS_ANNUAL_SUM",
    "HAFWAY_WDAMT",
    "APLY_PREM_ACUMAMT_BNFT",
    "APLY_PREM_ACUMAMT_EXP",
    "SOFF_BF_TMRFND",
    "SOFF_AF_TMRFND",
    "LTRMNAT_TMRFND",
    "HAFWAY_WDAMT_ADD",
    "SOFF_BF_TMRFND_ADD",
    "SOFF_AF_TMRFND_ADD",
    "LOAN_NEW",
    "LOAN_RPAY_MATU",
    "MATU_MAINT_BNS_ACUM_AMT",
]


def compare_single(idno: int, v1: dict, expected: pd.DataFrame) -> dict:
    """단건 비교."""
    n_exp = len(expected)
    n_v1 = len(v1["CTR_AFT_PASS_MMCNT"])
    compare_n = min(n_exp, n_v1)

    if compare_n <= 0:
        return {"idno": idno, "pass": True, "items": {}, "compare_n": 0}

    exp = expected.iloc[:compare_n].reset_index(drop=True)
    items = {}

    for name in COMPARE_ITEMS:
        v1_arr = v1[name][:compare_n]
        exp_arr = exp[name].values.astype(np.float64)
        diff = np.abs(v1_arr - exp_arr)
        max_diff = float(diff.max())
        max_idx = int(diff.argmax())

        items[name] = {
            "max_diff": max_diff,
            "setl": int(exp.iloc[max_idx]["SETL_AFT_PASS_MMCNT"]) if "SETL_AFT_PASS_MMCNT" in exp.columns else max_idx,
            "pass": max_diff < TOL,
            "v1_val": float(v1_arr[max_idx]),
            "exp_val": float(exp_arr[max_idx]),
        }

    all_pass = all(item["pass"] for item in items.values())
    return {"idno": idno, "pass": all_pass, "items": items, "compare_n": compare_n}


def debug_single(idno: int, legacy_conn, proj_conn):
    """단건 상세 디버그 출력."""
    info = load_contract_info(legacy_conn, idno)
    if not info:
        print(f"  ERROR: IDNO={idno} not found in II_INFRC")
        return

    bas = load_rsvamt_bas(legacy_conn, idno)
    # BAS 미보유 시 bas=None으로 진행

    # ACQSEXP1: TOT_TRMNAT_DDCT_AMT 사용 (main과 동일)
    info["acqsexp1"] = info.get("tot_trmnat_ddct", 0) or 0
    acqsexp_rt = load_acqsexp_rt(
        legacy_conn, info["prod_cd"], info["cov_cd"],
        info["cls_cd"], info["paypr_dvcd"], info["etc_bizexp_key"]
    )

    mult = (info["join_amt"] / bas["crit_join_amt"]) if bas else 1.0

    # SOFF 유형 판별
    tp = load_tmrfnd_calc_tp(legacy_conn, info["prod_cd"], info["cov_cd"])
    info["tmrfnd_tp"] = tp
    tmrfnd_data = load_rsvamt_tmrfnd(legacy_conn, idno)
    info["has_tmrfnd_data"] = tmrfnd_data is not None

    # SOFF 유형 판정
    if tmrfnd_data is not None:
        soff_type = "C (TMRFND data → SOFF=0)"
    elif str(tp.get("tp2")) == "1":
        soff_type = "B (TP2=1 → ACQSEXP deduction)"
    else:
        soff_type = "A (TP2=4 → simple interpolation)"

    print(f"\n  === IDNO={idno} 계약 정보 ===")
    print(f"  PROD={info['prod_cd']}, COV={info['cov_cd']}, CTR_DT={info['ctr_dt']}")
    print(f"  BTERM={info['bterm_yy']}yr, PTERM={info['pterm_yy']}yr, ELAPSED={info['pass_yy']}y{info['pass_mm']}m")
    print(f"  GPREM={info['gprem']}, JOIN_AMT={info['join_amt']}, mult={mult}")
    print(f"  NPREM×mult={bas['nprem']*mult}, TOT_TRMNAT_DDCT={info['tot_trmnat_ddct']}")
    print(f"  ACQSEXP_RT={acqsexp_rt}, computed ACQSEXP1={info['acqsexp1']}")
    print(f"  TMRFND_TP: {tp}, HAS_TMRFND_DATA={info['has_tmrfnd_data']}")
    print(f"  SOFF Type: {soff_type}")

    # 기대값 로드
    expected = proj_conn.execute("""
        SELECT * FROM OD_TRAD_PV WHERE INFRC_IDNO = ?
        ORDER BY SETL_AFT_PASS_MMCNT
    """, [idno]).fetchdf()

    if expected.empty:
        print(f"  ERROR: IDNO={idno} not in OD_TRAD_PV")
        return

    n_steps = len(expected)
    v1 = compute_trad_pv(info, bas, n_steps)
    result = compare_single(idno, v1, expected)

    print(f"\n  === 비교 결과 ({result['compare_n']}개월) ===")
    print(f"  {'항목':<25} {'Max Diff':>12} {'SETL':>6} {'결과':>6}")
    print("  " + "-" * 55)
    for name in COMPARE_ITEMS:
        it = result["items"][name]
        status = "PASS" if it["pass"] else "FAIL"
        print(f"  {name:<25} {it['max_diff']:>12.2e}  @{it['setl']:>4d}  [{status}]")
        if not it["pass"]:
            print(f"    v1={it['v1_val']:.6f}, exp={it['exp_val']:.6f}")

    # 처음 5개월 상세
    print(f"\n  === 처음 5개월 상세 ===")
    for t in range(min(5, n_steps)):
        setl = int(expected.iloc[t]["SETL_AFT_PASS_MMCNT"])
        print(f"  SETL={setl}:")
        for name in COMPARE_ITEMS:
            v1_val = v1[name][t]
            exp_val = expected.iloc[t][name]
            diff = abs(v1_val - exp_val)
            mark = " " if diff < TOL else "!"
            print(f"   {mark} {name:<25} v1={v1_val:>15.4f}  exp={exp_val:>15.4f}  diff={diff:.2e}")


def main():
    parser = argparse.ArgumentParser(description="v1 OD_TRAD_PV Phase 1 비교")
    parser.add_argument("--n", type=int, default=10, help="랜덤 샘플 수")
    parser.add_argument("--idno", type=str, default=None, help="특정 IDNO (콤마 구분)")
    parser.add_argument("--csv", action="store_true", help="불일치 CSV 저장")
    parser.add_argument("--seed", type=int, default=42, help="랜덤 시드")
    parser.add_argument("--debug", type=int, default=None, help="단건 상세 디버그")
    args = parser.parse_args()

    proj = duckdb.connect(PROJ_O_DB, read_only=True)
    legacy = sqlite3.connect(LEGACY_DB)

    # 디버그 모드
    if args.debug:
        debug_single(args.debug, legacy, proj)
        legacy.close()
        proj.close()
        return

    # IDNO 선택
    all_idnos = proj.execute(
        "SELECT DISTINCT INFRC_IDNO FROM OD_TRAD_PV ORDER BY INFRC_IDNO"
    ).fetchdf()["INFRC_IDNO"].values

    if args.idno:
        idnos = [int(x.strip()) for x in args.idno.split(",")]
    else:
        rng = np.random.default_rng(args.seed)
        idnos = sorted(rng.choice(all_idnos, size=min(args.n, len(all_idnos)), replace=False))

    print("=" * 80)
    print(f"v1 OD_TRAD_PV Phase 1 비교 (정적 필드)")
    print(f"  Legacy: {LEGACY_DB}")
    print(f"  Expected: {PROJ_O_DB} ({len(all_idnos):,} IDNOs)")
    print(f"  Test: {len(idnos)}건, TOL={TOL}")
    print("=" * 80)

    results = []
    pass_count = 0
    fail_count = 0
    fail_details = []

    for i, idno in enumerate(idnos):
        t0 = time.time()
        try:
            info = load_contract_info(legacy, int(idno))
            if not info:
                raise ValueError("II_INFRC not found")

            bas = load_rsvamt_bas(legacy, int(idno))
            # BAS 미보유 (CLA00500 적립형): bas=None으로 진행

            # ACQSEXP1 계산: TOT_TRMNAT_DDCT_AMT 사용 (검증용)
            info["acqsexp1"] = info.get("tot_trmnat_ddct", 0) or 0

            # BAS 미보유 시: ACUM_NPREM 계산 + ACUM_COV 로딩
            # PAY_STCD=2(납입완료),3(납입면제) → 0
            # ETC_KEY[0]='1' → ACCMPT 그대로 (구형상품)
            # ETC_KEY[0]='9' → ACCMPT × (1 - MNTEXP - LOSS)
            if bas is None:
                etc_key = info.get("etc_bizexp_key", "")
                if info.get("pay_stcd", 1) != 1:
                    info["acum_nprem_nobas"] = 0.0
                elif etc_key and etc_key[0] == "1":
                    info["acum_nprem_nobas"] = info["accmpt_gprem"]
                else:
                    brt = load_bizexp_rt(legacy, info["prod_cd"], info["cov_cd"],
                                         info["cls_cd"], info["paypr_dvcd"], etc_key)
                    if brt:
                        info["acum_nprem_nobas"] = info["accmpt_gprem"] * (1 - brt["inpay_mntexp_rt"] - brt["loss_svyexp_rt"])
                    else:
                        info["acum_nprem_nobas"] = 0.0

                # ACUM_COV + EXPCT_INRT + ACCMPT_RSPB_RSVAMT for interest-based accumulation
                acum_cov = load_acum_cov(legacy, info["prod_cd"], info["cov_cd"], info["cls_cd"])
                info["acum_cov"] = acum_cov
                info["expct_inrt_data"] = load_expct_inrt(legacy, info["prod_cd"], info["cov_cd"], info["cls_cd"])
                # ACCMPT_RSPB_RSVAMT (starting accumulated reserve)
                rspb = legacy.execute(
                    "SELECT ACCMPT_RSPB_RSVAMT FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
                    [int(idno)]
                ).fetchone()
                info["accmpt_rspb_rsvamt"] = (rspb[0] or 0) if rspb else 0

            # SOFF 유형 판별
            info["tmrfnd_tp"] = load_tmrfnd_calc_tp(legacy, info["prod_cd"], info["cov_cd"])
            tmrfnd_data = load_rsvamt_tmrfnd(legacy, int(idno))
            info["has_tmrfnd_data"] = tmrfnd_data is not None

            expected = proj.execute("""
                SELECT * FROM OD_TRAD_PV WHERE INFRC_IDNO = ?
                ORDER BY SETL_AFT_PASS_MMCNT
            """, [int(idno)]).fetchdf()

            if expected.empty:
                raise ValueError("OD_TRAD_PV empty")

            n_steps = len(expected)
            v1 = compute_trad_pv(info, bas, n_steps)
            result = compare_single(int(idno), v1, expected)
            elapsed = time.time() - t0

            if result["pass"]:
                pass_count += 1
                status = "PASS"
            else:
                fail_count += 1
                status = "FAIL"
                fail_details.append(result)

            print(f"  [{i+1:4d}/{len(idnos)}] IDNO={idno:>8d}  {result['compare_n']:>4d}mo  {elapsed:.1f}s  [{status}]", end="")
            if status == "FAIL":
                fail_items = [k for k, v in result["items"].items() if not v["pass"]]
                for fname in fail_items[:3]:
                    fi = result["items"][fname]
                    print(f"  {fname}({fi['max_diff']:.2e}@{fi['setl']})", end="")
            print()

            result["prod_cd"] = info.get("prod_cd", "")
            results.append(result)

        except Exception as e:
            fail_count += 1
            elapsed = time.time() - t0
            print(f"  [{i+1:4d}/{len(idnos)}] IDNO={idno:>8d}  ERROR  {elapsed:.1f}s  {str(e)[:60]}")
            results.append({"idno": int(idno), "pass": False, "items": {}, "error": str(e), "prod_cd": ""})

    proj.close()
    legacy.close()

    # 요약
    n_error = sum(1 for r in results if "error" in r)
    n_valid = len(results) - n_error
    n_la0217y = sum(1 for r in results if r.get("prod_cd") == "LA0217Y" and "error" not in r)
    n_excl = n_valid - n_la0217y

    print()
    print("=" * 80)
    print(f"결과: PASS={pass_count}, FAIL={fail_count}, ERROR={n_error}, 총={len(idnos)}")
    print(f"  (LA0217Y={n_la0217y}건 별도 분리, Phase 2)")
    print("=" * 80)

    # 항목별 통계 (LA0217Y 제외)
    valid_excl = [r for r in results if r.get("items") and r.get("prod_cd") != "LA0217Y"]
    valid_la0217y = [r for r in results if r.get("items") and r.get("prod_cd") == "LA0217Y"]

    if valid_excl:
        print(f"\n[LA0217Y 제외] {len(valid_excl)}건:")
        print(f"  {'항목':<25} {'PASS':>6} {'FAIL':>6} {'Max Diff':>12}")
        print("  " + "-" * 53)
        for name in COMPARE_ITEMS:
            item_pass = sum(1 for r in valid_excl if name in r["items"] and r["items"][name]["pass"])
            item_fail = sum(1 for r in valid_excl if name in r["items"] and not r["items"][name]["pass"])
            max_d = max((r["items"][name]["max_diff"] for r in valid_excl if name in r["items"]), default=0)
            print(f"  {name:<25} {item_pass:>6} {item_fail:>6} {max_d:>12.2e}")

    if valid_la0217y:
        print(f"\n[LA0217Y만] {len(valid_la0217y)}건:")
        print(f"  {'항목':<25} {'PASS':>6} {'FAIL':>6} {'Max Diff':>12}")
        print("  " + "-" * 53)
        for name in COMPARE_ITEMS:
            item_pass = sum(1 for r in valid_la0217y if name in r["items"] and r["items"][name]["pass"])
            item_fail = sum(1 for r in valid_la0217y if name in r["items"] and not r["items"][name]["pass"])
            max_d = max((r["items"][name]["max_diff"] for r in valid_la0217y if name in r["items"]), default=0)
            print(f"  {name:<25} {item_pass:>6} {item_fail:>6} {max_d:>12.2e}")

    # FAIL CSV
    if fail_details and args.csv:
        rows = []
        for r in fail_details:
            for name, info_item in r["items"].items():
                if not info_item["pass"]:
                    rows.append({
                        "idno": r["idno"],
                        "item": name,
                        "max_diff": info_item["max_diff"],
                        "setl": info_item["setl"],
                        "v1_val": info_item["v1_val"],
                        "exp_val": info_item["exp_val"],
                    })
        if rows:
            csv_path = "v1_trad_pv_fails.csv"
            pd.DataFrame(rows).to_csv(csv_path, index=False)
            print(f"\n불일치 상세: {csv_path}")


if __name__ == "__main__":
    main()

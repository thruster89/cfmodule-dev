"""
전통 준비금/보험료 프로젝션 (OD_TRAD_PV) 모듈

DB에서 로딩한 계약 정보, 준비금(BAS), 사업비율 등을 이용하여
OD_TRAD_PV 테이블의 50개 컬럼을 산출한다.

검증 기준: test_v1_trad_pv_vs_proj_o.py (proj_o.duckdb 기대값)
"""

import json
import os
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from cf_module.utils.logger import get_logger

logger = get_logger("trad_pv")

# SOFF 차감 적용 상품
SOFF_DEDUCT_PRODS = {"LA0211Z", "LA0215R", "LA0215X", "LA0216R", "LA0216W", "LA0217W"}

# INRT lookup 캐시
_INRT_CACHE: Optional[dict] = None


def _load_inrt_lookup() -> dict:
    """inrt_lookup.json 로드 (캐싱)."""
    global _INRT_CACHE
    if _INRT_CACHE is not None:
        return _INRT_CACHE
    path = os.path.join(os.path.dirname(__file__), "..", "..", "inrt_lookup.json")
    path = os.path.normpath(path)
    if os.path.exists(path):
        with open(path, "r") as f:
            _INRT_CACHE = json.load(f)
    else:
        _INRT_CACHE = {}
    return _INRT_CACHE


# ---------------------------------------------------------------------------
# 데이터 구조
# ---------------------------------------------------------------------------

@dataclass
class ContractInfo:
    """단건 계약 기본 정보 (II_INFRC 기반)."""
    idno: int
    prod_cd: str
    cov_cd: str
    cls_cd: str
    ctr_tpcd: str           # CTR_TPCD (TEXT: '0','1','3','5','9')
    pass_yy: int            # 경과년수
    pass_mm: int            # 경과월수 (연 내)
    bterm_yy: int           # 보장기간(년)
    pterm_yy: int           # 납입기간(년)
    gprem: float            # 영업보험료 (GRNTPT_GPREM)
    join_amt: float         # 가입금액 (GRNTPT_JOIN_AMT)
    pay_stcd: int = 1       # PAY_STCD (1=납입중, 2=완납, 3=면제)
    paycyc: int = 1         # 납입주기 (1=월납, 3=분기납, 6=반기납, 12=연납)
    prem_dc_rt: float = 0.0 # 할인율
    acqsexp1: float = 0.0   # 신계약비

    # BAS 보유 시
    bas: Optional[dict] = None
    # BAS 미보유 시 (이율 기반)
    acum_nprem_nobas: float = 0.0
    acum_nprem_old: float = 0.0       # 상각기간 내 NPREM (alpha1 차감)
    amort_mm: int = 0                 # 신계약비 상각기간(월) — CTR_MM <= amort_mm이면 old prem
    accmpt_rspb_rsvamt: float = 0.0
    acum_cov: Optional[dict] = None
    expct_inrt_data: Optional[dict] = None
    pubano_params: Optional[dict] = None   # IE_PUBANO_INRT 파라미터
    dc_rt_curve: Optional[np.ndarray] = None  # IE_DC_RT 할인율 커브
    ctr_loan_remamt: float = 0.0           # II_INFRC.CTR_LOAN_REMAMT (초기 대출잔액)
    ctr_loan_tpcd: int = 1                # IP_P_PROD.CTR_LOAN_TPCD (0=대출불가)
    loan_params: Optional[dict] = None    # IA_A_CTR_LOAN 약관대출 가정
    # IP_P_LTRMNAT: SOFF 비율 (경과년 1~20, PAY_STCD별)
    soff_rates_paying: Optional[np.ndarray] = None    # PAY_STCD=1 (납입기간)
    soff_rates_paidup: Optional[np.ndarray] = None    # PAY_STCD=2 (납입후)


@dataclass
class TradPVResult:
    """OD_TRAD_PV 산출 결과 (단건, 1D arrays indexed by SETL)."""
    n_steps: int

    # 시간축/보험료
    ctr_mm: np.ndarray              # 계약후경과월
    prem_pay_yn: np.ndarray         # 납입여부
    orig_prem: np.ndarray           # 원수보험료
    dc_prem: np.ndarray             # 할인보험료
    acum_nprem: np.ndarray          # 적립순보험료
    pad_prem: np.ndarray            # 기납입보험료

    # 사업비
    acqsexp1_bizexp: np.ndarray     # 신계약비

    # 준비금
    ystr_rsvamt: np.ndarray         # 연시준비금
    yyend_rsvamt: np.ndarray        # 연말준비금

    # 적립금 (이율 부리)
    aply_prem_acumamt_bnft: np.ndarray   # 적용보험료적립금(급부)
    aply_prem_acumamt_exp: np.ndarray    # 적용보험료적립금(사업비)

    # 환급금
    soff_bf_tmrfnd: np.ndarray      # 소멸전환급금
    soff_af_tmrfnd: np.ndarray      # 소멸후환급금
    ltrmnat_tmrfnd: np.ndarray      # 해지환급금

    # Phase 2/3 (미구현 → 0)
    aply_pubano_inrt: np.ndarray    # 적용공시이율
    aply_adint_tgt_amt: np.ndarray  # 부리대상금액
    lwst_adint_tgt_amt: np.ndarray  # 최저부리대상금액
    lwst_prem_acumamt: np.ndarray   # 최저보험료적립금
    cncttp_acumamt_kics: np.ndarray # KICS적립금

    # 약관대출
    loan_int: np.ndarray               # 대출이자
    loan_remamt: np.ndarray             # 대출잔액
    loan_rpay_hafway: np.ndarray        # 반기상환

    # zero 컬럼
    acum_nprem_prpd: np.ndarray
    prpd_mmcnt: np.ndarray
    prpd_prem: np.ndarray
    add_accmpt_gprem: np.ndarray
    add_accmpt_nprem: np.ndarray
    acqsexp2_bizexp: np.ndarray
    afpay_mntexp: np.ndarray
    lumpay_bizexp: np.ndarray
    pay_grcpr_acqsexp: np.ndarray
    pens_inrt: np.ndarray
    pens_defry_rt: np.ndarray
    pens_annual_sum: np.ndarray
    hafway_wdamt: np.ndarray
    hafway_wdamt_add: np.ndarray
    soff_bf_tmrfnd_add: np.ndarray
    soff_af_tmrfnd_add: np.ndarray
    loan_new: np.ndarray
    loan_rpay_matu: np.ndarray
    matu_maint_bns_acum_amt: np.ndarray

    def to_dict(self) -> dict:
        """OD_TRAD_PV 컬럼명으로 변환."""
        return {
            "CTR_AFT_PASS_MMCNT": self.ctr_mm,
            "PREM_PAY_YN": self.prem_pay_yn,
            "ORIG_PREM": self.orig_prem,
            "DC_PREM": self.dc_prem,
            "ACUM_NPREM": self.acum_nprem,
            "ACUM_NPREM_PRPD": self.acum_nprem_prpd,
            "PRPD_MMCNT": self.prpd_mmcnt,
            "PRPD_PREM": self.prpd_prem,
            "PAD_PREM": self.pad_prem,
            "ADD_ACCMPT_GPREM": self.add_accmpt_gprem,
            "ADD_ACCMPT_NPREM": self.add_accmpt_nprem,
            "ACQSEXP1_BIZEXP": self.acqsexp1_bizexp,
            "ACQSEXP2_BIZEXP": self.acqsexp2_bizexp,
            "AFPAY_MNTEXP": self.afpay_mntexp,
            "LUMPAY_BIZEXP": self.lumpay_bizexp,
            "PAY_GRCPR_ACQSEXP": self.pay_grcpr_acqsexp,
            "YSTR_RSVAMT": self.ystr_rsvamt,
            "YYEND_RSVAMT": self.yyend_rsvamt,
            "YSTR_RSVAMT_TRM": self.ystr_rsvamt,
            "YYEND_RSVAMT_TRM": self.yyend_rsvamt,
            "PENS_INRT": self.pens_inrt,
            "PENS_DEFRY_RT": self.pens_defry_rt,
            "PENS_ANNUAL_SUM": self.pens_annual_sum,
            "HAFWAY_WDAMT": self.hafway_wdamt,
            "APLY_PUBANO_INRT": self.aply_pubano_inrt,
            "APLY_ADINT_TGT_AMT": self.aply_adint_tgt_amt,
            "APLY_PREM_ACUMAMT_BNFT": self.aply_prem_acumamt_bnft,
            "APLY_PREM_ACUMAMT_EXP": self.aply_prem_acumamt_exp,
            "LWST_ADINT_TGT_AMT": self.lwst_adint_tgt_amt,
            "LWST_PREM_ACUMAMT": self.lwst_prem_acumamt,
            "SOFF_BF_TMRFND": self.soff_bf_tmrfnd,
            "SOFF_AF_TMRFND": self.soff_af_tmrfnd,
            "LTRMNAT_TMRFND": self.ltrmnat_tmrfnd,
            "HAFWAY_WDAMT_ADD": self.hafway_wdamt_add,
            "SOFF_BF_TMRFND_ADD": self.soff_bf_tmrfnd_add,
            "SOFF_AF_TMRFND_ADD": self.soff_af_tmrfnd_add,
            "LOAN_INT": self.loan_int,
            "LOAN_REMAMT": self.loan_remamt,
            "LOAN_RPAY_HAFWAY": self.loan_rpay_hafway,
            "LOAN_NEW": self.loan_new,
            "LOAN_RPAY_MATU": self.loan_rpay_matu,
            "CNCTTP_ACUMAMT_KICS": self.cncttp_acumamt_kics,
            "MATU_MAINT_BNS_ACUM_AMT": self.matu_maint_bns_acum_amt,
        }


# ---------------------------------------------------------------------------
# STEP 1: 보험료 산출
# ---------------------------------------------------------------------------

def _calc_premium(info: ContractInfo, n_steps: int) -> dict:
    """보험료 관련 필드 산출.

    Returns dict with: ctr_mm, prem_pay_yn, orig_prem, dc_prem,
                       acum_nprem, pad_prem, acqsexp1_bizexp
    """
    elapsed = info.pass_yy * 12 + info.pass_mm
    pterm_mm = info.pterm_yy * 12

    ctr_mm = np.arange(n_steps, dtype=np.float64) + elapsed

    # 납입여부
    if info.pay_stcd != 1:
        prem_pay_yn = np.zeros(n_steps, dtype=np.float64)
    else:
        in_pay = (ctr_mm <= pterm_mm)
        cyc = info.paycyc if info.paycyc > 0 else 1
        if cyc == 1:
            prem_pay_yn = in_pay.astype(np.float64)
        else:
            # 납입주기: CTR_AFT_PASS_MMCNT mod cyc == 1 일 때 납입
            prem_pay_yn = (in_pay & (ctr_mm % cyc == 1)).astype(np.float64)

    # 보험료
    orig_prem = np.full(n_steps, info.gprem, dtype=np.float64)
    dc_val = int(info.gprem * (1 - info.prem_dc_rt) + 0.5)
    dc_prem = np.full(n_steps, dc_val, dtype=np.float64)

    # 적립순보험료
    if info.bas is not None:
        mult = (info.join_amt / info.bas["crit_join_amt"]
                if info.bas["crit_join_amt"] else 1.0)
        nprem_val = info.bas["nprem"] * mult
        acum_nprem = np.full(n_steps, nprem_val, dtype=np.float64)
    else:
        nprem_val = info.acum_nprem_nobas
        nprem_old = info.acum_nprem_old if info.acum_nprem_old else nprem_val
        # CTR_MM <= amort_mm: old NPREM, 이후: new NPREM (벡터화)
        amort_mm = info.amort_mm
        if amort_mm > 0 and nprem_old != nprem_val:
            acum_nprem = np.where(ctr_mm <= amort_mm, nprem_old, nprem_val) * prem_pay_yn
        else:
            acum_nprem = np.full(n_steps, nprem_val, dtype=np.float64) * prem_pay_yn

    # 기납입보험료 (누적) — 벡터화
    cyc = info.paycyc if info.paycyc > 0 else 1
    if info.paycyc == 0:
        initial_paid = info.gprem
    else:
        cm0 = int(ctr_mm[0])
        paid_mm = min(cm0, pterm_mm) if info.pay_stcd == 1 else cm0
        pay_count = (paid_mm - 1) // cyc + 1 if paid_mm > 0 else 0
        initial_paid = info.gprem * pay_count
    # pad_prem[0] = initial_paid, pad_prem[t] = initial_paid + Σ(gprem × pay_yn[1:t])
    pad_prem = np.empty(n_steps, dtype=np.float64)
    pad_prem[0] = initial_paid
    if n_steps > 1:
        pad_prem[1:] = initial_paid + np.cumsum(info.gprem * prem_pay_yn[1:])

    acqsexp1 = np.full(n_steps, info.acqsexp1, dtype=np.float64)

    return {
        "ctr_mm": ctr_mm,
        "prem_pay_yn": prem_pay_yn,
        "orig_prem": orig_prem,
        "dc_prem": dc_prem,
        "acum_nprem": acum_nprem,
        "pad_prem": pad_prem,
        "acqsexp1_bizexp": acqsexp1,
        "nprem_val": nprem_val,
    }


# ---------------------------------------------------------------------------
# STEP 4: 적립금 (ACUM)
# ---------------------------------------------------------------------------

def _calc_accumulation(info: ContractInfo, n_steps: int,
                       ctr_mm: np.ndarray, prem_pay_yn: np.ndarray,
                       nprem_val: float,
                       pubano_inrt: np.ndarray,
                       lwst_grnt_inrt: np.ndarray,
                       pay_trmo: Optional[np.ndarray] = None,
                       ctr_trmo: Optional[np.ndarray] = None) -> dict:
    """적립금(APLY_PREM_ACUMAMT_BNFT) 산출.

    BAS 보유: 연시/연말 선형보간
    BAS 미보유: 이율 기반 부리 (PAY_TRMO/CTR_TRMO 비율 보정)

    Returns dict with: ystr_rsvamt, yyend_rsvamt, aply_prem_acumamt_bnft,
                       aply_adint_tgt_amt, lwst_adint_tgt_amt, lwst_prem_acumamt
    """
    has_bas = info.bas is not None
    mult = (info.join_amt / info.bas["crit_join_amt"]
            if has_bas and info.bas["crit_join_amt"] else 1.0) if has_bas else 1.0

    ystr_rsvamt = np.zeros(n_steps, dtype=np.float64)
    yyend_rsvamt = np.zeros(n_steps, dtype=np.float64)
    aply_prem_acumamt_bnft = np.zeros(n_steps, dtype=np.float64)

    aply_adint_tgt_amt = np.zeros(n_steps, dtype=np.float64)
    lwst_adint_tgt_amt = np.zeros(n_steps, dtype=np.float64)
    lwst_prem_acumamt = np.zeros(n_steps, dtype=np.float64)

    # BAS 미보유: 이율 기반 ACUM
    acum_nobas = None
    if not has_bas and info.acum_cov and info.accmpt_rspb_rsvamt:
        elapsed = info.pass_yy * 12 + info.pass_mm
        pterm_mm = info.pterm_yy * 12

        # LWST=0인 시점은 최저보증 없음 → PUBANO 그대로 사용
        lwst_for_acum = np.where(lwst_grnt_inrt > 0, lwst_grnt_inrt, pubano_inrt)

        acum_nobas, lwst_acum, adint_aply, adint_lwst = _compute_acum_interest_based(
            info.accmpt_rspb_rsvamt, nprem_val,
            lwst_for_acum, n_steps,
            elapsed, pterm_mm, info.pay_stcd,
            pubano_inrt_arr=pubano_inrt,
            pay_trmo=pay_trmo,
            ctr_trmo=ctr_trmo,
            nprem_old=info.acum_nprem_old,
            amort_mm=info.amort_mm,
            prem_pay_yn=prem_pay_yn,
        )
        aply_adint_tgt_amt = adint_aply
        lwst_adint_tgt_amt = adint_lwst
        lwst_prem_acumamt = lwst_acum

    if has_bas:
        # BAS 경로 벡터화
        cm_int = ctr_mm.astype(np.int64)
        ins_year = (cm_int - 1) // 12 + 1
        month_in_year = cm_int - (ins_year - 1) * 12
        yr_idx = ins_year - 1
        valid = yr_idx < 120
        # 범위 초과 방지용 클리핑
        safe_idx = np.minimum(yr_idx, 119)
        bas_ystr = np.array(info.bas["ystr"], dtype=np.float64)
        bas_yyend = np.array(info.bas["yyend"], dtype=np.float64)
        ystr_rsvamt[:] = np.where(valid, bas_ystr[safe_idx] * mult, 0.0)
        yyend_rsvamt[:] = np.where(valid, bas_yyend[safe_idx] * mult, 0.0)
        aply_prem_acumamt_bnft[:] = ystr_rsvamt + (yyend_rsvamt - ystr_rsvamt) * month_in_year / 12
    else:
        if acum_nobas is not None:
            aply_prem_acumamt_bnft[:] = acum_nobas
        # else: 이미 0으로 초기화됨

    return {
        "ystr_rsvamt": ystr_rsvamt,
        "yyend_rsvamt": yyend_rsvamt,
        "aply_prem_acumamt_bnft": aply_prem_acumamt_bnft,
        "aply_adint_tgt_amt": aply_adint_tgt_amt,
        "lwst_adint_tgt_amt": lwst_adint_tgt_amt,
        "lwst_prem_acumamt": lwst_prem_acumamt,
    }


def _build_lwst_grnt_inrt_arr(info: ContractInfo, n_steps: int) -> np.ndarray:
    """최저보증이율 배열 산출 (경과년수 기반 변동)."""
    arr = np.zeros(n_steps, dtype=np.float64)
    if not info.acum_cov:
        return arr

    ac = info.acum_cov
    lwst1 = ac.get("lwst_grnt_inrt", 0.0)
    lwst2 = ac.get("lwst_grnt_inrt2", 0.0)
    lwst3 = ac.get("lwst_grnt_inrt3", 0.0)
    chng_cd = ac.get("lwst_chng_crit_cd", 0)
    yy1 = ac.get("lwst_chng_yycnt1", 0)
    yy2 = ac.get("lwst_chng_yycnt2", 0)

    if chng_cd == 0:
        # 단일률
        arr[:] = lwst1
    else:
        # 경과년수 기반 변동 (벡터화)
        elapsed_mm = info.pass_yy * 12 + info.pass_mm
        ctr_yy = (elapsed_mm + np.arange(n_steps)) // 12
        lwst3_val = lwst3 if lwst3 else lwst2
        if yy1 > 0 and yy2 > 0:
            arr[:] = np.where(ctr_yy < yy1, lwst1,
                              np.where(ctr_yy < yy2, lwst2, lwst3_val))
        elif yy1 > 0:
            arr[:] = np.where(ctr_yy < yy1, lwst1, lwst3_val)
        else:
            arr[:] = lwst3_val

    return arr


def _build_pubano_inrt_arr(info: ContractInfo, n_steps: int) -> np.ndarray:
    """DB 공식 기반 APLY_PUBANO_INRT 배열 산출.

    공식: max(PUBANO_공식, LWST_GRNT_INRT)
    IP_P_ACUM_COV 미대상: 0 배열 반환.
    """
    arr = np.zeros(n_steps, dtype=np.float64)
    if not info.acum_cov:
        # acum_cov 없으면 EXPCT_INRT를 PUBANO로 사용 (CD='00' 취급)
        if info.expct_inrt_data and info.expct_inrt_data.get("expct_inrt"):
            arr[:] = info.expct_inrt_data["expct_inrt"]
        return arr

    lwst_arr = _build_lwst_grnt_inrt_arr(info, n_steps)
    inrt_cd = info.acum_cov["aply_inrt_cd"]

    # APLY_INRT_CD='00': 예정이율 고정
    if inrt_cd == "00":
        fixed = info.expct_inrt_data["expct_inrt"] if info.expct_inrt_data else 0.0
        arr[:] = fixed
        return np.maximum(arr, lwst_arr)

    if not (info.pubano_params and info.dc_rt_curve is not None):
        return np.maximum(arr, lwst_arr)

    pp = info.pubano_params
    adj = pp["adj_rt"]
    iv = pp["iv_adexp_rt"]
    ew = pp["ext_wght"]
    ei = pp["ext_itr"]
    dc_curve = info.dc_rt_curve

    # SETL=0: inrt_lookup 캐시에서 현재 적용율
    inrt_lookup = _load_inrt_lookup()
    inrt_map = inrt_lookup.get(inrt_cd, {})
    s0_str = inrt_map.get("0")
    if s0_str is not None:
        arr[0] = float(s0_str)

    # SETL>=1: DB 공식 벡터화 (PASS_PRD_NO=t → dc_curve[t-1])
    n_dc = len(dc_curve)
    if n_dc > 0 and n_steps > 1:
        indices = np.minimum(np.arange(n_steps - 1), n_dc - 1)
        dc = dc_curve[indices]
        arr[1:] = (ew * ei + (dc - iv) * (1 - ew)) * adj

    return np.maximum(arr, lwst_arr)


# ---------------------------------------------------------------------------
# STEP 2: 미경과보험료 (PRPD) — KICS 산출에 선행
# ---------------------------------------------------------------------------

def _calc_prpd_mmcnt(info: ContractInfo, n_steps: int,
                     ctr_mm: np.ndarray, prem_pay_yn: np.ndarray) -> np.ndarray:
    """PRPD_MMCNT: 납입주기 내 미경과월수."""
    paycyc = info.paycyc
    if paycyc <= 1:
        return np.zeros(n_steps, dtype=np.float64)
    pterm_mm = info.pterm_yy * 12
    result = np.zeros(n_steps, dtype=np.float64)
    for t in range(n_steps):
        cm = int(ctr_mm[t])
        if cm > pterm_mm:
            break
        rem = cm % paycyc
        result[t] = (paycyc - rem) if rem != 0 else 0.0
    return result


def _calc_prpd_prem(info: ContractInfo, n_steps: int,
                    ctr_mm: np.ndarray, prem_pay_yn: np.ndarray,
                    orig_prem: np.ndarray) -> np.ndarray:
    """PRPD_PREM: 미경과보험료 (연납만 해당)."""
    if info.paycyc != 12:
        return np.zeros(n_steps, dtype=np.float64)
    mmcnt = _calc_prpd_mmcnt(info, n_steps, ctr_mm, prem_pay_yn)
    prem_val = orig_prem[0] if len(orig_prem) > 0 else info.gprem
    return prem_val * mmcnt / 12


def _calc_prpd_acum(info: ContractInfo, n_steps: int) -> np.ndarray:
    """ACUM_NPREM_PRPD: 미경과적립순보험료 (비월납 시 nprem 상수)."""
    if info.paycyc <= 1:
        return np.zeros(n_steps, dtype=np.float64)
    return np.full(n_steps, info.acum_nprem_nobas, dtype=np.float64)


# ---------------------------------------------------------------------------
# STEP 5: 환급금 (SOFF / LTRMNAT)
# ---------------------------------------------------------------------------

def _calc_surrender(info: ContractInfo, n_steps: int,
                    ctr_mm: np.ndarray, prem_pay_yn: np.ndarray,
                    aply_prem_acumamt_bnft: np.ndarray) -> dict:
    """환급금(SOFF, LTRMNAT) 산출.

    SOFF 비율 결정:
      1. IP_P_LTRMNAT 등록 시: (PROD_CD, CLS_CD, CTR_TPCD, PAY_STCD) 룩업
         - PAY_STCD 판정: cm ≤ pterm_mm → 1(납입기간), else → 2(납입후)
         - 비율: TMRFND_RT[ins_year] (경과년별, 현재 데이터는 상수)
      2. 미등록 시: 기본 rate = 1.0

    ACQSEXP 차감:
      - SOFF_BF: DEDUCT_PRODS(PTERM>5) 또는 TPCD='0'(ACQSEXP>0), PAY_STCD≠3
      - LTRMNAT: CTR_TPCD≠'9'이고 ACQSEXP>0이면 항상 적용

    Returns dict with: soff_bf_tmrfnd, soff_af_tmrfnd, ltrmnat_tmrfnd
    """
    prod_cd = info.prod_cd
    ctr_tpcd = str(info.ctr_tpcd)
    acqsexp1_val = info.acqsexp1
    pterm_mm = info.pterm_yy * 12

    # --- SOFF 비율 결정 (IP_P_LTRMNAT 기반) ---
    has_ltrmnat_rates = (info.soff_rates_paying is not None
                         or info.soff_rates_paidup is not None)

    if has_ltrmnat_rates:
        # IP_P_LTRMNAT 기반: cm ≤ pterm_mm이면 PAY_STCD=1, 아니면 2
        in_pay_period = (ctr_mm <= pterm_mm)  # 납입기간 판정 (PAY_STCD 아닌 기간 기반)

        if info.soff_rates_paying is not None:
            # 경과년 기반 비율 (현재 상수이지만 일반화)
            ins_year = ((ctr_mm.astype(np.int64) - 1) // 12).clip(0, 19)
            pay_rate = info.soff_rates_paying[ins_year]
        else:
            pay_rate = np.ones(n_steps, dtype=np.float64)  # 미등록 → 1.0

        if info.soff_rates_paidup is not None:
            ins_year_pu = ((ctr_mm.astype(np.int64) - 1) // 12).clip(0, 19)
            paidup_rate = info.soff_rates_paidup[ins_year_pu]
        else:
            paidup_rate = np.ones(n_steps, dtype=np.float64)  # 미등록 → 1.0

        rate = np.where(in_pay_period, pay_rate, paidup_rate)
    else:
        # IP_P_LTRMNAT 미등록: 기본 rate = 1.0
        rate = np.ones(n_steps, dtype=np.float64)

    # SOFF_BF = ACUM × rate
    soff_bf_tmrfnd = aply_prem_acumamt_bnft * rate

    # SOFF ACQSEXP 차감 (PAY_STCD=3 납입면제 시 미적용)
    apply_soff_deduction = (
        info.pay_stcd != 3
        and (
            (prod_cd in SOFF_DEDUCT_PRODS and info.pterm_yy > 5)
            or (ctr_tpcd == "0" and acqsexp1_val > 0)
        )
    )
    if apply_soff_deduction:
        remaining_84 = np.maximum(84 - ctr_mm, 0)
        soff_bf_tmrfnd = soff_bf_tmrfnd - acqsexp1_val * remaining_84 / 84

    # soff_af는 netting에서 수정될 수 있으므로 반드시 복사
    soff_af_tmrfnd = soff_bf_tmrfnd.copy()

    # --- LTRMNAT ---
    # CTR_TPCD='9' → 0
    # else → max(0, ACUM - ACQSEXP×max(84-CM,0)/84) (PAY_STCD≠3이고 ACQSEXP>0)
    if ctr_tpcd == "9":
        ltrmnat_tmrfnd = np.zeros(n_steps, dtype=np.float64)
    elif acqsexp1_val > 0 and info.pay_stcd != 3:
        deduction = acqsexp1_val * np.maximum(84 - ctr_mm, 0) / 84
        ltrmnat_tmrfnd = np.maximum(0, aply_prem_acumamt_bnft - deduction)
    else:
        ltrmnat_tmrfnd = np.maximum(0, aply_prem_acumamt_bnft)

    return {
        "soff_bf_tmrfnd": soff_bf_tmrfnd,
        "soff_af_tmrfnd": soff_af_tmrfnd,
        "ltrmnat_tmrfnd": ltrmnat_tmrfnd,
    }


# ---------------------------------------------------------------------------
# STEP 7: 약관대출 (LOAN)
# ---------------------------------------------------------------------------

def _calc_loan(info: ContractInfo, n_steps: int,
               aply_pubano_inrt: np.ndarray) -> dict:
    """약관대출 관련 필드 산출.

    검증된 공식 (9038건 기준):
      LOAN_REMAMT[0] = CTR_LOAN_REMAMT (초기 대출잔액)
      LOAN_REMAMT[t>=1] = 0 (즉시 상환)
      LOAN_INT[1] = REMAMT[0]/2 × ((1+PUBANO[1])^(1/12)-1)
      LOAN_RPAY_HAFWAY[1] = REMAMT[0]
    """
    loan_remamt = np.zeros(n_steps, dtype=np.float64)
    loan_int = np.zeros(n_steps, dtype=np.float64)
    loan_rpay = np.zeros(n_steps, dtype=np.float64)

    rem0 = info.ctr_loan_remamt
    # CTR_LOAN_TPCD=0 (약관대출 불가 상품) → LOAN 미처리
    if rem0 == 0 or n_steps < 2 or info.ctr_loan_tpcd == 0:
        return {"loan_int": loan_int, "loan_remamt": loan_remamt,
                "loan_rpay_hafway": loan_rpay}

    loan_remamt[0] = rem0
    # t=1: 즉시 전액 상환
    pub1 = aply_pubano_inrt[1] if 1 < len(aply_pubano_inrt) else 0.0
    monthly_rate = (1 + pub1) ** (1.0 / 12) - 1
    loan_int[1] = rem0 / 2 * monthly_rate
    loan_rpay[1] = rem0

    return {"loan_int": loan_int, "loan_remamt": loan_remamt,
            "loan_rpay_hafway": loan_rpay}


# ---------------------------------------------------------------------------
# STEP 3: 이율 배열 (PUBANO_INRT / LWST_GRNT_INRT)
# → _build_pubano_inrt_arr, _build_lwst_grnt_inrt_arr 는 상단에 정의
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 통합 함수
# ---------------------------------------------------------------------------

def compute_trad_pv(info: ContractInfo, n_steps: int,
                    pay_trmo: Optional[np.ndarray] = None,
                    ctr_trmo: Optional[np.ndarray] = None,
                    ctr_trme: Optional[np.ndarray] = None,
                    fast_mode: bool = False) -> TradPVResult:
    """단건 OD_TRAD_PV 전체 산출.

    계산 흐름 (의존성 순서):
      STEP 1: 보험료 (시간축, 납입여부, 원수/할인/적립보험료)
      STEP 2: 미경과보험료 PRPD (KICS 산출에 선행)
      STEP 3: 이율 배열 (적립금 부리에 선행)
      STEP 4: 적립금 (BAS 보간 / NoBAS 이율부리)
      STEP 5: 환급금 (SOFF_BF, SOFF_AF, LTRMNAT)
      STEP 6: KICS = (SOFF_AF + PRPD_PREM) × CTR_TRME
      STEP 7: 약관대출

    Args:
        info: 계약 정보 (ContractInfo)
        n_steps: 프로젝션 시점 수
        pay_trmo: PAY_TRMO_MTNPSN_CNT 배열 (NoBAS ADINT 비율 보정용)
        ctr_trmo: CTR_TRMO_MTNPSN_CNT 배열
        ctr_trme: CTR_TRME_MTNPSN_CNT 배열 (KICS 산출용)
        fast_mode: True=성능 우선 (배열 공유, 결과 읽기전용 취급)
    """
    # 공유 제로 배열: fast_mode에서는 단일 인스턴스 참조 공유
    zeros = np.zeros(n_steps, dtype=np.float64)

    # STEP 1: 보험료
    prem = _calc_premium(info, n_steps)
    ctr_mm = prem["ctr_mm"]
    prem_pay_yn = prem["prem_pay_yn"]

    # STEP 2: 미경과보험료 (PRPD) — KICS에 선행
    prpd_mmcnt = _calc_prpd_mmcnt(info, n_steps, ctr_mm, prem_pay_yn)
    prpd_prem = _calc_prpd_prem(info, n_steps, ctr_mm, prem_pay_yn, prem["orig_prem"])
    acum_nprem_prpd = _calc_prpd_acum(info, n_steps)

    # STEP 3: 이율 배열 — 적립금 부리에 선행
    pubano_inrt = _build_pubano_inrt_arr(info, n_steps)
    lwst_grnt_inrt = _build_lwst_grnt_inrt_arr(info, n_steps)

    # STEP 4: 적립금
    acum = _calc_accumulation(
        info, n_steps, ctr_mm, prem_pay_yn, prem["nprem_val"],
        pubano_inrt=pubano_inrt, lwst_grnt_inrt=lwst_grnt_inrt,
        pay_trmo=pay_trmo, ctr_trmo=ctr_trmo,
    )

    # STEP 5: 환급금
    surr = _calc_surrender(info, n_steps, ctr_mm, prem_pay_yn,
                           acum["aply_prem_acumamt_bnft"])

    # STEP 6: KICS = (SOFF_AF + PRPD_PREM) × CTR_TRME
    if ctr_trme is not None:
        cncttp_kics = (surr["soff_af_tmrfnd"] + prpd_prem) * ctr_trme[:n_steps]
    else:
        cncttp_kics = zeros if fast_mode else zeros.copy()

    # STEP 7: 약관대출
    loan = _calc_loan(info, n_steps, pubano_inrt)

    # fast_mode: 배열 복사 생략 (읽기전용 취급, 16개 zero copy + 2개 데이터 copy 절약)
    acumamt_bnft = acum["aply_prem_acumamt_bnft"]

    return TradPVResult(
        n_steps=n_steps,
        # STEP 1: 보험료
        ctr_mm=ctr_mm,
        prem_pay_yn=prem_pay_yn,
        orig_prem=prem["orig_prem"],
        dc_prem=prem["dc_prem"],
        acum_nprem=prem["acum_nprem"],
        pad_prem=prem["pad_prem"],
        acqsexp1_bizexp=prem["acqsexp1_bizexp"],
        # STEP 2: 미경과보험료
        acum_nprem_prpd=acum_nprem_prpd,
        prpd_mmcnt=prpd_mmcnt,
        prpd_prem=prpd_prem,
        # STEP 3: 이율
        aply_pubano_inrt=pubano_inrt,
        # STEP 4: 적립금
        ystr_rsvamt=acum["ystr_rsvamt"],
        yyend_rsvamt=acum["yyend_rsvamt"],
        aply_prem_acumamt_bnft=acumamt_bnft,
        aply_prem_acumamt_exp=acumamt_bnft if fast_mode else acumamt_bnft.copy(),
        aply_adint_tgt_amt=acum["aply_adint_tgt_amt"],
        lwst_adint_tgt_amt=acum["lwst_adint_tgt_amt"],
        lwst_prem_acumamt=acum["lwst_prem_acumamt"],
        # STEP 5: 환급금
        soff_bf_tmrfnd=surr["soff_bf_tmrfnd"],
        soff_af_tmrfnd=surr["soff_af_tmrfnd"],
        ltrmnat_tmrfnd=surr["ltrmnat_tmrfnd"],
        # STEP 6: KICS
        cncttp_acumamt_kics=cncttp_kics,
        # STEP 7: 약관대출
        loan_int=loan["loan_int"],
        loan_remamt=loan["loan_remamt"],
        loan_rpay_hafway=loan["loan_rpay_hafway"],
        # 0-컬럼 (미구현) — fast_mode: 단일 zeros 참조 공유
        add_accmpt_gprem=zeros if fast_mode else zeros.copy(),
        add_accmpt_nprem=zeros if fast_mode else zeros.copy(),
        acqsexp2_bizexp=zeros if fast_mode else zeros.copy(),
        afpay_mntexp=zeros if fast_mode else zeros.copy(),
        lumpay_bizexp=zeros if fast_mode else zeros.copy(),
        pay_grcpr_acqsexp=zeros if fast_mode else zeros.copy(),
        pens_inrt=zeros if fast_mode else zeros.copy(),
        pens_defry_rt=zeros if fast_mode else zeros.copy(),
        pens_annual_sum=zeros if fast_mode else zeros.copy(),
        hafway_wdamt=zeros if fast_mode else zeros.copy(),
        hafway_wdamt_add=zeros if fast_mode else zeros.copy(),
        soff_bf_tmrfnd_add=zeros if fast_mode else zeros.copy(),
        soff_af_tmrfnd_add=zeros if fast_mode else zeros.copy(),
        loan_new=zeros if fast_mode else zeros.copy(),
        loan_rpay_matu=zeros if fast_mode else zeros.copy(),
        matu_maint_bns_acum_amt=zeros if fast_mode else zeros.copy(),
    )


# ---------------------------------------------------------------------------
# STEP 4 내부: 이율 기반 부리 (BAS 미보유 계약용)
# ---------------------------------------------------------------------------

def _compute_acum_interest_based(
    V: float, nprem: float,
    lwst_inrt_arr: np.ndarray, n_steps: int,
    elapsed_mm: int, pterm_mm: int,
    pay_stcd: int,
    pubano_inrt_arr: np.ndarray,
    pay_trmo: Optional[np.ndarray] = None,
    ctr_trmo: Optional[np.ndarray] = None,
    nprem_old: Optional[float] = None,
    amort_mm: int = 0,
    prem_pay_yn: Optional[np.ndarray] = None,
) -> tuple:
    """이율 기반 적립금 계산 (BAS 미보유 계약).

    검증된 ACUM 공식:
      - SETL=0: ADINT = V, ACUM = V
      - SETL>0: ADINT[t] = prev_base + NPREM × (PAY_TRMO[t]/CTR_TRMO[t])
                cum_int += ADINT[t] * INRT[t] / 12
                ACUM[t] = ADINT[t] + cum_int
      - 연도 경계 (CTR_MM % 12 == 1): prev_base = ACUM[t-1], cum_int = 0
      - 연도 내: prev_base = ADINT[t-1]

    pay_trmo/ctr_trmo 미제공 시 비율=1.0 (선형 근사) 폴백.

    Returns (aply_acum, lwst_acum, adint_aply, adint_lwst) arrays.
    """
    aply_acum = np.zeros(n_steps, dtype=np.float64)
    lwst_acum = np.zeros(n_steps, dtype=np.float64)
    adint_aply_arr = np.zeros(n_steps, dtype=np.float64)
    adint_lwst_arr = np.zeros(n_steps, dtype=np.float64)

    if n_steps == 0:
        return aply_acum, lwst_acum, adint_aply_arr, adint_lwst_arr

    # t=0 초기값
    adint_aply_arr[0] = V
    adint_lwst_arr[0] = V
    aply_acum[0] = V
    lwst_acum[0] = V

    if n_steps == 1:
        return aply_acum, lwst_acum, adint_aply_arr, adint_lwst_arr

    # V < 0: 부리 없음 (t>=1 모두 상수)
    if V < 0:
        aply_acum[1:] = V
        lwst_acum[1:] = V
        return aply_acum, lwst_acum, adint_aply_arr, adint_lwst_arr

    # --- 사전 계산: P*ratio 배열, 연도경계 마스크, 이율 (벡터화) ---
    ctr_mm_arr = np.arange(n_steps, dtype=np.int64) + elapsed_mm
    is_year_bd = (ctr_mm_arr % 12 == 1)  # 연도 경계 마스크

    # P 배열: nprem × is_pay_month (상각 고려)
    has_ratio = (pay_trmo is not None and ctr_trmo is not None)
    if prem_pay_yn is not None:
        is_pay = (prem_pay_yn[:n_steps] > 0)
    else:
        is_pay = np.ones(n_steps, dtype=bool) if pay_stcd == 1 else np.zeros(n_steps, dtype=bool)
        if pay_stcd == 1:
            is_pay[ctr_mm_arr > pterm_mm] = False

    if nprem_old is not None and amort_mm > 0:
        cur_nprem_arr = np.where(ctr_mm_arr <= amort_mm, nprem_old, nprem)
    else:
        cur_nprem_arr = nprem  # 스칼라 → broadcast

    P_arr = np.where(is_pay, cur_nprem_arr, 0.0)

    # ratio 배열
    if has_ratio:
        safe_ctr = np.where(ctr_trmo[:n_steps] > 0, ctr_trmo[:n_steps], 1.0)
        ratio_arr = np.where(ctr_trmo[:n_steps] > 0,
                             pay_trmo[:n_steps] / safe_ctr,
                             np.where(is_pay, 1.0, 0.0))
    else:
        ratio_arr = np.where(is_pay, 1.0, 0.0)

    PR = P_arr * ratio_arr  # 사전 계산된 P*ratio

    inrt_aply = pubano_inrt_arr[:n_steps] / 12.0
    inrt_lwst = lwst_inrt_arr[:n_steps] / 12.0

    # --- 순차 루프 (사전 계산 값 참조, 최소 오버헤드) ---
    cum_int_a = 0.0
    cum_int_l = 0.0

    for t in range(1, n_steps):
        pr = PR[t]
        if is_year_bd[t]:
            base_a = aply_acum[t - 1]
            base_l = lwst_acum[t - 1]
            cum_int_a = 0.0
            cum_int_l = 0.0
        else:
            base_a = adint_aply_arr[t - 1]
            base_l = adint_lwst_arr[t - 1]

        ad_a = base_a + pr
        ad_l = base_l + pr
        adint_aply_arr[t] = ad_a
        adint_lwst_arr[t] = ad_l

        cum_int_a += ad_a * inrt_aply[t]
        cum_int_l += ad_l * inrt_lwst[t]
        aply_acum[t] = ad_a + cum_int_a
        lwst_acum[t] = ad_l + cum_int_l

    return aply_acum, lwst_acum, adint_aply_arr, adint_lwst_arr


# ---------------------------------------------------------------------------
# CTR_POLNO 단위 SOFF_AF netting (후처리)
# ---------------------------------------------------------------------------

def apply_soff_af_netting(
    results: dict,
    polno_to_idnos: dict,
    ctr_trme_map: Optional[dict] = None,
    idno_to_cov: Optional[dict] = None,
) -> None:
    """CTR_POLNO 그룹 내 SOFF_AF netting (in-place).

    규칙 (시점 t에서 그룹 SOFF_BF 합 < 0일 때):
      - 주계약(CLA00500): AF = 0 (그룹 합으로 상계)
      - 특약: AF = max(0, BF) (개별 floor)
    합 >= 0이면 전 계약 AF = BF (변동 없음).
    CNCTTP_ACUMAMT_KICS도 재산출.

    Args:
        results: {idno: TradPVResult} — 개별 계산 완료된 결과
        polno_to_idnos: {ctr_polno: [idno, ...]} — CTR_POLNO 역매핑
        ctr_trme_map: {idno: np.ndarray} — CTR_TRME 배열 (KICS 재산출용)
        idno_to_cov: {idno: cov_cd} — COV_CD 매핑 (주계약 식별용)
    """
    for polno, idno_list in polno_to_idnos.items():
        group = [(i, results[i]) for i in idno_list if i in results]
        if len(group) <= 1:
            continue

        max_t = max(r.n_steps for _, r in group)
        # 시점별 SOFF_BF 합산
        bf_sum = np.zeros(max_t, dtype=np.float64)
        for _, r in group:
            bf_sum[:r.n_steps] += r.soff_bf_tmrfnd

        neg_mask = bf_sum < 0  # 합산 음수 시점

        for idno, r in group:
            n = r.n_steps
            mask = neg_mask[:n]
            if not np.any(mask):
                continue

            is_main = (idno_to_cov or {}).get(idno) == "CLA00500"
            if is_main:
                # 주계약: 합산 음수 시점 → AF = 0
                r.soff_af_tmrfnd[mask] = 0.0
            else:
                # 특약: 합산 음수 시점 → AF = max(0, BF)
                r.soff_af_tmrfnd[mask] = np.maximum(0.0, r.soff_bf_tmrfnd[mask])

            # CNCTTP_ACUMAMT_KICS 재산출: (AF + PRPD_PREM) × TRME
            if ctr_trme_map and idno in ctr_trme_map:
                trme = ctr_trme_map[idno]
                r.cncttp_acumamt_kics[:] = (r.soff_af_tmrfnd + r.prpd_prem) * trme[:n]

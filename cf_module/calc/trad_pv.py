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
    loan_params: Optional[dict] = None    # IA_A_CTR_LOAN 약관대출 가정


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
            "YSTR_RSVAMT_TRM": self.ystr_rsvamt.copy(),
            "YYEND_RSVAMT_TRM": self.yyend_rsvamt.copy(),
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
# STEP 3: 보험료 산출
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
        # CTR_MM <= amort_mm: old NPREM, 이후: new NPREM
        acum_nprem = np.empty(n_steps, dtype=np.float64)
        amort_mm = info.amort_mm
        for t in range(n_steps):
            cm = int(ctr_mm[t])
            if amort_mm > 0 and cm <= amort_mm:
                acum_nprem[t] = prem_pay_yn[t] * nprem_old
            else:
                acum_nprem[t] = prem_pay_yn[t] * nprem_val

    # 기납입보험료 (누적)
    pad_prem = np.empty(n_steps, dtype=np.float64)
    cyc = info.paycyc if info.paycyc > 0 else 1
    if info.paycyc == 0:
        # 일시납: 1회 납입
        pad_prem[0] = info.gprem
    else:
        # 납입횟수 = (min(CTR_MM, PTERM_MM) - 1) // paycyc + 1
        cm0 = int(ctr_mm[0])
        paid_mm = min(cm0, pterm_mm) if info.pay_stcd == 1 else cm0
        pay_count = (paid_mm - 1) // cyc + 1 if paid_mm > 0 else 0
        pad_prem[0] = info.gprem * pay_count
    for t in range(1, n_steps):
        pad_prem[t] = pad_prem[t - 1] + info.gprem * prem_pay_yn[t]

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
# STEP 4-5: 급부/사업비 → 적립금 (ACUM)
# ---------------------------------------------------------------------------

def _calc_accumulation(info: ContractInfo, n_steps: int,
                       ctr_mm: np.ndarray, prem_pay_yn: np.ndarray,
                       nprem_val: float,
                       pay_trmo: Optional[np.ndarray] = None,
                       ctr_trmo: Optional[np.ndarray] = None) -> dict:
    """적립금(APLY_PREM_ACUMAMT_BNFT) 산출.

    BAS 보유: 연시/연말 선형보간
    BAS 미보유: 이율 기반 부리 (PAY_TRMO/CTR_TRMO 비율 보정)

    Returns dict with: ystr_rsvamt, yyend_rsvamt, aply_prem_acumamt_bnft, nobas_extra
    """
    has_bas = info.bas is not None
    mult = (info.join_amt / info.bas["crit_join_amt"]
            if has_bas and info.bas["crit_join_amt"] else 1.0) if has_bas else 1.0

    ystr_rsvamt = np.zeros(n_steps, dtype=np.float64)
    yyend_rsvamt = np.zeros(n_steps, dtype=np.float64)
    aply_prem_acumamt_bnft = np.zeros(n_steps, dtype=np.float64)

    # BAS 미보유: 이율 기반 ACUM (pubano_inrt_arr 필요)
    acum_nobas = None
    nobas_extra = None
    if not has_bas and info.acum_cov and info.accmpt_rspb_rsvamt:
        elapsed = info.pass_yy * 12 + info.pass_mm
        pterm_mm = info.pterm_yy * 12

        # PUBANO_INRT / LWST_GRNT_INRT 배열 사전 산출
        lwst_arr = _build_lwst_grnt_inrt_arr(info, n_steps)
        pubano_arr = _build_pubano_inrt_arr(info, n_steps)
        # LWST=0인 시점은 최저보증 없음 → PUBANO 그대로 사용
        lwst_for_acum = np.where(lwst_arr > 0, lwst_arr, pubano_arr)

        acum_nobas, lwst_acum, adint_aply, adint_lwst = _compute_acum_interest_based(
            info.accmpt_rspb_rsvamt, nprem_val,
            lwst_for_acum, n_steps,
            elapsed, pterm_mm, info.pay_stcd,
            pubano_inrt_arr=pubano_arr,
            pay_trmo=pay_trmo,
            ctr_trmo=ctr_trmo,
            nprem_old=info.acum_nprem_old,
            amort_mm=info.amort_mm,
            prem_pay_yn=prem_pay_yn,
        )
        nobas_extra = {
            "adint_aply": adint_aply,
            "adint_lwst": adint_lwst,
            "lwst_acum": lwst_acum,
            "pubano_arr": pubano_arr,
        }

    for t in range(n_steps):
        cm = int(ctr_mm[t])
        ins_year = (cm - 1) // 12 + 1
        month_in_year = cm - (ins_year - 1) * 12

        if has_bas:
            yr_idx = ins_year - 1
            if yr_idx < 120:
                ystr_v = info.bas["ystr"][yr_idx] * mult
                yyend_v = info.bas["yyend"][yr_idx] * mult
            else:
                ystr_v = 0.0
                yyend_v = 0.0
            ystr_rsvamt[t] = ystr_v
            yyend_rsvamt[t] = yyend_v
            interp = ystr_v + (yyend_v - ystr_v) * month_in_year / 12
        else:
            interp = acum_nobas[t] if acum_nobas is not None else 0.0

        aply_prem_acumamt_bnft[t] = interp

    return {
        "ystr_rsvamt": ystr_rsvamt,
        "yyend_rsvamt": yyend_rsvamt,
        "aply_prem_acumamt_bnft": aply_prem_acumamt_bnft,
        "nobas_extra": nobas_extra,
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
        # 경과년수 기반 변동
        elapsed_mm = info.pass_yy * 12 + info.pass_mm
        for t in range(n_steps):
            ctr_yy = (elapsed_mm + t) // 12  # 경과년수
            if yy1 > 0 and ctr_yy < yy1:
                arr[t] = lwst1
            elif yy2 > 0 and ctr_yy < yy2:
                arr[t] = lwst2
            else:
                arr[t] = lwst3 if lwst3 else lwst2

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

    # SETL>=1: DB 공식 (PASS_PRD_NO=t → dc_curve[t-1])
    n_dc = len(dc_curve)
    for t in range(1, n_steps):
        idx = min(t - 1, n_dc - 1) if n_dc > 0 else -1
        if idx >= 0:
            dc = dc_curve[idx]
            arr[t] = (ew * ei + (dc - iv) * (1 - ew)) * adj

    return np.maximum(arr, lwst_arr)


# ---------------------------------------------------------------------------
# STEP 5b: 미경과보험료 (PRPD)
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
# STEP 6: 환급금 (SOFF / LTRMNAT)
# ---------------------------------------------------------------------------

def _calc_surrender(info: ContractInfo, n_steps: int,
                    ctr_mm: np.ndarray, prem_pay_yn: np.ndarray,
                    aply_prem_acumamt_bnft: np.ndarray) -> dict:
    """환급금(SOFF, LTRMNAT) 산출.

    Returns dict with: soff_bf_tmrfnd, soff_af_tmrfnd, ltrmnat_tmrfnd
    """
    prod_cd = info.prod_cd
    ctr_tpcd = str(info.ctr_tpcd)
    cls_cd = str(info.cls_cd)
    acqsexp1_val = info.acqsexp1

    # ACQSEXP 차감 판정
    apply_deduction = (
        (prod_cd in SOFF_DEDUCT_PRODS and info.pterm_yy > 5)
        or (ctr_tpcd == "0" and acqsexp1_val > 0)
    )

    # SOFF 납입중 비율
    is_tmrfnd_prod = prod_cd in ("LA0217Y",)
    if ctr_tpcd == "3":
        soff_pay_rate = 0.3
    elif ctr_tpcd == "5":
        soff_pay_rate = 0.5
    elif is_tmrfnd_prod and ctr_tpcd == "1" and cls_cd in ("01", "02"):
        soff_pay_rate = 0.0
    else:
        soff_pay_rate = 1.0

    soff_bf_tmrfnd = np.zeros(n_steps, dtype=np.float64)
    for t in range(n_steps):
        cm = int(ctr_mm[t])
        rate = soff_pay_rate if prem_pay_yn[t] > 0 else 1.0
        soff_val = aply_prem_acumamt_bnft[t] * rate
        if apply_deduction:
            remaining_84 = max(84 - cm, 0)
            soff_val -= acqsexp1_val * remaining_84 / 84
        soff_bf_tmrfnd[t] = soff_val

    soff_af_tmrfnd = soff_bf_tmrfnd.copy()

    # LTRMNAT: CTR_TPCD='9' → 0, else → max(0, ACUM - deduction)
    if ctr_tpcd == "9":
        ltrmnat_tmrfnd = np.zeros(n_steps, dtype=np.float64)
    else:
        deduction = acqsexp1_val * np.maximum(84 - ctr_mm, 0) / 84
        ltrmnat_tmrfnd = np.maximum(0, aply_prem_acumamt_bnft - deduction)

    return {
        "soff_bf_tmrfnd": soff_bf_tmrfnd,
        "soff_af_tmrfnd": soff_af_tmrfnd,
        "ltrmnat_tmrfnd": ltrmnat_tmrfnd,
    }


# ---------------------------------------------------------------------------
# STEP 7: 이율 / 할인 (Phase 2 — 미구현)
# ---------------------------------------------------------------------------

def _calc_interest(info: ContractInfo, n_steps: int,
                   ctr_mm: np.ndarray,
                   nobas_extra: Optional[dict] = None) -> dict:
    """이율 관련 필드 산출.

    APLY_PUBANO_INRT: IP_P_ACUM_COV 대상만 DB 공식 기반 계산.
      공식: (EXT_WGHT*EXT_ITR + (DC_RT-IV_ADEXP_RT)*(1-EXT_WGHT)) * ADJ_RT
      SETL=0: inrt_lookup.json 캐시 (현재 적용 이율)
      SETL>=1: IE_PUBANO_INRT × IE_DC_RT 공식
    IP_P_ACUM_COV 미대상: 0 (계산 스킵)
    """
    zeros = np.zeros(n_steps, dtype=np.float64)

    # APLY_PUBANO_INRT: NoBAS에서 이미 산출된 배열 재활용, 아니면 새로 산출
    if nobas_extra and "pubano_arr" in nobas_extra:
        aply_pubano_inrt = nobas_extra["pubano_arr"]
    else:
        aply_pubano_inrt = _build_pubano_inrt_arr(info, n_steps)

    # NoBAS: ADINT / LWST
    if nobas_extra:
        aply_adint_tgt_amt = nobas_extra["adint_aply"]
        lwst_adint_tgt_amt = nobas_extra["adint_lwst"]
        lwst_prem_acumamt = nobas_extra["lwst_acum"]
    else:
        aply_adint_tgt_amt = zeros.copy()
        lwst_adint_tgt_amt = zeros.copy()
        lwst_prem_acumamt = zeros.copy()

    # KICS: compute_trad_pv에서 SOFF_AF × CTR_TRME로 산출
    cncttp_acumamt_kics = zeros.copy()  # placeholder

    return {
        "aply_pubano_inrt": aply_pubano_inrt,
        "aply_adint_tgt_amt": aply_adint_tgt_amt,
        "lwst_adint_tgt_amt": lwst_adint_tgt_amt,
        "lwst_prem_acumamt": lwst_prem_acumamt,
        "cncttp_acumamt_kics": cncttp_acumamt_kics,
    }


# ---------------------------------------------------------------------------
# STEP 7b: 약관대출 (LOAN)
# ---------------------------------------------------------------------------

def _calc_loan(info: ContractInfo, n_steps: int,
               cncttp_acumamt_kics: np.ndarray,
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
    if rem0 == 0 or n_steps < 2:
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
# STEP 8: PV (Phase 3 — 미구현)
# ---------------------------------------------------------------------------

# PV 단계는 MN tpx, BN 급부금, 할인율과 결합하여 산출.
# 현재는 개별 CF 항목만 산출하며, PV 합산은 상위 모듈에서 수행.


# ---------------------------------------------------------------------------
# 통합 함수
# ---------------------------------------------------------------------------

def compute_trad_pv(info: ContractInfo, n_steps: int,
                    pay_trmo: Optional[np.ndarray] = None,
                    ctr_trmo: Optional[np.ndarray] = None,
                    ctr_trme: Optional[np.ndarray] = None) -> TradPVResult:
    """단건 OD_TRAD_PV 전체 산출.

    Args:
        info: 계약 정보 (ContractInfo)
        n_steps: 프로젝션 시점 수
        pay_trmo: PAY_TRMO_MTNPSN_CNT 배열 (NoBAS ADINT 비율 보정용)
        ctr_trmo: CTR_TRMO_MTNPSN_CNT 배열
        ctr_trme: CTR_TRME_MTNPSN_CNT 배열 (KICS 산출용)

    Returns:
        TradPVResult
    """
    zeros = np.zeros(n_steps, dtype=np.float64)

    # STEP 3: 보험료
    prem = _calc_premium(info, n_steps)

    # STEP 4-5: 적립금
    acum = _calc_accumulation(
        info, n_steps,
        prem["ctr_mm"], prem["prem_pay_yn"], prem["nprem_val"],
        pay_trmo=pay_trmo, ctr_trmo=ctr_trmo,
    )

    # STEP 6: 환급금
    surr = _calc_surrender(
        info, n_steps,
        prem["ctr_mm"], prem["prem_pay_yn"],
        acum["aply_prem_acumamt_bnft"],
    )

    # STEP 7: 이율
    inrt = _calc_interest(info, n_steps, prem["ctr_mm"],
                          nobas_extra=acum.get("nobas_extra"))

    # KICS = SOFF_AF_TMRFND × CTR_TRME
    if ctr_trme is not None:
        cncttp_kics = surr["soff_af_tmrfnd"] * ctr_trme[:n_steps]
    else:
        cncttp_kics = inrt["cncttp_acumamt_kics"]

    # LOAN
    loan = _calc_loan(info, n_steps, cncttp_kics, inrt["aply_pubano_inrt"])

    return TradPVResult(
        n_steps=n_steps,
        # 보험료
        ctr_mm=prem["ctr_mm"],
        prem_pay_yn=prem["prem_pay_yn"],
        orig_prem=prem["orig_prem"],
        dc_prem=prem["dc_prem"],
        acum_nprem=prem["acum_nprem"],
        pad_prem=prem["pad_prem"],
        acqsexp1_bizexp=prem["acqsexp1_bizexp"],
        # 준비금
        ystr_rsvamt=acum["ystr_rsvamt"],
        yyend_rsvamt=acum["yyend_rsvamt"],
        # 적립금
        aply_prem_acumamt_bnft=acum["aply_prem_acumamt_bnft"],
        aply_prem_acumamt_exp=acum["aply_prem_acumamt_bnft"].copy(),
        # 환급금
        soff_bf_tmrfnd=surr["soff_bf_tmrfnd"],
        soff_af_tmrfnd=surr["soff_af_tmrfnd"],
        ltrmnat_tmrfnd=surr["ltrmnat_tmrfnd"],
        # 이율
        aply_pubano_inrt=inrt["aply_pubano_inrt"],
        aply_adint_tgt_amt=inrt["aply_adint_tgt_amt"],
        lwst_adint_tgt_amt=inrt["lwst_adint_tgt_amt"],
        lwst_prem_acumamt=inrt["lwst_prem_acumamt"],
        cncttp_acumamt_kics=cncttp_kics,
        # 약관대출
        loan_int=loan["loan_int"],
        loan_remamt=loan["loan_remamt"],
        loan_rpay_hafway=loan["loan_rpay_hafway"],
        # PRPD (미경과보험료)
        acum_nprem_prpd=_calc_prpd_acum(info, n_steps),
        prpd_mmcnt=_calc_prpd_mmcnt(info, n_steps, prem["ctr_mm"], prem["prem_pay_yn"]),
        prpd_prem=_calc_prpd_prem(info, n_steps, prem["ctr_mm"], prem["prem_pay_yn"], prem["orig_prem"]),
        add_accmpt_gprem=zeros.copy(),
        add_accmpt_nprem=zeros.copy(),
        acqsexp2_bizexp=zeros.copy(),
        afpay_mntexp=zeros.copy(),
        lumpay_bizexp=zeros.copy(),
        pay_grcpr_acqsexp=zeros.copy(),
        pens_inrt=zeros.copy(),
        pens_defry_rt=zeros.copy(),
        pens_annual_sum=zeros.copy(),
        hafway_wdamt=zeros.copy(),
        hafway_wdamt_add=zeros.copy(),
        soff_bf_tmrfnd_add=zeros.copy(),
        soff_af_tmrfnd_add=zeros.copy(),
        loan_new=zeros.copy(),
        loan_rpay_matu=zeros.copy(),
        matu_maint_bns_acum_amt=zeros.copy(),
    )


# ---------------------------------------------------------------------------
# 이율 기반 부리 (BAS 미보유 계약용)
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

    has_ratio = (pay_trmo is not None and ctr_trmo is not None)
    cum_int_aply = 0.0
    cum_int_lwst = 0.0

    for t in range(n_steps):
        ctr_mm = elapsed_mm + t

        # 실제 납입월인지 판정 (prem_pay_yn 배열 우선, 없으면 월납 가정)
        if prem_pay_yn is not None and t < len(prem_pay_yn):
            is_pay_month = prem_pay_yn[t] > 0
        else:
            is_pay_month = (pay_stcd == 1) and (ctr_mm <= pterm_mm)
        # CTR_MM <= amort_mm이면 old prem, 이후 new prem
        cur_nprem = nprem_old if (nprem_old is not None and amort_mm > 0 and ctr_mm <= amort_mm) else nprem
        P = cur_nprem if is_pay_month else 0.0

        # 납입자/유지자 비율
        if has_ratio and t < len(ctr_trmo) and ctr_trmo[t] > 0:
            ratio = pay_trmo[t] / ctr_trmo[t]
        else:
            ratio = 1.0 if is_pay_month else 0.0

        inrt_aply = pubano_inrt_arr[t] if t < len(pubano_inrt_arr) else 0.0
        inrt_lwst = lwst_inrt_arr[t] if t < len(lwst_inrt_arr) else 0.0

        if t == 0:
            adint_aply_arr[t] = V
            adint_lwst_arr[t] = V
            aply_acum[t] = V
            lwst_acum[t] = V
        elif V < 0:
            # 음수 적립금: 부리 없음, t>=1 ADINT=0, ACUM=V 고정
            adint_aply_arr[t] = 0.0
            adint_lwst_arr[t] = 0.0
            aply_acum[t] = V
            lwst_acum[t] = V
        else:
            # 연도 경계: CTR_MM % 12 == 1
            is_year_boundary = (ctr_mm % 12 == 1)
            if is_year_boundary:
                base_aply = aply_acum[t - 1]
                base_lwst = lwst_acum[t - 1]
                cum_int_aply = 0.0
                cum_int_lwst = 0.0
            else:
                base_aply = adint_aply_arr[t - 1]
                base_lwst = adint_lwst_arr[t - 1]

            adint_aply = base_aply + P * ratio
            adint_lwst = base_lwst + P * ratio

            adint_aply_arr[t] = adint_aply
            adint_lwst_arr[t] = adint_lwst

            cum_int_aply += adint_aply * inrt_aply / 12
            cum_int_lwst += adint_lwst * inrt_lwst / 12
            aply_acum[t] = adint_aply + cum_int_aply
            lwst_acum[t] = adint_lwst + cum_int_lwst

    return aply_acum, lwst_acum, adint_aply_arr, adint_lwst_arr

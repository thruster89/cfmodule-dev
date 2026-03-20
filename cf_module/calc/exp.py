"""OD_EXP 사업비 산출.

EXP_VAL[t] = driver에 따른 기초금액 × rate[t] × 물가상승 보정
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

from cf_module.constants import (
    BASE_YYYYMM, EXP_DEFAULT_EPRD, EXP_DEFAULT_EYM, ExpDrvr,
)
from cf_module.data.exp_loader import ExpDataCache


@dataclass
class ExpResult:
    """단일 사업비 항목 결과."""
    tpcd: str       # ACQS / MNT / LSVY
    kdcd: int       # EXP_KDCD
    d_ind: int      # D_IND_EXP_DVCD (0=indirect, 1=direct)
    values: np.ndarray  # (n_steps,) EXP_VAL


def compute_exp(
    n_steps: int,
    elapsed_mm: int,
    gprem: float,
    exp_items: List[Tuple[str, int, dict]],
    cache: ExpDataCache,
    *,
    pterm_mm: int = 999999,
    loan_remamt: Optional[np.ndarray] = None,
    cncttp_kics: Optional[np.ndarray] = None,
    bnft_insuamt: Optional[np.ndarray] = None,
    base_yyyymm: int = BASE_YYYYMM,
) -> List[ExpResult]:
    """사업비 산출.

    Args:
        n_steps: 프로젝션 스텝 수
        elapsed_mm: 경과월 (pass_yy*12 + pass_mm)
        gprem: 총보험료
        exp_items: [(tpcd, kdcd, item_dict)] from ExpDataCache.get_exp_items()
        cache: ExpDataCache (monthly_esc 등)
        loan_remamt: TRAD_PV LOAN_REMAMT 배열 (DRVR=6용)
        cncttp_kics: TRAD_PV CNCTTP_ACUMAMT_KICS 배열 (DRVR=9용)
        bnft_insuamt: TBL_BN BNFT_INSUAMT 합계 배열 (DRVR=4용)
        base_yyyymm: 기준연월 (EYM 비교용)

    Returns:
        List[ExpResult]
    """
    results = []

    # 공통 벡터 사전 계산
    steps = np.arange(n_steps)
    t_arr = elapsed_mm + steps  # CTR_AFT_PASS_MMCNT

    # 물가상승 벡터 (step >= 2일 때 적용)
    if cache.monthly_esc > 1.0:
        esc_arr = np.where(steps >= 2, cache.monthly_esc ** (steps - 1), 1.0)
    else:
        esc_arr = None

    for tpcd, kdcd, item in exp_items:
        drvr = item["drvr"]
        prce = item.get("prce", 0)
        d_ind = item.get("dc", 1)
        pay_dvcd = item.get("pay", 0)

        # --- 마스크 구성 (유효 스텝) ---
        mask = np.ones(n_steps, dtype=bool)

        # ACQS: step=0 제외
        if tpcd == "ACQS":
            mask[0] = False

        # PAY_MTNPSN_DVCD=0: pterm까지만
        if pay_dvcd == 0 and tpcd != "LSVY":
            mask &= (t_arr <= pterm_mm)

        # ACQS EPRD 제약
        if tpcd == "ACQS":
            eprd = item.get("eprd", EXP_DEFAULT_EPRD)
            mask &= (t_arr < eprd)

        # MNT EYM 제약
        if tpcd == "MNT" and item.get("eym_yn", 0) == 1:
            eym = item.get("eym", EXP_DEFAULT_EYM)
            # yyyymm 벡터 계산
            base_y = base_yyyymm // 100
            base_m = base_yyyymm % 100
            total_m = base_m + steps
            yyyymm_arr = (base_y + (total_m - 1) // 12) * 100 + (total_m - 1) % 12 + 1
            mask &= (yyyymm_arr <= eym)

        # --- rate 벡터 구성 ---
        rates = item.get("rates")
        if rates is not None:
            rate_idx = np.clip(t_arr - 1, 0, len(rates) - 1)  # 0-based, 초과 시 마지막 값
            rate_arr = rates[rate_idx]
            # t_arr=0 → rate_idx=-1 → clip 0으로 처리됨 (원본에서는 rate=0)
            rate_arr = np.where(t_arr >= 1, rate_arr, 0.0)
        else:
            rate_arr = np.full(n_steps, item.get("rate", 0.0))

        # --- 드라이버별 기초금액 (벡터) ---
        if drvr == ExpDrvr.GPREM_RATE:
            vals = rate_arr * gprem
        elif drvr == ExpDrvr.FIXED_AMOUNT:
            vals = rate_arr.copy()
        elif drvr == ExpDrvr.FIXED_VALUE:
            vals = np.full(n_steps, item.get("rate", 0.0))
        elif drvr == ExpDrvr.LOAN_RATE:
            lv = loan_remamt if loan_remamt is not None else np.zeros(n_steps)
            vals = rate_arr * lv[:n_steps]
        elif drvr == ExpDrvr.CNCTTP_RATE:
            kv = cncttp_kics if cncttp_kics is not None else np.zeros(n_steps)
            vals = rate_arr * kv[:n_steps]
        elif drvr == ExpDrvr.CNCTTP_MINUS_LOAN:
            kv = cncttp_kics if cncttp_kics is not None else np.zeros(n_steps)
            lv = loan_remamt if loan_remamt is not None else np.zeros(n_steps)
            vals = rate_arr * (kv[:n_steps] - lv[:n_steps])
        else:
            vals = rate_arr * gprem  # fallback

        # 물가상승 보정
        if prce == 1 and esc_arr is not None:
            vals = vals * esc_arr

        # 마스크 적용
        vals = vals * mask

        results.append(ExpResult(
            tpcd=tpcd, kdcd=kdcd, d_ind=d_ind, values=vals,
        ))

    return results


def _t_to_yyyymm(base_yyyymm: int, step: int) -> int:
    """기준연월 + step개월 → YYYYMM."""
    y = base_yyyymm // 100
    m = base_yyyymm % 100 + step
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return y * 100 + m

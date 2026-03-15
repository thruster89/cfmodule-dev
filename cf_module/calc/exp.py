"""OD_EXP 사업비 산출.

EXP_VAL[t] = driver에 따른 기초금액 × rate[t] × 물가상승 보정
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

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
    base_yyyymm: int = 202309,
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

    for tpcd, kdcd, item in exp_items:
        vals = np.zeros(n_steps, dtype=np.float64)
        drvr = item["drvr"]
        prce = item.get("prce", 0)

        # D_IND: DC_BF_AF_DVCD=1 → direct(1), DC_BF_AF_DVCD=0 → indirect(0)
        d_ind = item.get("dc", 1)

        # ACQS: step=0 (첫 프로젝션 월) 제외
        start_step = 1 if tpcd == "ACQS" else 0
        pay_dvcd = item.get("pay", 0)

        for step in range(start_step, n_steps):
            t = elapsed_mm + step  # CTR_AFT_PASS_MMCNT

            # PAY_MTNPSN_DVCD=0 (ACQS/MNT): pterm까지만 적용
            if pay_dvcd == 0 and tpcd != "LSVY" and t > pterm_mm:
                continue

            # 시간 제약: ACQS EPRD
            if tpcd == "ACQS":
                eprd = item.get("eprd", 999999)
                if t >= eprd:
                    continue

            # 시간 제약: MNT EYM
            if tpcd == "MNT" and item.get("eym_yn", 0) == 1:
                eym = item.get("eym", 299999)
                yyyymm = _t_to_yyyymm(base_yyyymm, step)
                if yyyymm > eym:
                    continue

            # rate 인덱싱: [t] (1-based → 0-based), 초과 시 마지막 값 연장
            rates = item.get("rates")
            if rates is not None:
                max_idx = len(rates)
                rate_idx = t - 1  # t는 1-based → 0-based
                if rate_idx < 0:
                    rate = 0.0
                elif rate_idx >= max_idx:
                    rate = rates[-1]  # 마지막 값 연장
                else:
                    rate = rates[rate_idx]
            else:
                rate = item.get("rate", 0.0)

            # 기초금액 계산
            if drvr == 1:
                base_val = rate * gprem
            elif drvr == 2:
                base_val = rate  # 절대금액
            elif drvr == 4:
                base_val = item.get("rate", 0.0)  # LSVY 고정값
            elif drvr == 6:
                lv = loan_remamt[step] if loan_remamt is not None and step < len(loan_remamt) else 0
                base_val = rate * lv
            elif drvr == 9:
                kv = cncttp_kics[step] if cncttp_kics is not None and step < len(cncttp_kics) else 0
                base_val = rate * kv
            elif drvr == 10:
                kv = cncttp_kics[step] if cncttp_kics is not None and step < len(cncttp_kics) else 0
                lv = loan_remamt[step] if loan_remamt is not None and step < len(loan_remamt) else 0
                base_val = rate * (kv - lv)
            else:
                base_val = rate * gprem  # fallback

            # 물가상승 보정 (PRCE_ASC_RT_APLY_YN=1)
            if prce == 1 and cache.monthly_esc > 1.0 and step >= 2:
                base_val *= cache.monthly_esc ** (step - 1)

            vals[step] = base_val

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

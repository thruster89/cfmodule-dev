"""
보험료 Cash Flow 모듈

수입보험료, 영업보험료, 위험보험료, 저축보험료 등 보험료 관련 CF를 산출한다.
shape: (n_points, n_steps)
"""

from dataclasses import dataclass

import numpy as np

from cf_module.calc.decrement import DecrementResult
from cf_module.calc.timing import TimingResult
from cf_module.data.model_point import ModelPointSet


@dataclass
class PremiumCF:
    """보험료 Cash Flow

    Attributes:
        gross_premium: 영업보험료 (수입보험료)
        net_premium: 순보험료
        risk_premium: 위험보험료
        saving_premium: 저축보험료
        loading: 부가보험료 (사업비)
    """
    gross_premium: np.ndarray
    net_premium: np.ndarray
    risk_premium: np.ndarray
    saving_premium: np.ndarray
    loading: np.ndarray


def calc_premium_cf(
    mp: ModelPointSet,
    timing: TimingResult,
    decrement: DecrementResult,
    net_premium_ratio: float = 0.85,
    risk_premium_ratio: float = 0.60,
) -> PremiumCF:
    """보험료 CF를 산출한다.

    Args:
        mp: Model Point
        timing: 시간축
        decrement: 탈퇴율
        net_premium_ratio: 순보험료/영업보험료 비율 (기본 85%)
        risk_premium_ratio: 위험보험료/순보험료 비율 (기본 60%)

    Returns:
        PremiumCF
    """
    n = mp.n_points
    s = timing.n_steps

    if s == 0:
        empty = np.zeros((n, 0), dtype=np.float64)
        return PremiumCF(
            gross_premium=empty, net_premium=empty,
            risk_premium=empty, saving_premium=empty, loading=empty,
        )

    # 월납 보험료 (영업보험료)
    prem_col = mp.premium[:, np.newaxis]  # (n, 1)

    # 납입기간 마스크: 납입기간 내에만 보험료 수입
    pay_mask = timing.is_pay_period.astype(np.float64)

    # 생존자 기준 보험료: tpx * 보험료 * 납입마스크
    gross_premium = decrement.tpx * prem_col * pay_mask

    # 순보험료
    net_premium = gross_premium * net_premium_ratio

    # 위험보험료
    risk_premium = net_premium * risk_premium_ratio

    # 저축보험료 = 순보험료 - 위험보험료
    saving_premium = net_premium - risk_premium

    # 부가보험료 = 영업보험료 - 순보험료
    loading = gross_premium - net_premium

    return PremiumCF(
        gross_premium=gross_premium,
        net_premium=net_premium,
        risk_premium=risk_premium,
        saving_premium=saving_premium,
        loading=loading,
    )

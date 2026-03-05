"""
급부(Benefit) Cash Flow 모듈

사망보험금, 만기보험금, 생존급부, 배당 등 급부 관련 CF를 산출한다.
shape: (n_points, n_steps)
"""

from dataclasses import dataclass

import numpy as np

from cf_module.calc.decrement import DecrementResult
from cf_module.calc.timing import MONTHS_IN_YEAR, TimingResult
from cf_module.data.model_point import ModelPointSet


@dataclass
class BenefitCF:
    """급부 Cash Flow

    Attributes:
        death_benefit: 사망보험금
        maturity_benefit: 만기보험금
        survival_benefit: 생존급부
        surrender_benefit: 해약환급금
        total_benefit: 총 급부
    """
    death_benefit: np.ndarray
    maturity_benefit: np.ndarray
    survival_benefit: np.ndarray
    surrender_benefit: np.ndarray
    total_benefit: np.ndarray


def calc_benefit_cf(
    mp: ModelPointSet,
    timing: TimingResult,
    decrement: DecrementResult,
    surrender_rate_table: np.ndarray | None = None,
) -> BenefitCF:
    """급부 CF를 산출한다.

    Args:
        mp: Model Point
        timing: 시간축
        decrement: 탈퇴율
        surrender_rate_table: 해약환급률 테이블 (n_points, n_steps) or None

    Returns:
        BenefitCF
    """
    n = mp.n_points
    s = timing.n_steps

    if s == 0:
        empty = np.zeros((n, 0), dtype=np.float64)
        return BenefitCF(
            death_benefit=empty, maturity_benefit=empty,
            survival_benefit=empty, surrender_benefit=empty,
            total_benefit=empty,
        )

    sa_col = mp.sum_assured[:, np.newaxis]  # (n, 1)

    # -- 사망보험금: 사망탈퇴자 × 보험가입금액 --
    death_benefit = decrement.d_death * sa_col

    # -- 만기보험금: 마지막 시점 생존자에게 지급 --
    maturity_benefit = np.zeros((n, s), dtype=np.float64)
    bterm_months = timing.bterm_months[:, np.newaxis]  # (n, 1)
    is_maturity = timing.duration_months == bterm_months
    maturity_benefit = np.where(
        is_maturity,
        decrement.d_survive * sa_col,
        0.0,
    )

    # -- 생존급부: 일정 주기(예: 매년)로 생존자에게 지급 (현재 0) --
    survival_benefit = np.zeros((n, s), dtype=np.float64)

    # -- 해약환급금: 해약탈퇴자 × 해약환급률 × 보험가입금액 --
    if surrender_rate_table is not None:
        surrender_benefit = decrement.d_lapse * surrender_rate_table * sa_col
    else:
        # 기본 해약환급률: 경과연수에 비례하는 단순 모델
        dur_years = timing.duration_years.astype(np.float64)
        bterm_years = (timing.bterm_months / MONTHS_IN_YEAR).astype(np.float64)[:, np.newaxis]
        # 선형 증가: 경과연수/보장연수 (최대 0.9)
        surrender_ratio = np.where(
            bterm_years > 0,
            np.minimum(dur_years / bterm_years, 0.9),
            0.0,
        )
        surrender_benefit = decrement.d_lapse * surrender_ratio * sa_col

    # -- 총 급부 --
    total_benefit = death_benefit + maturity_benefit + survival_benefit + surrender_benefit

    return BenefitCF(
        death_benefit=death_benefit,
        maturity_benefit=maturity_benefit,
        survival_benefit=survival_benefit,
        surrender_benefit=surrender_benefit,
        total_benefit=total_benefit,
    )

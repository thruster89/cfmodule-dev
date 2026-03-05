"""
할인(Discount) 모듈

금리 시나리오에 따른 할인율, 현가(PV) 계산을 수행한다.
shape: (n_points, n_steps)
"""

from dataclasses import dataclass

import numpy as np

from cf_module.calc.timing import MONTHS_IN_YEAR, TimingResult
from cf_module.data.assumptions import InterestRate


@dataclass
class DiscountResult:
    """할인 결과

    Attributes:
        discount_factor: 할인계수 v(t) = 1/(1+i)^t
        forward_rate_monthly: 월별 선도금리
        spot_rate_annual: 연간 현물금리 (보간)
    """
    discount_factor: np.ndarray
    forward_rate_monthly: np.ndarray
    spot_rate_annual: np.ndarray


def build_discount(
    timing: TimingResult,
    interest: InterestRate,
    flat_rate: float | None = None,
    rate_shock: float = 0.0,
) -> DiscountResult:
    """할인 계수를 생성한다.

    Args:
        timing: 시간축
        interest: 금리 커브 (InterestRate)
        flat_rate: 고정 금리 (연율). 지정 시 interest를 무시한다.
        rate_shock: 금리 충격 (bp 단위 가산). 예: 50 → +0.5%

    Returns:
        DiscountResult
    """
    n_points = timing.t.shape[0]
    s = timing.n_steps

    if s == 0:
        empty = np.zeros((n_points, 0), dtype=np.float64)
        return DiscountResult(
            discount_factor=empty,
            forward_rate_monthly=empty,
            spot_rate_annual=empty,
        )

    shock = rate_shock / 10_000.0  # bp → 소수

    # -- 금리 결정 --
    if flat_rate is not None:
        # 고정금리 사용
        annual_rate = flat_rate + shock
        monthly_rate = _annual_to_monthly_rate(annual_rate)
        spot_annual = np.full((n_points, s), annual_rate, dtype=np.float64)
        fwd_monthly = np.full((n_points, s), monthly_rate, dtype=np.float64)
    elif interest.spot_rates.size > 0:
        # 금리 커브 사용
        spot_annual, fwd_monthly = _interpolate_curve(
            interest, s, n_points, shock
        )
    else:
        # 금리 미제공: 0% 가정
        spot_annual = np.zeros((n_points, s), dtype=np.float64)
        fwd_monthly = np.zeros((n_points, s), dtype=np.float64)

    # -- 할인계수 산출 --
    # v(t) = prod(1 / (1 + fwd_monthly[k])) for k=0..t-1
    discount_factor = np.ones((n_points, s), dtype=np.float64)
    if s > 1:
        cum_factor = np.cumprod(1.0 / (1.0 + fwd_monthly[:, :-1]), axis=1)
        discount_factor[:, 1:] = cum_factor

    return DiscountResult(
        discount_factor=discount_factor,
        forward_rate_monthly=fwd_monthly,
        spot_rate_annual=spot_annual,
    )


def calc_present_value(
    cashflow: np.ndarray,
    discount_factor: np.ndarray,
) -> np.ndarray:
    """Cash Flow의 현가(PV)를 산출한다.

    Args:
        cashflow: CF 배열 (n_points, n_steps)
        discount_factor: 할인계수 (n_points, n_steps)

    Returns:
        pv: 현가 (n_points,) - 시점별 CF × 할인계수의 합
    """
    return np.sum(cashflow * discount_factor, axis=1)


def _annual_to_monthly_rate(annual_rate: float | np.ndarray) -> float | np.ndarray:
    """연금리를 월금리로 변환한다. (1+i)^(1/12) - 1"""
    return np.power(1.0 + annual_rate, 1.0 / MONTHS_IN_YEAR) - 1.0


def _interpolate_curve(
    interest: InterestRate,
    n_steps: int,
    n_points: int,
    shock: float,
) -> tuple[np.ndarray, np.ndarray]:
    """금리 커브를 프로젝션 시점에 맞게 보간한다.

    Returns:
        (spot_annual, forward_monthly)  각각 shape (n_points, n_steps)
    """
    term_months = interest.term_months.astype(np.float64)
    spots = interest.spot_rates.astype(np.float64) + shock

    # 월별 시점
    t_months = np.arange(1, n_steps + 1, dtype=np.float64)

    # 선형 보간
    spot_interp = np.interp(t_months, term_months, spots)  # (n_steps,)

    # 선도금리: f(t) = ((1+s(t))^t / (1+s(t-1))^(t-1)) - 1
    spot_annual = np.broadcast_to(spot_interp[np.newaxis, :], (n_points, n_steps)).copy()

    # 월별 선도금리 계산
    cumulative = np.power(1.0 + spot_interp, t_months / MONTHS_IN_YEAR)
    fwd = np.ones(n_steps, dtype=np.float64)
    fwd[0] = cumulative[0] - 1.0
    if n_steps > 1:
        fwd[1:] = cumulative[1:] / cumulative[:-1] - 1.0

    fwd_monthly = np.broadcast_to(fwd[np.newaxis, :], (n_points, n_steps)).copy()

    return spot_annual, fwd_monthly

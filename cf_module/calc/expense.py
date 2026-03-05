"""
사업비(Expense) Cash Flow 모듈

신계약비, 유지비, 수금비 등 사업비 관련 CF를 산출한다.
shape: (n_points, n_steps)
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np

from cf_module.calc.decrement import DecrementResult
from cf_module.calc.timing import TimingResult
from cf_module.data.assumptions import ExpectedExpenseRate, ExpenseTable
from cf_module.data.model_point import ModelPointSet
from cf_module.utils.logger import get_logger

logger = get_logger("expense")


@dataclass
class ExpenseCF:
    """사업비 Cash Flow

    Attributes:
        acquisition: 신계약비 (초년도)
        maintenance: 유지비 (매기)
        collection: 수금비 (보험료 수금 시)
        total_expense: 총 사업비
    """
    acquisition: np.ndarray
    maintenance: np.ndarray
    collection: np.ndarray
    total_expense: np.ndarray


def calc_expense_cf(
    mp: ModelPointSet,
    timing: TimingResult,
    decrement: DecrementResult,
    expense: ExpenseTable,
    acq_rate: float = 0.0,
    maint_rate: float = 0.0,
    collect_rate: float = 0.0,
    expected_expense: Optional[ExpectedExpenseRate] = None,
) -> ExpenseCF:
    """사업비 CF를 산출한다.

    Args:
        mp: Model Point
        timing: 시간축
        decrement: 탈퇴율
        expense: 사업비율 테이블
        acq_rate: 신계약비율 (보험가입금액 대비, 기본값 사용 시)
        maint_rate: 유지비율 (보험가입금액 대비/년)
        collect_rate: 수금비율 (보험료 대비)
        expected_expense: 예정사업비율 (Pricing 모드 전용)

    Returns:
        ExpenseCF
    """
    n = mp.n_points
    s = timing.n_steps

    if s == 0:
        empty = np.zeros((n, 0), dtype=np.float64)
        return ExpenseCF(
            acquisition=empty, maintenance=empty,
            collection=empty, total_expense=empty,
        )

    # Pricing 모드: 예정사업비율 적용
    if expected_expense is not None:
        return _calc_expense_pricing(mp, timing, decrement, expected_expense)

    sa_col = mp.sum_assured[:, np.newaxis]   # (n, 1)
    prem_col = mp.premium[:, np.newaxis]     # (n, 1)

    # -- 신계약비: t=0 시점에만 발생 (1회성) --
    acquisition = np.zeros((n, s), dtype=np.float64)
    acquisition[:, 0] = mp.sum_assured * acq_rate

    # -- 유지비: 매 시점 생존자 기준 --
    # 월별이면 연율을 12로 나눔
    maint_monthly = maint_rate / 12.0
    maintenance = decrement.tpx * sa_col * maint_monthly
    maintenance = np.where(timing.is_in_force, maintenance, 0.0)

    # -- 수금비: 보험료 납입 시점에만 발생 --
    pay_mask = timing.is_pay_period.astype(np.float64)
    collection = decrement.tpx * prem_col * collect_rate * pay_mask

    # -- 총 사업비 --
    total_expense = acquisition + maintenance + collection

    return ExpenseCF(
        acquisition=acquisition,
        maintenance=maintenance,
        collection=collection,
        total_expense=total_expense,
    )


def _calc_expense_pricing(
    mp: ModelPointSet,
    timing: TimingResult,
    decrement: DecrementResult,
    ee: ExpectedExpenseRate,
) -> ExpenseCF:
    """Pricing 모드 사업비 CF를 산출한다.

    - 신계약비(t=0): FRYY_GPREM × 연보(월보×12) + JOIN_AMT × 가입금액
    - 유지비(납입중): (MNT_RT × 월보 + JOIN_AMT_MNT × SA/12 + FXAMT/12) × tpx
    - 유지비(납입후): (AFPAY_MNT × 월보 + AFPAY_JOIN_AMT × SA/12) × tpx
    - 손해조사비: LOSS_SVYEXP_RT × 월보 × tpx (유지비에 합산)
    - 수금비: COLM_RT × 월보 × tpx × pay_mask

    Args:
        mp: Model Point
        timing: 시간축
        decrement: 탈퇴율
        ee: 예정사업비율
    """
    n = mp.n_points
    s = timing.n_steps

    monthly_prem = mp.premium[:, np.newaxis]       # (n, 1) 월보
    annual_prem = mp.premium[:, np.newaxis] * 12.0  # (n, 1) 연보 (=월보×12)
    sa_col = mp.sum_assured[:, np.newaxis]          # (n, 1) 가입금액

    pay_mask = timing.is_pay_period.astype(np.float64)   # (n, s)
    npay_mask = (timing.is_in_force & ~timing.is_pay_period).astype(np.float64)  # (n, s)
    in_force = timing.is_in_force.astype(np.float64)

    # -- 신계약비(t=0): 1회성 --
    acquisition = np.zeros((n, s), dtype=np.float64)
    acquisition[:, 0] = (
        ee.fryy_gprem_acqs_rt * mp.premium * 12.0   # 영보 × 신계약비율
        + ee.fryy_join_amt_acqs_rt * mp.sum_assured  # 가입금액 × 신계약비율
    )

    # -- 유지비(납입중): 매월 --
    maint_inpay = (
        ee.inpay_gprem_mnt_rt * monthly_prem           # 영보 × 유지비율
        + ee.inpay_join_amt_mnt_rt * sa_col / 12.0      # 가입금액 × 유지비율 / 12
        + ee.inpay_fxamt_mntexp / 12.0                   # 고정금액 유지비 / 12
        + ee.inpay_gprem_loss_svyexp_rt * monthly_prem   # 손해조사비
    ) * decrement.tpx * pay_mask

    # -- 유지비(납입후): 매월 --
    maint_afpay = (
        ee.afpay_gprem_mnt_rt * monthly_prem            # 영보 × 납입후 유지비율
        + ee.afpay_join_amt_mnt_rt * sa_col / 12.0       # 가입금액 × 납입후 유지비율 / 12
    ) * decrement.tpx * npay_mask

    maintenance = maint_inpay + maint_afpay

    # -- 수금비: 납입기간만 --
    collection = ee.inpay_gprem_colm_rt * monthly_prem * decrement.tpx * pay_mask

    # -- 총 사업비 --
    total_expense = acquisition + maintenance + collection

    logger.debug("[EXP-PRICING] 신계약비(t=0)=%.2f, 유지비(t=1)=%.6f, 수금비(t=1)=%.6f",
                 float(acquisition[0, 0]) if n > 0 else 0,
                 float(maintenance[0, 1]) if n > 0 and s > 1 else 0,
                 float(collection[0, 1]) if n > 0 and s > 1 else 0)

    return ExpenseCF(
        acquisition=acquisition,
        maintenance=maintenance,
        collection=collection,
        total_expense=total_expense,
    )

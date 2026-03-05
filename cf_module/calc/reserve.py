"""
준비금(Reserve) 모듈

DB에서 로딩한 V(기말 준비금)와 해약공제액을 이용하여
상각률을 경과월 기반으로 재계산하고, W = V - deductible × amort_rate 로 해약환급금을 산출한다.

shape: (n_points, n_steps)
"""

from dataclasses import dataclass

import numpy as np

from cf_module.calc.decrement import DecrementResult
from cf_module.calc.timing import MONTHS_IN_YEAR, TimingResult
from cf_module.data.assumptions import ReserveTable
from cf_module.data.model_point import ModelPointSet


@dataclass
class ReserveResult:
    """준비금 결과

    Attributes:
        v_end: DB에서 로딩한 기말 V (시점별 매핑)
        deductible: 해약공제액 (n_points, n_steps) 브로드캐스트
        amort_rate: 상각률
        w: W = V - deductible × amort_rate
        surrender_value: = w (해약환급금)
    """
    v_end: np.ndarray
    deductible: np.ndarray
    amort_rate: np.ndarray
    w: np.ndarray
    surrender_value: np.ndarray


def _map_yearly_to_steps(v_yearly: np.ndarray, duration_years: np.ndarray) -> np.ndarray:
    """연도별 V를 시점별로 매핑한다.

    Args:
        v_yearly: (n_points, max_years) 연도별 기말 준비금
        duration_years: (n_points, n_steps) 경과연수

    Returns:
        (n_points, n_steps) 시점별 V
    """
    n_points, max_years = v_yearly.shape
    n_steps = duration_years.shape[1]

    # duration_years를 인덱스로 변환 (1-based → 0-based, 범위 클리핑)
    idx = np.clip(duration_years.astype(np.int64) - 1, 0, max_years - 1)

    # fancy indexing: 각 (i, t)에 대해 v_yearly[i, idx[i,t]]
    rows = np.arange(n_points)[:, np.newaxis]  # (n_points, 1)
    return v_yearly[rows, idx]


def calc_reserve(
    mp: ModelPointSet,
    timing: TimingResult,
    decrement: DecrementResult,
    reserve_table: ReserveTable,
) -> ReserveResult:
    """V 기반 W(해약환급금)를 산출한다.

    W = V - deductible × amort_rate
    상각률 = max((cap - duration_months) / cap, 0)
    cap = min(pterm_months, 84)

    Args:
        mp: Model Point
        timing: 시간축
        decrement: 탈퇴율
        reserve_table: DB에서 로딩한 준비금(V) 테이블

    Returns:
        ReserveResult
    """
    n = mp.n_points
    s = timing.n_steps

    if s == 0:
        empty = np.zeros((n, 0), dtype=np.float64)
        return ReserveResult(
            v_end=empty,
            deductible=empty,
            amort_rate=empty,
            w=empty,
            surrender_value=empty,
        )

    # -- V: 연도별 → 시점별 매핑 --
    v_mapped = _map_yearly_to_steps(reserve_table.v_end, timing.duration_years)

    # -- 해약공제액: (n_points,) → (n_points, n_steps) 브로드캐스트 --
    deductible_2d = mp.deductible[:, np.newaxis] * np.ones((1, s), dtype=np.float64)

    # -- 상각률 --
    # cap = min(pterm_months, 84)
    cap = np.minimum(timing.pterm_months, 84).astype(np.float64)  # (n_points,)
    duration_months = timing.duration_months.astype(np.float64)    # (n_points, n_steps)
    amort_rate = np.where(
        cap[:, np.newaxis] > 0,
        np.maximum((cap[:, np.newaxis] - duration_months) / cap[:, np.newaxis], 0.0),
        0.0,
    )

    # -- W = V - deductible × amort_rate --
    w = v_mapped - deductible_2d * amort_rate

    # 보험기간 외 0 처리
    v_mapped = np.where(timing.is_in_force, v_mapped, 0.0)
    amort_rate = np.where(timing.is_in_force, amort_rate, 0.0)
    w = np.where(timing.is_in_force, w, 0.0)
    deductible_2d = np.where(timing.is_in_force, deductible_2d, 0.0)

    return ReserveResult(
        v_end=v_mapped,
        deductible=deductible_2d,
        amort_rate=amort_rate,
        w=w,
        surrender_value=w,
    )

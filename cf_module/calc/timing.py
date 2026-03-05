"""
시간축 생성 모듈

프로젝션에 필요한 시간축(t, 경과월, 경과년, 나이, 납입기간, 보험기간)을 생성한다.
모든 계산은 numpy 벡터화로 수행하며, shape은 (n_model_points, n_time_steps)이다.

기존 timing3.py 로직을 벡터화하여 다건 MP를 한번에 처리한다.
"""

from dataclasses import dataclass

import numpy as np

from cf_module.config import ProjectionConfig
from cf_module.data.model_point import ModelPointSet
from cf_module.utils.logger import get_logger

logger = get_logger("timing")

MONTHS_IN_YEAR = 12


@dataclass
class TimingResult:
    """시간축 결과

    모든 배열은 shape (n_points, n_steps) (2D) 이다.
    월별(monthly) 기준 생성 후, 연별(yearly) 변환은 별도 메서드로 제공.

    Attributes:
        t: 프로젝션 시점 인덱스 (0, 1, 2, ...)
        elapsed_months: 경과개월 (결산년월 기준)
        duration_months: 경과개월 (각 시점별)
        duration_years: 경과연수 (올림)
        age: 각 시점별 나이
        is_pay_period: 납입기간 여부 (bool)
        is_in_force: 보험기간 내 여부 (bool)
        n_steps: 시점 수 (MP별로 다를 수 있으나, 최대값 기준)
        max_proj_months: 최대 프로젝션 개월수 (전체 MP 공통)

        -- 스칼라 배열 (n_points,) --
        elapsed_month_0: 기초 경과개월 (결산 시점)
        elapsed_year_0: 기초 경과연수
        proj_length: MP별 프로젝션 길이(개월)
        bterm_months: 보장기간(개월)
        pterm_months: 납입기간(개월)
    """
    t: np.ndarray
    elapsed_months: np.ndarray
    duration_months: np.ndarray
    duration_years: np.ndarray
    age: np.ndarray
    is_pay_period: np.ndarray
    is_in_force: np.ndarray
    n_steps: int
    max_proj_months: int

    elapsed_month_0: np.ndarray
    elapsed_year_0: np.ndarray
    proj_length: np.ndarray
    bterm_months: np.ndarray
    pterm_months: np.ndarray


def build_timing(
    mp: ModelPointSet,
    config: ProjectionConfig,
    from_inception: bool = False,
) -> TimingResult:
    """시간축을 생성한다.

    Args:
        mp: Model Point 데이터셋
        config: 프로젝션 설정
        from_inception: True이면 계약 시점(duration=1)부터 전체 기간 프로젝션 (Pricing용)

    Returns:
        TimingResult (월별 기준)
    """
    n = mp.n_points
    clos_ym = int(config.base_date)  # 결산년월 (yyyymm)
    logger.debug("[TIMING] -- 시간축 생성 시작 --")
    logger.debug("[TIMING] n_points=%d, base_date=%s, time_step=%s, from_inception=%s",
                 n, config.base_date, config.time_step, from_inception)

    if from_inception:
        # Pricing: 계약 시점부터 (duration=1)
        elapsed_month_0 = np.ones(n, dtype=np.int32)
    else:
        # Valuation: 결산 시점부터
        ctr_ym = mp.ctr_ym.astype(np.int64)
        clos_year = clos_ym // 100
        clos_month = clos_ym % 100
        ctr_year = ctr_ym // 100
        ctr_month = ctr_ym % 100

        # 경과개월 = (결산년-계약년)*12 + (결산월-계약월) + 1
        elapsed_month_0 = (
            (clos_year - ctr_year) * MONTHS_IN_YEAR
            + (clos_month - ctr_month)
            + 1
        ).astype(np.int32)
        elapsed_month_0 = np.maximum(elapsed_month_0, 1)

    ctr_ym = mp.ctr_ym.astype(np.int64)
    for idx in range(min(n, 3)):
        logger.debug(
            "[TIMING] #%d  ctr_ym=%s  elapsed_month_0=%d",
            idx, ctr_ym[idx], elapsed_month_0[idx],
        )

    # 경과연수 (올림)
    elapsed_year_0 = ((elapsed_month_0 - 1) // MONTHS_IN_YEAR + 1).astype(np.int32)

    # 보장/납입 기간 (개월)
    bterm_months = (mp.bterm * MONTHS_IN_YEAR).astype(np.int32)
    pterm_months = (mp.pterm * MONTHS_IN_YEAR).astype(np.int32)

    # MP별 프로젝션 길이 (개월)
    proj_length = np.maximum(bterm_months - elapsed_month_0 + 1, 0).astype(np.int32)

    # 최대 프로젝션 길이 (전체 MP 공통)
    max_steps = int(np.max(proj_length)) if n > 0 else 0
    max_steps = min(max_steps, config.max_proj_months)

    logger.debug(
        "[TIMING] bterm_months: min=%d max=%d, pterm_months: min=%d max=%d",
        int(np.min(bterm_months)), int(np.max(bterm_months)),
        int(np.min(pterm_months)), int(np.max(pterm_months)),
    )
    logger.debug("[TIMING] proj_length: min=%d max=%d → max_steps=%d", int(np.min(proj_length)), int(np.max(proj_length)), max_steps)

    if max_steps == 0:
        # 모든 MP가 만기 도래
        empty = np.empty((n, 0), dtype=np.float64)
        empty_bool = np.empty((n, 0), dtype=bool)
        return TimingResult(
            t=empty, elapsed_months=empty, duration_months=empty,
            duration_years=empty, age=empty,
            is_pay_period=empty_bool, is_in_force=empty_bool,
            n_steps=0, max_proj_months=0,
            elapsed_month_0=elapsed_month_0, elapsed_year_0=elapsed_year_0,
            proj_length=proj_length, bterm_months=bterm_months, pterm_months=pterm_months,
        )

    # -- 2D 시간축 생성: shape (n_points, max_steps) --
    # t: (1, max_steps) → 브로드캐스팅으로 (n, max_steps)
    t_1d = np.arange(max_steps, dtype=np.int32)  # (max_steps,)
    t = np.broadcast_to(t_1d[np.newaxis, :], (n, max_steps)).copy()

    # duration_months: 경과개월 + t
    elapsed_col = elapsed_month_0[:, np.newaxis]  # (n, 1)
    duration_months = elapsed_col + t  # (n, max_steps)

    # duration_years: 올림 (1개월~12개월 → 1년)
    duration_years = ((duration_months - 1) // MONTHS_IN_YEAR + 1).astype(np.int32)

    # age: 가입연령 + 경과연수 - 1
    age_col = mp.age_at_entry[:, np.newaxis]  # (n, 1)
    age = age_col + duration_years - 1

    # 납입기간 여부
    pterm_col = pterm_months[:, np.newaxis]  # (n, 1)
    is_pay_period = duration_months <= pterm_col

    # 보험기간 내 여부
    bterm_col = bterm_months[:, np.newaxis]  # (n, 1)
    is_in_force = duration_months <= bterm_col

    # 만기 초과 시점은 마스킹 (is_in_force=False)
    # (프로젝션 길이가 MP별로 다르므로, 초과 시점은 0으로 처리)
    t = np.where(is_in_force, t, 0)
    duration_months = np.where(is_in_force, duration_months, 0)
    duration_years = np.where(is_in_force, duration_years, 0)
    age = np.where(is_in_force, age, 0)

    for idx in range(min(n, 3)):
        logger.debug("[TIMING] #%d  duration_months[:12] = %s", idx, duration_months[idx, :12].tolist())
        logger.debug("[TIMING] #%d  duration_years[:12]  = %s", idx, duration_years[idx, :12].tolist())
        logger.debug("[TIMING] #%d  age[:12]             = %s", idx, age[idx, :12].tolist())
    logger.debug("[TIMING] -- 시간축 생성 완료: %d시점 --", max_steps)

    return TimingResult(
        t=t,
        elapsed_months=elapsed_col + t,
        duration_months=duration_months,
        duration_years=duration_years,
        age=age,
        is_pay_period=is_pay_period & is_in_force,
        is_in_force=is_in_force,
        n_steps=max_steps,
        max_proj_months=max_steps,
        elapsed_month_0=elapsed_month_0,
        elapsed_year_0=elapsed_year_0,
        proj_length=proj_length,
        bterm_months=bterm_months,
        pterm_months=pterm_months,
    )


def to_yearly(timing: TimingResult) -> TimingResult:
    """월별 TimingResult를 연별로 변환한다.

    12개월 단위로 연말 시점만 추출한다.
    """
    if timing.n_steps == 0:
        return timing

    # 연말 인덱스 (11, 23, 35, ...)
    yearly_indices = np.arange(MONTHS_IN_YEAR - 1, timing.n_steps, MONTHS_IN_YEAR)

    if len(yearly_indices) == 0:
        yearly_indices = np.array([timing.n_steps - 1])

    return TimingResult(
        t=timing.t[:, yearly_indices],
        elapsed_months=timing.elapsed_months[:, yearly_indices],
        duration_months=timing.duration_months[:, yearly_indices],
        duration_years=timing.duration_years[:, yearly_indices],
        age=timing.age[:, yearly_indices],
        is_pay_period=timing.is_pay_period[:, yearly_indices],
        is_in_force=timing.is_in_force[:, yearly_indices],
        n_steps=len(yearly_indices),
        max_proj_months=timing.max_proj_months,
        elapsed_month_0=timing.elapsed_month_0,
        elapsed_year_0=timing.elapsed_year_0,
        proj_length=timing.proj_length,
        bterm_months=timing.bterm_months,
        pterm_months=timing.pterm_months,
    )

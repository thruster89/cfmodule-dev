"""
메인 프로젝션 모듈

시간축 → 탈퇴율 → CF 산출 → 할인 의 전체 파이프라인을 통합 실행한다.
단일 청크(ModelPointSet)를 입력받아 프로젝션 결과를 반환한다.
"""

import os
from dataclasses import dataclass

import numpy as np
import pandas as pd

from typing import Optional

from cf_module.calc.benefit import BenefitCF, calc_benefit_cf
from cf_module.calc.decrement import DecrementResult, build_decrement
from cf_module.calc.discount import DiscountResult, build_discount, calc_present_value
from cf_module.calc.expense import ExpenseCF, calc_expense_cf
from cf_module.calc.premium import PremiumCF, calc_premium_cf
from cf_module.calc.reserve import ReserveResult, calc_reserve
from cf_module.calc.timing import TimingResult, build_timing, to_yearly
from cf_module.config import CFConfig
from cf_module.data.assumptions import AssumptionSet
from cf_module.data.model_point import ModelPointSet
from cf_module.io.reader import DataReader
from cf_module.utils.logger import get_logger

logger = get_logger("projector")


@dataclass
class ProjectionResult:
    """프로젝션 결과 (단일 청크)

    Attributes:
        mp: Model Point (입력)
        timing: 시간축
        decrement: 탈퇴율
        premium: 보험료 CF
        benefit: 급부 CF
        expense: 사업비 CF
        reserve: 준비금
        discount: 할인
        n_points: MP 건수
        n_steps: 시점 수

        -- 요약 지표 (n_points,) --
        pv_premium: 보험료 현가
        pv_benefit: 급부 현가
        pv_expense: 사업비 현가
        pv_net_cf: 순CF 현가 (보험료 - 급부 - 사업비)
    """
    mp: ModelPointSet
    timing: TimingResult
    decrement: DecrementResult
    premium: PremiumCF
    benefit: BenefitCF
    expense: ExpenseCF
    reserve: ReserveResult
    discount: DiscountResult
    n_points: int
    n_steps: int
    pv_premium: np.ndarray
    pv_benefit: np.ndarray
    pv_expense: np.ndarray
    pv_net_cf: np.ndarray
    pricing_result: Optional['PricingResult'] = None


def run_projection(
    mp: ModelPointSet,
    assumptions: AssumptionSet,
    config: CFConfig,
    reader: Optional[DataReader] = None,
) -> ProjectionResult:
    """단일 청크에 대해 프로젝션을 실행한다.

    파이프라인:
    1. 시간축 생성
    2. 탈퇴율 산출
    3. 보험료 CF
    4. 급부 CF
    5. 사업비 CF
    6. 준비금
    7. 할인/현가 계산

    Args:
        mp: Model Point 데이터셋
        assumptions: 가정 데이터
        config: 전역 설정
        reader: DataReader (DB 모드에서 키 빌더 사용 시)

    Returns:
        ProjectionResult
    """
    _sep = "-" * 60
    logger.info(f"프로젝션 시작: {mp.n_points}건")
    logger.debug("")
    logger.debug("=" * 60)
    logger.debug("[PROJ] 프로젝션 파이프라인 시작: %d건, mode=%s, time_step=%s",
                 mp.n_points, config.run_mode, config.projection.time_step)
    logger.debug("=" * 60)

    # ── Pricing 모드: 기수표 기반 파이프라인 ──
    if config.is_pricing:
        return _run_pricing_pipeline(mp, assumptions, config, reader)

    # ── Valuation 모드: 기존 8단계 파이프라인 ──
    return _run_valuation_pipeline(mp, assumptions, config, reader)


def _run_pricing_pipeline(
    mp: ModelPointSet,
    assumptions: AssumptionSet,
    config: CFConfig,
    reader: Optional[DataReader] = None,
) -> ProjectionResult:
    """Pricing 모드 파이프라인

    기수표(lx, Dx, Nx, Cx, Mx)를 먼저 구축하고,
    순보험료(P), 준비금(V), 영업보험료(G)를 산출한다.

    Steps:
        1. 위험률 구축 (qx_exit, qx_benefit)
        2. 기수표 구축 (CommutationTable)
        3. 순보험료 P + 준비금 V
        4. 영업보험료 G
    """
    _sep = "-" * 60
    from cf_module.calc.commutation import (
        build_qx_from_inception, build_commutation_table,
        calc_net_premium, check_has_maturity,
        calc_gross_premium, get_acq_amort_period,
    )

    exp_int = assumptions.expected_interest
    if exp_int is None:
        raise ValueError("Pricing 모드: 예정이율(expected_interest) 없음")

    flat_rate = exp_int.get_flat_rate()
    if flat_rate is None:
        flat_rate = float(exp_int.rates[0])
        logger.debug("[PRICING]   3단계 이율 → 1단계(%.4f) 사용", flat_rate)

    # ── 기준가입금액 / 납입주기 결정 ──
    actual_sa = float(mp.sum_assured[0])

    # PAYCYC_DVCD → pay_freq (연간 납입횟수)
    paycyc = int(mp.df["PAYCYC_DVCD"].iloc[0]) if "PAYCYC_DVCD" in mp.df.columns else 1
    pay_freq = 12 // paycyc if paycyc > 0 else 0
    logger.debug("[PRICING]   PAYCYC_DVCD=%d → pay_freq=%d", paycyc, pay_freq)

    # 기준가입금액 (II_RSVAMT_BAS.CRIT_JOIN_AMT)
    crit_join_amt = None
    multiplier = None
    if assumptions.reserve.crit_join_amt is not None and len(assumptions.reserve.crit_join_amt) > 0:
        crit_val = float(assumptions.reserve.crit_join_amt[0])
        if crit_val > 0:
            crit_join_amt = crit_val
            multiplier = actual_sa / crit_join_amt

    # 모든 금액 계산에 사용할 SA: 기준가입금액 우선
    calc_sa = crit_join_amt if crit_join_amt else actual_sa
    logger.debug("[PRICING]   calc_sa=%.0f (crit=%s, actual=%.0f, multiplier=%s)",
                 calc_sa, crit_join_amt, actual_sa,
                 f"{multiplier:.0f}" if multiplier else "N/A")

    # ── STEP 1: 위험률 구축 ──
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PRICING] STEP 1/4: 위험률 구축 (qx_exit, qx_benefit)")
    logger.debug(_sep)
    qx_exit, qx_benefit = build_qx_from_inception(mp, assumptions, reader, config)

    # ── STEP 2: 기수표 구축 ──
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PRICING] STEP 2/4: 기수표 구축 (lx, Dx, Nx, Cx, Mx)")
    logger.debug(_sep)
    table = build_commutation_table(
        qx_exit, qx_benefit, flat_rate,
        int(mp.age_at_entry[0]), int(mp.bterm[0]), int(mp.pterm[0]),
    )
    has_maturity = check_has_maturity(mp, reader)

    # ── STEP 3: 순보험료 P + 준비금 V (calc_sa 기준) ──
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PRICING] STEP 3/4: 순보험료 P + 준비금 V (calc_sa=%.0f 기준)", calc_sa)
    logger.debug(_sep)
    pricing_result = calc_net_premium(
        table, has_maturity=has_maturity, sum_assured=calc_sa, pay_freq=pay_freq,
    )

    # 기준가입금액 메타 + 라운드 처리
    if crit_join_amt:
        pricing_result.crit_join_amt = crit_join_amt
        pricing_result.multiplier = multiplier
        pricing_result.net_premium_monthly_rounded = round(pricing_result.net_premium_monthly)
        pricing_result.reserve_by_year_rounded = np.round(pricing_result.reserve_by_year).astype(np.int64)
        logger.debug("[PRICING]   P(1회납)=%.4f → round=%d",
                     pricing_result.net_premium_monthly, pricing_result.net_premium_monthly_rounded)
        logger.debug("[PRICING]   V(1)=%.4f → round=%d",
                     pricing_result.reserve_by_year[1] if table.n >= 1 else 0.0,
                     pricing_result.reserve_by_year_rounded[1] if table.n >= 1 else 0)
    else:
        logger.debug("[PRICING]   P(연)=%.4f, P(1회납)=%.4f",
                     pricing_result.net_premium_annual, pricing_result.net_premium_monthly)

    # ── STEP 4: 영업보험료 G (calc_sa 기준) ──
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PRICING] STEP 4/4: 영업보험료 G (calc_sa=%.0f 기준)", calc_sa)
    logger.debug(_sep)
    ee = assumptions.expected_expense
    if ee is not None:
        t_acq = get_acq_amort_period(mp, reader, has_maturity)
        G_annual, G_per_pay = calc_gross_premium(
            table, ee, has_maturity, calc_sa,
            t_acq, pricing_result.Ax, pay_freq=pay_freq,
        )
        pricing_result.gross_premium_annual = G_annual
        pricing_result.gross_premium_monthly = G_per_pay
        pricing_result.acq_amort_period = t_acq
        if pricing_result.net_premium_annual > 0:
            pricing_result.loading_ratio = G_annual / pricing_result.net_premium_annual

        if crit_join_amt:
            pricing_result.gross_premium_monthly_rounded = round(G_per_pay)
            logger.debug("[PRICING]   G(1회납)=%.4f → round=%d",
                         G_per_pay, pricing_result.gross_premium_monthly_rounded)
        else:
            logger.debug("[PRICING]   G(연)=%.4f, G(1회납)=%.4f", G_annual, G_per_pay)
    else:
        logger.warning("[PRICING]   예정사업비율 없음 — G 산출 스킵")

    # debug CSV
    if config.debug:
        _dump_debug_csv_pricing(config.output.output_dir, mp, pricing_result, ee)

    logger.debug("")
    logger.debug("=" * 60)
    logger.info(f"Pricing 완료: {mp.n_points}건")
    logger.debug("=" * 60)

    # 빈 구조체로 ProjectionResult 반환 (valuation 필드는 미사용)
    empty = np.zeros((mp.n_points, 0), dtype=np.float64)
    empty_1d = np.zeros(mp.n_points, dtype=np.float64)
    empty_1d_int = np.zeros(mp.n_points, dtype=np.int32)
    timing = TimingResult(
        t=empty, duration_months=empty, duration_years=empty,
        elapsed_months=empty, age=empty, is_in_force=np.zeros((mp.n_points, 0), dtype=bool),
        is_pay_period=np.zeros((mp.n_points, 0), dtype=bool), n_steps=0,
        max_proj_months=0,
        elapsed_month_0=empty_1d_int, elapsed_year_0=empty_1d_int,
        proj_length=empty_1d_int, bterm_months=empty_1d_int, pterm_months=empty_1d_int,
    )
    decrement = DecrementResult(
        qx_annual=empty, qx_monthly=empty, wx_annual=empty, wx_monthly=empty,
        dx_monthly=empty, tpx=empty, d_death=empty, d_lapse=empty, d_survive=empty,
    )
    premium = PremiumCF(
        gross_premium=empty, net_premium=empty, risk_premium=empty,
        saving_premium=empty, loading=empty,
    )
    benefit = BenefitCF(
        death_benefit=empty, maturity_benefit=empty, survival_benefit=empty,
        surrender_benefit=empty, total_benefit=empty,
    )
    expense = ExpenseCF(acquisition=empty, maintenance=empty, collection=empty, total_expense=empty)
    reserve = ReserveResult(v_end=empty, deductible=empty, amort_rate=empty, w=empty, surrender_value=empty)
    discount = DiscountResult(discount_factor=empty, forward_rate_monthly=empty, spot_rate_annual=empty)

    return ProjectionResult(
        mp=mp, timing=timing, decrement=decrement, premium=premium,
        benefit=benefit, expense=expense, reserve=reserve, discount=discount,
        n_points=mp.n_points, n_steps=0,
        pv_premium=empty_1d, pv_benefit=empty_1d, pv_expense=empty_1d, pv_net_cf=empty_1d,
        pricing_result=pricing_result,
    )


def _run_valuation_pipeline(
    mp: ModelPointSet,
    assumptions: AssumptionSet,
    config: CFConfig,
    reader: Optional[DataReader] = None,
) -> ProjectionResult:
    """Valuation 모드 파이프라인 (기존 8단계)"""
    _sep = "-" * 60

    # 1. 시간축 생성
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PROJ] STEP 1/8: 시간축 생성")
    logger.debug(_sep)
    timing = build_timing(mp, config.projection)
    if not config.is_monthly:
        logger.debug("[PROJ]   연별 변환 적용 (monthly -> yearly)")
        timing = to_yearly(timing)

    logger.info(f"시간축 생성 완료: {timing.n_steps}시점")

    # 2. 탈퇴율 산출
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PROJ] STEP 2/8: 탈퇴율 산출")
    logger.debug(_sep)
    decrement = build_decrement(
        mp, timing, assumptions, config.scenario,
        reader=reader, config=config,
    )

    # 3. 보험료 CF
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PROJ] STEP 3/8: 보험료 CF")
    logger.debug(_sep)
    premium = calc_premium_cf(mp, timing, decrement)
    logger.debug("[PROJ]   gross_premium shape=%s, sum=%.0f", premium.gross_premium.shape, float(np.sum(premium.gross_premium)))

    # 4. 급부 CF
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PROJ] STEP 4/8: 급부 CF")
    logger.debug(_sep)
    benefit = calc_benefit_cf(mp, timing, decrement)
    logger.debug("[PROJ]   total_benefit shape=%s, sum=%.0f", benefit.total_benefit.shape, float(np.sum(benefit.total_benefit)))

    # 5. 사업비 CF
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PROJ] STEP 5/8: 사업비 CF")
    logger.debug(_sep)
    expense = calc_expense_cf(mp, timing, decrement, assumptions.expense)
    logger.debug("[PROJ]   total_expense shape=%s, sum=%.0f", expense.total_expense.shape, float(np.sum(expense.total_expense)))

    # 6. 준비금
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PROJ] STEP 6/8: 준비금")
    logger.debug(_sep)
    reserve = calc_reserve(mp, timing, decrement, assumptions.reserve)
    logger.debug("[PROJ]   v_end shape=%s", reserve.v_end.shape)

    # 7. 할인
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PROJ] STEP 7/8: 할인")
    logger.debug(_sep)
    discount = build_discount(
        timing,
        assumptions.interest,
        rate_shock=config.scenario.interest_rate_shock,
    )
    logger.debug("[PROJ]   discount_factor shape=%s", discount.discount_factor.shape)

    # 8. 현가 계산
    logger.debug("")
    logger.debug(_sep)
    logger.debug("[PROJ] STEP 8/8: 현가 합산")
    logger.debug(_sep)
    pv_premium = calc_present_value(premium.gross_premium, discount.discount_factor)
    pv_benefit = calc_present_value(benefit.total_benefit, discount.discount_factor)
    pv_expense = calc_present_value(expense.total_expense, discount.discount_factor)
    pv_net_cf = pv_premium - pv_benefit - pv_expense

    for idx in range(min(mp.n_points, 3)):
        logger.debug(
            "[PROJ]   #%d  PV(prem)=%.0f  PV(ben)=%.0f  PV(exp)=%.0f  PV(net)=%.0f",
            idx, pv_premium[idx], pv_benefit[idx], pv_expense[idx], pv_net_cf[idx],
        )

    # debug 모드: 중간테이블 CSV 덤프
    if config.debug:
        _dump_debug_csv(
            config.output.output_dir, mp,
            timing, decrement, premium, benefit, expense, reserve, discount,
            pricing_mode=False,
        )

    logger.debug("")
    logger.debug("=" * 60)
    logger.info(f"프로젝션 완료: {mp.n_points}건")
    logger.debug("=" * 60)

    return ProjectionResult(
        mp=mp,
        timing=timing,
        decrement=decrement,
        premium=premium,
        benefit=benefit,
        expense=expense,
        reserve=reserve,
        discount=discount,
        n_points=mp.n_points,
        n_steps=timing.n_steps,
        pv_premium=pv_premium,
        pv_benefit=pv_benefit,
        pv_expense=pv_expense,
        pv_net_cf=pv_net_cf,
    )


def result_to_summary_df(result: ProjectionResult) -> pd.DataFrame:
    """프로젝션 결과를 요약 DataFrame으로 변환한다.

    Returns:
        MP별 요약 DataFrame (mp_id, pv_premium, pv_benefit, pv_expense, pv_net_cf)
    """
    return pd.DataFrame({
        "mp_id": result.mp.mp_ids,
        "product_cd": result.mp.product_cd,
        "age_at_entry": result.mp.age_at_entry,
        "bterm": result.mp.bterm,
        "pterm": result.mp.pterm,
        "pv_premium": result.pv_premium,
        "pv_benefit": result.pv_benefit,
        "pv_expense": result.pv_expense,
        "pv_net_cf": result.pv_net_cf,
    })


def result_to_cf_detail_df(result: ProjectionResult) -> pd.DataFrame:
    """프로젝션 결과를 시점별 CF 상세 DataFrame으로 변환한다.

    Returns:
        (mp_id, t, duration_month, age, gross_premium, death_benefit, ...) 형태의 long-format DataFrame
    """
    rows = []
    for i in range(result.n_points):
        mp_id = result.mp.mp_ids[i]
        for t in range(result.n_steps):
            if not result.timing.is_in_force[i, t]:
                continue
            rows.append({
                "mp_id": mp_id,
                "t": t,
                "duration_month": int(result.timing.duration_months[i, t]),
                "duration_year": int(result.timing.duration_years[i, t]),
                "age": int(result.timing.age[i, t]),
                "tpx": result.decrement.tpx[i, t],
                "qx_monthly": result.decrement.qx_monthly[i, t],
                "wx_monthly": result.decrement.wx_monthly[i, t],
                "gross_premium": result.premium.gross_premium[i, t],
                "death_benefit": result.benefit.death_benefit[i, t],
                "maturity_benefit": result.benefit.maturity_benefit[i, t],
                "surrender_benefit": result.benefit.surrender_benefit[i, t],
                "total_benefit": result.benefit.total_benefit[i, t],
                "total_expense": result.expense.total_expense[i, t],
                "discount_factor": result.discount.discount_factor[i, t],
                "net_cf": (
                    result.premium.gross_premium[i, t]
                    - result.benefit.total_benefit[i, t]
                    - result.expense.total_expense[i, t]
                ),
            })

    return pd.DataFrame(rows)


def _build_discount_expected(
    timing: TimingResult,
    exp_int: 'ExpectedInterestRate',
) -> DiscountResult:
    """3단계 예정이율 기반 할인계수를 생성한다.

    duration_years 기반으로 경과연수별 이율을 선택,
    월금리로 변환 후 할인계수를 산출한다.
    """
    from cf_module.data.assumptions import ExpectedInterestRate
    from cf_module.calc.discount import _annual_to_monthly_rate

    n_points = timing.t.shape[0]
    s = timing.n_steps

    if s == 0:
        empty = np.zeros((n_points, 0), dtype=np.float64)
        return DiscountResult(
            discount_factor=empty,
            forward_rate_monthly=empty,
            spot_rate_annual=empty,
        )

    # duration_years 기반 연금리 결정 (n_points, s)
    annual_rates = np.zeros((n_points, s), dtype=np.float64)
    for t_idx in range(s):
        for i in range(n_points):
            yr = int(timing.duration_years[i, t_idx])
            annual_rates[i, t_idx] = exp_int.get_rate_at_year(yr)

    # 월금리 변환
    fwd_monthly = _annual_to_monthly_rate(annual_rates)

    # 할인계수 산출
    discount_factor = np.ones((n_points, s), dtype=np.float64)
    if s > 1:
        cum_factor = np.cumprod(1.0 / (1.0 + fwd_monthly[:, :-1]), axis=1)
        discount_factor[:, 1:] = cum_factor

    return DiscountResult(
        discount_factor=discount_factor,
        forward_rate_monthly=fwd_monthly,
        spot_rate_annual=annual_rates,
    )


def _dump_mp_csv(debug_dir: str, mp: ModelPointSet) -> None:
    """00_model_point.csv: MP 정보를 출력한다."""
    mp_info = {
        "mp_id": [mp.mp_ids[0]],
        "product_cd": [mp.product_cd[0]],
        "sex_cd": [mp.sex_cd[0]],
        "age_at_entry": [mp.age_at_entry[0]],
        "bterm": [mp.bterm[0]],
        "pterm": [mp.pterm[0]],
        "premium": [mp.premium[0]],
        "sum_assured": [mp.sum_assured[0]],
        "ctr_ym": [mp.ctr_ym[0]],
        "cls_cd": [mp.cls_cd[0]] if len(mp.cls_cd) > 0 else [""],
        "cov_cd": [mp.cov_cd[0]] if len(mp.cov_cd) > 0 else [""],
        "clos_ym": [mp.clos_ym[0]] if len(mp.clos_ym) > 0 else [""],
        "deductible": [mp.deductible[0]],
    }
    if mp.assm_div_vals is not None:
        for k in range(mp.assm_div_vals.shape[1]):
            mp_info[f"ASSM_DIV_VAL{k+1}"] = [mp.assm_div_vals[0, k]]
    if mp.rsk_rt_div_vals is not None:
        for k in range(mp.rsk_rt_div_vals.shape[1]):
            mp_info[f"RSK_RT_DIV_VAL{k+1}"] = [mp.rsk_rt_div_vals[0, k]]
    pd.DataFrame(mp_info).to_csv(os.path.join(debug_dir, "00_model_point.csv"), index=False)


def _dump_debug_csv(
    output_dir: str,
    mp: ModelPointSet,
    timing: TimingResult,
    decrement: DecrementResult,
    premium: PremiumCF,
    benefit: BenefitCF,
    expense: ExpenseCF,
    reserve: ReserveResult,
    discount: DiscountResult,
    pricing_mode: bool = False,
) -> None:
    """debug 모드 (Valuation): 첫 번째 MP(index=0)의 중간테이블을 CSV로 출력한다."""
    debug_dir = os.path.join(output_dir, "debug")
    os.makedirs(debug_dir, exist_ok=True)

    # 이전 실행의 pricing CSV 정리
    for f in ["08_commutation.csv", "09_pricing_summary.csv", "10_gross_premium.csv"]:
        p = os.path.join(debug_dir, f)
        if os.path.exists(p):
            os.remove(p)

    _dump_mp_csv(debug_dir, mp)

    # ── Valuation 모드 전용 (01~07, 02a) ──

    # is_in_force인 시점만 추출
    mask = timing.is_in_force[0]  # (n_steps,)

    # 공통 시점 컬럼
    n_masked = int(np.sum(mask))
    common = {
        "mp_id": np.full(n_masked, mp.mp_ids[0]),
        "t": timing.t[0][mask],
        "duration_months": timing.duration_months[0][mask],
        "age": timing.age[0][mask],
    }

    specs = [
        ("01_timing.csv", {
            **common,
            "elapsed_months": timing.elapsed_months[0][mask],
            "duration_years": timing.duration_years[0][mask],
            "is_pay_period": timing.is_pay_period[0][mask],
            "is_in_force": timing.is_in_force[0][mask],
        }),
        ("02_decrement.csv", {
            **common,
            "qx_annual": decrement.qx_annual[0][mask],
            "qx_monthly": decrement.qx_monthly[0][mask],
            "wx_annual": decrement.wx_annual[0][mask],
            "wx_monthly": decrement.wx_monthly[0][mask],
            "dx_monthly": decrement.dx_monthly[0][mask],
            "tpx": decrement.tpx[0][mask],
            **({"d_rsvamt": decrement.d_rsvamt[0][mask],
                "d_bnft": decrement.d_bnft[0][mask],
               } if decrement.d_rsvamt is not None else {
                "d_death": decrement.d_death[0][mask],
               }),
            "d_lapse": decrement.d_lapse[0][mask],
            "d_survive": decrement.d_survive[0][mask],
            **({"pay_tpx": decrement.pay_tpx[0][mask],
                "pay_d_rsvamt": decrement.pay_d_rsvamt[0][mask],
                "pay_d_bnft": decrement.pay_d_bnft[0][mask],
                "pay_d_lapse": decrement.pay_d_lapse[0][mask],
                "pay_d_pyexsp": decrement.pay_d_pyexsp[0][mask],
                "pay_d_survive": decrement.pay_d_survive[0][mask],
               } if decrement.pay_tpx is not None else {}),
        }),
        ("03_premium.csv", {
            **common,
            "gross_premium": premium.gross_premium[0][mask],
            "net_premium": premium.net_premium[0][mask],
            "risk_premium": premium.risk_premium[0][mask],
            "saving_premium": premium.saving_premium[0][mask],
            "loading": premium.loading[0][mask],
        }),
        ("04_benefit.csv", {
            **common,
            "death_benefit": benefit.death_benefit[0][mask],
            "maturity_benefit": benefit.maturity_benefit[0][mask],
            "survival_benefit": benefit.survival_benefit[0][mask],
            "surrender_benefit": benefit.surrender_benefit[0][mask],
            "total_benefit": benefit.total_benefit[0][mask],
        }),
        ("05_expense.csv", {
            **common,
            "acquisition": expense.acquisition[0][mask],
            "maintenance": expense.maintenance[0][mask],
            "collection": expense.collection[0][mask],
            "total_expense": expense.total_expense[0][mask],
        }),
        ("06_reserve.csv", {
            **common,
            "v_end": reserve.v_end[0][mask],
            "deductible": reserve.deductible[0][mask],
            "amort_rate": reserve.amort_rate[0][mask],
            "w": reserve.w[0][mask],
            "surrender_value": reserve.surrender_value[0][mask],
        }),
        ("07_discount.csv", {
            **common,
            "discount_factor": discount.discount_factor[0][mask],
            "forward_rate_monthly": discount.forward_rate_monthly[0][mask],
            "spot_rate_annual": discount.spot_rate_annual[0][mask],
        }),
    ]

    for filename, columns in specs:
        df = pd.DataFrame(columns)
        df.to_csv(os.path.join(debug_dir, filename), index=False)

    # 위험률코드별 상세 (long format)
    if decrement.qx_raw_by_risk is not None and decrement.rsk_rt_cd is not None:
        rsk_codes = decrement.rsk_rt_cd
        mm_trf = decrement.mm_trf_way_cd
        qx_raw = decrement.qx_raw_by_risk[mask]          # 원수위험률
        qx_be_ann = decrement.qx_be_annual_by_risk[mask]  # BE 연간
        qx_be_mon = decrement.qx_be_by_risk[mask]         # BE 월간 (중복제거 후)

        t_arr = common["t"]
        dur_arr = common["duration_months"]
        age_arr = common["age"]
        rows = []
        for j, cd in enumerate(rsk_codes):
            raw_j = qx_raw[:, j]
            ann_j = qx_be_ann[:, j]
            dedup_j = qx_be_mon[:, j]
            trf = int(mm_trf[j]) if mm_trf is not None else 0
            # beprd = be_annual / raw (0 안전처리)
            beprd_j = np.where(raw_j != 0, ann_j / raw_j, 0.0)
            # 중복제거 전 월변환 (be_annual 기준 재계산)
            if trf == 1:
                pre_monthly_j = 1.0 - (1.0 - ann_j) ** (1.0 / 12.0)
            elif trf == 2:
                pre_monthly_j = ann_j / 12.0
            else:
                pre_monthly_j = ann_j.copy()
            for k in range(len(t_arr)):
                rows.append({
                    "mp_id": mp.mp_ids[0],
                    "t": t_arr[k],
                    "duration_months": dur_arr[k],
                    "age": age_arr[k],
                    "rsk_rt_cd": cd,
                    "mm_trf": trf,
                    "raw": raw_j[k],
                    "beprd": beprd_j[k],
                    "be_annual": ann_j[k],
                    "be_monthly": pre_monthly_j[k],
                    "dedup_monthly": dedup_j[k],
                })

        df_risk = pd.DataFrame(rows)
        df_risk.to_csv(os.path.join(debug_dir, "02a_qx_by_risk.csv"), index=False)

    logger.info(f"debug CSV 출력 완료 (valuation): {debug_dir}")


def _dump_debug_csv_pricing(output_dir: str, mp: ModelPointSet, pricing_result, ee=None) -> None:
    """Pricing 모드: MP정보 + 계산기수/준비금 테이블을 CSV로 출력한다."""
    debug_dir = os.path.join(output_dir, "debug")
    os.makedirs(debug_dir, exist_ok=True)

    # 이전 실행의 valuation CSV 정리
    for f in ["01_timing.csv", "02_decrement.csv", "02a_qx_by_risk.csv",
              "03_premium.csv", "04_benefit.csv", "05_expense.csv",
              "06_reserve.csv", "07_discount.csv"]:
        p = os.path.join(debug_dir, f)
        if os.path.exists(p):
            os.remove(p)

    # 00_model_point.csv
    _dump_mp_csv(debug_dir, mp)

    mp_id = mp.mp_ids[0]

    ct = pricing_result.commutation
    n = ct.n
    m = ct.m

    # Woolhouse 연금현가: ä^(m)_{x+t:k}
    pf = pricing_result.pay_freq
    from cf_module.calc.commutation import _woolhouse_coeff
    w_coeff = _woolhouse_coeff(pf)

    def _woolhouse_at(t_start, k):
        if k <= 0 or ct.Dx[t_start] == 0.0:
            return 0.0
        end = min(t_start + k, n)
        ax_ann = (ct.Nx[t_start] - ct.Nx[end]) / ct.Dx[t_start]
        ax_adj = ax_ann - w_coeff * (1.0 - ct.Dx[end] / ct.Dx[t_start])
        return ax_adj

    # 08_commutation.csv: 계산기수 테이블 (t=0..n)
    rows = []
    for t in range(n + 1):
        remain_m = max(m - t, 0)
        remain_n = max(n - t, 0)
        row = {
            "mp_id": mp_id,
            "t": t,
            "age": ct.x + t,
            "qx_exit": ct.qx_exit[t] if t < n else None,
            "qx_benefit": ct.qx_benefit[t] if t < n else None,
            "lx": ct.lx[t],
            "dx": ct.dx[t] if t < n else None,
            "Dx": ct.Dx[t],
            "Nx": ct.Nx[t],
            "Cx": ct.Cx[t] if t < n else None,
            "Mx": ct.Mx[t],
            "ax_due_m": (ct.Nx[t] - ct.Nx[min(t + remain_m, n)]) / ct.Dx[t] if ct.Dx[t] > 0 and remain_m > 0 else 0.0,
            "ax12_m": _woolhouse_at(t, remain_m),
            "ax12_n": _woolhouse_at(t, remain_n),
            "Ax_t": ((ct.Mx[t] - ct.Mx[n] + ct.Dx[n]) / ct.Dx[t] if pricing_result.has_maturity else (ct.Mx[t] - ct.Mx[n]) / ct.Dx[t]) if ct.Dx[t] > 0 else 0.0,
            "V": pricing_result.reserve_by_year[t],
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(debug_dir, "08_commutation.csv"), index=False)

    # 09_pricing_summary.csv: 순보험료 요약 (기준SA 기준)
    pr = pricing_result
    items = [
        "mp_id",
        "가입연령(x)", "보장기간(n)", "납입기간(m)", "예정이율(i)",
        "pay_freq", "만기급부(has_maturity)",
        "Ax", "ax_due", "ax_due_woolhouse",
        "", "--- P, V (기준SA 기준) ---", "",
        "calc_SA", "P_annual", "P_per_pay",
        "P_per_pay_rounded", "V(0)", "V(n)",
    ]
    calc_sa = pr.crit_join_amt or mp.sum_assured[0]
    values = [
        mp_id,
        ct.x, ct.n, ct.m, ct.i,
        pr.pay_freq, pr.has_maturity,
        pr.Ax, pr.ax_due, pr.ax_due_monthly,
        "", "", "",
        calc_sa, pr.net_premium_annual, pr.net_premium_monthly,
        pr.net_premium_monthly_rounded, pr.reserve_by_year[0], pr.reserve_by_year[n],
    ]
    # 라운드 V(t)
    if pr.reserve_by_year_rounded is not None:
        for t_v in range(min(6, n + 1)):
            items.append(f"V({t_v})_rounded")
            values.append(int(pr.reserve_by_year_rounded[t_v]))
    # G 정보
    if pr.gross_premium_annual is not None:
        items.extend(["", "--- G (기준SA 기준) ---", "",
                      "G_annual", "G_per_pay", "G_per_pay_rounded", "loading_ratio(G/P)"])
        values.extend(["", "", "",
                       pr.gross_premium_annual, pr.gross_premium_monthly,
                       pr.gross_premium_monthly_rounded, pr.loading_ratio])
    # multiplier 정보
    if pr.crit_join_amt is not None:
        items.extend(["", "--- multiplier ---", "",
                      "CRIT_JOIN_AMT", "actual_SA", "multiplier"])
        values.extend(["", "", "",
                       pr.crit_join_amt, float(mp.sum_assured[0]), pr.multiplier])
    summary = {"항목": items, "값": values}
    pd.DataFrame(summary).to_csv(os.path.join(debug_dir, "09_pricing_summary.csv"), index=False)

    # 10_gross_premium.csv: 영업보험료 산출 과정
    if pr.gross_premium_annual is not None and ee is not None:
        from cf_module.calc.commutation import _woolhouse_annuity
        t_acq = pr.acq_amort_period or m

        alpha = ee.fryy_gprem_acqs_rt
        beta_acqs = ee.inpay_gprem_acqs_rt
        beta_mnt = ee.inpay_gprem_mnt_rt
        gamma = ee.inpay_gprem_colm_rt if ee.inpay_gprem_colm_rt != 0.0 else 0.02
        delta = ee.inpay_gprem_loss_svyexp_rt
        alpha_sa = ee.fryy_join_amt_acqs_rt
        K = ee.inpay_fxamt_mntexp
        beta_afpay = ee.afpay_gprem_mnt_rt

        ax12_m = _woolhouse_annuity(ct, m, pr.pay_freq)
        ax12_n = _woolhouse_annuity(ct, n, pr.pay_freq)
        # 확정연금 ä_{m|} = 1 + v + ... + v^{m-1} — α 비율 환산용
        v_ = 1.0 / (1.0 + ct.i)
        a_certain_m = sum(v_ ** t for t in range(m)) if m > 0 else 1.0
        alpha_rate = alpha / a_certain_m if a_certain_m > 1e-12 else 0.0

        denom_factor = 1.0 - alpha_rate - beta_acqs - beta_mnt - gamma - delta
        denom_part1 = ax12_m * denom_factor
        denom_part3 = beta_afpay * (ax12_n - ax12_m)
        denominator = denom_part1 - denom_part3

        gp_items = [
            "mp_id", "",
            "--- 예정사업비율 (DB: IP_P_EXPCT_BIZEXP_RT) ---", "",
            "α  FRYY_GPREM_VS_ACQSEXP_RT (초년도 신계약비/영보)",
            "β_acqs  INPAY_GPREM_VS_ACQSEXP_RT (납입중 신계약비/영보)",
            "β_mnt  INPAY_GPREM_VS_MNTEXP_RT1 (납입중 유지비/영보)",
            "γ  INPAY_GPREM_VS_COLMEXP_RT1 (수금비/영보)",
            "δ  INPAY_GPREM_VS_LOSS_SVYEXP_RT (손해조사비/영보)",
            "α_sa  FRYY_JOIN_AMT_VS_ACQSEXP_RT (초년도 신계약비/가입금액)",
            "K  INPAY_FXAMT_MNTEXP (고정금액 유지비)",
            "β_afpay  AFPAY_GPREM_VS_MNTEXP_RT (납입후 유지비/영보)",
            "", "--- 연금현가 ---", "",
            "ä^(12)_{x:m}  납입기간 Woolhouse 연금현가",
            "ä_{m|}  확정연금 (1+v+...+v^{m-1})",
            "ä^(12)_{x:n}  보장기간 Woolhouse 연금현가",
            "α/ä_{m|}  α 연간 비율 환산",
            "", "--- G 산출: ä^(12)×(1-α/ä_{m|}-β-γ-δ) - β_afpay×(ä_n-ä_m) ---", "",
            "numerator = SA×(Ax+α_sa) + K×ä^(12)_{x:m}",
            "denom_part1 = ä^(12)_{x:m}×(1-α/ä_{m|}-β-γ-δ)",
            "denom_part3 = β_afpay×(ä^(12)_{x:n}-ä^(12)_{x:m})",
            "denominator = part1 - part3",
            "", "--- 결과 ---", "",
            "G_annual (영업보험료/연)",
            "G_monthly (영업보험료/월)",
            "P_annual (순보험료/연)",
            "loading_ratio (G/P)",
        ]
        gp_values = [
            mp_id, "",
            "", "",
            alpha, beta_acqs, beta_mnt, gamma, delta, alpha_sa, K, beta_afpay,
            "", "", "",
            ax12_m, a_certain_m, ax12_n, alpha_rate,
            "", "", "",
            pr.gross_premium_annual * denominator, denom_part1, denom_part3, denominator,
            "", "", "",
            pr.gross_premium_annual, pr.gross_premium_monthly,
            pr.net_premium_annual, pr.loading_ratio,
        ]
        df_gp = pd.DataFrame({"항목": gp_items, "값": gp_values})
        df_gp.to_csv(os.path.join(debug_dir, "10_gross_premium.csv"), index=False)

    # ── 11_pricing_pv.csv: 기수표 전체 + V(t) (재계산 가능) ──
    calc_sa = pr.crit_join_amt if pr.crit_join_amt else float(mp.sum_assured[0])

    pv_rows = []
    for t in range(n + 1):
        if ct.Dx[t] == 0.0:
            break

        # 연 단위 연금현가 ä_{x+t:m'}
        remain_pay = min(m, n) - t
        if remain_pay > 0:
            ax_ann_t = (ct.Nx[t] - ct.Nx[t + remain_pay]) / ct.Dx[t]
        else:
            ax_ann_t = 0.0

        # benefit Ax(t)
        if pr.has_maturity:
            Ax_t = (ct.Mx[t] - ct.Mx[n] + ct.Dx[n]) / ct.Dx[t]
        else:
            Ax_t = (ct.Mx[t] - ct.Mx[n]) / ct.Dx[t]

        ben_pv = Ax_t * calc_sa
        prem_pv = pr.net_premium_annual * ax_ann_t
        v_raw = pr.reserve_by_year[t]
        v_round = int(pr.reserve_by_year_rounded[t]) if pr.reserve_by_year_rounded is not None else None

        row = {
            # 파라미터 (재계산용 상수)
            "x": ct.x, "n": n, "m": m, "i": ct.i,
            "pay_freq": pr.pay_freq, "w_coeff": w_coeff,
            "has_maturity": pr.has_maturity, "SA": calc_sa,
            # 기수표
            "t": t, "age": ct.x + t,
            "qx_exit": ct.qx_exit[t] if t < n else None,
            "qx_benefit": ct.qx_benefit[t] if t < n else None,
            "px": (1.0 - ct.qx_exit[t]) if t < n else None,
            "lx": ct.lx[t],
            "v^t": (1.0 / (1.0 + ct.i)) ** t,
            "Dx": ct.Dx[t], "Nx": ct.Nx[t],
            "Cx": ct.Cx[t] if t < n else None,
            "Mx": ct.Mx[t],
            # 연금현가 (t 시점, 연 단위)
            "remain_pay": remain_pay,
            "Nx-Nx+t": ax_ann_t,
            # PV & V(t)
            "Mx-Mx+n": ct.Mx[t] - ct.Mx[n],
            "benefit_PV": ben_pv,
            "P_annual": pr.net_premium_annual,
            "premium_PV": prem_pv,
            "V_raw": v_raw, "V_round": v_round,
            # 1회 납입 순보험료 (Woolhouse 적용)
            "ax_woolhouse": pr.ax_due_monthly if t == 0 else None,
            "P_per_pay": pr.net_premium_monthly,
            "P_round": pr.net_premium_monthly_rounded,
        }
        pv_rows.append(row)

    pd.DataFrame(pv_rows).to_csv(os.path.join(debug_dir, "11_pricing_pv.csv"), index=False)

    # ── 12_expense_pv.csv: PV 수지상등 분해 ──
    if ee is not None and pr.gross_premium_annual is not None:
        from cf_module.calc.commutation import _woolhouse_annuity as _wa
        ax_m_pv = _wa(ct, m, pr.pay_freq)
        t_acq_pv = pr.acq_amort_period or m
        ax_t_pv = _wa(ct, t_acq_pv, pr.pay_freq)
        ax_n_pv = _wa(ct, n, pr.pay_freq)
        G_pv = pr.gross_premium_annual

        a_ = ee.fryy_gprem_acqs_rt
        a_sa_ = ee.fryy_join_amt_acqs_rt
        b_acq_ = ee.inpay_gprem_acqs_rt
        b_mnt_ = ee.inpay_gprem_mnt_rt
        g_rt_ = ee.inpay_gprem_colm_rt if ee.inpay_gprem_colm_rt != 0.0 else 0.02
        d_rt_ = ee.inpay_gprem_loss_svyexp_rt
        K_ = ee.inpay_fxamt_mntexp
        b_af_ = ee.afpay_gprem_mnt_rt

        # 확정연금 ä_{m|} = 1+v+...+v^{m-1}
        v_disc = 1.0 / (1.0 + ct.i)
        a_certain = sum(v_disc ** t for t in range(m)) if m > 0 else 1.0
        alpha_rate_ = a_ / a_certain if a_certain > 1e-12 else 0.0

        pv_inc = G_pv * ax_m_pv
        pv_ben = pr.Ax * calc_sa
        pv_a_g = alpha_rate_ * G_pv * ax_m_pv  # α/ä_{m|} × G × ä^(12)
        pv_a_sa = a_sa_ * calc_sa
        pv_b_acq = b_acq_ * G_pv * ax_m_pv
        pv_b_mnt = b_mnt_ * G_pv * ax_m_pv
        pv_colm = g_rt_ * G_pv * ax_m_pv
        pv_loss = d_rt_ * G_pv * ax_m_pv
        pv_fix = K_ * ax_m_pv
        pv_af = b_af_ * G_pv * (ax_n_pv - ax_m_pv)
        pv_exp = pv_ben + pv_a_g + pv_a_sa + pv_b_acq + pv_b_mnt + pv_colm + pv_loss + pv_fix + pv_af

        epv_rows = [
            ["[수입]", "", "", "", ""],
            ["영업보험료", "-", G_pv, ax_m_pv, pv_inc],
            ["", "", "", "", ""],
            ["[지출]", "", "", "", ""],
            ["급부(Ax*SA)", pr.Ax, calc_sa, "-", pv_ben],
            [f"신계약비(α/ä_{{{m}|}})", f"{alpha_rate_:.6f}", G_pv, ax_m_pv, pv_a_g],
            ["신계약비(SA)", a_sa_, calc_sa, "-", pv_a_sa],
            ["납입중신계약비", b_acq_, G_pv, ax_m_pv, pv_b_acq],
            ["유지비(영보)", b_mnt_, G_pv, ax_m_pv, pv_b_mnt],
            ["수금비", g_rt_, G_pv, ax_m_pv, pv_colm],
            ["손해조사비", d_rt_, G_pv, ax_m_pv, pv_loss],
            ["고정유지비", K_, "-", ax_m_pv, pv_fix],
            ["납입후유지비", b_af_, G_pv, ax_n_pv - ax_m_pv, pv_af],
            ["", "", "", "", ""],
            ["지출합계", "", "", "", pv_exp],
            ["차이(수입-지출)", "", "", "", pv_inc - pv_exp],
            ["", "", "", "", ""],
            [f"참고: α={a_:.4f}, ä_{{{m}|}}={a_certain:.6f}, α/ä={alpha_rate_:.6f}", "", "", "", ""],
        ]

        pd.DataFrame(epv_rows, columns=["항목", "비율", "금액", "연금현가", "PV"]).to_csv(
            os.path.join(debug_dir, "12_expense_pv.csv"), index=False)

    logger.info(f"pricing debug CSV 출력 완료: {debug_dir}")

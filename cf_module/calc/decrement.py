"""
탈퇴율(Decrement) 모듈

사망률(qx), 해약률(wx), 실효율 등 탈퇴율을 계산하고
생존확률(tpx), 탈퇴자수를 산출한다.

실제 DB 키 매칭 기반:
- 위험률: MortalityKeyBuilder + BEPRD_DEFRY_RT + 월변환
- 해약률: AssumptionKeyBuilder('해지율') + 납입기간 분기
- 스큐: AssumptionKeyBuilder('스큐') + 24개월 분기

모든 계산은 numpy 2D 배열 기반: shape (n_points, n_steps)
"""

from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from cf_module.calc.timing import TimingResult
from cf_module.config import CFConfig, ScenarioConfig
from cf_module.data.assumptions import AssumptionSet, MortalityTable, SkewTable, LapseTable
from cf_module.data.model_point import ModelPointSet
from cf_module.utils.logger import get_logger

logger = get_logger("decrement")

MONTHS_IN_YEAR = 12


@dataclass
class DecrementResult:
    """탈퇴율 결과

    모든 배열: shape (n_points, n_steps)

    Attributes:
        qx_annual: 연간 사망률
        qx_monthly: 월간 사망률
        wx_annual: 연간 해약률
        wx_monthly: 월간 해약률
        dx_monthly: 월간 총 탈퇴율 (사망+해약)
        tpx: 기시 생존확률 (t시점 초 기준)
        d_death: 사망 탈퇴자 비율 (= d_rsvamt + d_bnft, 호환성 유지)
        d_lapse: 해약 탈퇴자 비율
        d_survive: 생존자 비율 (기말)
        d_rsvamt: CTR 준비금사망탈퇴자 (tpx_bot * Σ dedup rates where rsvamt_yn=1)
        d_bnft: CTR 급부탈퇴자 (tpx_bot * Σ dedup rates where bnft_yn=1)
        qx_be_by_risk: (n_steps, n_risks) 위험률코드별 BE 월간위험률
        rsk_rt_cd: (n_risks,) 위험률코드 목록
        mm_trf_way_cd: (n_risks,) 월변환방식
        skew: (n_steps,) 스큐 값
    """
    qx_annual: np.ndarray
    qx_monthly: np.ndarray
    wx_annual: np.ndarray
    wx_monthly: np.ndarray
    dx_monthly: np.ndarray
    tpx: np.ndarray
    d_death: np.ndarray
    d_lapse: np.ndarray
    d_survive: np.ndarray
    # CTR 세분화 (d_death = d_rsvamt + d_bnft)
    d_rsvamt: Optional[np.ndarray] = None             # CTR 준비금사망탈퇴자
    d_bnft: Optional[np.ndarray] = None               # CTR 급부탈퇴자
    qx_be_by_risk: Optional[np.ndarray] = None      # (n_steps, n_risks) BE 월간위험률
    rsk_rt_cd: Optional[np.ndarray] = None            # (n_risks,) 위험률코드
    mm_trf_way_cd: Optional[np.ndarray] = None        # (n_risks,) 월변환방식
    skew: Optional[np.ndarray] = None                 # (n_steps,) 스큐
    qx_raw_by_risk: Optional[np.ndarray] = None       # (n_steps, n_risks) 원수위험률 (BE 적용 전)
    qx_be_annual_by_risk: Optional[np.ndarray] = None # (n_steps, n_risks) BE 연간위험률 (월변환 전)
    # PAY (납입자) 관련 필드
    pay_tpx: Optional[np.ndarray] = None              # PAY EOT survivors
    pay_d_rsvamt: Optional[np.ndarray] = None          # PAY 준비금탈퇴자
    pay_d_bnft: Optional[np.ndarray] = None            # PAY 급부탈퇴자
    pay_d_pyexsp: Optional[np.ndarray] = None          # PAY 납입면제탈퇴자
    pay_d_lapse: Optional[np.ndarray] = None           # PAY lapse exits (BOT*wx)
    pay_d_survive: Optional[np.ndarray] = None         # PAY survivors
    pay_qx_monthly: Optional[np.ndarray] = None        # PAY dedup exit rate sum
    pay_dx_monthly: Optional[np.ndarray] = None        # PAY total exit rate
    pay_qx_be_by_risk: Optional[np.ndarray] = None     # PAY dedup rates per code


def build_decrement(
    mp: ModelPointSet,
    timing: TimingResult,
    assumptions: AssumptionSet,
    scenario: ScenarioConfig,
    reader=None,
    config: Optional[CFConfig] = None,
) -> DecrementResult:
    """탈퇴율을 산출한다.

    Args:
        mp: Model Point
        timing: 시간축
        assumptions: 가정 데이터
        scenario: 시나리오 설정 (충격 배수 등)
        reader: DataReader (DB 모드에서 키 빌더 사용 시)
        config: CFConfig (DB 모드에서 키 빌더 사용 시)

    Returns:
        DecrementResult
    """
    n = mp.n_points
    s = timing.n_steps
    logger.debug("[DEC] -- 탈퇴율 산출 시작: n=%d, steps=%d --", n, s)

    if s == 0:
        empty = np.zeros((n, 0), dtype=np.float64)
        return DecrementResult(
            qx_annual=empty, qx_monthly=empty,
            wx_annual=empty, wx_monthly=empty,
            dx_monthly=empty, tpx=empty,
            d_death=empty, d_lapse=empty, d_survive=empty,
        )

    use_db_mode = (
        reader is not None
        and config is not None
        and assumptions.mortality.raw_val is not None
    )
    logger.debug("[DEC] mode=%s", "DB키매칭" if use_db_mode else "스텁(stub)")

    # -- [1] 사망률 매핑 (코드별 BE 월간) --
    logger.debug("[DEC] -- [1] 사망률(qx) 매핑 시작 --")
    qx_raw_by_risk = None
    qx_be_annual_by_risk = None
    qx_be_by_risk = None
    rsk_rt_cd_arr = None
    mm_trf_arr = None

    if use_db_mode:
        rsk_rt_be_monthly, rsk_rt_cd_arr, mm_trf_arr, qx_raw_by_risk, qx_be_annual_by_risk = _map_mortality_db(
            mp, timing, assumptions, reader, config
        )
        qx_be_by_risk = rsk_rt_be_monthly  # (n_steps, n_risks) 코드별 BE 월간
        qx_annual = np.tile(np.sum(qx_be_annual_by_risk, axis=1), (n, 1))
        logger.debug("[DEC] qx_be_by_risk: shape=%s (코드별, 중복제거 전)", qx_be_by_risk.shape)
    else:
        qx_annual = _map_mortality(mp, timing, assumptions.mortality, scenario)
        qx_monthly = _annual_to_monthly(qx_annual)
        logger.debug("[DEC] qx_monthly(stub): shape=%s", qx_monthly.shape)

    # -- [2] 해약률 매핑 --
    logger.debug("[DEC] -- [2] 해약률(wx) 매핑 시작 --")
    if use_db_mode:
        wx_annual = _map_lapse_db(mp, timing, assumptions, reader, config)
    else:
        wx_annual = _map_lapse(mp, timing, assumptions.lapse, assumptions.skew, scenario)
    wx_monthly = _annual_to_monthly(wx_annual)
    logger.debug("[DEC] wx_monthly: shape=%s, 처음4개월=%s",
                 wx_monthly.shape, wx_monthly[0, :4].tolist())

    # -- [3] 스큐 매핑 --
    logger.debug("[DEC] -- [3] 스큐(skew) 매핑 시작 --")
    skew_arr = None
    if use_db_mode:
        skew_arr = _map_skew_db(mp, timing, assumptions, reader, config)

    if skew_arr is not None:
        logger.debug("[DEC] skew: 처음4개월=%s", skew_arr[:4].tolist() if len(skew_arr) >= 4 else skew_arr.tolist())
    else:
        logger.debug("[DEC] skew: None (미적용)")

    # 시나리오 배수 적용
    if use_db_mode:
        qx_be_by_risk = qx_be_by_risk * scenario.mortality_multiplier
    wx_monthly *= scenario.lapse_multiplier

    # 보험기간 외 마스킹 (wx)
    wx_monthly = np.where(timing.is_in_force, wx_monthly, 0.0)

    # -- [4] 중복제거 위험률 산출 --
    d_rsvamt = d_bnft = None
    pay_tpx = pay_d_lapse = pay_d_survive = None
    pay_d_rsvamt = pay_d_bnft = pay_d_pyexsp = None
    pay_qx_monthly_arr = pay_dx_monthly_arr = pay_qx_be_by_risk = None

    if use_db_mode:
        logger.debug("[DEC] -- [4] 중복제거 위험률 산출 --")
        mort = assumptions.mortality

        # 탈퇴 위험률 플래그
        rsvamt_yn = mort.rsvamt_defry_yn if mort.rsvamt_defry_yn is not None else np.zeros(len(rsk_rt_cd_arr), dtype=int)
        bnft_yn = mort.bnft_drpo_yn if mort.bnft_drpo_yn is not None else np.zeros(len(rsk_rt_cd_arr), dtype=int)
        pyexsp_yn = mort.pyexsp_drpo_yn if mort.pyexsp_drpo_yn is not None else np.zeros(len(rsk_rt_cd_arr), dtype=int)
        is_exit_ctr = ((rsvamt_yn == 1) | (bnft_yn == 1)).astype(int)
        is_exit_pay = ((rsvamt_yn == 1) | (bnft_yn == 1) | (pyexsp_yn == 1)).astype(int)

        dead_rt_dvcd = mort.dead_rt_dvcd if mort.dead_rt_dvcd is not None else np.zeros(len(rsk_rt_cd_arr), dtype=int)
        rsk_grp_no = mort.rsk_grp_no if mort.rsk_grp_no is not None else np.zeros(len(rsk_rt_cd_arr), dtype=int)

        logger.debug("[DEC] 탈퇴대상: CTR is_exit=%s, PAY is_exit=%s",
                     is_exit_ctr.tolist(), is_exit_pay.tolist())
        logger.debug("[DEC]   RSVAMT=%s, BNFT=%s, PYEXSP=%s",
                     rsvamt_yn.tolist(), bnft_yn.tolist(), pyexsp_yn.tolist())
        logger.debug("[DEC] dead_rt_dvcd=%s, rsk_grp_no=%s",
                     dead_rt_dvcd.tolist(), rsk_grp_no.tolist())

        # 중복제거 전 원본 보존 (PAY에서 재사용)
        qx_be_by_risk_orig = qx_be_by_risk.copy()
        wx_monthly_orig_1d = wx_monthly[0, :].copy()

        # [4-1] CTR 중복제거 (단건 i=0 기준)
        logger.debug("[DEC] -- [4-1] CTR 중복제거 --")
        wx_deduped_1d, qx_deduped_by_risk = _calc_dedup_rates(
            wx_monthly_orig_1d,
            qx_be_by_risk_orig,
            dead_rt_dvcd,
            rsk_grp_no,
            is_exit_ctr,
        )

        # qx_monthly: 탈퇴 대상 코드의 중복제거 합계
        qx_total_1d = np.sum(qx_deduped_by_risk[:, is_exit_ctr == 1], axis=1)  # (n_steps,)
        qx_monthly = np.tile(qx_total_1d, (n, 1))
        qx_monthly = np.clip(qx_monthly, 0.0, 1.0)

        # wx_monthly: 중복제거 후
        wx_monthly = np.tile(wx_deduped_1d, (n, 1))

        # 보험기간 외 마스킹
        qx_monthly = np.where(timing.is_in_force, qx_monthly, 0.0)
        wx_monthly = np.where(timing.is_in_force, wx_monthly, 0.0)

        # qx_be_by_risk도 중복제거 버전으로 갱신
        qx_be_by_risk = qx_deduped_by_risk

        # CTR 세분화: rsvamt_yn / bnft_yn별 중복제거 합산율
        rsvamt_idx = np.where(rsvamt_yn == 1)[0]
        bnft_idx = np.where(bnft_yn == 1)[0]
        ctr_rsvamt_rt = np.sum(qx_deduped_by_risk[:, rsvamt_idx], axis=1) if len(rsvamt_idx) > 0 else np.zeros(s)
        ctr_bnft_rt = np.sum(qx_deduped_by_risk[:, bnft_idx], axis=1) if len(bnft_idx) > 0 else np.zeros(s)

        # [4-2] PAY 중복제거 (PYEXSP 추가)
        logger.debug("[DEC] -- [4-2] PAY 중복제거 --")
        pay_wx_deduped_1d, pay_qx_deduped_by_risk = _calc_dedup_rates(
            wx_monthly_orig_1d,
            qx_be_by_risk_orig,
            dead_rt_dvcd,
            rsk_grp_no,
            is_exit_pay,
        )
        pay_qx_be_by_risk = pay_qx_deduped_by_risk

        # PAY qx_monthly: PAY 탈퇴 대상 코드의 중복제거 합계
        pay_qx_total_1d = np.sum(pay_qx_deduped_by_risk[:, is_exit_pay == 1], axis=1)
        pay_qx_monthly_2d = np.tile(pay_qx_total_1d, (n, 1))
        pay_qx_monthly_2d = np.clip(pay_qx_monthly_2d, 0.0, 1.0)
        pay_qx_monthly_2d = np.where(timing.is_in_force, pay_qx_monthly_2d, 0.0)

        # PAY wx_monthly: 중복제거 후
        pay_wx_monthly_2d = np.tile(pay_wx_deduped_1d, (n, 1))
        pay_wx_monthly_2d = np.where(timing.is_in_force, pay_wx_monthly_2d, 0.0)

        # PAY 세분화: rsvamt / bnft / pyexsp_only별 중복제거 합산율
        pyexsp_only_idx = np.where((pyexsp_yn == 1) & (rsvamt_yn != 1) & (bnft_yn != 1))[0]
        pay_rsvamt_rt = np.sum(pay_qx_deduped_by_risk[:, rsvamt_idx], axis=1) if len(rsvamt_idx) > 0 else np.zeros(s)
        pay_bnft_rt = np.sum(pay_qx_deduped_by_risk[:, bnft_idx], axis=1) if len(bnft_idx) > 0 else np.zeros(s)
        pay_pyexsp_rt = np.sum(pay_qx_deduped_by_risk[:, pyexsp_only_idx], axis=1) if len(pyexsp_only_idx) > 0 else np.zeros(s)

        # PAY dx, tpx, d_lapse, d_survive
        pay_dx_monthly_2d = np.clip(pay_qx_monthly_2d + pay_wx_monthly_2d, 0.0, 1.0)
        pay_survive_monthly = 1.0 - pay_dx_monthly_2d
        pay_tpx = np.ones((n, s), dtype=np.float64)
        if s > 1:
            pay_tpx[:, 1:] = np.cumprod(pay_survive_monthly[:, 1:], axis=1)
        pay_tpx_bot = np.ones((n, s), dtype=np.float64)
        if s > 1:
            pay_tpx_bot[:, 1:] = pay_tpx[:, :-1]
        pay_d_lapse = pay_tpx_bot * pay_wx_monthly_2d
        pay_d_survive = pay_tpx_bot * pay_survive_monthly

        # PAY 세분화 탈퇴자
        pay_d_rsvamt = pay_tpx_bot * np.tile(pay_rsvamt_rt, (n, 1))
        pay_d_bnft = pay_tpx_bot * np.tile(pay_bnft_rt, (n, 1))
        pay_d_pyexsp = pay_tpx_bot * np.tile(pay_pyexsp_rt, (n, 1))

        pay_qx_monthly_arr = pay_qx_monthly_2d
        pay_dx_monthly_arr = pay_dx_monthly_2d

        # PAY 로그
        show_pay = min(s, 4)
        logger.debug("[DEC]")
        logger.debug("[DEC] --- PAY t별 계산 흐름 (i=0, t=0~%d) ---", show_pay - 1)
        logger.debug("[DEC]   %-4s  %-12s  %-12s  %-12s  %-12s  %-12s  %-12s  %-12s  %-12s  %-12s  %-12s",
                     "t", "pay_qx", "pay_wx", "pay_dx", "pay_tpx_bot", "pay_tpx",
                     "pay_d_rsv", "pay_d_bnf", "pay_d_pyx", "pay_d_lps", "pay_d_srv")
        for t in range(show_pay):
            logger.debug(
                "[DEC]   %-4d  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f",
                t, pay_qx_monthly_2d[0, t], pay_wx_monthly_2d[0, t], pay_dx_monthly_2d[0, t],
                pay_tpx_bot[0, t], pay_tpx[0, t],
                pay_d_rsvamt[0, t], pay_d_bnft[0, t], pay_d_pyexsp[0, t],
                pay_d_lapse[0, t], pay_d_survive[0, t],
            )
        if s > show_pay:
            logger.debug("[DEC]   ... (이하 %d시점 생략)", s - show_pay)
    else:
        # 스텁 모드: 기존 로직
        if not use_db_mode:
            pass  # scenario multiplier already applied in _map_mortality
        qx_monthly = np.where(timing.is_in_force, qx_monthly, 0.0)

    # -- 총 탈퇴율 (가산 모델: dx = qx + wx) --
    dx_monthly = qx_monthly + wx_monthly
    dx_monthly = np.clip(dx_monthly, 0.0, 1.0)

    # -- 생존확률: tpx[t] = TRME[t] (기말 유지자수) --
    # t=0은 평가시점(lastday)이므로 탈퇴 없음: tpx[0]=1.0
    # tpx[t+1] = tpx[t] * (1 - dx[t+1])  (t+1 시점의 dx 적용)
    survive_monthly = 1.0 - dx_monthly
    tpx = np.ones((n, s), dtype=np.float64)
    if s > 1:
        tpx[:, 1:] = np.cumprod(survive_monthly[:, 1:], axis=1)

    # -- 기시유지자수 (TRMO): tpx_bot[t] = tpx[t-1] --
    # 탈퇴자 = 기시유지자 × 탈퇴율 (BOT survivors × rate)
    # DB: TRMPSN[t] = TRMO[t] * TRMNAT_RT[t]
    tpx_bot = np.ones((n, s), dtype=np.float64)
    if s > 1:
        tpx_bot[:, 1:] = tpx[:, :-1]

    d_death = tpx_bot * qx_monthly
    d_lapse = tpx_bot * wx_monthly
    d_survive = tpx_bot * survive_monthly

    # CTR 세분화 탈퇴자 (DB 모드에서만)
    if use_db_mode:
        d_rsvamt = tpx_bot * np.tile(ctr_rsvamt_rt, (n, 1))
        d_bnft = tpx_bot * np.tile(ctr_bnft_rt, (n, 1))

    # t=0~3 시점별 계산 흐름 요약
    show = min(s, 4)
    logger.debug("[DEC]")
    logger.debug("[DEC] --- t별 계산 흐름 (i=0, t=0~%d) ---", show - 1)
    logger.debug("[DEC]   %-4s  %-12s  %-12s  %-12s  %-12s  %-12s  %-12s  %-12s  %-12s", "t", "qx_mon", "wx_mon", "dx_mon", "tpx_bot", "tpx(EOT)", "d_death", "d_lapse", "d_survive")
    for t in range(show):
        logger.debug(
            "[DEC]   %-4d  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f  %-12.8f",
            t, qx_monthly[0, t], wx_monthly[0, t], dx_monthly[0, t],
            tpx_bot[0, t], tpx[0, t], d_death[0, t], d_lapse[0, t], d_survive[0, t],
        )
    if s > show:
        logger.debug("[DEC]   ... (이하 %d시점 생략)", s - show)
    logger.debug("[DEC]")
    logger.debug("[DEC] -- 탈퇴율 산출 완료 --")

    return DecrementResult(
        qx_annual=qx_annual,
        qx_monthly=qx_monthly,
        wx_annual=wx_annual,
        wx_monthly=wx_monthly,
        dx_monthly=dx_monthly,
        tpx=tpx,
        d_death=d_death,
        d_lapse=d_lapse,
        d_survive=d_survive,
        d_rsvamt=d_rsvamt,
        d_bnft=d_bnft,
        qx_be_by_risk=qx_be_by_risk,
        rsk_rt_cd=rsk_rt_cd_arr,
        mm_trf_way_cd=mm_trf_arr,
        skew=skew_arr,
        qx_raw_by_risk=qx_raw_by_risk,
        qx_be_annual_by_risk=qx_be_annual_by_risk,
        pay_tpx=pay_tpx,
        pay_d_rsvamt=pay_d_rsvamt,
        pay_d_bnft=pay_d_bnft,
        pay_d_pyexsp=pay_d_pyexsp,
        pay_d_lapse=pay_d_lapse,
        pay_d_survive=pay_d_survive,
        pay_qx_monthly=pay_qx_monthly_arr,
        pay_dx_monthly=pay_dx_monthly_arr,
        pay_qx_be_by_risk=pay_qx_be_by_risk,
    )


def _annual_to_monthly(annual_rate: np.ndarray) -> np.ndarray:
    """연율을 월율로 변환한다. 1 - (1 - q)^(1/12)"""
    return 1.0 - np.power(np.maximum(1.0 - annual_rate, 0.0), 1.0 / MONTHS_IN_YEAR)


# =========================================================================
# 중복제거 위험률 산출
# =========================================================================

def _calc_dedup_rates(
    wx_monthly_1d: np.ndarray,
    qx_be_monthly: np.ndarray,
    dead_rt_dvcd: np.ndarray,
    rsk_grp_no: np.ndarray,
    is_exit: np.ndarray,
):
    """중복탈퇴 위험률을 산출한다.

    q'ᵢ = qᵢ × (1 - Σⱼ(qⱼ × Cᵢⱼ) / 2)
    Cᵢⱼ = 0: ①자기자신, ②동일위험그룹, ③j가 사망위험
    그 외 Cᵢⱼ = 1

    Args:
        wx_monthly_1d: (n_steps,) 월간 해지율
        qx_be_monthly: (n_steps, n_risks) 코드별 BE 월간 위험률
        dead_rt_dvcd:  (n_risks,) 0=사망, 1=비사망
        rsk_grp_no:    (n_risks,) 위험그룹번호
        is_exit:       (n_risks,) 탈퇴위험률 여부 (RSVAMT/BNFT DRPO YN=1)

    Returns:
        (wx_deduped, qx_deduped_by_risk)
        - wx_deduped: (n_steps,) 중복제거 해지율
        - qx_deduped_by_risk: (n_steps, n_risks) 중복제거 위험률 (코드별)
          탈퇴 비대상 코드는 원래 값 유지 (중복제거 미적용)
    """
    n_steps, n_risks = qx_be_monthly.shape

    # 탈퇴 위험률만 추출
    exit_idx = np.where(is_exit)[0]
    n_exit = len(exit_idx)

    if n_exit == 0:
        # 탈퇴 위험률 없음 → 해지율만 그대로
        return wx_monthly_1d.copy(), qx_be_monthly.copy()

    n_rates = 1 + n_exit  # w + 탈퇴 qi들

    # r: (n_steps, n_rates)
    r = np.zeros((n_steps, n_rates), dtype=np.float64)
    r[:, 0] = wx_monthly_1d
    r[:, 1:] = qx_be_monthly[:, exit_idx]

    # d: (n_rates,) — DEAD_RT_DVCD (해지=1 강제)
    d = np.zeros(n_rates, dtype=np.float64)
    d[0] = 1.0
    d[1:] = dead_rt_dvcd[exit_idx].astype(np.float64)

    # g: (n_rates,) — RSK_GRP_NO (해지=0 강제)
    g = np.zeros(n_rates, dtype=int)
    g[0] = 0
    g[1:] = rsk_grp_no[exit_idx].astype(int)

    # C 행렬: (n_rates, n_rates)
    C = np.ones((n_rates, n_rates), dtype=np.float64)
    np.fill_diagonal(C, 0.0)                          # ① 자기자신
    C *= (g[:, None] != g[None, :]).astype(np.float64) # ② 동일위험그룹
    C *= d[None, :]                                    # ③ j가 사망 → column 0

    # 중복제거: adjustment[t,i] = Σⱼ r[t,j] × C[i,j] = (r @ C.T)[t,i]
    # C가 비대칭(③조건이 column mask)이므로 C.T 필요
    adjustment = r @ C.T                  # (n_steps, n_rates)
    r_deduped = r * (1.0 - adjustment / 2.0)

    # 결과 분리
    wx_deduped = r_deduped[:, 0]

    # 전체 risk 배열에 deduped 값 기록 (비탈퇴 코드는 원래 값 유지)
    qx_deduped = qx_be_monthly.copy()
    qx_deduped[:, exit_idx] = r_deduped[:, 1:]

    # 로그
    logger.debug("[DEC-DEDUP] C 행렬 (%d x %d):", n_rates, n_rates)
    for row_i in range(n_rates):
        label = "w" if row_i == 0 else str(exit_idx[row_i - 1])
        logger.debug("[DEC-DEDUP]   [%s] %s", label, C[row_i, :].tolist())

    show_t = min(n_steps, 4)
    logger.debug("[DEC-DEDUP]")
    logger.debug("[DEC-DEDUP] --- 중복제거 전/후 (t=0~%d) ---", show_t - 1)
    logger.debug("[DEC-DEDUP]   %-4s  %-14s  %-14s  %-14s  %-14s",
                 "t", "wx_before", "wx_after", "qx_rsvamt_bef", "qx_rsvamt_aft")
    for t in range(show_t):
        qx_sum_before = np.sum(qx_be_monthly[t, exit_idx])
        qx_sum_after = np.sum(r_deduped[t, 1:])
        logger.debug("[DEC-DEDUP]   %-4d  %-14.10f  %-14.10f  %-14.10f  %-14.10f",
                     t, wx_monthly_1d[t], wx_deduped[t], qx_sum_before, qx_sum_after)

    return wx_deduped, qx_deduped


# =========================================================================
# DB 모드: 실제 키 매칭 기반 매핑
# =========================================================================

def _map_mortality_db(
    mp: ModelPointSet,
    timing: TimingResult,
    assumptions: AssumptionSet,
    reader,
    config: CFConfig,
):
    """실제 DB 키 매칭 기반 위험률 매핑 (qx_read.py + qx_rate_table.py 로직)

    Returns:
        (qx_monthly, qx_be_by_risk, rsk_rt_cd, mm_trf_way_cd, qx_raw_by_risk, qx_be_annual_by_risk)
        - qx_monthly: (n_points, n_steps) 총 월간 사망률
        - qx_be_by_risk: (n_steps, n_risks) 위험률코드별 BE 월간위험률
        - rsk_rt_cd: (n_risks,) 위험률코드 목록
        - mm_trf_way_cd: (n_risks,) 월변환방식
        - qx_raw_by_risk: (n_steps, n_risks) 원수위험률 (BE 적용 전)
        - qx_be_annual_by_risk: (n_steps, n_risks) BE 연간위험률 (월변환 전)
    """
    from cf_module.data.assm_key_builder import AssumptionKeyBuilder, MortalityKeyBuilder

    mortality = assumptions.mortality
    n = mp.n_points
    s = timing.n_steps

    # 단건 처리 (i=0 기준, 다건 확장 가능)
    i = 0
    age = int(mp.age_at_entry[i])
    bterm = int(mp.bterm[i])
    duration_values = timing.duration_years[i, :].astype(int)

    logger.debug("[DEC-QX] DB모드: i=%d, age=%d, bterm=%d", i, age, bterm)

    # 1. MortalityKeyBuilder로 복합키 구축
    rsk_rt_div_vals = mp.rsk_rt_div_vals if mp.rsk_rt_div_vals is not None else np.full((n, 10), "", dtype=object)
    risk_keys, range_qx_code, range_qx_info = MortalityKeyBuilder.build_risk_keys(
        mortality.raw_chr, rsk_rt_div_vals[i:i+1, :]
    )
    logger.debug("[DEC-QX] risk_keys(%d): %s", len(risk_keys), list(risk_keys[:5]))

    # 2. match_rates
    rsk_rt = MortalityKeyBuilder.match_rates(
        risk_keys, range_qx_info, mortality.raw_val,
        age, bterm, duration_values,
    )
    # rsk_rt: (n_steps, n_risks)
    logger.debug("[DEC-QX] rsk_rt shape=%s", rsk_rt.shape)

    # 3. BEPRD_DEFRY_RT 매핑
    rsk_rt_cd_list = mortality.rsk_rt_cd
    beprd = _get_beprd_for_risks(
        mp, assumptions, reader, config, rsk_rt_cd_list, duration_values, i
    )
    # beprd: (n_steps, n_risks) 또는 ones

    logger.debug("[DEC-QX] BEPRD shape=%s", beprd.shape)

    # 4. rsk_rt_be = rsk_rt * BEPRD_DEFRY_RT
    rsk_rt_be = rsk_rt * beprd

    # 5. 월변환 (MM_TRF_WAY_CD별)
    mm_trf_raw = range_qx_info[:, 2]  # (n_risks,)
    # int/string 혼용 대응: 안전하게 int 변환
    mm_trf = np.array([int(x) if x is not None and str(x).strip() else 0 for x in mm_trf_raw])
    c1 = mm_trf == 1   # 연률 → 월률: 1-(1-q)^(1/12)
    c2 = mm_trf == 2   # 월율/12

    rsk_rt_be_monthly = rsk_rt_be.copy()
    if np.any(c1):
        rsk_rt_be_monthly[:, c1] = 1.0 - (1.0 - rsk_rt_be[:, c1]) ** (1.0 / 12.0)
    if np.any(c2):
        rsk_rt_be_monthly[:, c2] = rsk_rt_be[:, c2] / 12.0

    # -- 위험률코드별 계산 흐름 로그 (t=0~3) --
    show_t = min(s, 4)
    n_risks = len(rsk_rt_cd_list)
    logger.debug("[DEC-QX]")
    logger.debug("[DEC-QX] --- 위험률코드별 계산 흐름 (t=0~%d, %d개 코드) ---", show_t - 1, n_risks)
    for j in range(n_risks):
        rsk_cd = rsk_rt_cd_list[j]
        trf = mm_trf[j]
        trf_label = {1: "1-(1-q)^(1/12)", 2: "q/12", 0: "직접"}
        logger.debug("[DEC-QX]   RSK_RT_CD=%s  mm_trf=%d(%s)", rsk_cd, trf, trf_label.get(trf, "?"))
        logger.debug("[DEC-QX]     %-4s  %-14s  %-14s  %-14s  %-14s",
                     "t", "RSK_RT(raw)", "BEPRD", "BE_annual", "BE_monthly")
        for t in range(show_t):
            logger.debug("[DEC-QX]     %-4d  %-14.10f  %-14.10f  %-14.10f  %-14.10f",
                         t, rsk_rt[t, j], beprd[t, j], rsk_rt_be[t, j], rsk_rt_be_monthly[t, j])
        if s > show_t:
            logger.debug("[DEC-QX]     ... (이하 %d시점 생략)", s - show_t)

    # 6. 코드별 BE 월간 결과만 반환 (합산은 중복제거 후 build_decrement에서)
    return rsk_rt_be_monthly, rsk_rt_cd_list, mm_trf, rsk_rt, rsk_rt_be


def _get_beprd_for_risks(
    mp, assumptions, reader, config, rsk_rt_cd_list, duration_values, point_idx=0
):
    """위험별 BEPRD_DEFRY_RT를 조회한다."""
    from cf_module.data.assm_key_builder import AssumptionKeyBuilder

    n_steps = len(duration_values)
    n_risks = len(rsk_rt_cd_list)

    # BEPRD raw 데이터가 없으면 1로 채움
    if assumptions.beprd_raw_keys is None or assumptions.beprd_raw_data is None:
        return np.ones((n_steps, n_risks), dtype=np.float64)

    # AssumptionKeyBuilder로 BEPRD 키 구축
    assm_div_vals = mp.assm_div_vals if mp.assm_div_vals is not None else np.full((mp.n_points, 15), "", dtype=object)

    key_builder = AssumptionKeyBuilder(reader, config.runset.assm_grp_id)
    keys, resolved = key_builder.build_keys_for_type(
        "경과년도별지급률",
        mp.product_cd[point_idx:point_idx+1],
        mp.cls_cd[point_idx:point_idx+1],
        assm_div_vals[point_idx:point_idx+1, :],
        rsk_rt_cd_list=rsk_rt_cd_list,
    )
    # keys: (n_risks,)

    # raw_keys에서 매칭
    beprd_find_keys = assumptions.beprd_raw_keys
    beprd_raw_data = assumptions.beprd_raw_data

    n_val_cols = beprd_raw_data.shape[1] - 20  # 키 18열 + 후미 2열 제외
    if n_val_cols <= 0:
        return np.ones((n_steps, n_risks), dtype=np.float64)

    result = np.ones((n_risks, n_val_cols), dtype=np.float64)
    for risk_idx, key in enumerate(keys):
        match_idx = np.where(beprd_find_keys == key)[0]
        if len(match_idx) > 0:
            result[risk_idx, :] = beprd_raw_data[match_idx[0], 18:-2].astype(np.float64)
            logger.debug("[DEC-BEPRD] RSK_RT_CD=%s  key=%s  match_row=%d",
                         rsk_rt_cd_list[risk_idx], key, match_idx[0])
        else:
            logger.debug("[DEC-BEPRD] RSK_RT_CD=%s  key=%s  ** NO MATCH (default=1.0) **",
                         rsk_rt_cd_list[risk_idx], key)

    # duration_values로 인덱싱: (n_risks, max_dur) → (n_steps, n_risks)
    dur_idx = np.clip(duration_values - 1, 0, result.shape[1] - 1)
    beprd_out = result[:, dur_idx].T  # (n_steps, n_risks)

    # BEPRD 매칭 결과 로그 (위험률코드별 처음4시점)
    show_t = min(n_steps, 4)
    logger.debug("[DEC-BEPRD] dur_idx[:4]=%s → BEPRD 값:", dur_idx[:show_t].tolist())
    for j in range(n_risks):
        vals = [beprd_out[t, j] for t in range(show_t)]
        logger.debug("[DEC-BEPRD]   RSK_RT_CD=%s  BEPRD[:4]=%s",
                     rsk_rt_cd_list[j], ["%.10f" % v for v in vals])

    return beprd_out


def _map_lapse_db(
    mp: ModelPointSet,
    timing: TimingResult,
    assumptions: AssumptionSet,
    reader,
    config: CFConfig,
) -> np.ndarray:
    """실제 DB 키 매칭 기반 해약률 매핑 (lapse.py 로직)

    Returns:
        wx_annual: (n_points, n_steps)
    """
    from cf_module.data.assm_key_builder import AssumptionKeyBuilder

    n = mp.n_points
    s = timing.n_steps
    wx = np.zeros((n, s), dtype=np.float64)

    lapse = assumptions.lapse
    if lapse.raw_data is None or lapse.raw_keys is None:
        return wx

    assm_div_vals = mp.assm_div_vals if mp.assm_div_vals is not None else np.full((n, 15), "", dtype=object)

    key_builder = AssumptionKeyBuilder(reader, config.runset.assm_grp_id)

    logger.debug("[DEC-WX] DB모드 해약률 매핑: n=%d", n)
    for i in range(n):
        pterm_i = int(mp.pterm[i])
        in_force = timing.is_in_force[i, :]
        duration_values = timing.duration_years[i, :].astype(int)

        # 납입기간(pay_type='1') 키
        keys_pay, _ = key_builder.build_keys_for_type(
            "해지율",
            mp.product_cd[i:i+1],
            mp.cls_cd[i:i+1],
            assm_div_vals[i:i+1, :],
            pay_type="1",
        )
        # 납입후(pay_type='2') 키
        keys_npay, _ = key_builder.build_keys_for_type(
            "해지율",
            mp.product_cd[i:i+1],
            mp.cls_cd[i:i+1],
            assm_div_vals[i:i+1, :],
            pay_type="2",
        )

        assm_find_key = lapse.raw_keys
        raw_data = lapse.raw_data

        # 납입기간 해약률
        match1 = np.where(assm_find_key == keys_pay[0])[0]
        # 납입후 해약률
        match2 = np.where(assm_find_key == keys_npay[0])[0]

        logger.debug("[DEC-WX] #%d key_pay=%s → match=%d, key_npay=%s → match=%d",
                     i, keys_pay[0], len(match1), keys_npay[0], len(match2))

        if len(match1) > 0 and len(match2) > 0:
            lapse_rate_1 = raw_data[match1[0], 20:-2].astype(np.float64)
            lapse_rate_2 = raw_data[match2[0], 20:-2].astype(np.float64)

            # out-of-force 타임스텝 duration=0을 안전하게 처리
            dur_safe = np.maximum(duration_values, 1)

            # 납입기간/납입후 마스크 (in_force 내에서만)
            pay_mask = (dur_safe <= pterm_i) & in_force
            npay_mask = (dur_safe > pterm_i) & in_force

            # 납입기간 해약률: duration_year-1 인덱스
            pay_idx = np.clip(dur_safe - 1, 0, len(lapse_rate_1) - 1)
            wx[i, :] = np.where(pay_mask, lapse_rate_1[pay_idx], wx[i, :])

            # 납입후 해약률: (duration_year - pterm - 1) 인덱스
            npay_idx = np.clip(dur_safe - pterm_i - 1, 0, len(lapse_rate_2) - 1)
            wx[i, :] = np.where(npay_mask, lapse_rate_2[npay_idx], wx[i, :])

    return wx


def _map_skew_db(
    mp: ModelPointSet,
    timing: TimingResult,
    assumptions: AssumptionSet,
    reader,
    config: CFConfig,
) -> np.ndarray:
    """실제 DB 키 매칭 기반 스큐 매핑 (skew.py 로직)

    Returns:
        skew: (n_steps,) — 단건 기준
    """
    from cf_module.data.assm_key_builder import AssumptionKeyBuilder

    n = mp.n_points
    s = timing.n_steps
    skew_result = np.full(s, 1.0 / 12.0, dtype=np.float64)

    skew_table = assumptions.skew
    if skew_table.raw_data is None or skew_table.raw_keys is None:
        return skew_result

    assm_div_vals = mp.assm_div_vals if mp.assm_div_vals is not None else np.full((n, 15), "", dtype=object)

    key_builder = AssumptionKeyBuilder(reader, config.runset.assm_grp_id)

    # 단건 (i=0) 기준
    i = 0
    in_force = timing.is_in_force[i, :]
    dm = timing.duration_months[i, :].astype(int)

    keys, _ = key_builder.build_keys_for_type(
        "스큐",
        mp.product_cd[i:i+1],
        mp.cls_cd[i:i+1],
        assm_div_vals[i:i+1, :],
    )

    assm_find_key = skew_table.raw_keys
    raw_data = skew_table.raw_data

    match_idx = np.where(assm_find_key == keys[0])[0]
    logger.debug("[DEC-SKEW] key=%s → match=%d", keys[0], len(match_idx))
    if len(match_idx) > 0:
        skew_raw = raw_data[match_idx[0], 17:41].astype(np.float64)  # 24개월분

        # duration_month 기반 직접 인덱싱 (out-of-force 안전 처리)
        dm_safe = np.clip(dm, 1, 9999)  # 0 방지
        dm_idx = np.clip(dm_safe - 1, 0, len(skew_raw) - 1)
        within_24 = in_force & (dm >= 1) & (dm <= 24)
        skew_result = np.where(within_24, skew_raw[dm_idx], skew_result)

    return skew_result


# =========================================================================
# 기존 스텁 모드: 범용 매핑 (DB 미사용 시 fallback)
# =========================================================================

def _map_mortality(
    mp: ModelPointSet,
    timing: TimingResult,
    mortality: MortalityTable,
    scenario: ScenarioConfig,
) -> np.ndarray:
    """위험률을 시간축에 매핑한다 (범용 스텁 모드).

    기존 qx_read.py의 복합키 매칭 로직을 벡터화한다.
    위험률 특성코드에 따라:
      - 'A' (연령별): age 기반 매핑
      - 'S' (고정): 단일값 적용

    Returns:
        qx_annual: shape (n_points, n_steps)
    """
    n = mp.n_points
    s = timing.n_steps
    qx = np.zeros((n, s), dtype=np.float64)

    if len(mortality.rsk_rt_cd) == 0 or not mortality.rates:
        return qx

    for i in range(n):
        ages_i = timing.age[i, :]
        in_force_i = timing.is_in_force[i, :]

        for code_idx, code in enumerate(mortality.rsk_rt_cd):
            chr_cd = mortality.chr_cd[code_idx] if code_idx < len(mortality.chr_cd) else "A"
            revi = mortality.revi_ym[code_idx] if code_idx < len(mortality.revi_ym) else ""

            if chr_cd == "A":
                for t_idx in range(s):
                    if not in_force_i[t_idx]:
                        continue
                    age_key = str(int(ages_i[t_idx]))
                    for key, rate in mortality.rates.items():
                        if key.endswith(f"^{age_key}") and code in key:
                            qx[i, t_idx] = float(rate) if np.isscalar(rate) else float(rate)
                            break
            else:
                for key, rate in mortality.rates.items():
                    if code in key:
                        val = float(rate) if np.isscalar(rate) else float(rate)
                        qx[i, :] = np.where(in_force_i, val, 0.0)
                        break

            break

    qx *= scenario.mortality_multiplier
    return np.clip(qx, 0.0, 1.0)


def _map_lapse(
    mp: ModelPointSet,
    timing: TimingResult,
    lapse: LapseTable,
    skew: SkewTable,
    scenario: ScenarioConfig,
) -> np.ndarray:
    """해약률을 시간축에 매핑한다 (범용 스텁 모드).

    Returns:
        wx_annual: shape (n_points, n_steps)
    """
    n = mp.n_points
    s = timing.n_steps
    wx = np.zeros((n, s), dtype=np.float64)

    if not lapse.rates_pay:
        return wx

    for key, rate_arr in lapse.rates_pay.items():
        dur_years = timing.duration_years
        max_dur = len(rate_arr)
        dur_idx = np.clip(dur_years - 1, 0, max_dur - 1)
        wx = rate_arr[dur_idx]
        break

    wx *= scenario.lapse_multiplier
    return np.clip(wx, 0.0, 1.0)

"""
계산기수(Commutation Functions) 기반 Pricing 순보험료/준비금 산출

SOA ALTAM 기반 계산기수(Dx, Nx, Cx, Mx)를 사용하여:
1. 가입시점(inception)부터 순보험료(Net Premium) 산출
2. 경과기간(duration)별 순보험료식 준비금(V) 산출

중복제거 위험률(multiple decrement)을 사용하여 qx를 구축한다.
Woolhouse 근사: ä^(m) ≈ ä - (m-1)/(2m) × (1 - D_{x+n}/D_x)
  m = pay_freq (연간 납입횟수 = 12 / PAYCYC_DVCD)
  월납(PAYCYC=1): m=12, coeff=11/24
  연납(PAYCYC=12): m=1, coeff=0 (Woolhouse 미적용)
  일시납(PAYCYC=0): 연금현가 불필요 (P = Ax × SA)
"""

from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from cf_module.utils.logger import get_logger

logger = get_logger("commutation")


@dataclass
class CommutationTable:
    """SOA ALTAM 계산기수 테이블 (연간 기준)

    qx_exit과 qx_benefit이 분리되어 있다:
    - qx_exit (BNFT_DRPO_RSKRT_YN=1): 탈퇴율 → lx, Dx, Nx 산출
    - qx_benefit (BNFT_RSKRT_YN=1): 급부위험률 → Cx, Mx 산출
    탈퇴 없이 급부만 지급하는 코드가 있을 수 있음 (예: 진단금)
    """
    x: int                    # 가입연령
    n: int                    # 보장기간 (년)
    m: int                    # 납입기간 (년)
    i: float                  # 예정이율 (연)
    qx_exit: np.ndarray       # (n,)   탈퇴 위험률 (BNFT_DRPO=1)
    qx_benefit: np.ndarray    # (n,)   급부 위험률 (BNFT_RSKRT=1)
    lx: np.ndarray            # (n+1,) 생존자수 (qx_exit 기반)
    dx: np.ndarray            # (n,)   급부 발생자수 d_t = l_t × qx_benefit_t
    Dx: np.ndarray            # (n+1,) 현가생명수 D_t = l_t × v^t
    Nx: np.ndarray            # (n+1,) Σ D_k (k=t..n)
    Cx: np.ndarray            # (n,)   현가사망액 C_t = l_t × qx_benefit_t × v^(t+0.5)
    Mx: np.ndarray            # (n+1,) Σ C_k (k=t..n-1)


@dataclass
class PricingResult:
    """순보험료 및 준비금 산출 결과 (기준가입금액 기준)

    모든 금액은 calc_sa (기준가입금액 또는 실제SA) 기준이다.
    crit_join_amt 모드에서 실제SA 값은 *_rounded × multiplier로 산출한다.
    """
    net_premium_annual: float      # 연납 순보험료 P = Ax × SA / ä_{x:m}
    net_premium_monthly: float     # 1회 납입 순보험료 (Woolhouse 적용, pay_freq 분할)
    Ax: float                      # 보험료 현가 비율
    ax_due: float                  # 연금현가 ä_{x:m}
    ax_due_monthly: float          # Woolhouse 연금현가 ä^(m)_{x:m}
    reserve_by_year: np.ndarray    # (n+1,) V(t) — calc_sa 기준 (라운드 전)
    commutation: CommutationTable
    has_maturity: bool
    pay_freq: int = 12             # 연간 납입횟수
    gross_premium_annual: Optional[float] = None   # G (연, calc_sa 기준)
    gross_premium_monthly: Optional[float] = None  # G (1회납, calc_sa 기준)
    loading_ratio: Optional[float] = None
    acq_amort_period: Optional[int] = None
    # 기준가입금액 기반 라운드
    crit_join_amt: Optional[float] = None
    multiplier: Optional[float] = None
    net_premium_monthly_rounded: Optional[int] = None
    gross_premium_monthly_rounded: Optional[int] = None
    reserve_by_year_rounded: Optional[np.ndarray] = None


def build_commutation_table(
    qx_exit: np.ndarray,
    qx_benefit: np.ndarray,
    i: float,
    x: int,
    n: int,
    m: int,
) -> CommutationTable:
    """계산기수 테이블을 구축한다.

    Args:
        qx_exit: (n,) 탈퇴 위험률 (BNFT_DRPO=1) → lx 산출
        qx_benefit: (n,) 급부 위험률 (BNFT_RSKRT=1) → Cx/Mx 산출
        i: 예정이율 (연)
        x: 가입연령
        n: 보장기간 (년)
        m: 납입기간 (년)

    Returns:
        CommutationTable
    """
    v = 1.0 / (1.0 + i)

    # 생존자수: lx[0]=1, lx[t+1] = lx[t]*(1-qx_exit[t])
    lx = np.ones(n + 1, dtype=np.float64)
    for t in range(n):
        lx[t + 1] = lx[t] * (1.0 - qx_exit[t])

    # 급부 발생자수: dx[t] = lx[t] * qx_benefit[t]
    dx = lx[:n] * qx_benefit

    # 현가생명수: Dx[t] = lx[t] * v^t
    t_idx = np.arange(n + 1, dtype=np.float64)
    Dx = lx * np.power(v, t_idx)

    # Nx[t] = Σ Dk for k=t..n (역순 누적합)
    Nx = np.cumsum(Dx[::-1])[::-1].copy()

    # 현가사망액: Cx[t] = lx[t] * qx_benefit[t] * v^(t+0.5)  (UDD: 연중 사망 가정)
    t_death = np.arange(n, dtype=np.float64) + 0.5
    Cx = dx * np.power(v, t_death)

    # Mx[t] = Σ Ck for k=t..n-1 (역순 누적합) + Mx[n]=0
    Mx = np.zeros(n + 1, dtype=np.float64)
    Mx[:n] = np.cumsum(Cx[::-1])[::-1]

    logger.debug("[COMM] build_commutation_table: x=%d, n=%d, m=%d, i=%.4f", x, n, m, i)
    logger.debug("[COMM]   qx_exit[0..4]=%s", qx_exit[:min(5,n)].tolist())
    logger.debug("[COMM]   qx_benefit[0..4]=%s", qx_benefit[:min(5,n)].tolist())
    logger.debug("[COMM]   lx[0..4]=%s, lx[n]=%.10f", lx[:5].tolist(), lx[n])
    logger.debug("[COMM]   Dx[0..4]=%s", Dx[:5].tolist())
    logger.debug("[COMM]   Nx[0]=%.10f, Nx[m]=%.10f, Nx[n]=%.10f", Nx[0], Nx[m], Nx[n])
    logger.debug("[COMM]   Cx[0..4]=%s", Cx[:min(5,n)].tolist())
    logger.debug("[COMM]   Mx[0]=%.10f, Mx[n]=%.10f", Mx[0], Mx[n])

    return CommutationTable(
        x=x, n=n, m=m, i=i,
        qx_exit=qx_exit, qx_benefit=qx_benefit,
        lx=lx, dx=dx,
        Dx=Dx, Nx=Nx, Cx=Cx, Mx=Mx,
    )


def _woolhouse_coeff(pay_freq: int) -> float:
    """Woolhouse 근사 계수 (m-1)/(2m)를 반환한다.

    Args:
        pay_freq: 연간 납입횟수 (12=월납, 4=분기납, 2=반기납, 1=연납, 0=일시납)

    Returns:
        (m-1)/(2m).  pay_freq <= 1이면 0.0 (Woolhouse 미적용).
    """
    if pay_freq <= 1:
        return 0.0
    return (pay_freq - 1.0) / (2.0 * pay_freq)


def calc_net_premium(
    table: CommutationTable,
    has_maturity: bool = True,
    sum_assured: float = 1.0,
    pay_freq: int = 12,
) -> PricingResult:
    """순보험료를 산출한다.

    sum_assured 기준으로 P, V를 산출한다.
    기준가입금액 모드에서는 호출부에서 crit_join_amt를 sum_assured로 전달하고,
    라운드/multiplier 처리도 호출부에서 수행한다.

    Args:
        table: CommutationTable
        has_maturity: True=양로보험(endowment), False=정기보험(term)
        sum_assured: 가입금액 (기준가입금액 또는 실제SA)
        pay_freq: 연간 납입횟수 (12=월납, 1=연납, 0=일시납)

    Returns:
        PricingResult (sum_assured 기준, 라운드 미적용)
    """
    n = table.n
    m = table.m
    Dx = table.Dx
    Nx = table.Nx
    Mx = table.Mx

    # 보험료 현가 Ax
    if has_maturity:
        Ax = (Mx[0] - Mx[n] + Dx[n]) / Dx[0]
    else:
        Ax = (Mx[0] - Mx[n]) / Dx[0]

    # 연금현가: ä_{x:m} = (Nx[0] - Nx[m]) / Dx[0]
    ax_due = (Nx[0] - Nx[m]) / Dx[0]

    # Woolhouse 근사: ä^(m)_{x:m} = ä_{x:m} - coeff × (1 - Dx[m]/Dx[0])
    w_coeff = _woolhouse_coeff(pay_freq)
    ax_due_adj = ax_due - w_coeff * (1.0 - Dx[m] / Dx[0])

    logger.debug("[COMM] pay_freq=%d, woolhouse_coeff=%.4f", pay_freq, w_coeff)

    # 순보험료
    if pay_freq == 0:
        # 일시납: P = Ax × SA (연금현가 불필요)
        P_annual = Ax * sum_assured
        P_per_pay = P_annual
    else:
        P_annual = Ax * sum_assured / ax_due
        P_per_pay_annual = Ax * sum_assured / ax_due_adj  # Woolhouse 기준 연간 총액
        P_per_pay = P_per_pay_annual / pay_freq            # 1회 납입액

    # 준비금 산출 (연납 순보험료 P_annual 기준, 연 단위 연금현가)
    reserve = calc_reserve_by_duration(
        table, P_annual, has_maturity, sum_assured,
    )

    logger.debug("[COMM] calc_net_premium: has_maturity=%s, SA=%.0f, pay_freq=%d",
                 has_maturity, sum_assured, pay_freq)
    logger.debug("[COMM]   Ax=%.10f, ax_due=%.10f, ax_due_adj=%.10f", Ax, ax_due, ax_due_adj)
    logger.debug("[COMM]   P_annual=%.4f, P_per_pay=%.4f", P_annual, P_per_pay)

    return PricingResult(
        net_premium_annual=P_annual,
        net_premium_monthly=P_per_pay,
        Ax=Ax,
        ax_due=ax_due,
        ax_due_monthly=ax_due_adj,
        reserve_by_year=reserve,
        commutation=table,
        has_maturity=has_maturity,
        pay_freq=pay_freq,
    )


def calc_reserve_by_duration(
    table: CommutationTable,
    P_annual: float,
    has_maturity: bool,
    sum_assured: float = 1.0,
) -> np.ndarray:
    """경과기간(duration)별 순보험료식 준비금을 산출한다.

    tV = benefit_PV(t) - premium_PV(t)

    연 단위 계산:
      P_annual = Ax × SA / ä_{x:m}
      premium_PV(t) = P_annual × ä_{x+t:m'}
      ä_{x+t:m'} = (Nx[t] - Nx[t+m']) / Dx[t]

    Args:
        table: CommutationTable
        P_annual: 연납 순보험료 (= Ax × SA / ä_{x:m})
        has_maturity: 만기급부 여부
        sum_assured: 가입금액

    Returns:
        (n+1,) 경과연도별 준비금 V(t)
    """
    n = table.n
    m = table.m
    Dx = table.Dx
    Nx = table.Nx
    Mx = table.Mx

    reserve = np.zeros(n + 1, dtype=np.float64)

    for t in range(n + 1):
        if Dx[t] == 0.0:
            break

        # benefit PV(t)
        if has_maturity:
            benefit_pv = (Mx[t] - Mx[n] + Dx[n]) / Dx[t] * sum_assured
        else:
            benefit_pv = (Mx[t] - Mx[n]) / Dx[t] * sum_assured

        # premium PV(t) — 연 단위 연금현가
        remaining_pay = min(m, n) - t
        if remaining_pay > 0:
            ax_annual = (Nx[t] - Nx[t + remaining_pay]) / Dx[t]
            premium_pv = P_annual * ax_annual
        else:
            premium_pv = 0.0

        reserve[t] = benefit_pv - premium_pv

    # V(0)은 수지상등 원칙에 의해 0이어야 함 (부동소수점 오차 보정)
    reserve[0] = 0.0

    logger.debug("[COMM] reserve: V(0)=%.4f, V(1)=%.4f, V(n)=%.4f",
                 reserve[0], reserve[1] if n >= 1 else 0.0, reserve[n])

    return reserve


def check_has_maturity(mp, reader) -> bool:
    """적립담보(만기급부) 존재 여부를 판단한다.

    조건 (OR):
    1. IP_P_ACUM_COV 테이블에 (PROD_CD, CLS_CD, COV_CD) 매칭 존재
    2. COV_CD = 'CLA00500'

    Args:
        mp: ModelPointSet
        reader: DataReader

    Returns:
        True면 양로보험(endowment), False면 정기보험(term)
    """
    cov_cd = str(mp.cov_cd[0])
    if cov_cd == 'CLA00500':
        logger.debug("[COMM] has_maturity=True (COV_CD=CLA00500)")
        return True

    prod_cd = str(mp.product_cd[0])
    cls_cd = str(mp.cls_cd[0])

    try:
        df = reader.execute_query(
            "SELECT COUNT(*) AS cnt FROM IP_P_ACUM_COV WHERE PROD_CD=:prod_cd AND CLS_CD=:cls_cd AND COV_CD=:cov_cd",
            {"prod_cd": prod_cd, "cls_cd": cls_cd, "cov_cd": cov_cd},
        )
        has = int(df.iloc[0, 0]) > 0
    except Exception:
        has = False

    logger.debug("[COMM] has_maturity=%s (PROD=%s, CLS=%s, COV=%s)", has, prod_cd, cls_cd, cov_cd)
    return has


def build_qx_from_inception(mp, assumptions, reader, config):
    """가입시점부터 보장기간 전체에 대해 연간 위험률을 구축한다.

    Pricing 전용:
    - qx_exit: BNFT_DRPO=1 → 탈퇴율 → lx 산출용
    - qx_benefit: BNFT_RSKRT=1 → 급부위험률 → Cx/Mx 산출용

    Args:
        mp: ModelPointSet
        assumptions: AssumptionSet
        reader: DataReader
        config: CFConfig

    Returns:
        (qx_exit, qx_benefit) — 각 (bterm,) 배열
    """
    import os
    import pandas as pd
    from cf_module.data.assm_key_builder import MortalityKeyBuilder
    from cf_module.calc.decrement import _calc_dedup_rates, _get_beprd_for_risks

    mortality = assumptions.mortality
    i = 0  # 단건 (index=0)
    age = int(mp.age_at_entry[i])
    bterm = int(mp.bterm[i])
    n_points = mp.n_points

    logger.debug("[COMM-QX] build_qx_from_inception: age=%d, bterm=%d", age, bterm)

    # ── 1. 플래그 기반 관련 코드 필터링 ──
    rsk_rt_cd_all = mortality.rsk_rt_cd
    n_all = len(rsk_rt_cd_all)
    bnft_drpo_all = mortality.bnft_drpo_yn if mortality.bnft_drpo_yn is not None else np.zeros(n_all, dtype=int)
    bnft_rskrt_all = mortality.bnft_rskrt_yn if mortality.bnft_rskrt_yn is not None else np.zeros(n_all, dtype=int)
    rsvamt_defry_all = mortality.rsvamt_defry_yn if mortality.rsvamt_defry_yn is not None else np.zeros(n_all, dtype=int)

    # pricing에서 사용하는 코드: BNFT_DRPO=1 OR BNFT_RSKRT=1
    relevant = (bnft_drpo_all == 1) | (bnft_rskrt_all == 1)
    relevant_idx = np.where(relevant)[0]

    logger.debug("[COMM-QX] 전체 %d개 코드 → 관련 %d개 필터 (BNFT_DRPO|BNFT_RSKRT)", n_all, len(relevant_idx))
    for j in range(n_all):
        tag = "USED" if relevant[j] else "SKIP"
        logger.debug("[COMM-QX]   %s  DRPO=%d  BNFT=%d  RSVAMT=%d  → %s",
                     rsk_rt_cd_all[j], bnft_drpo_all[j], bnft_rskrt_all[j], rsvamt_defry_all[j], tag)

    if len(relevant_idx) == 0:
        logger.warning("[COMM-QX] 관련 코드 없음 — qx=0 반환")
        return np.zeros(bterm, dtype=np.float64), np.zeros(bterm, dtype=np.float64)

    # ── 2. 필터된 CHR로 복합키 구축 + rate matching ──
    raw_chr_filtered = mortality.raw_chr.iloc[relevant_idx].reset_index(drop=True)

    rsk_rt_div_vals = mp.rsk_rt_div_vals if mp.rsk_rt_div_vals is not None else np.full((n_points, 10), "", dtype=object)
    risk_keys, range_qx_code, range_qx_info = MortalityKeyBuilder.build_risk_keys(
        raw_chr_filtered, rsk_rt_div_vals[i:i+1, :]
    )

    duration_values = np.arange(1, bterm + 1)  # [1, 2, ..., bterm]
    rsk_rt = MortalityKeyBuilder.match_rates(
        risk_keys, range_qx_info, mortality.raw_val,
        age, bterm, duration_values,
    )
    # rsk_rt: (bterm, n_relevant)

    logger.debug("[COMM-QX] rsk_rt shape=%s (bterm=%d, n_relevant=%d)",
                 rsk_rt.shape, bterm, len(relevant_idx))

    # ── 3. BEPRD 적용 (관련 코드만) ──
    rsk_rt_cd_filtered = rsk_rt_cd_all[relevant_idx]
    beprd = _get_beprd_for_risks(
        mp, assumptions, reader, config, rsk_rt_cd_filtered, duration_values, i
    )
    rsk_rt_be = rsk_rt * beprd

    logger.debug("[COMM-QX] BEPRD 적용 후: rsk_rt_be shape=%s", rsk_rt_be.shape)

    # ── 4. 필터된 코드의 플래그 ──
    dead_rt_dvcd_f = mortality.dead_rt_dvcd[relevant_idx] if mortality.dead_rt_dvcd is not None else np.zeros(len(relevant_idx), dtype=int)
    rsk_grp_no_f = mortality.rsk_grp_no[relevant_idx] if mortality.rsk_grp_no is not None else np.zeros(len(relevant_idx), dtype=int)
    bnft_drpo_f = bnft_drpo_all[relevant_idx]
    bnft_rskrt_f = bnft_rskrt_all[relevant_idx]

    # 탈퇴 원인: BNFT_DRPO=1
    is_exit = (bnft_drpo_f == 1).astype(int)

    # ── 5. 중복제거 ──
    wx_zeros = np.zeros(bterm, dtype=np.float64)  # pricing: 해약률 없음
    _, qx_deduped_by_risk = _calc_dedup_rates(
        wx_zeros, rsk_rt_be, dead_rt_dvcd_f, rsk_grp_no_f, is_exit
    )

    # ── 6. qx_exit: BNFT_DRPO=1 합산 (중복제거 적용) ──
    exit_idx = np.where(bnft_drpo_f == 1)[0]
    if len(exit_idx) > 0:
        qx_exit = np.sum(qx_deduped_by_risk[:, exit_idx], axis=1)
    else:
        qx_exit = np.zeros(bterm, dtype=np.float64)
    qx_exit = np.clip(qx_exit, 0.0, 1.0)

    # ── 7. qx_benefit: BNFT_RSKRT=1 합산 (원수위험률) ──
    benefit_idx = np.where(bnft_rskrt_f == 1)[0]
    if len(benefit_idx) > 0:
        qx_benefit = np.sum(rsk_rt_be[:, benefit_idx], axis=1)
    else:
        qx_benefit = np.zeros(bterm, dtype=np.float64)
    qx_benefit = np.clip(qx_benefit, 0.0, 1.0)

    # ── 8. 감액률 조정 (IP_B_REDUC_RT) — 1차년도만 ──
    reduc_by_code = _load_reduc_rt(mp, reader, rsk_rt_cd_filtered[benefit_idx] if len(benefit_idx) > 0 else np.array([]))
    if reduc_by_code and len(benefit_idx) > 0:
        # t=0에 대해 코드별 감액률 적용 후 재합산
        qx_benefit_t0 = 0.0
        for bi in benefit_idx:
            code = str(rsk_rt_cd_filtered[bi])
            reduc = reduc_by_code.get(code, 1.0)
            qx_benefit_t0 += rsk_rt_be[0, bi] * reduc
            if reduc != 1.0:
                logger.debug("[COMM-QX] 감액률: %s × %.4f = %.6f → %.6f",
                             code, reduc, rsk_rt_be[0, bi], rsk_rt_be[0, bi] * reduc)
        qx_benefit[0] = qx_benefit_t0

    # ── 9. 면책기간 조정 (IP_R_INVLD_TRMNAT) ──
    # 면책기간 k개월 → 첫해 qx_benefit[0] *= (12 - k) / 12
    invld_months = _load_invld_trmnat(mp, reader, rsk_rt_cd_filtered[benefit_idx] if len(benefit_idx) > 0 else np.array([]))
    if invld_months > 0:
        adj = (12 - invld_months) / 12.0
        qx_benefit_before = qx_benefit[0]
        logger.debug("[COMM-QX] 면책기간 %d개월 → qx_benefit[0] 조정: %.6f × %.4f = %.6f",
                     invld_months, qx_benefit[0], adj, qx_benefit[0] * adj)
        qx_benefit[0] *= adj
    else:
        qx_benefit_before = qx_benefit[0]
        invld_months = 0

    logger.debug("[COMM-QX] qx_exit[0..4]=%s (codes: %s)",
                 qx_exit[:min(5,bterm)].tolist(), rsk_rt_cd_filtered[exit_idx].tolist() if len(exit_idx) > 0 else [])
    logger.debug("[COMM-QX] qx_benefit[0..4]=%s (codes: %s)",
                 qx_benefit[:min(5,bterm)].tolist(), rsk_rt_cd_filtered[benefit_idx].tolist() if len(benefit_idx) > 0 else [])

    # ── 9. debug CSV: qx 중간계산 ──
    if config.debug:
        debug_dir = os.path.join(config.output.output_dir, "debug")
        os.makedirs(debug_dir, exist_ok=True)

        # (A) 코드별 상세: 코드, 플래그, t별 원수율/BEPRD/중복제거후
        rows = []
        for ci, code in enumerate(rsk_rt_cd_filtered):
            for t in range(bterm):
                rows.append({
                    "rsk_rt_cd": code,
                    "dead_rt_dvcd": int(dead_rt_dvcd_f[ci]),
                    "rsk_grp_no": int(rsk_grp_no_f[ci]),
                    "bnft_drpo": int(bnft_drpo_f[ci]),
                    "bnft_rskrt": int(bnft_rskrt_f[ci]),
                    "rsvamt_defry": int(rsvamt_defry_all[relevant_idx[ci]]),
                    "is_exit": int(is_exit[ci]),
                    "t": t + 1,
                    "age_t": age + t,
                    "rsk_rt_raw": float(rsk_rt[t, ci]),
                    "beprd": float(beprd[t, ci]),
                    "rsk_rt_be": float(rsk_rt_be[t, ci]),
                    "qx_deduped": float(qx_deduped_by_risk[t, ci]),
                })
        df_detail = pd.DataFrame(rows)
        df_detail.to_csv(os.path.join(debug_dir, "02c_qx_by_code.csv"), index=False, encoding="utf-8-sig")

        # (B) 중복제거 과정 상세: C행렬 + adjustment 재현
        # r: [wx, exit_code0, exit_code1, ...], C행렬, adjustment, dedup
        n_exit_codes = len(exit_idx)
        if n_exit_codes > 0:
            n_rates = 1 + n_exit_codes
            # r 행렬 구성
            r = np.zeros((bterm, n_rates), dtype=np.float64)
            r[:, 0] = wx_zeros  # pricing: wx=0
            r[:, 1:] = rsk_rt_be[:, exit_idx]
            # C 행렬
            d = np.zeros(n_rates, dtype=np.float64)
            d[0] = 1.0
            d[1:] = dead_rt_dvcd_f[exit_idx].astype(np.float64)
            g = np.zeros(n_rates, dtype=int)
            g[0] = 0
            g[1:] = rsk_grp_no_f[exit_idx].astype(int)
            C = np.ones((n_rates, n_rates), dtype=np.float64)
            np.fill_diagonal(C, 0.0)
            C *= (g[:, None] != g[None, :]).astype(np.float64)
            C *= d[None, :]
            adjustment = r @ C.T
            r_deduped = r * (1.0 - adjustment / 2.0)

            # C행렬 CSV
            labels = ["wx"] + [str(rsk_rt_cd_filtered[ei]) for ei in exit_idx]
            c_rows = []
            for ri in range(n_rates):
                row = {"rate": labels[ri], "dead_rt_dvcd": int(d[ri]), "rsk_grp_no": int(g[ri])}
                for ci2 in range(n_rates):
                    row[f"C_{labels[ci2]}"] = float(C[ri, ci2])
                c_rows.append(row)
            pd.DataFrame(c_rows).to_csv(
                os.path.join(debug_dir, "02e_dedup_C_matrix.csv"), index=False, encoding="utf-8-sig")

            # t별 dedup 과정 CSV
            dedup_rows = []
            for t in range(bterm):
                for ri in range(n_rates):
                    dedup_rows.append({
                        "t": t + 1,
                        "age_t": age + t,
                        "rate_label": labels[ri],
                        "r_before": float(r[t, ri]),
                        "adjustment": float(adjustment[t, ri]),
                        "factor_1_minus_adj_half": float(1.0 - adjustment[t, ri] / 2.0),
                        "r_after": float(r_deduped[t, ri]),
                    })
            pd.DataFrame(dedup_rows).to_csv(
                os.path.join(debug_dir, "02f_dedup_process.csv"), index=False, encoding="utf-8-sig")

        # (C) 합산 요약: t, qx_exit, qx_benefit 각 단계
        summary_rows = []
        for t in range(bterm):
            qx_b_raw = float(np.sum(rsk_rt_be[t, benefit_idx])) if len(benefit_idx) > 0 else 0.0
            # 감액 후 (t=0만 적용)
            if t == 0 and reduc_by_code and len(benefit_idx) > 0:
                qx_b_after_reduc = sum(
                    rsk_rt_be[0, bi] * reduc_by_code.get(str(rsk_rt_cd_filtered[bi]), 1.0)
                    for bi in benefit_idx
                )
            else:
                qx_b_after_reduc = qx_b_raw
            qx_b_after = float(qx_benefit[t])
            summary_rows.append({
                "t": t + 1,
                "age_t": age + t,
                "qx_exit": float(qx_exit[t]),
                "qx_benefit_raw": qx_b_raw,
                "qx_benefit_after_reduc": float(qx_b_after_reduc),
                "qx_benefit_after_invld": qx_b_after,
                "invld_months": invld_months if t == 0 else 0,
                "invld_adj": float((12 - invld_months) / 12.0) if t == 0 and invld_months > 0 else 1.0,
            })
        df_summary = pd.DataFrame(summary_rows)
        df_summary.to_csv(os.path.join(debug_dir, "02d_qx_summary.csv"), index=False, encoding="utf-8-sig")

        logger.debug("[COMM-QX] debug CSV 저장: 02c~02f")

    return qx_exit, qx_benefit


def _load_invld_trmnat(mp, reader, benefit_codes: np.ndarray) -> int:
    """면책기간(IP_R_INVLD_TRMNAT)을 로딩한다.

    면책기간이 있는 급부코드의 최대 면책월수를 반환한다.
    INVLD_TRMNAT_PRD_TPCD='M' (월단위) 기준.

    Args:
        mp: ModelPointSet
        reader: DataReader
        benefit_codes: BNFT_RSKRT=1인 위험률코드 배열

    Returns:
        최대 면책월수 (없으면 0)
    """
    if len(benefit_codes) == 0:
        return 0

    prod_cd = str(mp.product_cd[0])
    cls_cd = str(mp.cls_cd[0])
    cov_cd = str(mp.cov_cd[0])

    try:
        df = reader.execute_query(
            "SELECT RSK_RT_CD, INVLD_TRMNAT_PRD_TPCD, INVLD_TRMNAT_PRD_CNT "
            "FROM IP_R_INVLD_TRMNAT "
            "WHERE PROD_CD=:prod_cd AND CLS_CD=:cls_cd AND COV_CD=:cov_cd",
            {"prod_cd": prod_cd, "cls_cd": cls_cd, "cov_cd": cov_cd},
        )
    except Exception:
        return 0

    if df.empty:
        return 0

    # 급부코드에 해당하는 행만 필터
    benefit_set = set(str(c) for c in benefit_codes)
    mask = df["RSK_RT_CD"].astype(str).isin(benefit_set)
    df_filtered = df[mask]

    if df_filtered.empty:
        return 0

    # 월단위(M) 면책기간의 최대값
    m_mask = df_filtered["INVLD_TRMNAT_PRD_TPCD"].astype(str) == "M"
    if m_mask.any():
        max_months = int(df_filtered.loc[m_mask, "INVLD_TRMNAT_PRD_CNT"].max())
        logger.debug("[COMM-QX] 면책기간: %s → %d개월", benefit_set, max_months)
        return max_months

    return 0


def _load_reduc_rt(mp, reader, benefit_codes: np.ndarray) -> dict:
    """감액률(IP_B_REDUC_RT)을 로딩한다.

    IP_R_BNFT_RSKRT_C의 BNFT_NO를 통해 RSK_RT_CD별 감액률을 매핑한다.
    REDUC_PRD_CD > 0인 경우에만 적용 (1차년도 한정).

    Args:
        mp: ModelPointSet
        reader: DataReader
        benefit_codes: BNFT_RSKRT=1인 위험률코드 배열

    Returns:
        {RSK_RT_CD: REDUC_RT} dict (감액 대상 없으면 빈 dict)
    """
    if len(benefit_codes) == 0:
        return {}

    prod_cd = str(mp.product_cd[0])
    cls_cd = str(mp.cls_cd[0])
    cov_cd = str(mp.cov_cd[0])

    try:
        # BNFT_NO → RSK_RT_CD 매핑 (BNFT_RSKRT=1인 것만)
        df_bnft = reader.execute_query(
            "SELECT BNFT_NO, RSK_RT_CD FROM IP_R_BNFT_RSKRT_C "
            "WHERE PROD_CD=:prod_cd AND CLS_CD=:cls_cd AND COV_CD=:cov_cd "
            "AND BNFT_RSKRT_YN='1'",
            {"prod_cd": prod_cd, "cls_cd": cls_cd, "cov_cd": cov_cd},
        )
        if df_bnft.empty:
            return {}

        # 감액률 로딩
        df_reduc = reader.execute_query(
            "SELECT BNFT_NO, REDUC_PRD_CD, REDUC_RT FROM IP_B_REDUC_RT "
            "WHERE PROD_CD=:prod_cd AND CLS_CD=:cls_cd AND COV_CD=:cov_cd",
            {"prod_cd": prod_cd, "cls_cd": cls_cd, "cov_cd": cov_cd},
        )
        if df_reduc.empty:
            return {}
    except Exception:
        return {}

    # BNFT_NO → REDUC_RT dict (REDUC_PRD_CD > 0)
    reduc_map = {}
    for _, row in df_reduc.iterrows():
        prd = int(row["REDUC_PRD_CD"])
        if prd > 0:
            reduc_map[int(row["BNFT_NO"])] = float(row["REDUC_RT"])

    if not reduc_map:
        return {}

    # RSK_RT_CD → REDUC_RT 매핑
    benefit_set = set(str(c) for c in benefit_codes)
    result = {}
    for _, row in df_bnft.iterrows():
        code = str(row["RSK_RT_CD"])
        bnft_no = int(row["BNFT_NO"])
        if code in benefit_set and bnft_no in reduc_map:
            result[code] = reduc_map[bnft_no]
            logger.debug("[COMM-QX] 감액률 로딩: %s (BNFT_NO=%d) → REDUC_RT=%.4f",
                         code, bnft_no, reduc_map[bnft_no])

    return result


def _woolhouse_annuity(table: CommutationTable, k: int, pay_freq: int = 12) -> float:
    """임의 k년 Woolhouse 연금현가 ä^(m)_{x:k}를 계산한다.

    ä^(m)_{x:k} = (Nx[0] - Nx[k]) / Dx[0] - coeff × (1 - Dx[k] / Dx[0])
    coeff = (m-1)/(2m), m = pay_freq

    Args:
        table: CommutationTable
        k: 기간 (년). 0이면 0.0 반환.
        pay_freq: 연간 납입횟수 (12=월납, 1=연납, 0=일시납)

    Returns:
        ä^(m)_{x:k}
    """
    if k <= 0:
        return 0.0
    Dx = table.Dx
    Nx = table.Nx
    w_coeff = _woolhouse_coeff(pay_freq)
    ax_annual = (Nx[0] - Nx[k]) / Dx[0]
    ax_adj = ax_annual - w_coeff * (1.0 - Dx[k] / Dx[0])
    return ax_adj


def get_acq_amort_period(mp, reader, has_maturity: bool) -> int:
    """신계약비 분산인식 기간(t_acq)을 결정한다.

    - 비적립담보: pterm
    - 적립담보: min(IP_P_ACUM_COV.ACQSEXP_ADDL_PRD1, pterm)

    Args:
        mp: ModelPointSet
        reader: DataReader
        has_maturity: 적립담보 여부

    Returns:
        t_acq (년)
    """
    pterm = int(mp.pterm[0])

    if not has_maturity:
        logger.debug("[COMM] get_acq_amort_period: 비적립 → t_acq=pterm=%d", pterm)
        return pterm

    # 적립담보: IP_P_ACUM_COV에서 ACQSEXP_ADDL_PRD1 조회
    prod_cd = str(mp.product_cd[0])
    cls_cd = str(mp.cls_cd[0])
    cov_cd = str(mp.cov_cd[0])

    try:
        df = reader.execute_query(
            "SELECT ACQSEXP_ADDL_PRD1 FROM IP_P_ACUM_COV "
            "WHERE PROD_CD=:prod_cd AND CLS_CD=:cls_cd AND COV_CD=:cov_cd",
            {"prod_cd": prod_cd, "cls_cd": cls_cd, "cov_cd": cov_cd},
        )
        if not df.empty and "ACQSEXP_ADDL_PRD1" in df.columns:
            addl_prd = int(df.iloc[0]["ACQSEXP_ADDL_PRD1"])
            if addl_prd > 0:
                t_acq = min(addl_prd, pterm)
                logger.debug("[COMM] get_acq_amort_period: 적립 ACQSEXP_ADDL_PRD1=%d → t_acq=%d",
                             addl_prd, t_acq)
                return t_acq
    except Exception:
        logger.debug("[COMM] IP_P_ACUM_COV 조회 실패, t_acq=pterm 사용")

    logger.debug("[COMM] get_acq_amort_period: 적립(기본) → t_acq=pterm=%d", pterm)
    return pterm


def calc_gross_premium(
    table: CommutationTable,
    ee: 'ExpectedExpenseRate',
    has_maturity: bool,
    sum_assured: float,
    t_acq: int,
    Ax: float,
    pay_freq: int = 12,
) -> tuple:
    """영업보험료(Gross Premium)를 산출한다.

    sum_assured 기준으로 G를 산출한다.
    기준가입금액 모드에서는 호출부에서 crit_join_amt를 sum_assured로 전달하고,
    라운드/multiplier 처리도 호출부에서 수행한다.

    수지상등 원칙:
        G × ä^(m)_{x:m} = SA × Ax + expense PV

    공식:
        numerator   = SA × (Ax + α_sa) + K × ä^(m)_{x:m}
        denominator = ä^(m)_{x:m} × (1 - β_acqs - β_mnt - γ - δ)
                    - α × ä^(m)_{x:t_acq}
                    - β_afpay × (ä^(m)_{x:n} - ä^(m)_{x:m})
        G_annual  = numerator / denominator
        G_per_pay = G_annual / pay_freq

    Args:
        table: CommutationTable
        ee: ExpectedExpenseRate (예정사업비율)
        has_maturity: 만기급부 여부
        sum_assured: 가입금액 (기준가입금액 또는 실제SA)
        t_acq: 신계약비 분산인식 기간 (년)
        Ax: 일시납 순보험료 비율
        pay_freq: 연간 납입횟수 (12=월납, 1=연납, 0=일시납)

    Returns:
        (G_annual, G_per_pay) — sum_assured 기준, 라운드 미적용
    """
    n = table.n  # 보장기간
    m = table.m  # 납입기간

    # 사업비율 매핑
    alpha = ee.fryy_gprem_acqs_rt
    beta_acqs = ee.inpay_gprem_acqs_rt
    beta_mnt = ee.inpay_gprem_mnt_rt
    # gamma = RT1 + RT2 이지만 RT2 미로딩 → 0이면 0.02 대체
    gamma = ee.inpay_gprem_colm_rt if ee.inpay_gprem_colm_rt != 0.0 else 0.02
    delta = ee.inpay_gprem_loss_svyexp_rt
    alpha_sa = ee.fryy_join_amt_acqs_rt
    K = ee.inpay_fxamt_mntexp
    beta_afpay = ee.afpay_gprem_mnt_rt

    # Woolhouse 연금현가
    ax_m = _woolhouse_annuity(table, m, pay_freq)
    ax_n = _woolhouse_annuity(table, n, pay_freq)

    # 확정연금 ä_{m|} = 1 + v + v^2 + ... + v^{m-1} — α 비율 환산용
    v = 1.0 / (1.0 + table.i)
    a_certain_m = sum(v ** t for t in range(m)) if m > 0 else 1.0

    logger.debug("[COMM-GP] 사업비율: α=%.4f, β_acqs=%.4f, β_mnt=%.4f, γ=%.4f, δ=%.4f",
                 alpha, beta_acqs, beta_mnt, gamma, delta)
    logger.debug("[COMM-GP] α_sa=%.6f, K=%.0f, β_afpay=%.4f", alpha_sa, K, beta_afpay)
    logger.debug("[COMM-GP] pay_freq=%d, ax_m=%.10f, ax_n=%.10f, a_certain_m=%.10f",
                 pay_freq, ax_m, ax_n, a_certain_m)

    # numerator = SA × (Ax + α_sa) + K × ä^(m)_{x:m}
    numerator = sum_assured * (Ax + alpha_sa) + K * ax_m

    # denominator
    # α를 확정연금 ä_{m|}로 나눠 연간 비율로 환산 후 괄호 안에 포함
    alpha_rate = alpha / a_certain_m if a_certain_m > 1e-12 else 0.0
    denominator = (
        ax_m * (1.0 - alpha_rate - beta_acqs - beta_mnt - gamma - delta)
        - beta_afpay * (ax_n - ax_m)
    )

    logger.debug("[COMM-GP] alpha_rate=alpha/a_certain=%.10f", alpha_rate)
    logger.debug("[COMM-GP] numerator=%.6f, denominator=%.10f", numerator, denominator)

    if abs(denominator) < 1e-12:
        logger.warning("[COMM-GP] denominator ≈ 0, G 산출 불가")
        return 0.0, 0.0

    G_annual = numerator / denominator
    G_per_pay = G_annual / max(pay_freq, 1)

    logger.debug("[COMM-GP] G_annual=%.4f, G_per_pay=%.4f", G_annual, G_per_pay)

    return G_annual, G_per_pay

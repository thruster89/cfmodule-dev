"""OD_LAPSE_RT 산출 엔진.

해지율 원율 → 스큐 적용 → 월변환 단계별 분해.

입력: RawAssumptionLoader에서 로드한 raw 데이터
출력: dict of arrays (n_steps)

핵심 인덱싱:
  - TRMNAT_RT1~20: 프로젝션 시작(pass_yy,pass_mm) 기준 경과년수로 인덱싱
    proj_year = (pass_mm + t) // 12 + 1
  - SKEW1~36: 프로젝션 년도 내 월 위치
    skew_month = (pass_mm + t) % 12 (year 1)
    skew_month = 12 + (pass_mm + t) % 12 (year 2+)
"""
import numpy as np

from cf_module.data.rsk_lapse_loader import ContractInfo


def compute_lapse_rt(
    ctr: ContractInfo,
    lapse_paying: np.ndarray,
    lapse_paidup: np.ndarray,
    skew: np.ndarray,
    n_steps: int,
) -> dict:
    """계약 1건의 OD_LAPSE_RT 산출.

    Args:
        ctr: 계약 정보
        lapse_paying: (max_years,) 납입중 연해지율 (index 0 = 프로젝션 1년차)
        lapse_paidup: (max_years,) 납입후 연해지율 (index 0 = 납입후 1년차)
        skew: (36 or max_months,) 스큐 지수 (SKEW1~36 값, index 0=SKEW1)
        n_steps: 프로젝션 스텝 수

    Returns:
        {
            "TRMNAT_RT": ndarray,
            "SKEW": ndarray,
            "APLY_TRMNAT_RT": ndarray,
        }
    """
    elapsed = ctr.pass_yy * 12 + ctr.pass_mm
    pterm_months = ctr.pterm_yy * 12

    t_range = np.arange(n_steps, dtype=np.int32)
    duration_months = elapsed + t_range          # 계약 시작 기준 절대 경과월
    proj_months = ctr.pass_mm + t_range          # 프로젝션 시작(pass) 기준 상대 경과월

    # 프로젝션 경과년수: ceil(duration_months/12) — CTR_MM=12→Year1, 13→Year2
    proj_years = np.maximum((duration_months - 1) // 12 + 1, 1)

    # 납입중/납입후 판정 — 절대 경과월 기준 (pterm월 포함 = 납입중)
    is_paying = duration_months <= pterm_months

    # 납입중 해지율 (연 단위) — 프로젝션 year index
    pay_yr_idx = np.clip(proj_years - 1, 0, len(lapse_paying) - 1)
    wx_paying = lapse_paying[pay_yr_idx]

    # 납입후 해지율 (연 단위) — 납입후 경과 기준
    paidup_months = np.maximum(duration_months - pterm_months, 0)
    paidup_years = np.maximum((paidup_months - 1) // 12 + 1, 1)
    paidup_yr_idx = np.clip(paidup_years - 1, 0, len(lapse_paidup) - 1)
    wx_paidup = lapse_paidup[paidup_yr_idx]

    # 납입중/납입후 선택
    trmnat_rt = np.where(is_paying, wx_paying, wx_paidup)

    # 스큐 — 프로젝션 년도 내 월 위치 (SKEW1~12 순환)
    # Year 1: SKEW[0..11], Year 2+: SKEW[12..23] (있으면), 없으면 SKEW[0..11]
    skew_len = len(skew)
    month_in_year = proj_months % 12

    if skew_len >= 24:
        # Year 1: SKEW1-12 (index 0-11), Year 2+: SKEW13-24 (index 12-23)
        is_year1 = proj_months < 12
        skew_idx = np.where(is_year1, month_in_year, 12 + month_in_year)
        skew_idx = np.clip(skew_idx, 0, skew_len - 1)
    else:
        # 12개 이하: 순환
        skew_idx = np.clip(month_in_year, 0, skew_len - 1)

    skew_arr = skew[skew_idx]

    # APLY = 1 - (1 - TRMNAT)^SKEW
    trmnat_clipped = np.clip(trmnat_rt, 0, 1)
    aply = np.where(
        skew_arr > 0,
        1.0 - (1.0 - trmnat_clipped) ** skew_arr,
        0.0,
    )

    return {
        "TRMNAT_RT": trmnat_rt,
        "SKEW": skew_arr,
        "APLY_TRMNAT_RT": aply,
    }

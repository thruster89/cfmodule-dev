"""OD_LAPSE_RT 산출 엔진.

해지율 원율 → 월환산(SKEW=1/12) 단계별 분해.

입력: RawAssumptionLoader에서 로드한 raw 데이터
출력: dict of arrays (n_steps)

핵심 인덱싱:
  - TRMNAT_RT1~20: 경과년수 = ceil(CTR_AFT_PASS_MMCNT / 12)
  - SKEW: 월환산 지수 = 1/12 (상수)
    IA_T_SKEW의 월별 가중치와 별개 — OD_LAPSE_RT에서는 단순 월환산만 적용
  - 납입중/납입후: duration_months <= pterm_months (pterm월 포함 = 납입중)
  - 만기도래: elapsed >= bterm_months → 전부 0
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
        lapse_paying: (max_years,) 납입중 연해지율 (index 0 = 1년차)
        lapse_paidup: (max_years,) 납입후 연해지율 (index 0 = 납입후 1년차)
        skew: (unused) IA_T_SKEW 월별 가중치 — OD_LAPSE_RT에서는 1/12 상수 사용
        n_steps: 프로젝션 스텝 수

    Returns:
        {
            "TRMNAT_RT": ndarray,
            "SKEW": ndarray,
            "APLY_TRMNAT_RT": ndarray,
        }
    """
    elapsed = ctr.pass_yy * 12 + ctr.pass_mm
    bterm_months = ctr.bterm_yy * 12
    pterm_months = ctr.main_pterm_yy * 12

    # 만기도래: 프로젝션 시작 시점에서 이미 만기 → 전부 0
    if elapsed >= bterm_months:
        z = np.zeros(n_steps, dtype=np.float64)
        return {"TRMNAT_RT": z, "SKEW": z.copy(), "APLY_TRMNAT_RT": z.copy()}

    t_range = np.arange(n_steps, dtype=np.int32)
    duration_months = elapsed + t_range          # 계약 시작 기준 절대 경과월

    # 경과년수: ceil(months/12) — CTR_MM=12→Year1, 13→Year2
    proj_years = np.maximum((duration_months - 1) // 12 + 1, 1)

    # 납입중/납입후 판정 — 절대 경과월 기준 (pterm월 포함 = 납입중)
    is_paying = duration_months <= pterm_months

    # 납입중 해지율 (연 단위) — 경과년수 인덱싱
    pay_yr_idx = np.clip(proj_years - 1, 0, len(lapse_paying) - 1)
    wx_paying = lapse_paying[pay_yr_idx]

    # 납입후 해지율 (연 단위) — 납입후 경과 기준
    paidup_months = np.maximum(duration_months - pterm_months, 0)
    paidup_years = np.maximum((paidup_months - 1) // 12 + 1, 1)
    paidup_yr_idx = np.clip(paidup_years - 1, 0, len(lapse_paidup) - 1)
    wx_paidup = lapse_paidup[paidup_yr_idx]

    # 납입중/납입후 선택
    trmnat_rt = np.where(is_paying, wx_paying, wx_paidup)

    # SKEW = 1/12 (월환산 상수)
    skew_arr = np.full(n_steps, 1.0 / 12.0, dtype=np.float64)

    # APLY = 1 - (1 - TRMNAT)^(1/12)
    trmnat_clipped = np.clip(trmnat_rt, 0, 1)
    aply = 1.0 - (1.0 - trmnat_clipped) ** skew_arr

    return {
        "TRMNAT_RT": trmnat_rt,
        "SKEW": skew_arr,
        "APLY_TRMNAT_RT": aply,
    }

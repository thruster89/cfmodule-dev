"""OD_RSK_RT 산출 엔진.

위험률코드별 원율 → BEPRD → 월변환 → 면책 적용 단계별 분해.

입력: RawAssumptionLoader에서 로드한 raw 데이터
출력: {rsk_cd: dict of arrays} (n_steps per risk)
"""
from typing import Dict, List

import numpy as np

from cf_module.data.rsk_lapse_loader import ContractInfo, RiskInfo


def compute_rsk_rt(
    ctr: ContractInfo,
    risks: List[RiskInfo],
    mortality_rates: Dict[str, np.ndarray],
    beprd: Dict[str, np.ndarray],
    invld_months: Dict[str, int],
    n_steps: int,
) -> Dict[str, dict]:
    """계약 1건의 OD_RSK_RT 산출.

    Args:
        ctr: 계약 정보
        risks: 위험률코드 메타 리스트
        mortality_rates: {rsk_cd: rate_by_age or scalar}
        beprd: {rsk_cd: beprd_by_year}
        invld_months: {rsk_cd: 면책기간(월)}
        n_steps: 프로젝션 스텝 수

    Returns:
        {rsk_cd: {column_name: ndarray}}
    """
    elapsed = ctr.pass_yy * 12 + ctr.pass_mm
    t_range = np.arange(n_steps, dtype=np.int32)
    duration_months = elapsed + t_range          # (n_steps,)
    # 경과년수: ceil(month/12) — CTR_MM=12→Year1, CTR_MM=13→Year2
    duration_years = np.maximum((duration_months - 1) // 12 + 1, 1)
    ages = ctr.entry_age + np.maximum(duration_months - 1, 0) // 12

    results = {}

    for risk in risks:
        rsk_cd = risk.risk_cd
        rate_arr = mortality_rates.get(rsk_cd)

        # RSK_RT: 원율 (연 단위)
        rsk_rt = np.zeros(n_steps, dtype=np.float64)
        if rate_arr is not None:
            if risk.chr_cd == "S":
                rsk_rt[:] = rate_arr[0]
            else:
                clipped_ages = np.clip(ages.astype(np.int32), 0, len(rate_arr) - 1)
                rsk_rt = rate_arr[clipped_ages]

        # 고정 계수들
        loss_rt = np.ones(n_steps, dtype=np.float64)
        mth_efect = np.ones(n_steps, dtype=np.float64)
        trd_coef = np.ones(n_steps, dtype=np.float64)
        arvl_age = np.ones(n_steps, dtype=np.float64)

        # BEPRD
        beprd_vec = beprd.get(rsk_cd)
        beprd_rt = np.ones(n_steps, dtype=np.float64)
        if beprd_vec is not None:
            beprd_idx = np.clip(duration_years - 1, 0, len(beprd_vec) - 1)
            beprd_rt = beprd_vec[beprd_idx]

        # BF_YR = RSK × LOSS × MTH_EFECT × BEPRD × TRD × ARVL
        bf_yr = rsk_rt * loss_rt * mth_efect * beprd_rt * trd_coef * arvl_age

        # BF_MM: 월변환
        if risk.mm_trf_way_cd == 1:
            bf_mm = 1.0 - (1.0 - np.clip(bf_yr, 0, None)) ** (1.0 / 12.0)
        else:
            bf_mm = bf_yr / 12.0

        # AF: 면책기간 적용
        invld_mm = invld_months.get(rsk_cd, 0)
        af = bf_mm.copy()
        if invld_mm > 0:
            af[duration_months < invld_mm] = 0.0

        results[rsk_cd] = {
            "RSK_RT": rsk_rt,
            "LOSS_RT": loss_rt,
            "MTH_EFECT_COEF": mth_efect,
            "BEPRD_DEFRY_RT": beprd_rt,
            "TRD_COEF": trd_coef,
            "ARVL_AGE_COEF": arvl_age,
            "INVLD_TRMNAT_BF_YR_RSK_RT": bf_yr,
            "INVLD_TRMNAT_BF_MM_RSK_RT": bf_mm,
            "INVLD_TRMNAT_AF_APLY_RSK_RT": af,
        }

    return results

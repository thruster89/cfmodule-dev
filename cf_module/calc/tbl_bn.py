"""OD_TBL_BN 산출 엔진.

급부(BNFT_NO)별 독립 tpx 및 지급금액 계산.

핵심 로직:
  1. v2 엔진(engine.py)과 동일한 위험률 중복제거 수행
  2. per-risk dedup qx를 BNFT_NO별로 분배
  3. 급부별 독립 exit rate → tpx → TRME/TRMO
  4. DEFRY_RT, PRTT_RT, PYAMT, BNFT_INSUAMT 산출
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np


@dataclass
class BNFTResult:
    """단일 급부 결과."""
    bnft_no: int
    n_steps: int
    trmnat_rt: np.ndarray           # (n_steps,)
    rsvamt_defry_drpo_rskrt: np.ndarray
    bnft_drpo_rskrt: np.ndarray
    bnft_rskrt: np.ndarray
    trmo_mtnpsn_cnt: np.ndarray
    trmpsn_cnt: np.ndarray
    rsvamt_defry_drpsn_cnt: np.ndarray
    defry_drpsn_cnt: np.ndarray
    trme_mtnpsn_cnt: np.ndarray
    bnft_ocurpe_cnt: np.ndarray
    crit_amt: np.ndarray
    defry_rt: np.ndarray
    prtt_rt: np.ndarray
    gradin_rt: np.ndarray
    pyamt: np.ndarray
    bnft_insuamt: np.ndarray

    def to_dict(self):
        return {
            "BNFT_NO": self.bnft_no,
            "TRMNAT_RT": self.trmnat_rt,
            "RSVAMT_DEFRY_DRPO_RSKRT": self.rsvamt_defry_drpo_rskrt,
            "BNFT_DRPO_RSKRT": self.bnft_drpo_rskrt,
            "BNFT_RSKRT": self.bnft_rskrt,
            "TRMO_MTNPSN_CNT": self.trmo_mtnpsn_cnt,
            "TRMPSN_CNT": self.trmpsn_cnt,
            "RSVAMT_DEFRY_DRPSN_CNT": self.rsvamt_defry_drpsn_cnt,
            "DEFRY_DRPSN_CNT": self.defry_drpsn_cnt,
            "TRME_MTNPSN_CNT": self.trme_mtnpsn_cnt,
            "BNFT_OCURPE_CNT": self.bnft_ocurpe_cnt,
            "CRIT_AMT": self.crit_amt,
            "DEFRY_RT": self.defry_rt,
            "PRTT_RT": self.prtt_rt,
            "GRADIN_RT": self.gradin_rt,
            "PYAMT": self.pyamt,
            "BNFT_INSUAMT": self.bnft_insuamt,
        }


@dataclass
class BNResult:
    """한 계약(IDNO)의 전체 BN 결과."""
    idno: int
    n_steps: int
    bnft_results: Dict[int, BNFTResult]  # {bnft_no: BNFTResult}


def compute_bn(
    idno: int,
    n_steps: int,
    prod_cd: str,
    cls_cd: str,
    cov_cd: str,
    pass_yy: int,
    pass_mm: int,
    bterm_yy: int,
    join_amt: float,
    # MN-level dedup 결과 (per-risk)
    risk_cds: List[str],             # 위험률코드 목록
    qx_ctr_per_risk: np.ndarray,     # (n_exit_ctr, n_steps) — CTR dedup per exit risk
    exit_idx_ctr: np.ndarray,        # risk_cds 내 exit risk 인덱스
    is_exit_rsv: np.ndarray,         # (n_risks,) bool
    wx_ctr: np.ndarray,              # (n_steps,) CTR dedup 해지율
    # BNFT 매핑
    bnft_mapping: Dict[int, dict],   # {bnft_no: {"rskrt_cds": [...], "drpo_cds": [...]}}
    # 참조 데이터 콜백
    get_defry_rate_fn=None,          # fn(bnft_no, duration_year) -> float
    get_prtt_rate_fn=None,           # fn(bnft_no, duration_year) -> float
    get_ncov_months_fn=None,         # fn(bnft_no) -> int
    # TRAD_PV 적립금 (PRTT 산출용)
    aply_prem_acumamt_bnft: Optional[np.ndarray] = None,
) -> BNResult:
    """한 계약의 전체 BN 산출.

    Args:
        qx_ctr_per_risk: (n_exit_ctr, n_steps) 각 exit risk의 dedup qx
        exit_idx_ctr: qx_ctr_per_risk[i]가 risk_cds[exit_idx_ctr[i]]에 대응
    """
    # 시간축
    elapsed_mm = pass_yy * 12 + pass_mm
    ctr_mm = np.arange(n_steps) + elapsed_mm   # 0-based 경과월
    duration_years = ctr_mm // 12 + 1           # 1-based 경과연수

    # risk_cd → exit_risk 인덱스 매핑
    # qx_ctr_per_risk[dedup_pos] = dedup qx for risk_cds[exit_idx_ctr[dedup_pos]]
    risk_to_dedup = {}
    for dp, ri in enumerate(exit_idx_ctr):
        risk_to_dedup[risk_cds[ri]] = dp

    # rsvamt_drpo: is_exit_rsv에 해당하는 risk들의 dedup qx 합
    rsvamt_drpo_rate = np.zeros(n_steps, dtype=np.float64)
    for ri in range(len(risk_cds)):
        if is_exit_rsv[ri] and risk_cds[ri] in risk_to_dedup:
            dp = risk_to_dedup[risk_cds[ri]]
            rsvamt_drpo_rate += qx_ctr_per_risk[dp]

    # 급부별 계산
    bnft_results = {}

    for bnft_no, mapping in sorted(bnft_mapping.items()):
        # NCOV (부담보): 경과월 미만이면 전부 0
        ncov_mm = get_ncov_months_fn(bnft_no) if get_ncov_months_fn else 0
        # 부담보 마스크: elapsed_mm + t < ncov_mm 이면 미보장
        # ctr_mm은 경과월(0-based). 부담보는 가입 후 ncov_mm까지.
        # ctr_mm >= ncov_mm 이면 보장 시작
        in_coverage = (ctr_mm >= ncov_mm)

        # bnft_drpo_k: 이 급부의 DRPO 위험률 합
        bnft_drpo = np.zeros(n_steps, dtype=np.float64)
        for rsk_cd in mapping["drpo_cds"]:
            if rsk_cd in risk_to_dedup:
                dp = risk_to_dedup[rsk_cd]
                bnft_drpo += qx_ctr_per_risk[dp]

        # bnft_rskrt_k: 이 급부의 RSKRT 위험률 합
        bnft_rskrt = np.zeros(n_steps, dtype=np.float64)
        for rsk_cd in mapping["rskrt_cds"]:
            if rsk_cd in risk_to_dedup:
                dp = risk_to_dedup[rsk_cd]
                bnft_rskrt += qx_ctr_per_risk[dp]

        # 부담보 적용
        trmnat = np.where(in_coverage, wx_ctr, 0.0)
        rsvamt_drpo = np.where(in_coverage, rsvamt_drpo_rate, 0.0)
        bnft_drpo_masked = np.where(in_coverage, bnft_drpo, 0.0)
        bnft_rskrt_masked = np.where(in_coverage, bnft_rskrt, 0.0)

        # exit rate & tpx
        # BN 규칙: t=0은 초기 시점 (탈퇴 없음), t≥1부터 탈퇴
        bn_exit = trmnat + rsvamt_drpo + bnft_drpo_masked
        bn_exit = np.clip(bn_exit, 0, 1)

        trmo = np.ones(n_steps, dtype=np.float64)
        trme = np.ones(n_steps, dtype=np.float64)
        trmpsn = np.zeros(n_steps, dtype=np.float64)
        rsvamt_drpsn = np.zeros(n_steps, dtype=np.float64)
        defry_drpsn = np.zeros(n_steps, dtype=np.float64)
        bnft_ocurpe = np.zeros(n_steps, dtype=np.float64)

        # t=0: TRMO=1, TRME=1, 모든 count=0
        for t in range(1, n_steps):
            trmo[t] = trme[t - 1]
            trme[t] = trmo[t] * (1.0 - bn_exit[t])
            trmpsn[t] = trmo[t] * trmnat[t]
            rsvamt_drpsn[t] = trmo[t] * bnft_drpo_masked[t]  # BN.RSVAMT_DEFRY_DRPSN = TRMO × BNFT_DRPO
            defry_drpsn[t] = trmo[t] * rsvamt_drpo[t]        # BN.DEFRY_DRPSN = TRMO × RSVAMT_DRPO
            bnft_ocurpe[t] = trmo[t] * bnft_rskrt_masked[t]

        # CRIT_AMT
        crit_amt_arr = np.full(n_steps, join_amt, dtype=np.float64)

        # DEFRY_RT (경과연수별)
        defry_rt_arr = np.ones(n_steps, dtype=np.float64)
        if get_defry_rate_fn:
            for t in range(n_steps):
                defry_rt_arr[t] = get_defry_rate_fn(bnft_no, int(duration_years[t]))

        # PRTT_RT (분담률, 준비금 배수)
        prtt_rt_arr = np.zeros(n_steps, dtype=np.float64)
        if get_prtt_rate_fn:
            # PRTT base rate from IP_B_PRTT_BNFT_RT
            has_prtt = False
            for t in range(n_steps):
                v = get_prtt_rate_fn(bnft_no, int(duration_years[t]))
                prtt_rt_arr[t] = v
                if v != 0:
                    has_prtt = True

            # PRTT × 적립금/가입금액 (준비금 배수)
            if has_prtt and aply_prem_acumamt_bnft is not None and join_amt > 0:
                acum_ratio = aply_prem_acumamt_bnft / join_amt
                prtt_rt_arr = prtt_rt_arr * acum_ratio

        # GRADIN_RT (현재 전부 0)
        gradin_rt_arr = np.zeros(n_steps, dtype=np.float64)

        # PYAMT
        # PRTT_RT != 0 → PYAMT = CRIT_AMT × PRTT_RT
        # PRTT_RT == 0 → PYAMT = CRIT_AMT × DEFRY_RT
        pyamt = np.where(
            prtt_rt_arr != 0,
            crit_amt_arr * prtt_rt_arr,
            crit_amt_arr * defry_rt_arr,
        )

        # BNFT_INSUAMT = BNFT_OCURPE × PYAMT
        bnft_insuamt = bnft_ocurpe * pyamt

        bnft_results[bnft_no] = BNFTResult(
            bnft_no=bnft_no,
            n_steps=n_steps,
            trmnat_rt=trmnat,
            rsvamt_defry_drpo_rskrt=rsvamt_drpo,
            bnft_drpo_rskrt=bnft_drpo_masked,
            bnft_rskrt=bnft_rskrt_masked,
            trmo_mtnpsn_cnt=trmo,
            trmpsn_cnt=trmpsn,
            rsvamt_defry_drpsn_cnt=rsvamt_drpsn,
            defry_drpsn_cnt=defry_drpsn,
            trme_mtnpsn_cnt=trme,
            bnft_ocurpe_cnt=bnft_ocurpe,
            crit_amt=crit_amt_arr,
            defry_rt=defry_rt_arr,
            prtt_rt=prtt_rt_arr,
            gradin_rt=gradin_rt_arr,
            pyamt=pyamt,
            bnft_insuamt=bnft_insuamt,
        )

    return BNResult(idno=idno, n_steps=n_steps, bnft_results=bnft_results)

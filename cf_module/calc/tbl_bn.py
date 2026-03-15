"""OD_TBL_BN 산출 엔진 (Phase 2).

급부(BNFT_NO)별 독립 중복제거 + tpx + 지급금액 계산.

핵심 로직:
  1. BNFT별 독립 exit set 구성: wx + RSVAMT + BNFT_DRPO + BNFT_RSKRT(only)
  2. Per-BNFT C행렬 구축 + dedup (RSKRT-only는 수동적 참여: 열=0)
  3. 급부별 독립 exit rate -> tpx -> TRME/TRMO
  4. DEFRY_RT, PRTT_RT, PYAMT, BNFT_INSUAMT 산출
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

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


def _bn_dedup(
    wx: np.ndarray,
    qx_rates: Dict[str, np.ndarray],
    exit_cds: List[str],
    rskrt_only_cds: Set[str],
    risk_meta: Dict[str, dict],
    n_steps: int,
) -> np.ndarray:
    """Per-BNFT 중복제거.

    C행렬 규칙:
      1. 대각선 = 0
      2. 동일 RSK_GRP_NO = 0
      3. DEAD_RT_DVCD=0(사망위험) 열 = 0
      4. RSKRT-only risk 열 = 0 (수동적 dedup: 다른 risk에 영향 안 줌)

    Args:
        wx: (n_steps,) 해지율
        qx_rates: {risk_cd: (n_steps,)} raw qx
        exit_cds: C행렬에 포함할 risk codes (RSVAMT + DRPO + RSKRT_only)
        rskrt_only_cds: RSKRT-only risks (DRPO에 없는 RSKRT)
        risk_meta: {risk_cd: {"grp": str, "dead": int}}
        n_steps: 스텝 수

    Returns:
        r_dedup: (1 + n_exit, n_steps) — [0]=wx, [1:]=exit risks
    """
    n_exit = len(exit_cds)
    n_rates = 1 + n_exit

    # rate 배열 구성
    r = np.zeros((n_rates, n_steps), dtype=np.float64)
    r[0] = wx
    for i, cd in enumerate(exit_cds):
        arr = qx_rates.get(cd)
        if arr is not None:
            n = min(len(arr), n_steps)
            r[i + 1, :n] = arr[:n]

    if n_exit == 0:
        return r

    # C행렬
    C = np.ones((n_rates, n_rates), dtype=np.float64)
    np.fill_diagonal(C, 0.0)

    # 동일그룹 = 0
    groups = np.empty(n_rates, dtype=object)
    groups[0] = "__wx__"
    for k, cd in enumerate(exit_cds):
        groups[k + 1] = risk_meta.get(cd, {}).get("grp", f"__{cd}__")
    same_grp = groups[:, None] == groups[None, :]
    np.fill_diagonal(same_grp, False)
    C[same_grp] = 0.0

    # 사망위험(dead=0) 열 = 0
    death = np.zeros(n_rates, dtype=bool)
    for k, cd in enumerate(exit_cds):
        death[k + 1] = (risk_meta.get(cd, {}).get("dead", 1) == 0)
    C[:, death] = 0.0

    # RSKRT-only: 열 = 0 (다른 risk의 dedup에 영향 안 줌)
    for k, cd in enumerate(exit_cds):
        if cd in rskrt_only_cds:
            C[:, k + 1] = 0.0

    # dedup: q'i = qi × (1 - Σj(qj × Cij) / 2)
    adj = (r.T @ C.T).T / 2.0
    return r * (1.0 - adj)


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
    # Raw rates (OD_RSK_RT / OD_LAPSE_RT)
    qx_monthly_rates: Dict[str, np.ndarray],  # {risk_cd: (n_steps,)}
    wx_monthly: np.ndarray,                     # (n_steps,)
    # Risk meta
    risk_meta: Dict[str, dict],   # {risk_cd: {"grp": str, "dead": int}}
    # Exit flags
    rsvamt_cds: Set[str],         # RSVAMT exit risk codes
    # BNFT mapping
    bnft_mapping: Dict[int, dict],  # {bnft_no: {"rskrt_cds": [...], "drpo_cds": [...]}}
    # Reference data callbacks
    get_defry_rate_fn=None,       # fn(bnft_no, duration_year) -> float
    get_prtt_rate_fn=None,        # fn(bnft_no, duration_year) -> float
    get_ncov_months_fn=None,      # fn(bnft_no) -> int
    # TRAD_PV 적립금 (PRTT 산출용)
    aply_prem_acumamt_bnft: Optional[np.ndarray] = None,
) -> BNResult:
    """한 계약의 전체 BN 산출 (Phase 2: Per-BNFT 독립 dedup).

    각 BNFT별로 독립 exit set를 구성하고 C행렬 dedup 수행:
      exit set = wx + RSVAMT risks + BNFT DRPO risks + BNFT RSKRT-only risks
      RSKRT-only risks: C행렬 열=0 (수동적 dedup)
    """
    # 시간축
    elapsed_mm = pass_yy * 12 + pass_mm
    ctr_mm = np.arange(n_steps) + elapsed_mm
    duration_years = ctr_mm // 12 + 1

    bnft_results = {}

    for bnft_no, mapping in sorted(bnft_mapping.items()):
        drpo_cds = set(mapping["drpo_cds"])
        rskrt_cds = set(mapping["rskrt_cds"])
        rskrt_only_cds = rskrt_cds - drpo_cds

        # NCOV (부담보)
        ncov_mm = get_ncov_months_fn(bnft_no) if get_ncov_months_fn else 0
        in_coverage = (ctr_mm >= ncov_mm)

        # -- Per-BNFT dedup --
        # exit set: RSVAMT + DRPO + RSKRT_only (unique, 순서 유지)
        exit_cds = list(dict.fromkeys(
            list(rsvamt_cds) + list(drpo_cds) + list(rskrt_only_cds)
        ))

        r_dedup = _bn_dedup(
            wx_monthly, qx_monthly_rates, exit_cds,
            rskrt_only_cds, risk_meta, n_steps,
        )

        # rate 추출
        trmnat = r_dedup[0]
        rsvamt_drpo = np.zeros(n_steps, dtype=np.float64)
        bnft_drpo = np.zeros(n_steps, dtype=np.float64)
        bnft_rskrt = np.zeros(n_steps, dtype=np.float64)
        for i, cd in enumerate(exit_cds):
            if cd in rsvamt_cds:
                rsvamt_drpo += r_dedup[i + 1]
            if cd in drpo_cds:
                bnft_drpo += r_dedup[i + 1]
            if cd in rskrt_cds:
                bnft_rskrt += r_dedup[i + 1]

        # 부담보 적용
        trmnat = np.where(in_coverage, trmnat, 0.0)
        rsvamt_drpo = np.where(in_coverage, rsvamt_drpo, 0.0)
        bnft_drpo = np.where(in_coverage, bnft_drpo, 0.0)
        bnft_rskrt = np.where(in_coverage, bnft_rskrt, 0.0)

        # exit rate & tpx
        bn_exit = np.clip(trmnat + rsvamt_drpo + bnft_drpo, 0, 1)

        trmo = np.ones(n_steps, dtype=np.float64)
        trme = np.ones(n_steps, dtype=np.float64)
        trmpsn = np.zeros(n_steps, dtype=np.float64)
        rsvamt_drpsn = np.zeros(n_steps, dtype=np.float64)
        defry_drpsn = np.zeros(n_steps, dtype=np.float64)
        bnft_ocurpe = np.zeros(n_steps, dtype=np.float64)

        # t=0: TRMO=1, TRME=1, counts=0
        for t in range(1, n_steps):
            trmo[t] = trme[t - 1]
            trme[t] = trmo[t] * (1.0 - bn_exit[t])
            trmpsn[t] = trmo[t] * trmnat[t]
            rsvamt_drpsn[t] = trmo[t] * bnft_drpo[t]    # BN: RSVAMT_DEFRY_DRPSN = TRMO x BNFT_DRPO
            defry_drpsn[t] = trmo[t] * rsvamt_drpo[t]    # BN: DEFRY_DRPSN = TRMO x RSVAMT_DRPO
            bnft_ocurpe[t] = trmo[t] * bnft_rskrt[t]

        # CRIT_AMT
        crit_amt_arr = np.full(n_steps, join_amt, dtype=np.float64)

        # DEFRY_RT
        defry_rt_arr = np.ones(n_steps, dtype=np.float64)
        if get_defry_rate_fn:
            for t in range(n_steps):
                defry_rt_arr[t] = get_defry_rate_fn(bnft_no, int(duration_years[t]))

        # PRTT_RT
        prtt_rt_arr = np.zeros(n_steps, dtype=np.float64)
        has_prtt = False
        if get_prtt_rate_fn:
            for t in range(n_steps):
                v = get_prtt_rate_fn(bnft_no, int(duration_years[t]))
                prtt_rt_arr[t] = v
                if v != 0:
                    has_prtt = True

        # PRTT 활성 시: DEFRY_RT = 0
        if has_prtt:
            defry_rt_arr = np.zeros(n_steps, dtype=np.float64)

        # GRADIN_RT
        gradin_rt_arr = np.zeros(n_steps, dtype=np.float64)

        # PYAMT: PRTT 있으면 CRIT×PRTT, 없으면 CRIT×DEFRY
        pyamt = np.where(
            prtt_rt_arr != 0,
            crit_amt_arr * prtt_rt_arr,
            crit_amt_arr * defry_rt_arr,
        )

        # BNFT_INSUAMT
        bnft_insuamt = bnft_ocurpe * pyamt

        bnft_results[bnft_no] = BNFTResult(
            bnft_no=bnft_no,
            n_steps=n_steps,
            trmnat_rt=trmnat,
            rsvamt_defry_drpo_rskrt=rsvamt_drpo,
            bnft_drpo_rskrt=bnft_drpo,
            bnft_rskrt=bnft_rskrt,
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

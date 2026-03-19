"""OD_TBL_MN 산출 엔진.

중복제거 위험률 + 해지율 → tpx(유지자수) → 탈퇴자 분해.

입력:
  - OD_RSK_RT의 INVLD_TRMNAT_AF_APLY_RSK_RT (최종 월율)
  - OD_LAPSE_RT의 APLY_TRMNAT_RT (월 해지율)
  - exit_flags (IP_R_COV_RSKRT_C / IP_R_BNFT_RSKRT_C)
출력: MNResult — OD_TBL_MN 18개 컬럼

핵심 로직:
  1. OD_RSK_RT → 월율 (이미 면책/BEPRD/월변환 적용됨)
  2. OD_LAPSE_RT → 월 해지율 (이미 월변환 적용됨)
  3. C행렬 중복제거: CTR (RSVAMT|BNFT), PAY (RSVAMT|BNFT|PYEXSP)
  4. tpx = cumprod(1 - total_exit_rate)
  5. 탈퇴자수 = tpx_bot × rate
"""
from typing import Dict, List, Tuple

import numpy as np

from cf_module.data.rsk_lapse_loader import ContractInfo, RiskInfo


# ---------------------------------------------------------------------------
# MNResult: dict 대신 명시적 타입으로 중간 결과 전달
# ---------------------------------------------------------------------------

class MNResult(dict):
    """TBL_MN 산출 결과.

    dict를 상속하여 기존 ``tbl_mn.get("CTR_TRMO_MTNPSN_CNT")`` 호출을
    깨뜨리지 않으면서, 속성 접근도 지원한다.

    Usage::
        mn = compute_tbl_mn(...)
        # dict 스타일 (하위호환)
        mn.get("CTR_TRMO_MTNPSN_CNT")
        mn["CTR_TRME_MTNPSN_CNT"]
        # 속성 스타일 (IDE 자동완성)
        mn.ctr_trmo
        mn.ctr_trme
    """

    # --- CTR (유지자 기준) 속성 ---
    @property
    def ctr_trmo(self) -> np.ndarray:
        return self["CTR_TRMO_MTNPSN_CNT"]

    @property
    def ctr_trme(self) -> np.ndarray:
        return self["CTR_TRME_MTNPSN_CNT"]

    @property
    def ctr_trmnat_rt(self) -> np.ndarray:
        return self["CTR_TRMNAT_RT"]

    @property
    def ctr_rsvamt_rt(self) -> np.ndarray:
        return self["CTR_RSVAMT_DEFRY_DRPO_RSKRT"]

    @property
    def ctr_trmpsn(self) -> np.ndarray:
        return self["CTR_TRMPSN_CNT"]

    @property
    def ctr_rsvamt_drpsn(self) -> np.ndarray:
        return self["CTR_RSVAMT_DEFRY_DRPSN_CNT"]

    # --- PAY (납입자 기준) 속성 ---
    @property
    def pay_trmo(self) -> np.ndarray:
        return self["PAY_TRMO_MTNPSN_CNT"]

    @property
    def pay_trme(self) -> np.ndarray:
        return self["PAY_TRME_MTNPSN_CNT"]


def build_c_matrix(
    risks: List[RiskInfo],
    exit_flags: Dict[str, Dict[str, int]],
    exit_type: str = "ctr",
) -> Tuple[np.ndarray, np.ndarray]:
    """중복제거 C행렬 구축.

    C행렬 규칙 (Cij = 0):
    1. i == j (자기자신)
    2. risk_group[i] == risk_group[j] (동일위험그룹)
    3. is_death[j] == True (j가 사망위험, dead_rt_dvcd=0)

    Args:
        risks: 위험률코드 메타 리스트
        exit_flags: {rsk_cd: {"rsvamt": 0|1, "bnft": 0|1, "pyexsp": 0|1}}
        exit_type: "ctr" or "pay"

    Returns:
        C: (n_rates, n_rates) — n_rates = 1(wx) + n_exit_risks
        exit_idx: (n_exit,) — risks 리스트 내 인덱스
    """
    exit_idx = []
    for i, risk in enumerate(risks):
        f = exit_flags.get(risk.risk_cd, {})
        rsv = f.get("rsvamt", 0)
        bnft = f.get("bnft", 0)
        pye = f.get("pyexsp", 0)
        if exit_type == "ctr":
            if rsv or bnft:
                exit_idx.append(i)
        else:  # pay
            if rsv or bnft or pye:
                exit_idx.append(i)

    exit_idx = np.array(exit_idx, dtype=np.int32)
    n_exit = len(exit_idx)
    n_rates = 1 + n_exit  # wx(0번) + exit risks

    C = np.ones((n_rates, n_rates), dtype=np.float64)

    # 1. 대각선 = 0
    np.fill_diagonal(C, 0.0)

    # 2. 동일위험그룹 = 0
    groups = np.empty(n_rates, dtype=object)
    groups[0] = "__wx__"
    for k, ri in enumerate(exit_idx):
        groups[k + 1] = risks[ri].rsk_grp_no

    same_group = (groups[:, None] == groups[None, :])
    np.fill_diagonal(same_group, False)
    C[same_group] = 0.0

    # 3. j가 사망위험(dead_rt_dvcd=0)이면 해당 열 = 0
    death_mask = np.zeros(n_rates, dtype=bool)
    death_mask[0] = False  # wx
    for k, ri in enumerate(exit_idx):
        death_mask[k + 1] = (risks[ri].dead_rt_dvcd == 0)
    C[:, death_mask] = 0.0

    return C, exit_idx


def _apply_dedup(
    qx_monthly: np.ndarray,      # (n_risks, n_steps)
    wx_monthly: np.ndarray,       # (n_steps,)
    c_matrix: np.ndarray,         # (n_rates, n_rates)
    exit_idx: np.ndarray,         # (n_exit,)
) -> Tuple[np.ndarray, np.ndarray]:
    """중복제거 적용.

    q'ᵢ = qᵢ × (1 - Σⱼ(qⱼ × Cᵢⱼ) / 2)

    Returns:
        qx_dedup: (n_exit, n_steps)
        wx_dedup: (n_steps,)
    """
    n_exit = len(exit_idx)
    if n_exit == 0:
        return np.zeros((0, len(wx_monthly)), dtype=np.float64), wx_monthly.copy()

    n_rates = 1 + n_exit
    n_steps = len(wx_monthly)

    # r: (n_rates, n_steps)
    r = np.empty((n_rates, n_steps), dtype=np.float64)
    r[0] = wx_monthly
    r[1:] = qx_monthly[exit_idx]

    # 중복제거: r_flat @ C.T → adjustment
    C_T = c_matrix.T
    r_flat = r.T  # (n_steps, n_rates)
    adj_flat = r_flat @ C_T  # (n_steps, n_rates)
    adjustment = adj_flat.T / 2.0  # (n_rates, n_steps)

    r_dedup = r * (1.0 - adjustment)

    return r_dedup[1:], r_dedup[0]


def compute_tbl_mn(
    ctr: ContractInfo,
    risks: List[RiskInfo],
    qx_monthly_rates: Dict[str, np.ndarray],
    wx_monthly: np.ndarray,
    exit_flags: Dict[str, Dict[str, int]],
    n_steps: int,
    return_dedup: bool = False,
) -> dict:
    """계약 1건의 OD_TBL_MN 산출.

    Args:
        ctr: 계약 정보
        risks: 위험률코드 메타 리스트 (exit_flags에 있는 모든 코드 포함)
        qx_monthly_rates: {risk_cd: ndarray(n_steps)} — OD_RSK_RT의 INVLD_TRMNAT_AF_APLY_RSK_RT
        wx_monthly: ndarray(n_steps) — OD_LAPSE_RT의 APLY_TRMNAT_RT
        exit_flags: {rsk_cd: {"rsvamt": 0|1, "bnft": 0|1, "pyexsp": 0|1}}
        n_steps: 프로젝션 스텝 수
        return_dedup: True이면 (result, dedup_detail) 튜플 반환 (BN 연계용)

    Returns:
        return_dedup=False: {column_name: ndarray(n_steps)} — 18개 컬럼
        return_dedup=True: (result_dict, dedup_detail_dict)
    """
    elapsed = ctr.pass_yy * 12 + ctr.pass_mm
    bterm_months = ctr.bterm_yy * 12

    # 만기도래 계약: elapsed >= bterm → 전부 0 (TRME=1, PAY_TRMO=1만 예외)
    if elapsed >= bterm_months:
        z = np.zeros(n_steps, dtype=np.float64)
        result = {}
        for col in ["CTR_TRMO_MTNPSN_CNT", "CTR_TRMNAT_RT",
                     "CTR_RSVAMT_DEFRY_DRPO_RSKRT", "CTR_BNFT_DRPO_RSKRT",
                     "CTR_TRMPSN_CNT", "CTR_RSVAMT_DEFRY_DRPSN_CNT",
                     "CTR_BNFT_DEFRY_DRPSN_CNT",
                     "PAY_TRMNAT_RT", "PAY_RSVAMT_DEFRY_DRPO_RSKRT",
                     "PAY_BNFT_DRPO_RSKRT", "PYEXSP_DRPO_RSKRT",
                     "PAY_TRMPSN_CNT", "PAY_RSVAMT_DEFRY_DRPSN_CNT",
                     "PAY_BNFT_DEFRY_DRPSN_CNT", "PYEXSP_DRPSN_CNT"]:
            result[col] = z.copy()
        result["CTR_TRME_MTNPSN_CNT"] = np.ones(n_steps, dtype=np.float64)
        result["PAY_TRME_MTNPSN_CNT"] = np.ones(n_steps, dtype=np.float64)
        if ctr.pay_stcd == "3":
            result["PAY_TRMO_MTNPSN_CNT"] = z.copy()
            result["PAY_TRME_MTNPSN_CNT"] = z.copy()
        else:
            result["PAY_TRMO_MTNPSN_CNT"] = np.ones(n_steps, dtype=np.float64)
        result = MNResult(result)
        if return_dedup:
            dedup_detail = {
                "risk_cds": [r.risk_cd for r in risks],
                "qx_ctr_per_risk": np.zeros((0, n_steps), dtype=np.float64),
                "exit_idx_ctr": np.array([], dtype=np.int32),
                "is_exit_rsv": np.zeros(len(risks), dtype=bool),
                "wx_ctr": z.copy(),
            }
            return result, dedup_detail
        return result

    t_range = np.arange(n_steps, dtype=np.int32)
    duration_months = elapsed + t_range
    is_in_force = duration_months <= bterm_months

    # ── 1. 위험률 월율 배열 구축 ──
    n_risks = len(risks)
    qx_monthly = np.zeros((n_risks, n_steps), dtype=np.float64)

    for r_idx, risk in enumerate(risks):
        rate_arr = qx_monthly_rates.get(risk.risk_cd)
        if rate_arr is not None:
            n = min(len(rate_arr), n_steps)
            qx_monthly[r_idx, :n] = rate_arr[:n]

    # ── 2. C행렬 + 중복제거 ──
    c_ctr, exit_idx_ctr = build_c_matrix(risks, exit_flags, "ctr")
    c_pay, exit_idx_pay = build_c_matrix(risks, exit_flags, "pay")

    qx_dedup_ctr, wx_dedup_ctr = _apply_dedup(qx_monthly, wx_monthly, c_ctr, exit_idx_ctr)
    qx_dedup_pay, wx_dedup_pay = _apply_dedup(qx_monthly, wx_monthly, c_pay, exit_idx_pay)

    # ── 3. tpx (CTR/PAY) ──
    total_qx_ctr = qx_dedup_ctr.sum(axis=0) if len(exit_idx_ctr) > 0 else np.zeros(n_steps)
    dx_ctr = np.clip(total_qx_ctr + wx_dedup_ctr, 0, 1)
    dx_ctr *= is_in_force
    dx_ctr[0] = 0.0  # t=0: 탈퇴 없음
    tpx = np.cumprod(1.0 - dx_ctr)

    total_qx_pay = qx_dedup_pay.sum(axis=0) if len(exit_idx_pay) > 0 else np.zeros(n_steps)
    dx_pay = np.clip(total_qx_pay + wx_dedup_pay, 0, 1)
    dx_pay *= is_in_force
    dx_pay[0] = 0.0  # t=0: 탈퇴 없음
    pay_tpx = np.cumprod(1.0 - dx_pay)

    # ── 4. tpx_bot (기시유지자) ──
    tpx_bot = np.ones(n_steps, dtype=np.float64)
    tpx_bot[1:] = tpx[:-1]

    pay_tpx_bot = np.ones(n_steps, dtype=np.float64)
    pay_tpx_bot[1:] = pay_tpx[:-1]

    # ── 5. 탈퇴율/탈퇴자 분해 ──
    # CTR RSVAMT/BNFT
    rsv_mask = np.array([exit_flags.get(risks[i].risk_cd, {}).get("rsvamt", 0)
                         for i in exit_idx_ctr], dtype=bool)
    bnft_mask = np.array([exit_flags.get(risks[i].risk_cd, {}).get("bnft", 0)
                          for i in exit_idx_ctr], dtype=bool)

    ctr_rsvamt_rt = qx_dedup_ctr[rsv_mask].sum(axis=0) if rsv_mask.any() else np.zeros(n_steps)
    ctr_bnft_rt = qx_dedup_ctr[bnft_mask].sum(axis=0) if bnft_mask.any() else np.zeros(n_steps)

    # PAY RSVAMT/BNFT/PYEXSP
    pay_rsv_mask = np.array([exit_flags.get(risks[i].risk_cd, {}).get("rsvamt", 0)
                             for i in exit_idx_pay], dtype=bool)
    pay_bnft_mask = np.array([exit_flags.get(risks[i].risk_cd, {}).get("bnft", 0)
                              for i in exit_idx_pay], dtype=bool)
    pyexsp_only_mask = np.array([
        exit_flags.get(risks[i].risk_cd, {}).get("pyexsp", 0) == 1
        and exit_flags.get(risks[i].risk_cd, {}).get("rsvamt", 0) == 0
        and exit_flags.get(risks[i].risk_cd, {}).get("bnft", 0) == 0
        for i in exit_idx_pay
    ], dtype=bool)

    pay_rsvamt_rt = qx_dedup_pay[pay_rsv_mask].sum(axis=0) if pay_rsv_mask.any() else np.zeros(n_steps)
    pay_bnft_rt = qx_dedup_pay[pay_bnft_mask].sum(axis=0) if pay_bnft_mask.any() else np.zeros(n_steps)
    pyexsp_rt = qx_dedup_pay[pyexsp_only_mask].sum(axis=0) if pyexsp_only_mask.any() else np.zeros(n_steps)

    # 탈퇴자수
    ctr_trmpsn = tpx_bot * wx_dedup_ctr
    ctr_rsvamt_drpsn = tpx_bot * ctr_rsvamt_rt
    ctr_bnft_drpsn = tpx_bot * ctr_bnft_rt

    pay_trmpsn = pay_tpx_bot * wx_dedup_pay
    pay_rsvamt_drpsn = pay_tpx_bot * pay_rsvamt_rt
    pay_bnft_drpsn = pay_tpx_bot * pay_bnft_rt
    pyexsp_drpsn = pay_tpx_bot * pyexsp_rt

    # ── 6. PAY_STCD=3 (납입면제) → PAY tpx=0, rates 유지, counts=0 ──
    if ctr.pay_stcd == "3":
        z = np.zeros(n_steps, dtype=np.float64)
        pay_tpx = z.copy()
        pay_tpx_bot = z.copy()
        pay_trmpsn = z.copy()
        pay_rsvamt_drpsn = z.copy()
        pay_bnft_drpsn = z.copy()
        pyexsp_drpsn = z.copy()

    # ── 7. t=0 초기화 ──
    tpx_bot[0] = 0.0           # CTR_TRMO[0] = 0
    tpx[0] = 1.0               # CTR_TRME[0] = 1
    if ctr.pay_stcd != "3":
        pay_tpx[0] = 1.0      # PAY_TRME[0] = 1
    ctr_trmpsn[0] = 0.0
    ctr_rsvamt_drpsn[0] = 0.0
    ctr_bnft_drpsn[0] = 0.0
    pay_trmpsn[0] = 0.0
    pay_rsvamt_drpsn[0] = 0.0
    pay_bnft_drpsn[0] = 0.0
    pyexsp_drpsn[0] = 0.0

    result = MNResult({
        # CTR (유지자 기준)
        "CTR_TRMO_MTNPSN_CNT": tpx_bot,
        "CTR_TRMNAT_RT": wx_dedup_ctr,
        "CTR_RSVAMT_DEFRY_DRPO_RSKRT": ctr_rsvamt_rt,
        "CTR_BNFT_DRPO_RSKRT": ctr_bnft_rt,
        "CTR_TRMPSN_CNT": ctr_trmpsn,
        "CTR_RSVAMT_DEFRY_DRPSN_CNT": ctr_rsvamt_drpsn,
        "CTR_BNFT_DEFRY_DRPSN_CNT": ctr_bnft_drpsn,
        "CTR_TRME_MTNPSN_CNT": tpx,
        # PAY (납입자 기준)
        "PAY_TRMO_MTNPSN_CNT": pay_tpx_bot,
        "PAY_TRMNAT_RT": wx_dedup_pay,
        "PAY_RSVAMT_DEFRY_DRPO_RSKRT": pay_rsvamt_rt,
        "PAY_BNFT_DRPO_RSKRT": pay_bnft_rt,
        "PYEXSP_DRPO_RSKRT": pyexsp_rt,
        "PAY_TRMPSN_CNT": pay_trmpsn,
        "PAY_RSVAMT_DEFRY_DRPSN_CNT": pay_rsvamt_drpsn,
        "PAY_BNFT_DEFRY_DRPSN_CNT": pay_bnft_drpsn,
        "PYEXSP_DRPSN_CNT": pyexsp_drpsn,
        "PAY_TRME_MTNPSN_CNT": pay_tpx,
    })

    if return_dedup:
        dedup_detail = {
            "risk_cds": [r.risk_cd for r in risks],
            "qx_ctr_per_risk": qx_dedup_ctr,
            "exit_idx_ctr": exit_idx_ctr,
            "is_exit_rsv": rsv_mask,
            "wx_ctr": wx_dedup_ctr,
        }
        return result, dedup_detail

    return result

"""OD_CF 캐시플로우 산출.

MN × TRAD_PV (보험료) + BN (보험금) + MN × EXP (사업비) 결합.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np

from cf_module.calc.exp import ExpResult


@dataclass
class CFResult:
    """단일 계약 CF 결과."""
    n_steps: int
    prem_base: np.ndarray       # CTR_TRMO × ORIG_PREM × PREM_PAY_YN
    prem_pyex: np.ndarray       # (CTR_TRME[s-1] - PAY_TRME[s-1]) × ORIG × PAY_YN
    prem_add: np.ndarray        # 추가보험료 (미구현, 0)
    tmrfnd: np.ndarray          # CTR_TRMPSN × CNCTTP_ACUMAMT_KICS
    drpo_pyrv: np.ndarray       # CTR_RSVAMT_DEFRY_DRPSN × APLY_PREM_ACUMAMT_BNFT
    insuamt_gen: np.ndarray     # Σ BN.BNFT_INSUAMT
    insuamt_hafway: np.ndarray  # 중도보험금 (미구현, 0)
    insuamt_matu: np.ndarray    # 만기보험금 (미구현, 0)
    insuamt_pens: np.ndarray    # 연금 (미구현, 0)
    acqsexp_dr: np.ndarray      # Σ(ACQS × TRMO)
    acqsexp_indr: np.ndarray    # 간접 신계약비 (미구현, 0)
    acqsexp_redem: np.ndarray   # 상환 신계약비 (미구현, 0)
    mntexp_dr: np.ndarray       # Σ(MNT × TRMO)
    mntexp_indr: np.ndarray     # 간접 유지비 (미구현, 0)
    iv_mgmexp_mntexp_ccrfnd: np.ndarray  # (미구현, 0)
    iv_mgmexp_mntexp_cl_remamt: np.ndarray  # (미구현, 0)
    loss_svyexp: np.ndarray     # LSVY_rate × BNFT_INSUAMT
    hafwdr: np.ndarray          # 중도인출 (미구현, 0)
    loan_new: np.ndarray        # 신규대출 (미구현, 0)
    loan_int: np.ndarray        # 대출이자 (미구현, 0)
    loan_rpay_hafway: np.ndarray  # 중도상환 (미구현, 0)
    loan_rpay_matu: np.ndarray    # 만기상환 (미구현, 0)
    prem_acum_rsvamt_alter: np.ndarray  # (미구현, 0)
    prem_add_acumamt_depl: np.ndarray   # (미구현, 0)
    tmrfnd_inpay: np.ndarray    # 납입중 해약환급금 (미구현)
    tmrfnd_pyex: np.ndarray     # 납면후 해약환급금 (미구현)

    def to_dict(self) -> Dict[str, np.ndarray]:
        """컬럼명 → 배열 dict."""
        return {
            "PREM_BASE": self.prem_base,
            "PREM_PYEX": self.prem_pyex,
            "PREM_ADD": self.prem_add,
            "TMRFND": self.tmrfnd,
            "DRPO_PYRV": self.drpo_pyrv,
            "INSUAMT_GEN": self.insuamt_gen,
            "INSUAMT_HAFWAY": self.insuamt_hafway,
            "INSUAMT_MATU": self.insuamt_matu,
            "INSUAMT_PENS": self.insuamt_pens,
            "ACQSEXP_DR": self.acqsexp_dr,
            "ACQSEXP_INDR": self.acqsexp_indr,
            "ACQSEXP_REDEM": self.acqsexp_redem,
            "MNTEXP_DR": self.mntexp_dr,
            "MNTEXP_INDR": self.mntexp_indr,
            "IV_MGMEXP_MNTEXP_CCRFND": self.iv_mgmexp_mntexp_ccrfnd,
            "IV_MGMEXP_MNTEXP_CL_REMAMT": self.iv_mgmexp_mntexp_cl_remamt,
            "LOSS_SVYEXP": self.loss_svyexp,
            "HAFWDR": self.hafwdr,
            "LOAN_NEW": self.loan_new,
            "LOAN_INT": self.loan_int,
            "LOAN_RPAY_HAFWAY": self.loan_rpay_hafway,
            "LOAN_RPAY_MATU": self.loan_rpay_matu,
            "PREM_ACUM_RSVAMT_ALTER": self.prem_acum_rsvamt_alter,
            "PREM_ADD_ACUMAMT_DEPL": self.prem_add_acumamt_depl,
            "TMRFND_INPAY": self.tmrfnd_inpay,
            "TMRFND_PYEX": self.tmrfnd_pyex,
        }


def compute_cf(
    n_steps: int,
    mn: Dict[str, np.ndarray],
    pv: Dict[str, np.ndarray],
    bn_insuamt: np.ndarray,
    exp_results: List[ExpResult],
    exp_items: list,
    lsvy_rate: float = 0.0,
) -> CFResult:
    """OD_CF 산출.

    Args:
        n_steps: 프로젝션 스텝 수
        mn: TBL_MN 결과 dict
        pv: TRAD_PV 결과 dict (to_dict())
        bn_insuamt: BN BNFT_INSUAMT 합산 배열 (n_steps,)
        exp_results: EXP 산출 결과 리스트
        exp_items: EXP 항목 리스트 (PAY_MTNPSN_DVCD 참조용)
        lsvy_rate: LOSS_SVYEXP rate

    Returns:
        CFResult
    """
    z = np.zeros(n_steps, dtype=np.float64)

    ctr_trmo = mn["CTR_TRMO_MTNPSN_CNT"]
    ctr_trme = mn["CTR_TRME_MTNPSN_CNT"]
    pay_trmo = mn["PAY_TRMO_MTNPSN_CNT"]
    pay_trme = mn["PAY_TRME_MTNPSN_CNT"]
    ctr_trmpsn = mn["CTR_TRMPSN_CNT"]
    rsvamt_drpsn = mn["CTR_RSVAMT_DEFRY_DRPSN_CNT"]

    orig_prem = pv["ORIG_PREM"]
    prem_pay_yn = pv["PREM_PAY_YN"]
    acum_bnft = pv["APLY_PREM_ACUMAMT_BNFT"]
    cncttp = pv["CNCTTP_ACUMAMT_KICS"]

    # --- PREM_BASE ---
    prem_base = ctr_trmo * orig_prem * prem_pay_yn

    # --- PREM_PYEX (기말 lag) ---
    prem_pyex = z.copy()
    for s in range(1, n_steps):
        prem_pyex[s] = (ctr_trme[s - 1] - pay_trme[s - 1]) * orig_prem[s] * prem_pay_yn[s]

    # --- TMRFND ---
    tmrfnd = ctr_trmpsn * cncttp

    # --- DRPO_PYRV ---
    drpo_pyrv = rsvamt_drpsn * acum_bnft

    # --- INSUAMT_GEN ---
    insuamt_gen = bn_insuamt.copy()

    # --- LOSS_SVYEXP ---
    loss_svyexp = lsvy_rate * bn_insuamt

    # --- ACQSEXP_DR / MNTEXP_DR ---
    acqsexp_dr = z.copy()
    mntexp_dr = z.copy()

    # EXP items → PAY_MTNPSN_DVCD 매핑
    item_map = {}
    for tpcd, kdcd, item in exp_items:
        item_map[(tpcd, kdcd)] = item

    for res in exp_results:
        item = item_map.get((res.tpcd, res.kdcd), {})
        pay_dvcd = item.get("pay", 0)
        trmo = pay_trmo if pay_dvcd == 1 else ctr_trmo

        if res.tpcd == "ACQS":
            acqsexp_dr += res.values * trmo
        elif res.tpcd == "MNT":
            mntexp_dr += res.values * trmo

    return CFResult(
        n_steps=n_steps,
        prem_base=prem_base,
        prem_pyex=prem_pyex,
        prem_add=z.copy(),
        tmrfnd=tmrfnd,
        drpo_pyrv=drpo_pyrv,
        insuamt_gen=insuamt_gen,
        insuamt_hafway=z.copy(),
        insuamt_matu=z.copy(),
        insuamt_pens=z.copy(),
        acqsexp_dr=acqsexp_dr,
        acqsexp_indr=z.copy(),
        acqsexp_redem=z.copy(),
        mntexp_dr=mntexp_dr,
        mntexp_indr=z.copy(),
        iv_mgmexp_mntexp_ccrfnd=z.copy(),
        iv_mgmexp_mntexp_cl_remamt=z.copy(),
        loss_svyexp=loss_svyexp,
        hafwdr=z.copy(),
        loan_new=z.copy(),
        loan_int=z.copy(),
        loan_rpay_hafway=z.copy(),
        loan_rpay_matu=z.copy(),
        prem_acum_rsvamt_alter=z.copy(),
        prem_add_acumamt_depl=z.copy(),
        tmrfnd_inpay=z.copy(),
        tmrfnd_pyex=z.copy(),
    )

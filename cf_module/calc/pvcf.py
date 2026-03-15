"""OD_PVCF 현가 캐시플로우 산출.

OD_CF × OD_DC_RT(TRMO/TRME) 결합.
기시(TRMO) 할인: 보험료, 사업비 (기초 발생)
기말(TRME) 할인: 보험금, 해약, 손해조사비 (기말 발생)
"""
from dataclasses import dataclass
from typing import Dict

import numpy as np

from cf_module.calc.cf import CFResult
from cf_module.calc.dc_rt import DCRTResult


@dataclass
class PVCFResult:
    """PVCF 결과."""
    n_steps: int
    pay_prem: np.ndarray
    add_pay_prem: np.ndarray
    pyex_bnamt: np.ndarray
    tmrfnd: np.ndarray
    drpo_pyrv: np.ndarray
    insuamt: np.ndarray
    hafway_bnamt: np.ndarray
    matu_insuamt: np.ndarray
    pens_bnamt: np.ndarray
    acqsexp_dr: np.ndarray
    acqsexp_indr: np.ndarray
    acqsexp_redem: np.ndarray
    mntexp_dr: np.ndarray
    mntexp_indr: np.ndarray
    iv_mgmexp_mntexp_ccrfnd: np.ndarray
    iv_mgmexp_mntexp_cl_remamt: np.ndarray
    loss_svyexp: np.ndarray
    hafwdr_amt: np.ndarray
    new_icl_amt: np.ndarray
    icl_int: np.ndarray
    icl_hafway_rpamt: np.ndarray
    matu_icl_amt: np.ndarray
    renw_prem_acum_alter_amt: np.ndarray
    acumamt_depl_adpay_amt: np.ndarray
    prem_alter_pay_amt: np.ndarray
    net_cf_amt: np.ndarray
    icl_net_cf_amt: np.ndarray

    def to_dict(self) -> Dict[str, np.ndarray]:
        return {
            "PAY_PREM": self.pay_prem,
            "ADD_PAY_PREM": self.add_pay_prem,
            "PYEX_BNAMT": self.pyex_bnamt,
            "TMRFND": self.tmrfnd,
            "DRPO_PYRV": self.drpo_pyrv,
            "INSUAMT": self.insuamt,
            "HAFWAY_BNAMT": self.hafway_bnamt,
            "MATU_INSUAMT": self.matu_insuamt,
            "PENS_BNAMT": self.pens_bnamt,
            "ACQSEXP_DR": self.acqsexp_dr,
            "ACQSEXP_INDR": self.acqsexp_indr,
            "ACQSEXP_REDEM": self.acqsexp_redem,
            "MNTEXP_DR": self.mntexp_dr,
            "MNTEXP_INDR": self.mntexp_indr,
            "IV_MGMEXP_MNTEXP_CCRFND": self.iv_mgmexp_mntexp_ccrfnd,
            "IV_MGMEXP_MNTEXP_CL_REMAMT": self.iv_mgmexp_mntexp_cl_remamt,
            "LOSS_SVYEXP": self.loss_svyexp,
            "HAFWDR_AMT": self.hafwdr_amt,
            "NEW_ICL_AMT": self.new_icl_amt,
            "ICL_INT": self.icl_int,
            "ICL_HAFWAY_RPAMT": self.icl_hafway_rpamt,
            "MATU_ICL_AMT": self.matu_icl_amt,
            "RENW_PREM_ACUM_ALTER_AMT": self.renw_prem_acum_alter_amt,
            "ACUMAMT_DEPL_ADPAY_AMT": self.acumamt_depl_adpay_amt,
            "PREM_ALTER_PAY_AMT": self.prem_alter_pay_amt,
            "NET_CF_AMT": self.net_cf_amt,
            "ICL_NET_CF_AMT": self.icl_net_cf_amt,
        }


def compute_pvcf(cf: CFResult, dc: DCRTResult) -> PVCFResult:
    """PVCF = CF × DC_RT.

    기시(TRMO): 보험료, 납면보험료, 사업비
    기말(TRME): 해약, 탈퇴, 보험금, 손해조사비
    """
    n = cf.n_steps
    trmo = dc.trmo_mm_dc_rt
    trme = dc.trme_mm_dc_rt
    z = np.zeros(n, dtype=np.float64)

    # 기시 할인
    pay_prem = cf.prem_base * trmo
    add_pay_prem = cf.prem_add * trmo
    pyex_bnamt = cf.prem_pyex * trmo
    acqsexp_dr = cf.acqsexp_dr * trmo
    acqsexp_indr = cf.acqsexp_indr * trmo
    acqsexp_redem = cf.acqsexp_redem * trmo
    mntexp_dr = cf.mntexp_dr * trmo
    mntexp_indr = cf.mntexp_indr * trmo

    # 기말 할인
    tmrfnd = cf.tmrfnd * trme
    drpo_pyrv = cf.drpo_pyrv * trme
    insuamt = cf.insuamt_gen * trme
    hafway_bnamt = cf.insuamt_hafway * trme
    matu_insuamt = cf.insuamt_matu * trme
    pens_bnamt = cf.insuamt_pens * trme
    loss_svyexp = cf.loss_svyexp * trme
    hafwdr_amt = cf.hafwdr * trme

    # 대출 관련 (기말)
    new_icl_amt = cf.loan_new * trme
    icl_int = cf.loan_int * trme
    icl_hafway_rpamt = cf.loan_rpay_hafway * trme
    matu_icl_amt = cf.loan_rpay_matu * trme

    # 기타 (기시)
    renw_prem_acum = cf.prem_acum_rsvamt_alter * trmo
    acumamt_depl = cf.prem_add_acumamt_depl * trmo
    prem_alter = cf.tmrfnd_inpay * trmo  # placeholder
    iv_ccrfnd = cf.iv_mgmexp_mntexp_ccrfnd * trmo
    iv_cl_remamt = cf.iv_mgmexp_mntexp_cl_remamt * trmo

    # NET_CF = 수입 - 지출
    # 수입: PAY_PREM + ADD_PAY_PREM + PYEX
    # 지출: TMRFND + DRPO + INSUAMT + HAFWAY + MATU + PENS + ACQS + MNT + LSVY + ...
    net_cf = (
        pay_prem + add_pay_prem + pyex_bnamt
        - tmrfnd - drpo_pyrv - insuamt - hafway_bnamt - matu_insuamt - pens_bnamt
        - acqsexp_dr - acqsexp_indr - acqsexp_redem
        - mntexp_dr - mntexp_indr - iv_ccrfnd - iv_cl_remamt
        - loss_svyexp - hafwdr_amt
    )

    icl_net_cf = net_cf - new_icl_amt - icl_int + icl_hafway_rpamt + matu_icl_amt

    return PVCFResult(
        n_steps=n,
        pay_prem=pay_prem,
        add_pay_prem=add_pay_prem,
        pyex_bnamt=pyex_bnamt,
        tmrfnd=tmrfnd,
        drpo_pyrv=drpo_pyrv,
        insuamt=insuamt,
        hafway_bnamt=hafway_bnamt,
        matu_insuamt=matu_insuamt,
        pens_bnamt=pens_bnamt,
        acqsexp_dr=acqsexp_dr,
        acqsexp_indr=acqsexp_indr,
        acqsexp_redem=acqsexp_redem,
        mntexp_dr=mntexp_dr,
        mntexp_indr=mntexp_indr,
        iv_mgmexp_mntexp_ccrfnd=iv_ccrfnd,
        iv_mgmexp_mntexp_cl_remamt=iv_cl_remamt,
        loss_svyexp=loss_svyexp,
        hafwdr_amt=hafwdr_amt,
        new_icl_amt=new_icl_amt,
        icl_int=icl_int,
        icl_hafway_rpamt=icl_hafway_rpamt,
        matu_icl_amt=matu_icl_amt,
        renw_prem_acum_alter_amt=renw_prem_acum,
        acumamt_depl_adpay_amt=acumamt_depl,
        prem_alter_pay_amt=z.copy(),
        net_cf_amt=net_cf,
        icl_net_cf_amt=icl_net_cf,
    )

"""OP_BEL 최선추정부채 산출.

PVCF 전 시점 합산 → 단일 행 결과.
"""
from dataclasses import dataclass
from typing import Dict

import numpy as np

from cf_module.calc.pvcf import PVCFResult


@dataclass
class BELResult:
    """BEL 결과 (단일 행)."""
    prem_base: float
    prem_pyex: float
    prem_add: float
    tmrfnd: float
    drpo_pyrv: float
    insuamt_gen: float
    insuamt_hafway: float
    insuamt_matu: float
    insuamt_pens: float
    acqsexp_dr: float
    acqsexp_indr: float
    acqsexp_redem: float
    mntexp_dr: float
    mntexp_indr: float
    iv_mgmexp_mntexp_ccrfnd: float
    iv_mgmexp_mntexp_cl_remamt: float
    loss_svyexp: float
    hafwdr: float
    loan_new: float
    loan_int: float
    loan_rpay_hafway: float
    loan_rpay_matu: float
    prem_acum_rsvamt_alter: float
    prem_add_acumamt_depl: float
    bel: float
    loan_aset: float

    def to_dict(self) -> Dict[str, float]:
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
            "BEL": self.bel,
            "LOAN_ASET": self.loan_aset,
        }


def compute_bel(pvcf: PVCFResult) -> BELResult:
    """PVCF 전체 합산 → BEL."""
    d = pvcf.to_dict()

    prem_base = d["PAY_PREM"].sum()
    prem_pyex = d["PYEX_BNAMT"].sum()
    prem_add = d["ADD_PAY_PREM"].sum()
    tmrfnd = d["TMRFND"].sum()
    drpo_pyrv = d["DRPO_PYRV"].sum()
    insuamt_gen = d["INSUAMT"].sum()
    insuamt_hafway = d["HAFWAY_BNAMT"].sum()
    insuamt_matu = d["MATU_INSUAMT"].sum()
    insuamt_pens = d["PENS_BNAMT"].sum()
    acqsexp_dr = d["ACQSEXP_DR"].sum()
    acqsexp_indr = d["ACQSEXP_INDR"].sum()
    acqsexp_redem = d["ACQSEXP_REDEM"].sum()
    mntexp_dr = d["MNTEXP_DR"].sum()
    mntexp_indr = d["MNTEXP_INDR"].sum()
    iv_ccrfnd = d["IV_MGMEXP_MNTEXP_CCRFND"].sum()
    iv_cl_remamt = d["IV_MGMEXP_MNTEXP_CL_REMAMT"].sum()
    loss_svyexp = d["LOSS_SVYEXP"].sum()
    hafwdr = d["HAFWDR_AMT"].sum()
    loan_new = d["NEW_ICL_AMT"].sum()
    loan_int = d["ICL_INT"].sum()
    loan_rpay_hafway = d["ICL_HAFWAY_RPAMT"].sum()
    loan_rpay_matu = d["MATU_ICL_AMT"].sum()
    prem_acum = d["RENW_PREM_ACUM_ALTER_AMT"].sum()
    acumamt_depl = d["ACUMAMT_DEPL_ADPAY_AMT"].sum()

    bel = d["NET_CF_AMT"].sum()
    loan_aset = d["ICL_NET_CF_AMT"].sum()

    return BELResult(
        prem_base=prem_base, prem_pyex=prem_pyex, prem_add=prem_add,
        tmrfnd=tmrfnd, drpo_pyrv=drpo_pyrv,
        insuamt_gen=insuamt_gen, insuamt_hafway=insuamt_hafway,
        insuamt_matu=insuamt_matu, insuamt_pens=insuamt_pens,
        acqsexp_dr=acqsexp_dr, acqsexp_indr=acqsexp_indr, acqsexp_redem=acqsexp_redem,
        mntexp_dr=mntexp_dr, mntexp_indr=mntexp_indr,
        iv_mgmexp_mntexp_ccrfnd=iv_ccrfnd, iv_mgmexp_mntexp_cl_remamt=iv_cl_remamt,
        loss_svyexp=loss_svyexp, hafwdr=hafwdr,
        loan_new=loan_new, loan_int=loan_int,
        loan_rpay_hafway=loan_rpay_hafway, loan_rpay_matu=loan_rpay_matu,
        prem_acum_rsvamt_alter=prem_acum, prem_add_acumamt_depl=acumamt_depl,
        bel=bel, loan_aset=loan_aset,
    )

"""OD_TBL_BN 산출용 데이터 캐시.

급부별 위험률 매핑, 지급률, 부담보 등 참조 데이터 일괄 로드.
"""
from typing import Dict, List, Optional, Set, Tuple

import duckdb
import numpy as np

from cf_module.utils.logger import get_logger

logger = get_logger("bn_loader")


class BNDataCache:
    """OD_TBL_BN 참조 데이터 캐시."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._load_bnft_rskrt_c(conn)
        self._load_bnft_bas(conn)
        self._load_bnft_defry_rt(conn)
        self._load_ncov(conn)
        self._load_invld_trmnat(conn)
        self._load_prtt_bnft_rt(conn)

    # ------------------------------------------------------------------
    # IP_R_BNFT_RSKRT_C: BNFT별 위험률 매핑
    # ------------------------------------------------------------------
    def _load_bnft_rskrt_c(self, conn):
        """(prod, cls, cov, bnft_no) → {rsk_rt_cd: {rskrt_yn, drpo_yn}}"""
        self.bnft_rskrt = {}  # (prod, cls, cov, bnft_no) -> [{rsk_cd, rskrt_yn, drpo_yn}]
        rows = conn.execute("""
            SELECT PROD_CD, CLS_CD, COV_CD, BNFT_NO,
                   RSK_RT_CD, BNFT_RSKRT_YN, BNFT_DRPO_RSKRT_YN
            FROM IP_R_BNFT_RSKRT_C
            ORDER BY PROD_CD, CLS_CD, COV_CD, BNFT_NO, RSK_RT_CD
        """).fetchall()
        for r in rows:
            key = (r[0], str(r[1]).zfill(2) if r[1] else "01", r[2], int(r[3]))
            entry = {
                "rsk_cd": str(r[4]),
                "rskrt_yn": bool(r[5]),
                "drpo_yn": bool(r[6]),
            }
            self.bnft_rskrt.setdefault(key, []).append(entry)
        logger.info(f"BNFT_RSKRT_C: {len(self.bnft_rskrt):,} (prod,cls,cov,bnft) keys")

    def get_bnft_risk_mapping(self, prod_cd, cls_cd, cov_cd):
        """특정 (prod,cls,cov)의 BNFT별 위험률 매핑 반환.

        Returns:
            {bnft_no: {"rskrt_cds": [rsk_cd,...], "drpo_cds": [rsk_cd,...]}}
            bnft_no=0 (전체 수준) 제외, 실제 급부만.
        """
        result = {}
        prefix = (prod_cd, cls_cd, cov_cd)
        for (p, c, v, bno), entries in self.bnft_rskrt.items():
            if (p, c, v) != prefix or bno == 0:
                continue
            rskrt_cds = [e["rsk_cd"] for e in entries if e["rskrt_yn"]]
            drpo_cds = [e["rsk_cd"] for e in entries if e["drpo_yn"]]
            result[bno] = {"rskrt_cds": rskrt_cds, "drpo_cds": drpo_cds}
        return result

    # ------------------------------------------------------------------
    # IP_B_BNFT_BAS: 급부 기본 정보
    # ------------------------------------------------------------------
    def _load_bnft_bas(self, conn):
        """(prod, cls, cov) → [bnft_no, ...]"""
        self.bnft_nos = {}  # (prod, cls, cov) -> sorted list of bnft_nos
        rows = conn.execute("""
            SELECT DISTINCT PROD_CD, CLS_CD, COV_CD, BNFT_NO
            FROM IP_B_BNFT_BAS
            ORDER BY PROD_CD, CLS_CD, COV_CD, BNFT_NO
        """).fetchall()
        for r in rows:
            key = (r[0], str(r[1]).zfill(2) if r[1] else "01", r[2])
            self.bnft_nos.setdefault(key, []).append(int(r[3]))
        logger.info(f"BNFT_BAS: {len(self.bnft_nos):,} (prod,cls,cov) keys")

    # ------------------------------------------------------------------
    # IP_B_BNFT_DEFRY_RT: 급부별 지급률
    # ------------------------------------------------------------------
    def _load_bnft_defry_rt(self, conn):
        """(prod, cls, cov, bnft_no) → [(strt, end, defry_rt), ...]"""
        self.defry_rt = {}
        rows = conn.execute("""
            SELECT PROD_CD, CLS_CD, COV_CD, BNFT_NO,
                   SETN_STRT_VAL, SETN_END_VAL, DEFRY_RT
            FROM IP_B_BNFT_DEFRY_RT
            ORDER BY PROD_CD, CLS_CD, COV_CD, BNFT_NO, SETN_STRT_VAL
        """).fetchall()
        for r in rows:
            key = (r[0], str(r[1]).zfill(2) if r[1] else "01", r[2], int(r[3]))
            entry = (int(r[4]) if r[4] else 0, int(r[5]) if r[5] else 999, float(r[6] or 0))
            self.defry_rt.setdefault(key, []).append(entry)
        logger.info(f"BNFT_DEFRY_RT: {len(self.defry_rt):,} keys")

    def get_defry_rate(self, prod_cd, cls_cd, cov_cd, bnft_no, duration_year):
        """경과연수별 지급률 조회."""
        key = (prod_cd, cls_cd, cov_cd, bnft_no)
        entries = self.defry_rt.get(key)
        if not entries:
            return 1.0
        for strt, end, rt in entries:
            if strt <= duration_year <= end:
                return rt
        return 0.0

    # ------------------------------------------------------------------
    # IP_B_NCOV: 부담보
    # ------------------------------------------------------------------
    def _load_ncov(self, conn):
        """(prod, cls, cov, bnft_no) → ncov_months (부담보 기간, 월)"""
        self.ncov = {}
        rows = conn.execute("""
            SELECT PROD_CD, CLS_CD, COV_CD, BNFT_NO,
                   NCOV_PRD_TPCD, NCOV_PRD_CNT
            FROM IP_B_NCOV
        """).fetchall()
        for r in rows:
            key = (r[0], str(r[1]).zfill(2) if r[1] else "01", r[2], int(r[3]))
            tpcd = str(r[4]) if r[4] else "M"
            cnt = int(r[5]) if r[5] else 0
            months = cnt * 12 if tpcd == "Y" else cnt
            self.ncov[key] = months
        logger.info(f"NCOV: {len(self.ncov):,} entries")

    def get_ncov_months(self, prod_cd, cls_cd, cov_cd, bnft_no):
        """부담보 기간(월) 조회. 없으면 0."""
        return self.ncov.get((prod_cd, cls_cd, cov_cd, bnft_no), 0)

    # ------------------------------------------------------------------
    # IP_R_INVLD_TRMNAT: 면책기간
    # ------------------------------------------------------------------
    def _load_invld_trmnat(self, conn):
        """(prod, cls, cov, rsk_cd) → invld_months"""
        self.invld = {}
        rows = conn.execute("""
            SELECT PROD_CD, CLS_CD, COV_CD, RSK_RT_CD,
                   INVLD_TRMNAT_PRD_TPCD, INVLD_TRMNAT_PRD_CNT
            FROM IP_R_INVLD_TRMNAT
        """).fetchall()
        for r in rows:
            key = (r[0], str(r[1]).zfill(2) if r[1] else "01", r[2], str(r[3]))
            tpcd = str(r[4]) if r[4] else "M"
            cnt = int(r[5]) if r[5] else 0
            months = cnt * 12 if tpcd == "Y" else cnt
            self.invld[key] = months
        logger.info(f"INVLD_TRMNAT: {len(self.invld):,} entries")

    # ------------------------------------------------------------------
    # IP_B_PRTT_BNFT_RT: 분담률
    # ------------------------------------------------------------------
    def _load_prtt_bnft_rt(self, conn):
        """(prod, cls, cov, bnft_no) → [(strt, end, defry_rt), ...]"""
        self.prtt_rt = {}
        rows = conn.execute("""
            SELECT PROD_CD, CLS_CD, COV_CD, BNFT_NO,
                   SETN_STRT_VAL, SETN_END_VAL, DEFRY_RT
            FROM IP_B_PRTT_BNFT_RT
            ORDER BY PROD_CD, CLS_CD, COV_CD, BNFT_NO, SETN_STRT_VAL
        """).fetchall()
        for r in rows:
            key = (r[0], str(r[1]).zfill(2) if r[1] else "01", r[2], int(r[3]))
            entry = (int(r[4]) if r[4] else 0, int(r[5]) if r[5] else 999, float(r[6] or 0))
            self.prtt_rt.setdefault(key, []).append(entry)
        logger.info(f"PRTT_BNFT_RT: {len(self.prtt_rt):,} keys")

    def get_prtt_rate(self, prod_cd, cls_cd, cov_cd, bnft_no, duration_year):
        """경과연수별 분담률 조회. 없으면 0."""
        key = (prod_cd, cls_cd, cov_cd, bnft_no)
        entries = self.prtt_rt.get(key)
        if not entries:
            return 0.0
        for strt, end, rt in entries:
            if strt <= duration_year <= end:
                return rt
        return 0.0

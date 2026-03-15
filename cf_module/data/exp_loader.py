"""OD_EXP 산출용 데이터 로더.

IA_E_ACQSEXP_DR, IA_E_MNTEXP_DR, IA_E_LOSS_SVYEXP 로드 + 드라이버 키매칭.
"""
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np

from cf_module.utils.logger import get_logger

logger = get_logger("exp_loader")


class ExpDataCache:
    """사업비 참조 데이터 캐시."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._load_prod_grp(conn)
        self._load_etc_key(conn)
        self._load_infl(conn)
        self._load_acqsexp(conn)
        self._load_mntexp(conn)
        self._load_loss_svyexp(conn)
        self._build_index(conn)

    # ------------------------------------------------------------------
    # PROD_GRP mapping (3개 FILE_ID)
    # ------------------------------------------------------------------
    def _load_prod_grp(self, conn):
        """IA_M_PROD_GRP: (file_id, prod, cls) → prod_grp_cd"""
        self.prod_grp = {}
        rows = conn.execute("""
            SELECT ASSM_FILE_ID, PROD_CD, CLS_CD, PROD_GRP_CD
            FROM IA_M_PROD_GRP
        """).fetchall()
        for r in rows:
            self.prod_grp[(r[0], r[1], r[2])] = r[3]

    # ------------------------------------------------------------------
    # ETC key mapping (dim5 = channel)
    # ------------------------------------------------------------------
    def _load_etc_key(self, conn):
        """IA_M_ETC_ASSM_KEY: (file_id, key_no, div_val) → grp_cd"""
        self.etc_key = {}
        rows = conn.execute("""
            SELECT ASSM_FILE_ID, ASSM_KEY_NO, ASSM_DIV_VAL, ASSM_GRP_CD
            FROM IA_M_ETC_ASSM_KEY
        """).fetchall()
        for r in rows:
            self.etc_key[(r[0], int(r[1]), str(r[2]))] = str(r[3])

    # ------------------------------------------------------------------
    # IA_M_ASSM_DRIV (활성 차원)
    # ------------------------------------------------------------------
    def _get_assm_grp(self, conn, file_id, dim_vals):
        """드라이버 활성 차원에 따라 ASSM_GRP_CD 결정.

        Returns:
            dict: {dim_no: grp_cd} (활성 차원만)
        """
        row = conn.execute("""
            SELECT ASSM_DIV_VAL1_YN, ASSM_DIV_VAL2_YN, ASSM_DIV_VAL3_YN,
                   ASSM_DIV_VAL4_YN, ASSM_DIV_VAL5_YN, ASSM_DIV_VAL6_YN,
                   ASSM_DIV_VAL7_YN, ASSM_DIV_VAL8_YN, ASSM_DIV_VAL9_YN,
                   ASSM_DIV_VAL10_YN, ASSM_DIV_VAL11_YN, ASSM_DIV_VAL12_YN,
                   ASSM_DIV_VAL13_YN, ASSM_DIV_VAL14_YN, ASSM_DIV_VAL15_YN
            FROM IA_M_ASSM_DRIV WHERE ASSM_FILE_ID = ?
        """, [file_id]).fetchone()
        if not row:
            return {}
        result = {}
        for i in range(15):
            yn = row[i]
            if yn == 1:  # ETC 매핑
                val = dim_vals.get(i + 1, "")
                grp = self.etc_key.get((file_id, i + 1, str(val)))
                if grp:
                    result[i + 1] = grp
            elif yn == 2:  # 원본 유지
                result[i + 1] = str(dim_vals.get(i + 1, ""))
        return result

    # ------------------------------------------------------------------
    # IE_INFL (물가상승률)
    # ------------------------------------------------------------------
    def _load_infl(self, conn):
        """IE_INFL → 월 물가상승률 계수."""
        row = conn.execute("SELECT INFL FROM IE_INFL LIMIT 1").fetchone()
        annual_infl = row[0] if row and row[0] else 0.0
        self.monthly_esc = (1 + annual_infl) ** (1 / 12) if annual_infl > 0 else 1.0
        logger.info(f"IE_INFL: annual={annual_infl} monthly_esc={self.monthly_esc:.10f}")

    # ------------------------------------------------------------------
    # IA_E_ACQSEXP_DR
    # ------------------------------------------------------------------
    def _load_acqsexp(self, conn):
        """(prod_grp, grp_cd5, kdcd) → {params, rates[1..37]}"""
        self.acqsexp = {}
        rate_cols = ", ".join(f"ACQSEXP{i}" for i in range(1, 38))
        rows = conn.execute(f"""
            SELECT PROD_GRP_CD, ASSM_GRP_CD5, ACQSEXP_KDCD,
                   ACQSEXP_PRCOST_DRVR_CD, DC_BF_AF_DVCD, PAY_MTNPSN_DVCD,
                   PRCE_ASC_RT_APLY_YN, BIZEXP_IMPACT_APLY_YN, BIZEXP_OCUR_EPRD,
                   {rate_cols}
            FROM IA_E_ACQSEXP_DR
        """).fetchall()
        for r in rows:
            key = (r[0], str(r[1]), int(r[2]))
            rates = np.array([r[9 + i] or 0.0 for i in range(37)], dtype=np.float64)
            self.acqsexp[key] = {
                "drvr": int(r[3]) if r[3] else 0,
                "dc": int(r[4]) if r[4] else 0,
                "pay": int(r[5]) if r[5] else 0,
                "prce": int(r[6]) if r[6] else 0,
                "impact": int(r[7]) if r[7] else 0,
                "eprd": int(r[8]) if r[8] else 999999,
                "rates": rates,
            }
        logger.info(f"ACQSEXP: {len(self.acqsexp)} keys")

    # ------------------------------------------------------------------
    # IA_E_MNTEXP_DR
    # ------------------------------------------------------------------
    def _load_mntexp(self, conn):
        """(prod_grp, grp_cd5, kdcd) → {params, rates[1..25]}"""
        self.mntexp = {}
        rate_cols = ", ".join(f"MNTEXP{i}" for i in range(1, 26))
        rows = conn.execute(f"""
            SELECT PROD_GRP_CD, ASSM_GRP_CD5, MNTEXP_KDCD,
                   MNTEXP_PRCOST_DRVR_CD, DC_BF_AF_DVCD, PAY_MTNPSN_DVCD,
                   PRCE_ASC_RT_APLY_YN, BIZEXP_IMPACT_APLY_YN,
                   BIZEXP_OCUR_EYM_YN, BIZEXP_OCUR_EYM,
                   {rate_cols}
            FROM IA_E_MNTEXP_DR
        """).fetchall()
        for r in rows:
            key = (r[0], str(r[1]), int(r[2]))
            rates = np.array([r[10 + i] or 0.0 for i in range(25)], dtype=np.float64)
            self.mntexp[key] = {
                "drvr": int(r[3]) if r[3] else 0,
                "dc": int(r[4]) if r[4] else 0,
                "pay": int(r[5]) if r[5] else 0,
                "prce": int(r[6]) if r[6] else 0,
                "impact": int(r[7]) if r[7] else 0,
                "eym_yn": int(r[8]) if r[8] else 0,
                "eym": int(r[9]) if r[9] else 299999,
                "rates": rates,
            }
        logger.info(f"MNTEXP: {len(self.mntexp)} keys")

    # ------------------------------------------------------------------
    # IA_E_LOSS_SVYEXP
    # ------------------------------------------------------------------
    def _load_loss_svyexp(self, conn):
        """(grp_cd5, kdcd) → {params, rate}"""
        self.loss_svyexp = {}
        rows = conn.execute("""
            SELECT ASSM_GRP_CD5, LOSS_SVYEXP_KDCD,
                   LOSS_SVYEXP_PRCOST_DRVR_CD, PRCE_ASC_RT_APLY_YN,
                   BIZEXP_IMPACT_APLY_YN, LOSS_SVYEXP
            FROM IA_E_LOSS_SVYEXP
        """).fetchall()
        for r in rows:
            key = (str(r[0]), int(r[1]))
            self.loss_svyexp[key] = {
                "drvr": int(r[2]) if r[2] else 0,
                "prce": 0,  # LOSS_SVYEXP의 PRCE는 문자열 '^' → 미적용
                "impact": int(r[4]) if r[4] else 0,
                "rate": float(r[5] or 0),
            }
        logger.info(f"LOSS_SVYEXP: {len(self.loss_svyexp)} keys")

    # ------------------------------------------------------------------
    # 사전 인덱스 구축
    # ------------------------------------------------------------------
    def _build_index(self, conn):
        """배치용 인덱스: FILE_ID별 (prod,cls)→grp 매핑, ETC dim5 매핑."""
        # ACQS/MNT FILE_ID
        self.acqs_file_ids = set()
        self.mnt_file_ids = set()
        for (pg, g5, kd) in self.acqsexp:
            pass  # file_id는 prod_grp에서 역추적
        r = conn.execute("SELECT DISTINCT ASSM_FILE_ID FROM IA_E_ACQSEXP_DR").fetchall()
        self.acqs_file_ids = {row[0] for row in r}
        r = conn.execute("SELECT DISTINCT ASSM_FILE_ID FROM IA_E_MNTEXP_DR").fetchall()
        self.mnt_file_ids = {row[0] for row in r}

        # (prod, cls) → (acqs_grp, mnt_grp) 사전 매핑
        self._prod_cls_grp = {}  # {(prod, cls): (acqs_grp, mnt_grp)}
        for (fid, prod, cls), grp in self.prod_grp.items():
            key = (prod, cls)
            if key not in self._prod_cls_grp:
                self._prod_cls_grp[key] = [None, None]
            if fid in self.acqs_file_ids:
                self._prod_cls_grp[key][0] = grp
            if fid in self.mnt_file_ids:
                self._prod_cls_grp[key][1] = grp

        # dim5 ETC 매핑 인덱스: val5 → grp_cd5 (key_no=5 한정)
        self._dim5_map = {}  # {val5: grp_cd5}
        for (fid, key_no, div_val), grp_cd in self.etc_key.items():
            if key_no == 5:
                self._dim5_map[div_val] = grp_cd

        # (acqs_grp, mnt_grp, grp5) → items 캐시
        self._items_cache = {}

    def get_prod_grps(self, prod_cd, cls_cd):
        """(prod, cls) → (acqs_grp, mnt_grp)."""
        entry = self._prod_cls_grp.get((prod_cd, cls_cd))
        if entry:
            return entry[0], entry[1]
        return None, None

    def get_grp_cd5(self, assm_div_val5):
        """ASSM_DIV_VAL5 → ASSM_GRP_CD5."""
        if not assm_div_val5:
            return "01"
        return self._dim5_map.get(str(assm_div_val5), str(assm_div_val5))

    # ------------------------------------------------------------------
    # 계약별 매칭
    # ------------------------------------------------------------------
    def get_exp_items(self, prod_grp_acqs, prod_grp_mnt, grp_cd5):
        """(prod_grp, grp_cd5) 매칭 사업비 항목 반환 (캐시)."""
        cache_key = (prod_grp_acqs, prod_grp_mnt, grp_cd5)
        if cache_key in self._items_cache:
            return self._items_cache[cache_key]

        items = []
        for (pg, g5, kd), item in self.acqsexp.items():
            if pg == prod_grp_acqs and g5 == grp_cd5:
                items.append(("ACQS", kd, item))
        for (pg, g5, kd), item in self.mntexp.items():
            if pg == prod_grp_mnt and g5 == grp_cd5:
                items.append(("MNT", kd, item))
        for (g5, kd), item in self.loss_svyexp.items():
            if g5 == grp_cd5:
                items.append(("LSVY", kd, item))

        self._items_cache[cache_key] = items
        return items

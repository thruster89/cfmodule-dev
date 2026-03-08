"""
OD_TRAD_PV 산출에 필요한 DB 데이터 로딩 모듈.

DuckDB (duckdb_transform.duckdb) 또는 Legacy SQLite DB에서
계약/준비금/사업비 정보를 로드한다.
"""

import sqlite3
import time
from typing import Optional, Union

import duckdb
import numpy as np

from cf_module.calc.trad_pv import ContractInfo
from cf_module.utils.logger import get_logger

logger = get_logger("trad_pv_loader")

# DuckDB / SQLite 공통 타입
DBConn = Union[duckdb.DuckDBPyConnection, sqlite3.Connection]


# ---------------------------------------------------------------------------
# 일괄 캐시 클래스
# ---------------------------------------------------------------------------

class TradPVDataCache:
    """전체 참조 테이블을 메모리에 일괄 로드하는 캐시.

    Usage:
        cache = TradPVDataCache(conn)
        info = build_contract_info_cached(cache, idno)
    """

    def __init__(self, conn: DBConn):
        t0 = time.time()
        self._load_infrc(conn)
        self._load_rsvamt_bas(conn)
        self._load_acum_cov(conn)
        self._load_expct_inrt(conn)
        self._load_bizexp_cmpt_crit(conn)
        self._load_bizexp_rt(conn)
        self._load_pubano_inrt(conn)
        self._load_dc_rt(conn)
        self._load_loan_tables(conn)
        self._load_prod_loan_tpcd(conn)
        self._load_ltrmnat(conn)
        elapsed = time.time() - t0
        logger.info(f"TradPVDataCache loaded in {elapsed:.2f}s "
                     f"(infrc={len(self.infrc)}, bas={len(self.rsvamt_bas)})")

    # --- II_INFRC ---
    def _load_infrc(self, conn):
        self.infrc = {}
        rows = conn.execute("""
            SELECT INFRC_IDNO, PROD_CD, COV_CD, CLS_CD, CTR_TPCD,
                   PASS_YYCNT, PASS_MMCNT, INSTRM_YYCNT, PAYPR_YYCNT,
                   GRNTPT_GPREM, GRNTPT_JOIN_AMT, PAY_STCD, PREM_DC_RT1,
                   ACCMPT_GPREM, ACCMPT_RSPB_RSVAMT, TOT_TRMNAT_DDCT_AMT,
                   PAYPR_DVCD, ETC_EXPCT_BIZEXP_KEY_VAL,
                   ASSM_DIV_VAL1, CTR_LOAN_REMAMT,
                   INSTRM_DVCD, RENW_STCD, PAYCYC_DVCD, CTR_POLNO
            FROM II_INFRC WHERE INFRC_SEQ = 1
        """).fetchall()
        for r in rows:
            gprem = r[9] or 0
            accmpt_gprem = r[13] or 0
            self.infrc[r[0]] = {
                "prod_cd": r[1], "cov_cd": r[2],
                "cls_cd": str(r[3]).zfill(2) if r[3] else "01",
                "ctr_tpcd": str(r[4]) if r[4] is not None else "",
                "pass_yy": r[5] or 0, "pass_mm": r[6] or 0,
                "bterm_yy": r[7] or 0, "pterm_yy": r[8] or 0,
                "gprem": gprem,
                "effective_gprem": gprem if gprem else accmpt_gprem,
                "join_amt": r[10] or 0, "pay_stcd": r[11] or 1,
                "prem_dc_rt": r[12] or 0, "accmpt_gprem": accmpt_gprem,
                "accmpt_rspb_rsvamt": r[14] or 0,
                "tot_trmnat_ddct": r[15] or 0,
                "paypr_dvcd": r[16] or "",
                "etc_key": r[17] or "0000",
                "assm_div_val1": r[18] or "",
                "ctr_loan_remamt": r[19] or 0,
                "instrm_dvcd": r[20] or "",
                "renw_stcd": r[21] or 0,
                "paycyc_dvcd": r[22] or 0,
                "ctr_polno": r[23] or "",
            }
        # CTR_POLNO → IDNO 리스트 역매핑
        self.polno_to_idnos = {}
        for idno, v in self.infrc.items():
            polno = v["ctr_polno"]
            if polno:
                self.polno_to_idnos.setdefault(polno, []).append(idno)

    # --- II_RSVAMT_BAS ---
    def _load_rsvamt_bas(self, conn):
        self.rsvamt_bas = {}
        ystr_cols = ", ".join(f"YSTR_RSVAMT{yr}" for yr in range(1, 121))
        yyend_cols = ", ".join(f"YYEND_RSVAMT{yr}" for yr in range(1, 121))
        sql = (f"SELECT INFRC_IDNO, CRIT_JOIN_AMT, NPREM, "
               f"{ystr_cols}, {yyend_cols} "
               f"FROM II_RSVAMT_BAS WHERE INFRC_SEQ = 1")
        rows = conn.execute(sql).fetchall()
        for r in rows:
            idno = r[0]
            ystr = np.array([r[3 + i] or 0 for i in range(120)], dtype=np.float64)
            yyend = np.array([r[123 + i] or 0 for i in range(120)], dtype=np.float64)
            self.rsvamt_bas[idno] = {
                "crit_join_amt": r[1],
                "nprem": r[2],
                "ystr": ystr, "yyend": yyend,
            }

    # --- IP_P_ACUM_COV (+ ACQSEXP_ADDL_PRD1) ---
    def _load_acum_cov(self, conn):
        self.acum_cov_exact = {}   # (prod, cov, cls) -> dict
        self.acum_cov_fallback = {}  # (prod, cls) -> dict
        rows = conn.execute("""
            SELECT PROD_CD, COV_CD, CLS_CD,
                   APLY_INRT_CD, INRT_ADINT_CD,
                   LWST_GRNT_INRT1, LWST_GRNT_INRT2, LWST_GRNT_INRT3, LWST_GRNT_INRT4,
                   LWST_GRNT_INRT_CHNG_YYCNT1, LWST_GRNT_INRT_CHNG_YYCNT2,
                   LWST_GRNT_INRT_CHNG_YYCNT3, LWST_GRNT_INRT_CHNG_CRIT_CD,
                   ACQSEXP_ADDL_PRD1
            FROM IP_P_ACUM_COV
        """).fetchall()
        for r in rows:
            d = {
                "aply_inrt_cd": str(r[3]).zfill(2) if r[3] else "00",
                "inrt_adint_cd": r[4],
                "lwst_grnt_inrt": r[5] or 0.0,
                "lwst_grnt_inrt2": r[6] or 0.0,
                "lwst_grnt_inrt3": r[7] or 0.0,
                "lwst_grnt_inrt4": r[8] or 0.0,
                "lwst_chng_yycnt1": r[9] or 0,
                "lwst_chng_yycnt2": r[10] or 0,
                "lwst_chng_yycnt3": r[11] or 0,
                "lwst_chng_crit_cd": r[12] or 0,
                "acqsexp_addl_prd1": r[13] or 0,
            }
            key3 = (r[0], r[1], r[2])
            key2 = (r[0], r[2])
            if key3 not in self.acum_cov_exact:
                self.acum_cov_exact[key3] = d
            if key2 not in self.acum_cov_fallback:
                self.acum_cov_fallback[key2] = d

    def get_acum_cov(self, prod_cd, cov_cd, cls_cd):
        r = self.acum_cov_exact.get((prod_cd, cov_cd, cls_cd))
        if r:
            return r
        return self.acum_cov_fallback.get((prod_cd, cls_cd))

    # --- IP_P_EXPCT_INRT ---
    def _load_expct_inrt(self, conn):
        self.expct_inrt = {}
        rows = conn.execute("""
            SELECT PROD_CD, COV_CD, CLS_CD, EXPCT_INRT1, STD_INRT1
            FROM IP_P_EXPCT_INRT
        """).fetchall()
        for r in rows:
            self.expct_inrt[(r[0], r[1], r[2])] = {
                "expct_inrt": r[3] or 0.0, "std_inrt": r[4] or 0.0,
            }

    # --- IP_P_EXPCT_BIZEXP_CMPT_CRIT ---
    def _load_bizexp_cmpt_crit(self, conn):
        self.bizexp_cmpt_crit = {}
        rows = conn.execute("""
            SELECT PROD_CD, COV_CD, CLS_CD,
                   INSTRM_DIV_YN, PAYPR_DIV_YN, RENW_ST_DIV_YN,
                   PAYCYC_DIV_YN, ETC_EXPCT_BIZEXP_KEY_YN
            FROM IP_P_EXPCT_BIZEXP_CMPT_CRIT
        """).fetchall()
        for r in rows:
            self.bizexp_cmpt_crit[(r[0], r[1], r[2])] = {
                "instrm": r[3] or 0, "paypr": r[4] or 0,
                "renw_st": r[5] or 0, "paycyc": r[6] or 0,
                "etc_key": r[7] or 0,
            }

    # --- IP_P_EXPCT_BIZEXP_RT ---
    def _load_bizexp_rt(self, conn):
        self.bizexp_rt = {}  # (prod, cov, cls) -> list[dict]
        rows = conn.execute("""
            SELECT PROD_CD, COV_CD, CLS_CD,
                   INSTRM_DVCD, PAYPR_DVCD, RENW_STCD, PAYCYC_DVCD,
                   ETC_EXPCT_BIZEXP_KEY_VAL,
                   FRYY_GPREM_VS_ACQSEXP_RT,
                   INPAY_GPREM_VS_MNTEXP_RT1, INPAY_GPREM_VS_LOSS_SVYEXP_RT
            FROM IP_P_EXPCT_BIZEXP_RT
        """).fetchall()
        for r in rows:
            key = (r[0], r[1], r[2])
            entry = {
                "instrm_dvcd": r[3] or "", "paypr_dvcd": r[4] or "",
                "renw_stcd": r[5] or 0, "paycyc_dvcd": r[6] or 0,
                "etc_key": r[7] or "0000",
                "alpha1": r[8] or 0,
                "inpay_mntexp_rt": r[9] or 0,
                "loss_svyexp_rt": r[10] or 0,
            }
            self.bizexp_rt.setdefault(key, []).append(entry)

    def get_bizexp_rt(self, prod_cd, cov_cd, cls_cd,
                      instrm_dvcd, paypr_dvcd, renw_stcd, paycyc_dvcd,
                      etc_key, cmpt_crit):
        candidates = self.bizexp_rt.get((prod_cd, cov_cd, cls_cd), [])
        if not candidates:
            return None
        for c in candidates:
            ok = True
            if cmpt_crit:
                if cmpt_crit["instrm"] and c["instrm_dvcd"] != instrm_dvcd:
                    ok = False
                if cmpt_crit["paypr"] and c["paypr_dvcd"] != paypr_dvcd:
                    ok = False
                if cmpt_crit["renw_st"] and c["renw_stcd"] != renw_stcd:
                    ok = False
                if cmpt_crit["paycyc"] and c["paycyc_dvcd"] != paycyc_dvcd:
                    ok = False
                if cmpt_crit["etc_key"] and c["etc_key"] != etc_key:
                    ok = False
            else:
                if c["paypr_dvcd"] != paypr_dvcd:
                    ok = False
                if c["etc_key"] != etc_key:
                    ok = False
            if ok:
                return c
        return None

    def get_etc_key(self, prod_cd, cov_cd, cls_cd):
        candidates = self.bizexp_rt.get((prod_cd, cov_cd, cls_cd), [])
        if candidates:
            return candidates[0]["etc_key"]
        return "0000"

    # --- IE_PUBANO_INRT ---
    def _load_pubano_inrt(self, conn):
        self.pubano_inrt = {}
        rows = conn.execute("""
            SELECT PUBANO_INRT_CD, ADJ_RT, IV_ADEXP_RT,
                   EXTER_INDT_ITR_WGHT_VAL, EXTER_INDT_ITR
            FROM IE_PUBANO_INRT
        """).fetchall()
        for r in rows:
            self.pubano_inrt[r[0]] = {
                "adj_rt": r[1] or 0.0, "iv_adexp_rt": r[2] or 0.0,
                "ext_wght": r[3] or 0.0, "ext_itr": r[4] or 0.0,
            }

    # --- IE_DC_RT ---
    def _load_dc_rt(self, conn):
        rows = conn.execute("""
            SELECT PASS_PRD_NO, DC_RT FROM IE_DC_RT
            WHERE CMPT_PRPO_DVCD = 1 AND IMPACT_DVCD = 0 AND SCN_NO = 0
            ORDER BY PASS_PRD_NO
        """).fetchall()
        if not rows:
            self.dc_rt_curve = np.array([], dtype=np.float64)
        else:
            max_prd = max(r[0] for r in rows)
            arr = np.zeros(max_prd, dtype=np.float64)
            for prd, dc in rows:
                arr[prd - 1] = dc
            self.dc_rt_curve = arr

    # --- IA_M_PROD_GRP + IA_A_CTR_LOAN ---
    def _load_loan_tables(self, conn):
        # loan file id
        r = conn.execute(
            "SELECT DISTINCT ASSM_FILE_ID FROM IA_A_CTR_LOAN LIMIT 1"
        ).fetchone()
        self.loan_file_id = r[0] if r else None

        # prod_grp mapping
        self.prod_grp = {}
        if self.loan_file_id:
            rows = conn.execute("""
                SELECT PROD_CD, CLS_CD, PROD_GRP_CD
                FROM IA_M_PROD_GRP WHERE ASSM_FILE_ID = ?
            """, [self.loan_file_id]).fetchall()
            for rr in rows:
                key = (rr[0], rr[1])
                if key not in self.prod_grp:
                    self.prod_grp[key] = rr[2]

        # loan params
        self.ctr_loan = {}
        rows = conn.execute("""
            SELECT PROD_GRP_CD, ASSM_GRP_CD1,
                   ADINTR_SUM, ADINTR_MLTP, REMAMT_RT,
                   NEW_LOAN_OCUR_RT, LOAN_RPAY_RT, LOAN_MAX_LIMT_RT
            FROM IA_A_CTR_LOAN
        """).fetchall()
        for rr in rows:
            self.ctr_loan[(rr[0], rr[1])] = {
                "adintr_sum": rr[2] or 0.0, "adintr_mltp": rr[3] or 0.0,
                "remamt_rt": rr[4] or 0.0, "new_loan_ocur_rt": rr[5] or 0.0,
                "loan_rpay_rt": rr[6] or 0.0, "loan_max_limt_rt": rr[7] or 0.0,
            }

    # --- IP_P_LTRMNAT (환급금 비율) ---
    def _load_ltrmnat(self, conn):
        """IP_P_LTRMNAT: SOFF/LTRMNAT 환급금 비율 로드.

        키: (PROD_CD, CLS_CD, CTR_TPCD, PAY_STCD)
        값: TMRFND_RT[1~20] (경과년별 비율)
        """
        self.ltrmnat = {}  # (prod, cls, tpcd, pay_stcd) -> np.ndarray[20]
        rt_cols = ", ".join(f"TMRFND_RT{i}" for i in range(1, 21))
        rows = conn.execute(f"""
            SELECT PROD_CD, CLS_CD, CTR_TPCD, PAY_STCD, {rt_cols}
            FROM IP_P_LTRMNAT
            WHERE CTR_TPCD_YN = 1
        """).fetchall()
        for r in rows:
            cls = str(r[1]).zfill(2) if r[1] else "01"
            tpcd = str(r[2]) if r[2] is not None else ""
            pay_stcd = r[3] or 1
            rates = np.array([r[4 + i] or 0.0 for i in range(20)], dtype=np.float64)
            self.ltrmnat[(r[0], cls, tpcd, pay_stcd)] = rates

    def get_soff_rate(self, prod_cd, cls_cd, ctr_tpcd, pay_stcd_effective):
        """SOFF 환급금 비율 조회.

        Args:
            prod_cd, cls_cd: 상품/종
            ctr_tpcd: CTR_TPCD (str)
            pay_stcd_effective: 1=납입기간 내, 2=납입기간 후

        Returns:
            np.ndarray[20] (경과년별 비율) or None (미등록 → 기본 1.0)
        """
        return self.ltrmnat.get((prod_cd, cls_cd, ctr_tpcd, pay_stcd_effective))

    # --- IP_P_PROD (CTR_LOAN_TPCD) ---
    def _load_prod_loan_tpcd(self, conn):
        self.prod_loan_tpcd = {}
        rows = conn.execute("""
            SELECT PROD_CD, CLS_CD, CTR_LOAN_TPCD
            FROM IP_P_PROD
        """).fetchall()
        for r in rows:
            cls = str(r[1]).zfill(2) if r[1] else "01"
            self.prod_loan_tpcd[(r[0], cls)] = r[2] if r[2] is not None else 1

    def get_loan_params(self, prod_cd, cls_cd, assm_div_val1):
        grp = self.prod_grp.get((prod_cd, cls_cd))
        if not grp:
            return None
        assm1 = assm_div_val1 or "S"
        return self.ctr_loan.get((grp, assm1))


def build_contract_info_cached(cache: TradPVDataCache, idno: int) -> Optional[ContractInfo]:
    """캐시 기반 ContractInfo 구성 (DB I/O 없음)."""
    raw = cache.infrc.get(idno)
    if not raw:
        return None

    prod_cd = raw["prod_cd"]
    cov_cd = raw["cov_cd"]
    cls_cd = raw["cls_cd"]

    bas = cache.rsvamt_bas.get(idno)
    acqsexp1 = raw["tot_trmnat_ddct"]
    acum_cov = cache.get_acum_cov(prod_cd, cov_cd, cls_cd)
    expct_inrt_data = cache.expct_inrt.get((prod_cd, cov_cd, cls_cd))

    pubano_params = None
    dc_rt_curve = None
    if acum_cov:
        pubano_params = cache.pubano_inrt.get(acum_cov["aply_inrt_cd"])
        dc_rt_curve = cache.dc_rt_curve

    loan_params = cache.get_loan_params(prod_cd, cls_cd, raw.get("assm_div_val1", ""))
    ctr_loan_tpcd = cache.prod_loan_tpcd.get((prod_cd, cls_cd), 1)

    # SOFF 비율 (IP_P_LTRMNAT)
    ctr_tpcd_str = raw["ctr_tpcd"]
    soff_rates_paying = cache.get_soff_rate(prod_cd, cls_cd, ctr_tpcd_str, 1)
    soff_rates_paidup = cache.get_soff_rate(prod_cd, cls_cd, ctr_tpcd_str, 2)

    acum_nprem_nobas = 0.0
    acum_nprem_old = 0.0
    amort_mm = 0
    if not bas:
        paypr_dvcd = raw["paypr_dvcd"] or _get_paypr_dvcd(raw["pterm_yy"])
        etc_key = raw["etc_key"] or cache.get_etc_key(prod_cd, cov_cd, cls_cd)
        instrm_dvcd = raw.get("instrm_dvcd", "")
        renw_stcd = raw.get("renw_stcd", 0)
        paycyc_dvcd = raw.get("paycyc_dvcd", 0)

        cmpt_crit = cache.bizexp_cmpt_crit.get((prod_cd, cov_cd, cls_cd))
        brt_full = cache.get_bizexp_rt(
            prod_cd, cov_cd, cls_cd,
            instrm_dvcd, paypr_dvcd, renw_stcd, paycyc_dvcd, etc_key,
            cmpt_crit,
        )

        if etc_key and etc_key[0] == "1":
            loss = brt_full["loss_svyexp_rt"] if brt_full else 0
            acum_nprem_nobas = raw["accmpt_gprem"] * (1 - loss)
            acum_nprem_old = acum_nprem_nobas
        elif brt_full:
            mnt = brt_full["inpay_mntexp_rt"]
            loss = brt_full["loss_svyexp_rt"]
            accmpt = raw["accmpt_gprem"]
            acum_nprem_nobas = accmpt * (1 - mnt - loss)

            addl_prd = acum_cov.get("acqsexp_addl_prd1", 0) if acum_cov else 0
            m = min(raw["pterm_yy"], addl_prd) if addl_prd else raw["pterm_yy"]
            amort_mm = m * 12

            elapsed_mm = raw["pass_yy"] * 12 + raw["pass_mm"]
            if elapsed_mm > amort_mm:
                acum_nprem_old = acum_nprem_nobas
            else:
                alpha1 = brt_full["alpha1"]
                ei = expct_inrt_data["expct_inrt"] if expct_inrt_data else 0
                v = 1 / (1 + ei) if ei else 1.0
                paycyc = raw.get("paycyc_dvcd", 1) or 1
                if paycyc == 0:
                    acum_nprem_old = acum_nprem_nobas
                else:
                    k = 12 / paycyc
                    if abs(v - 1) > 1e-12 and m > 0:
                        ann_k = (1 - v ** m) / (k * (1 - v ** (1 / k)))
                    else:
                        ann_k = m * k
                    acqs_deduct = alpha1 / ann_k if ann_k else 0
                    acum_nprem_old = accmpt * (1 - acqs_deduct - mnt - loss)
        else:
            acum_nprem_nobas = raw["accmpt_gprem"]
            acum_nprem_old = raw["accmpt_gprem"]

    return ContractInfo(
        idno=idno,
        prod_cd=prod_cd, cov_cd=cov_cd, cls_cd=cls_cd,
        ctr_tpcd=raw["ctr_tpcd"],
        pass_yy=raw["pass_yy"], pass_mm=raw["pass_mm"],
        bterm_yy=raw["bterm_yy"], pterm_yy=raw["pterm_yy"],
        gprem=raw["effective_gprem"], join_amt=raw["join_amt"],
        pay_stcd=raw["pay_stcd"],
        paycyc=raw.get("paycyc_dvcd", 0),
        prem_dc_rt=raw["prem_dc_rt"],
        acqsexp1=acqsexp1, bas=bas,
        acum_nprem_nobas=acum_nprem_nobas,
        acum_nprem_old=acum_nprem_old,
        amort_mm=amort_mm,
        accmpt_rspb_rsvamt=raw["accmpt_rspb_rsvamt"],
        ctr_loan_remamt=raw["ctr_loan_remamt"],
        ctr_loan_tpcd=ctr_loan_tpcd,
        acum_cov=acum_cov, expct_inrt_data=expct_inrt_data,
        pubano_params=pubano_params, dc_rt_curve=dc_rt_curve,
        loan_params=loan_params,
        soff_rates_paying=soff_rates_paying,
        soff_rates_paidup=soff_rates_paidup,
    )


def load_contract_info(conn: sqlite3.Connection, idno: int) -> Optional[dict]:
    """II_INFRC에서 계약 기본 정보 로드."""
    row = conn.execute("""
        SELECT PROD_CD, COV_CD, CLS_CD, CTR_TPCD,
               PASS_YYCNT, PASS_MMCNT,
               INSTRM_YYCNT, PAYPR_YYCNT,
               GRNTPT_GPREM, GRNTPT_JOIN_AMT,
               PAY_STCD, PREM_DC_RT1,
               ACCMPT_GPREM, ACCMPT_RSPB_RSVAMT,
               TOT_TRMNAT_DDCT_AMT,
               PAYPR_DVCD, ETC_EXPCT_BIZEXP_KEY_VAL,
               ASSM_DIV_VAL1, CTR_LOAN_REMAMT,
               INSTRM_DVCD, RENW_STCD, PAYCYC_DVCD
        FROM II_INFRC
        WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1
    """, [idno]).fetchone()
    if not row:
        return None
    gprem = row[8] or 0
    accmpt_gprem = row[12] or 0
    effective_gprem = gprem if gprem else accmpt_gprem
    return {
        "prod_cd": row[0],
        "cov_cd": row[1],
        "cls_cd": str(row[2]).zfill(2) if row[2] else "01",
        "ctr_tpcd": str(row[3]) if row[3] is not None else "",
        "pass_yy": row[4] or 0,
        "pass_mm": row[5] or 0,
        "bterm_yy": row[6] or 0,
        "pterm_yy": row[7] or 0,
        "gprem": gprem,
        "effective_gprem": effective_gprem,
        "join_amt": row[9] or 0,
        "pay_stcd": row[10] or 1,
        "prem_dc_rt": row[11] or 0,
        "accmpt_gprem": accmpt_gprem,
        "accmpt_rspb_rsvamt": row[13] or 0,
        "tot_trmnat_ddct": row[14] or 0,
        "paypr_dvcd": row[15] or "",
        "etc_key": row[16] or "0000",
        "assm_div_val1": row[17] or "",
        "ctr_loan_remamt": row[18] or 0,
        "instrm_dvcd": row[19] or "",
        "renw_stcd": row[20] or 0,
        "paycyc_dvcd": row[21] or 0,
    }


def load_rsvamt_bas(conn: sqlite3.Connection, idno: int) -> Optional[dict]:
    """II_RSVAMT_BAS에서 준비금/순보험료 로드."""
    row = conn.execute(
        "SELECT * FROM II_RSVAMT_BAS WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
        [idno],
    ).fetchone()
    if not row:
        return None
    cols = [c[1] for c in conn.execute("PRAGMA table_info(II_RSVAMT_BAS)").fetchall()]
    data = dict(zip(cols, row))

    ystr_arr = []
    yyend_arr = []
    for yr in range(1, 121):
        ystr_arr.append(data.get(f"YSTR_RSVAMT{yr}", 0) or 0)
        yyend_arr.append(data.get(f"YYEND_RSVAMT{yr}", 0) or 0)

    return {
        "crit_join_amt": data["CRIT_JOIN_AMT"],
        "nprem": data["NPREM"],
        "ystr": np.array(ystr_arr, dtype=np.float64),
        "yyend": np.array(yyend_arr, dtype=np.float64),
    }


def load_acqsexp_value(conn: sqlite3.Connection, idno: int,
                       prod_cd: str, cov_cd: str, cls_cd: str,
                       gprem: float, pterm_yy: int) -> float:
    """신계약비(ACQSEXP1) 값 산출.

    ACQSEXP1 = GPREM × 12 × PTERM × acqsexp_rt
    """
    # PAYPR_DVCD, ETC key 결정
    paypr_dvcd = _get_paypr_dvcd(pterm_yy)
    etc_key = _get_etc_key(conn, prod_cd, cov_cd, cls_cd)

    row = conn.execute("""
        SELECT FRYY_GPREM_VS_ACQSEXP_RT
        FROM IP_P_EXPCT_BIZEXP_RT
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
          AND PAYPR_DVCD = ? AND ETC_EXPCT_BIZEXP_KEY_VAL = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd, paypr_dvcd, etc_key]).fetchone()

    if row and row[0]:
        return gprem * 12 * pterm_yy * row[0]
    return 0.0


def load_acum_cov(conn: sqlite3.Connection, prod_cd: str, cov_cd: str,
                  cls_cd: str) -> Optional[dict]:
    """IP_P_ACUM_COV에서 적립 관련 설정 로드.

    정확한 (PROD_CD, COV_CD, CLS_CD) 매칭 시도 후,
    없으면 (PROD_CD, CLS_CD)로 폴백 (동일 상품 다른 담보).
    """
    row = conn.execute("""
        SELECT APLY_INRT_CD, INRT_ADINT_CD,
               LWST_GRNT_INRT1, LWST_GRNT_INRT2, LWST_GRNT_INRT3, LWST_GRNT_INRT4,
               LWST_GRNT_INRT_CHNG_YYCNT1, LWST_GRNT_INRT_CHNG_YYCNT2, LWST_GRNT_INRT_CHNG_YYCNT3,
               LWST_GRNT_INRT_CHNG_CRIT_CD
        FROM IP_P_ACUM_COV
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd]).fetchone()
    if not row:
        row = conn.execute("""
            SELECT APLY_INRT_CD, INRT_ADINT_CD,
                   LWST_GRNT_INRT1, LWST_GRNT_INRT2, LWST_GRNT_INRT3, LWST_GRNT_INRT4,
                   LWST_GRNT_INRT_CHNG_YYCNT1, LWST_GRNT_INRT_CHNG_YYCNT2, LWST_GRNT_INRT_CHNG_YYCNT3,
                   LWST_GRNT_INRT_CHNG_CRIT_CD
            FROM IP_P_ACUM_COV
            WHERE PROD_CD = ? AND CLS_CD = ?
            LIMIT 1
        """, [prod_cd, cls_cd]).fetchone()
    if row:
        return {
            "aply_inrt_cd": str(row[0]).zfill(2),
            "inrt_adint_cd": row[1],
            "lwst_grnt_inrt": row[2] or 0.0,
            "lwst_grnt_inrt2": row[3] or 0.0,
            "lwst_grnt_inrt3": row[4] or 0.0,
            "lwst_grnt_inrt4": row[5] or 0.0,
            "lwst_chng_yycnt1": row[6] or 0,
            "lwst_chng_yycnt2": row[7] or 0,
            "lwst_chng_yycnt3": row[8] or 0,
            "lwst_chng_crit_cd": row[9] or 0,
        }
    return None


def load_expct_inrt(conn: sqlite3.Connection, prod_cd: str, cov_cd: str,
                    cls_cd: str) -> Optional[dict]:
    """IP_P_EXPCT_INRT에서 예정이율 로드."""
    row = conn.execute("""
        SELECT EXPCT_INRT1, STD_INRT1
        FROM IP_P_EXPCT_INRT
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd]).fetchone()
    if row:
        return {"expct_inrt": row[0] or 0.0, "std_inrt": row[1] or 0.0}
    return None


def load_bizexp_rt(conn: sqlite3.Connection, prod_cd: str, cov_cd: str,
                   cls_cd: str, paypr_dvcd: str, etc_key: str) -> Optional[dict]:
    """IP_P_EXPCT_BIZEXP_RT에서 사업비율 로드."""
    row = conn.execute("""
        SELECT FRYY_GPREM_VS_ACQSEXP_RT,
               INPAY_GPREM_VS_MNTEXP_RT1, INPAY_GPREM_VS_LOSS_SVYEXP_RT
        FROM IP_P_EXPCT_BIZEXP_RT
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
          AND PAYPR_DVCD = ? AND ETC_EXPCT_BIZEXP_KEY_VAL = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd, paypr_dvcd, etc_key]).fetchone()
    if row:
        return {
            "acqsexp_rt": row[0],
            "inpay_mntexp_rt": row[1] or 0,
            "loss_svyexp_rt": row[2] or 0,
        }
    return None


def load_pubano_inrt_params(conn: sqlite3.Connection,
                            inrt_cd: str) -> Optional[dict]:
    """IE_PUBANO_INRT에서 공시이율 산출 파라미터 로드."""
    row = conn.execute("""
        SELECT ADJ_RT, IV_ADEXP_RT, EXTER_INDT_ITR_WGHT_VAL, EXTER_INDT_ITR
        FROM IE_PUBANO_INRT
        WHERE PUBANO_INRT_CD = ?
    """, [inrt_cd]).fetchone()
    if not row:
        return None
    return {
        "adj_rt": row[0] or 0.0,
        "iv_adexp_rt": row[1] or 0.0,
        "ext_wght": row[2] or 0.0,
        "ext_itr": row[3] or 0.0,
    }


def load_dc_rt_curve(conn: sqlite3.Connection) -> np.ndarray:
    """IE_DC_RT에서 할인율 커브 로드 (PASS_PRD_NO 순).

    Returns:
        1D array indexed by PASS_PRD_NO (1-based → index 0=PRD1).
    """
    rows = conn.execute("""
        SELECT PASS_PRD_NO, DC_RT
        FROM IE_DC_RT
        WHERE CMPT_PRPO_DVCD = 1 AND IMPACT_DVCD = 0 AND SCN_NO = 0
        ORDER BY PASS_PRD_NO
    """).fetchall()
    if not rows:
        return np.array([], dtype=np.float64)
    max_prd = max(r[0] for r in rows)
    arr = np.zeros(max_prd, dtype=np.float64)
    for prd, dc in rows:
        arr[prd - 1] = dc
    return arr


def build_contract_info(conn: sqlite3.Connection, idno: int) -> Optional[ContractInfo]:
    """DB에서 전체 데이터를 로드하여 ContractInfo 구성.

    Args:
        conn: Legacy SQLite 연결
        idno: INFRC_IDNO

    Returns:
        ContractInfo or None (로드 실패 시)
    """
    raw = load_contract_info(conn, idno)
    if not raw:
        return None

    prod_cd = raw["prod_cd"]
    cov_cd = raw["cov_cd"]
    cls_cd = raw["cls_cd"]

    # BAS 로드
    bas = load_rsvamt_bas(conn, idno)

    # ACQSEXP1: II_INFRC의 TOT_TRMNAT_DDCT_AMT 직접 사용
    acqsexp1 = raw["tot_trmnat_ddct"]

    # 적립 설정 / 예정이율
    acum_cov = load_acum_cov(conn, prod_cd, cov_cd, cls_cd)
    expct_inrt_data = load_expct_inrt(conn, prod_cd, cov_cd, cls_cd)

    # 공시이율 파라미터 (IP_P_ACUM_COV 대상만)
    pubano_params = None
    dc_rt_curve = None
    if acum_cov:
        pubano_params = load_pubano_inrt_params(conn, acum_cov["aply_inrt_cd"])
        dc_rt_curve = load_dc_rt_curve(conn)

    # 약관대출 가정
    loan_params = load_loan_params(conn, prod_cd, cls_cd, raw.get("assm_div_val1", ""))

    # BAS 미보유: 추가 데이터
    acum_nprem_nobas = 0.0
    acum_nprem_old = 0.0
    amort_mm = 0
    if not bas:
        paypr_dvcd = raw["paypr_dvcd"] or _get_paypr_dvcd(raw["pterm_yy"])
        etc_key = raw["etc_key"] or _get_etc_key(conn, prod_cd, cov_cd, cls_cd)
        instrm_dvcd = raw.get("instrm_dvcd", "")
        renw_stcd = raw.get("renw_stcd", 0)
        paycyc_dvcd = raw.get("paycyc_dvcd", 0)

        # CMPT_CRIT 플래그 기반 동적 키매칭
        cmpt_crit = load_bizexp_cmpt_crit(conn, prod_cd, cov_cd, cls_cd)
        brt_full = load_bizexp_rt_full(
            conn, prod_cd, cov_cd, cls_cd,
            instrm_dvcd, paypr_dvcd, renw_stcd, paycyc_dvcd, etc_key,
            cmpt_crit
        )

        if etc_key and etc_key[0] == "1":
            # ETC_KEY '1'로 시작: MNT/alpha 차감 없이, LOSS만 차감
            loss = brt_full["loss_svyexp_rt"] if brt_full else 0
            acum_nprem_nobas = raw["accmpt_gprem"] * (1 - loss)
            acum_nprem_old = acum_nprem_nobas
        elif brt_full:
            mnt = brt_full["inpay_mntexp_rt"]
            loss = brt_full["loss_svyexp_rt"]
            accmpt = raw["accmpt_gprem"]

            # new NPREM (상각 완료 후): ACCMPT × (1-MNT-LOSS)
            acum_nprem_nobas = accmpt * (1 - mnt - loss)

            # old NPREM (상각기간 내): alpha1 차감 추가
            elapsed_mm = raw["pass_yy"] * 12 + raw["pass_mm"]
            addl_prd = load_acqsexp_addl_prd(conn, prod_cd, cov_cd, cls_cd)
            m = min(raw["pterm_yy"], addl_prd) if addl_prd else raw["pterm_yy"]
            amort_mm = m * 12  # CTR_MM <= amort_mm이면 old prem

            if elapsed_mm > amort_mm:
                # 상각 완료 → 차감 없음
                acum_nprem_old = acum_nprem_nobas
            else:
                alpha1 = brt_full["alpha1"]
                ei = expct_inrt_data["expct_inrt"] if expct_inrt_data else 0
                v = 1 / (1 + ei) if ei else 1.0
                paycyc = raw.get("paycyc_dvcd", 1) or 1
                if paycyc == 0:
                    # 일시납: 상각 불필요
                    acum_nprem_old = acum_nprem_nobas
                else:
                    k = 12 / paycyc  # 연간 납입 횟수
                    if abs(v - 1) > 1e-12 and m > 0:
                        ann_k = (1 - v ** m) / (k * (1 - v ** (1 / k)))
                    else:
                        ann_k = m * k
                    acqs_deduct = alpha1 / ann_k if ann_k else 0
                    acum_nprem_old = accmpt * (1 - acqs_deduct - mnt - loss)
        else:
            acum_nprem_nobas = raw["accmpt_gprem"]
            acum_nprem_old = raw["accmpt_gprem"]

    return ContractInfo(
        idno=idno,
        prod_cd=prod_cd,
        cov_cd=cov_cd,
        cls_cd=cls_cd,
        ctr_tpcd=raw["ctr_tpcd"],
        pass_yy=raw["pass_yy"],
        pass_mm=raw["pass_mm"],
        bterm_yy=raw["bterm_yy"],
        pterm_yy=raw["pterm_yy"],
        gprem=raw["effective_gprem"],
        join_amt=raw["join_amt"],
        pay_stcd=raw["pay_stcd"],
        paycyc=raw.get("paycyc_dvcd", 0),
        prem_dc_rt=raw["prem_dc_rt"],
        acqsexp1=acqsexp1,
        bas=bas,
        acum_nprem_nobas=acum_nprem_nobas,
        acum_nprem_old=acum_nprem_old,
        amort_mm=amort_mm,
        accmpt_rspb_rsvamt=raw["accmpt_rspb_rsvamt"],
        ctr_loan_remamt=raw["ctr_loan_remamt"],
        acum_cov=acum_cov,
        expct_inrt_data=expct_inrt_data,
        pubano_params=pubano_params,
        dc_rt_curve=dc_rt_curve,
        loan_params=loan_params,
    )


def load_acqsexp_addl_prd(conn: sqlite3.Connection, prod_cd: str,
                          cov_cd: str, cls_cd: str) -> int:
    """IP_P_ACUM_COV에서 신계약비 상각기간(ACQSEXP_ADDL_PRD1) 로드."""
    row = conn.execute("""
        SELECT ACQSEXP_ADDL_PRD1 FROM IP_P_ACUM_COV
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd]).fetchone()
    if not row:
        row = conn.execute("""
            SELECT ACQSEXP_ADDL_PRD1 FROM IP_P_ACUM_COV
            WHERE PROD_CD = ? AND CLS_CD = ?
            LIMIT 1
        """, [prod_cd, cls_cd]).fetchone()
    return row[0] if row and row[0] else 0


def load_bizexp_cmpt_crit(conn: sqlite3.Connection, prod_cd: str,
                          cov_cd: str, cls_cd: str) -> Optional[dict]:
    """IP_P_EXPCT_BIZEXP_CMPT_CRIT에서 키매칭 플래그 로드."""
    row = conn.execute("""
        SELECT INSTRM_DIV_YN, PAYPR_DIV_YN, RENW_ST_DIV_YN,
               PAYCYC_DIV_YN, ETC_EXPCT_BIZEXP_KEY_YN
        FROM IP_P_EXPCT_BIZEXP_CMPT_CRIT
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd]).fetchone()
    if row:
        return {
            "instrm": row[0] or 0,
            "paypr": row[1] or 0,
            "renw_st": row[2] or 0,
            "paycyc": row[3] or 0,
            "etc_key": row[4] or 0,
        }
    return None


def load_bizexp_rt_full(conn: sqlite3.Connection, prod_cd: str,
                        cov_cd: str, cls_cd: str,
                        instrm_dvcd: str, paypr_dvcd: str,
                        renw_stcd: int, paycyc_dvcd: int,
                        etc_key: str,
                        cmpt_crit: Optional[dict] = None) -> Optional[dict]:
    """IP_P_EXPCT_BIZEXP_RT에서 CMPT_CRIT 플래그 기반 동적 키매칭으로 사업비율 로드."""
    where = "PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?"
    params = [prod_cd, cov_cd, cls_cd]

    if cmpt_crit:
        if cmpt_crit["instrm"]:
            where += " AND INSTRM_DVCD = ?"
            params.append(instrm_dvcd)
        if cmpt_crit["paypr"]:
            where += " AND PAYPR_DVCD = ?"
            params.append(paypr_dvcd)
        if cmpt_crit["renw_st"]:
            where += " AND RENW_STCD = ?"
            params.append(renw_stcd)
        if cmpt_crit["paycyc"]:
            where += " AND PAYCYC_DVCD = ?"
            params.append(paycyc_dvcd)
        if cmpt_crit["etc_key"]:
            where += " AND ETC_EXPCT_BIZEXP_KEY_VAL = ?"
            params.append(etc_key)
    else:
        where += " AND PAYPR_DVCD = ? AND ETC_EXPCT_BIZEXP_KEY_VAL = ?"
        params.extend([paypr_dvcd, etc_key])

    row = conn.execute(f"""
        SELECT FRYY_GPREM_VS_ACQSEXP_RT,
               INPAY_GPREM_VS_MNTEXP_RT1, INPAY_GPREM_VS_LOSS_SVYEXP_RT
        FROM IP_P_EXPCT_BIZEXP_RT
        WHERE {where}
        LIMIT 1
    """, params).fetchone()
    if row:
        return {
            "alpha1": row[0] or 0,
            "inpay_mntexp_rt": row[1] or 0,
            "loss_svyexp_rt": row[2] or 0,
        }
    return None


def load_loan_params(conn: sqlite3.Connection, prod_cd: str,
                     cls_cd: str, assm_div_val1: str) -> Optional[dict]:
    """IA_A_CTR_LOAN에서 약관대출 가정 로드.

    키매칭: IA_M_PROD_GRP(ASSM_FILE_ID=loan_file) → PROD_GRP_CD
            ASSM_GRP_CD1 = ASSM_DIV_VAL1 (S/F)
    """
    # Loan ASSM_FILE_ID 조회
    loan_file = conn.execute(
        "SELECT DISTINCT ASSM_FILE_ID FROM IA_A_CTR_LOAN LIMIT 1"
    ).fetchone()
    if not loan_file:
        return None

    # PROD_GRP_CD 조회
    grp_row = conn.execute("""
        SELECT PROD_GRP_CD FROM IA_M_PROD_GRP
        WHERE ASSM_FILE_ID = ? AND PROD_CD = ? AND CLS_CD = ?
        LIMIT 1
    """, [loan_file[0], prod_cd, cls_cd]).fetchone()
    if not grp_row:
        return None

    prod_grp = grp_row[0]
    assm1 = assm_div_val1 or "S"

    row = conn.execute("""
        SELECT ADINTR_SUM, ADINTR_MLTP, REMAMT_RT,
               NEW_LOAN_OCUR_RT, LOAN_RPAY_RT, LOAN_MAX_LIMT_RT
        FROM IA_A_CTR_LOAN
        WHERE PROD_GRP_CD = ? AND ASSM_GRP_CD1 = ?
        LIMIT 1
    """, [prod_grp, assm1]).fetchone()
    if not row:
        return None

    return {
        "adintr_sum": row[0] or 0.0,
        "adintr_mltp": row[1] or 0.0,
        "remamt_rt": row[2] or 0.0,
        "new_loan_ocur_rt": row[3] or 0.0,
        "loan_rpay_rt": row[4] or 0.0,
        "loan_max_limt_rt": row[5] or 0.0,
    }


# ---------------------------------------------------------------------------
# 내부 헬퍼
# ---------------------------------------------------------------------------

def _get_paypr_dvcd(pterm_yy: int) -> str:
    """납입기간 → PAYPR_DVCD 매핑."""
    if pterm_yy <= 0:
        return "A000"
    dvcd_map = {
        5: "A050", 7: "A070", 10: "A100", 15: "A150",
        20: "A200", 25: "A250", 30: "A300",
    }
    return dvcd_map.get(pterm_yy, f"A{pterm_yy:03d}")


def _get_etc_key(conn: sqlite3.Connection, prod_cd: str,
                 cov_cd: str, cls_cd: str) -> str:
    """ETC_EXPCT_BIZEXP_KEY_VAL 결정."""
    # 대부분 '0000' 사용, 상품에 따라 다를 수 있음
    row = conn.execute("""
        SELECT DISTINCT ETC_EXPCT_BIZEXP_KEY_VAL
        FROM IP_P_EXPCT_BIZEXP_RT
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd]).fetchone()
    return row[0] if row else "0000"

"""OD_TBL_BN 산출용 데이터 캐시.

급부별 위험률 매핑, 지급률, 부담보 등 참조 데이터 일괄 로드.
"""
import time
from typing import Dict, List, Optional, Set, Tuple

import duckdb
import numpy as np

from cf_module.utils.logger import get_logger

logger = get_logger("bn_loader")


class BNDataCache:
    """OD_TBL_BN 참조 데이터 캐시."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, pcv_filter=None):
        """
        Args:
            conn: DuckDB 연결
            pcv_filter: (prod_cd, cov_cd) 튜플 리스트. 설정 시 해당 상품만 로드.
        """
        self._pcv_filter = pcv_filter
        t_total = time.perf_counter()
        steps = [
            ("BNFT_RSKRT_C", self._load_bnft_rskrt_c),
            ("BNFT_BAS", self._load_bnft_bas),
            ("BNFT_DEFRY_RT", self._load_bnft_defry_rt),
            ("NCOV", self._load_ncov),
            ("INVLD_TRMNAT", self._load_invld_trmnat),
            ("PRTT_BNFT_RT", self._load_prtt_bnft_rt),
            ("EXPCT_INRT", self._load_expct_inrt_for_prtt),
            ("RISK_META", self._load_risk_meta),
            ("RSVAMT_FLAGS", self._load_rsvamt_flags),
        ]
        for name, fn in steps:
            t0 = time.perf_counter()
            fn(conn)
            elapsed = time.perf_counter() - t0
            logger.info(f"  └ {name} load: {elapsed:.2f}s")
        total = time.perf_counter() - t_total
        logger.info(f"BNDataCache total load: {total:.2f}s")

    def _pcv_where(self, prefix="WHERE"):
        """pcv_filter 기반 SQL 조건절 생성."""
        if not self._pcv_filter:
            return ""
        conds = " OR ".join(
            f"(PROD_CD = '{p}' AND COV_CD = '{c}')" for p, c in self._pcv_filter
        )
        return f" {prefix} ({conds})"

    # ------------------------------------------------------------------
    # IP_R_BNFT_RSKRT_C: BNFT별 위험률 매핑
    # ------------------------------------------------------------------
    def _load_bnft_rskrt_c(self, conn):
        """(prod, cls, cov, bnft_no) → {rsk_rt_cd: {rskrt_yn, drpo_yn}}"""
        self.bnft_rskrt = {}  # (prod, cls, cov, bnft_no) -> [{rsk_cd, rskrt_yn, drpo_yn}]
        # (prod, cls, cov) -> {bnft_no: [entries]} 인덱스
        self._bnft_by_pcv = {}
        df = conn.execute(f"""
            SELECT PROD_CD, CLS_CD, COV_CD, BNFT_NO,
                   RSK_RT_CD, BNFT_RSKRT_YN, BNFT_DRPO_RSKRT_YN
            FROM IP_R_BNFT_RSKRT_C
            {self._pcv_where()}
            ORDER BY PROD_CD, CLS_CD, COV_CD, BNFT_NO, RSK_RT_CD
        """).fetchdf()
        # numpy 배열로 추출하여 Python 루프 최적화
        prod_arr = df["PROD_CD"].values
        cls_arr = df["CLS_CD"].values
        cov_arr = df["COV_CD"].values
        bno_arr = df["BNFT_NO"].values.astype(int)
        rsk_arr = df["RSK_RT_CD"].values.astype(str)
        rskrt_arr = df["BNFT_RSKRT_YN"].values.astype(bool)
        drpo_arr = df["BNFT_DRPO_RSKRT_YN"].values.astype(bool)
        del df

        for i in range(len(prod_arr)):
            cls = str(cls_arr[i]).zfill(2) if cls_arr[i] is not None else "01"
            bno = int(bno_arr[i])
            key = (prod_arr[i], cls, cov_arr[i], bno)
            entry = {
                "rsk_cd": rsk_arr[i],
                "rskrt_yn": bool(rskrt_arr[i]),
                "drpo_yn": bool(drpo_arr[i]),
            }
            self.bnft_rskrt.setdefault(key, []).append(entry)
            pcv = (prod_arr[i], cls, cov_arr[i])
            self._bnft_by_pcv.setdefault(pcv, {}).setdefault(bno, []).append(entry)
        logger.info(f"BNFT_RSKRT_C: {len(self.bnft_rskrt):,} (prod,cls,cov,bnft) keys")

    def get_bnft_risk_mapping(self, prod_cd, cls_cd, cov_cd):
        """특정 (prod,cls,cov)의 BNFT별 위험률 매핑 반환.

        Returns:
            {bnft_no: {"rskrt_cds": [rsk_cd,...], "drpo_cds": [rsk_cd,...]}}
            bnft_no=0 (전체 수준) 제외, 실제 급부만.
        """
        pcv_data = self._bnft_by_pcv.get((prod_cd, cls_cd, cov_cd))
        if not pcv_data:
            return {}
        result = {}
        for bno, entries in pcv_data.items():
            if bno == 0:
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
        rows = conn.execute(f"""
            SELECT DISTINCT PROD_CD, CLS_CD, COV_CD, BNFT_NO
            FROM IP_B_BNFT_BAS
            {self._pcv_where()}
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
        rows = conn.execute(f"""
            SELECT PROD_CD, CLS_CD, COV_CD, BNFT_NO,
                   SETN_STRT_VAL, SETN_END_VAL, DEFRY_RT
            FROM IP_B_BNFT_DEFRY_RT
            {self._pcv_where()}
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
        rows = conn.execute(f"""
            SELECT PROD_CD, CLS_CD, COV_CD, BNFT_NO,
                   NCOV_PRD_TPCD, NCOV_PRD_CNT
            FROM IP_B_NCOV
            {self._pcv_where()}
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
        rows = conn.execute(f"""
            SELECT PROD_CD, CLS_CD, COV_CD, RSK_RT_CD,
                   INVLD_TRMNAT_PRD_TPCD, INVLD_TRMNAT_PRD_CNT
            FROM IP_R_INVLD_TRMNAT
            {self._pcv_where()}
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
        """(prod, cls, cov, bnft_no) → [{strt, end, defry_rt, cd, cyc, tot}, ...]"""
        self.prtt_rt = {}
        rows = conn.execute(f"""
            SELECT PROD_CD, CLS_CD, COV_CD, BNFT_NO,
                   SETN_STRT_VAL, SETN_END_VAL, DEFRY_RT,
                   PRTT_LPSM_CVAL_APLY_CD, DEFRY_CYC, TOT_DEFRY_TIMS
            FROM IP_B_PRTT_BNFT_RT
            {self._pcv_where()}
            ORDER BY PROD_CD, CLS_CD, COV_CD, BNFT_NO, SETN_STRT_VAL
        """).fetchall()
        for r in rows:
            key = (r[0], str(r[1]).zfill(2) if r[1] else "01", r[2], int(r[3]))
            entry = {
                "strt": int(r[4]) if r[4] else 0,
                "end": int(r[5]) if r[5] else 999,
                "defry_rt": float(r[6] or 0),
                "cd": int(r[7]) if r[7] else 0,
                "cyc": int(r[8]) if r[8] else 1,
                "tot": int(r[9]) if r[9] else 0,
            }
            self.prtt_rt.setdefault(key, []).append(entry)
        logger.info(f"PRTT_BNFT_RT: {len(self.prtt_rt):,} keys")

    def _load_expct_inrt_for_prtt(self, conn):
        """IP_P_EXPCT_INRT → (prod, cls, cov) → {expct, std, avg_pubano}"""
        self.expct_inrt_prtt = {}
        rows = conn.execute(f"""
            SELECT PROD_CD, CLS_CD, COV_CD,
                   EXPCT_INRT1, STD_INRT1, AVG_PUBANO_INRT
            FROM IP_P_EXPCT_INRT
            {self._pcv_where()}
        """).fetchall()
        for r in rows:
            cls = str(r[1]).zfill(2) if r[1] else "01"
            self.expct_inrt_prtt[(r[0], cls, r[2])] = {
                "expct": float(r[3] or 0),
                "std": float(r[4] or 0),
                "avg_pubano": float(r[5] or 0),
            }

    @staticmethod
    def _ann_due(tot, i_annual, cyc=1):
        """확정연금 기초급 현가."""
        if i_annual <= 0 or tot <= 0:
            return float(tot)
        i_m = (1 + i_annual) ** (1 / 12) - 1
        v = 1 / (1 + i_m)
        if cyc == 1:
            return (1 - v ** tot) / (1 - v)
        v_c = v ** cyc
        return (1 - v_c ** tot) / (1 - v_c)

    def get_prtt_rate(self, prod_cd, cls_cd, cov_cd, bnft_no, duration_year):
        """PRTT_RT 산출. PRTT_LPSM_CVAL_APLY_CD에 따라 이율 선택 후 ann_due 계산."""
        key = (prod_cd, cls_cd, cov_cd, bnft_no)
        entries = self.prtt_rt.get(key)
        if not entries:
            return 0.0
        # SETN range 매칭
        entry = None
        for e in entries:
            if e["strt"] <= duration_year <= e["end"]:
                entry = e
                break
        if not entry or entry["tot"] <= 0:
            return 0.0

        # 이율 선택
        inrt = self.expct_inrt_prtt.get((prod_cd, cls_cd, cov_cd), {})
        cd = entry["cd"]
        if cd == 1:
            rate = inrt.get("expct", 0)
        elif cd == 2:
            rate = inrt.get("std", 0)
        elif cd == 3:
            rate = inrt.get("avg_pubano", 0)
        elif cd == 4:
            rate = min(inrt.get("expct", 0),
                       inrt.get("avg_pubano", 0) or inrt.get("expct", 0))
        else:
            rate = inrt.get("expct", 0)

        ann = self._ann_due(entry["tot"], rate, entry["cyc"])

        # CD=1: 2개월 이연 (v^2 적용)
        if cd == 1 and rate > 0:
            i_m = (1 + rate) ** (1 / 12) - 1
            v2 = (1 / (1 + i_m)) ** 2
            ann *= v2

        return entry["defry_rt"] * ann

    # ------------------------------------------------------------------
    # Risk meta: RSK_GRP_NO + DEAD_RT_DVCD (Per-BNFT dedup용)
    # ------------------------------------------------------------------
    def _load_risk_meta(self, conn):
        """(prod, cls, cov, rsk_cd) -> {"grp": str, "dead": int}

        IP_R_RSKRT_C(RSK_GRP_NO) + IR_RSKRT_CHR(DEAD_RT_DVCD) 조인.
        IP_R_COV_RSKRT_C/IP_R_BNFT_RSKRT_C에만 있는 코드도 포함.
        """
        self.risk_meta = {}  # (prod, cls, cov) -> {rsk_cd: {"grp", "dead"}}

        # IR_RSKRT_CHR 전체 캐시
        chr_map = {}
        chr_rows = conn.execute("SELECT RSK_RT_CD, DEAD_RT_DVCD FROM IR_RSKRT_CHR").fetchall()
        for r in chr_rows:
            chr_map[str(r[0])] = int(r[1]) if r[1] is not None else 0

        # IP_R_RSKRT_C — SQL LIST 집계로 (prod,cls,cov)별 그룹화 (3.87M→586K 루프)
        rows = conn.execute(f"""
            SELECT PROD_CD,
                   CASE WHEN CLS_CD IS NULL THEN '01'
                        ELSE LPAD(CAST(CLS_CD AS VARCHAR), 2, '0') END AS CLS_CD,
                   COV_CD,
                   LIST(DISTINCT STRUCT_PACK(
                       rsk := CAST(RSK_RT_CD AS VARCHAR),
                       grp := CAST(RSK_GRP_NO AS VARCHAR)
                   )) AS risks
            FROM IP_R_RSKRT_C
            {self._pcv_where()}
            GROUP BY PROD_CD, CLS_CD, COV_CD
        """).fetchall()
        for r in rows:
            pkey = (r[0], r[1], r[2])
            meta = {}
            for item in r[3]:
                cd = item["rsk"]
                if cd not in meta:
                    meta[cd] = {
                        "grp": item["grp"] if item["grp"] else "0",
                        "dead": chr_map.get(cd, 1),
                    }
            self.risk_meta[pkey] = meta

        # IP_R_COV_RSKRT_C / IP_R_BNFT_RSKRT_C에만 있는 extra codes (C1/C2 등)
        for tbl in ("IP_R_COV_RSKRT_C", "IP_R_BNFT_RSKRT_C"):
            extra = conn.execute(f"""
                SELECT DISTINCT PROD_CD, CLS_CD, COV_CD, RSK_RT_CD
                FROM {tbl}
                {self._pcv_where()}
            """).fetchall()
            for r in extra:
                pkey = (r[0], str(r[1]).zfill(2) if r[1] else "01", r[2])
                cd = str(r[3])
                meta = self.risk_meta.setdefault(pkey, {})
                if cd not in meta:
                    dead = chr_map.get(cd, 0)
                    meta[cd] = {"grp": f"__{cd}__", "dead": dead}

        total = sum(len(v) for v in self.risk_meta.values())
        logger.info(f"RISK_META: {len(self.risk_meta):,} (prod,cls,cov), {total:,} risks")

    def get_risk_meta(self, prod_cd, cls_cd, cov_cd):
        """(prod,cls,cov)의 risk meta 반환. {rsk_cd: {"grp", "dead"}}"""
        return self.risk_meta.get((prod_cd, cls_cd, cov_cd), {})

    # ------------------------------------------------------------------
    # RSVAMT exit flags (IP_R_COV_RSKRT_C)
    # ------------------------------------------------------------------
    def _load_rsvamt_flags(self, conn):
        """(prod, cls, cov) -> set of RSVAMT exit risk codes."""
        self.rsvamt_flags = {}  # (prod, cls, cov) -> set(rsk_cd)
        rows = conn.execute(f"""
            SELECT DISTINCT PROD_CD, CLS_CD, COV_CD, RSK_RT_CD,
                   RSVAMT_DEFRY_DRPO_RSKRT_YN
            FROM IP_R_COV_RSKRT_C
            {self._pcv_where()}
        """).fetchall()
        for r in rows:
            pkey = (r[0], str(r[1]).zfill(2) if r[1] else "01", r[2])
            if int(r[4]) == 1:
                self.rsvamt_flags.setdefault(pkey, set()).add(str(r[3]))
        logger.info(f"RSVAMT_FLAGS: {len(self.rsvamt_flags):,} (prod,cls,cov) keys")

    def get_rsvamt_cds(self, prod_cd, cls_cd, cov_cd):
        """(prod,cls,cov)의 RSVAMT exit risk codes 반환."""
        return self.rsvamt_flags.get((prod_cd, cls_cd, cov_cd), set())

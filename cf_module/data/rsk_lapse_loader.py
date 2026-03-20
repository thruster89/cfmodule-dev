"""OD_RSK_RT / OD_LAPSE_RT 산출용 원시 데이터 로더.

raw 테이블(IR_RSKRT_VAL, IA_T_TRMNAT_RT, IA_T_SKEW, IA_R_BEPRD_DEFRY_RT 등)에서
드라이버 기반 키매칭을 수행하여 계약별 가정을 직접 조회한다.
v2 ETL 불필요 — duckdb_transform.duckdb 또는 VSOLN2.vdb에서 직접 동작.

드라이버 패턴 (v1 assm_key_builder.py 참고):
  IA_M_ASSM_DRIV에서 활성 차원 결정
  → ASSM_DIV_VAL_YN: 0=무시('^'), 1=ETC매핑, 2=원본유지
  → IA_M_ETC_ASSM_KEY에서 ASSM_GRP_CD 매핑
  → IA_M_PROD_GRP에서 상품그룹 매핑
  → 해당 가정 테이블 WHERE 조건 구축

Usage:
    con = duckdb.connect('duckdb_transform.duckdb', read_only=True)
    loader = RawAssumptionLoader(con)
    # 또는 VSOLN2 연결:
    con = duckdb.connect()
    con.execute("ATTACH 'VSOLN2.vdb' AS src (TYPE SQLITE, READ_ONLY)")
    loader = RawAssumptionLoader(con, prefix='src.')
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd

from cf_module.constants import FIXED_DEF_CD_MAP, NULL_MARKER

# 하위 호환: 기존 코드가 _FIXED_DEF_CD_MAP 참조할 경우
_FIXED_DEF_CD_MAP = FIXED_DEF_CD_MAP


@dataclass
class ContractInfo:
    """계약 기본 정보."""
    idno: int
    prod_cd: str
    cls_cd: str
    cov_cd: str
    entry_age: int
    bterm_yy: int
    pterm_yy: int
    pass_yy: int
    pass_mm: int
    clos_ym: int
    ctr_dt: int
    assm_divs: List[str]   # ASSM_DIV_VAL1~15
    rsk_divs: List[str]    # RSK_RT_DIV_VAL1~10
    main_pterm_yy: int = 0  # MAIN_PAYPR_YYCNT (주계약 납입기간)
    ctr_tpcd: str = "0"     # CTR_TPCD (9=적립형)
    pay_stcd: str = "1"     # PAY_STCD (1=납입중, 2=납입완료, 3=납입면제)


@dataclass
class RiskInfo:
    """위험률코드 메타."""
    risk_cd: str
    chr_cd: str           # 'A' or 'S'
    mm_trf_way_cd: int    # 1=연월변환, 2=단순할당
    dead_rt_dvcd: int     # 0=사망, 1=비사망
    rsk_grp_no: str
    # DEF_CD1~10: RSK_RT_DIV_VAL_DEF_CD (위험률 분류키 매핑)
    def_cds: List[Optional[str]] = None


class RawAssumptionLoader:
    """Raw 테이블에서 드라이버 기반 가정 조회."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, prefix: str = ""):
        """
        Args:
            conn: DuckDB 연결
            prefix: 테이블 접두사 (예: 'src.' for attached SQLite)
        """
        self.conn = conn
        self.p = prefix
        # (prod,cls,cov) 기준 캐시
        self._cache_risk_codes = {}
        self._cache_invld = {}
        self._cache_exit_flags = {}
        self._cache_extra_risks = {}
        # 드라이버 캐시 (일괄 사전 로드)
        self._driv_cache = {}     # kdcd -> driv_row
        self._prod_grp_cache = {} # (kdcd, file_id, map_crit, prod, cls) -> prod_grp
        self._etc_key_cache = {}  # (kdcd, file_id, map_crit, key_no, div_val) -> grp_cd
        self._rsk_cat_cache = {}  # (file_id, rsk_cd) -> rsk_cat_val
        self._resolve_cache = {}  # (kdcd, prod, cls, assm_key) -> resolved
        # 데이터 쿼리 캐시
        self._data_cache = {}     # (table, where_clause) -> result
        self._contract_cache = {} # idno -> ContractInfo
        self._cov_def_cd_map = {} # (prod, cls, cov) -> {def_cd_value: rsk_div_index}
        # 배치 프리로드 (preload_data_tables() 호출 시 채워짐)
        self._mort_preload = None    # {(rsk_cd, div_val_tuple): rate_array}
        self._lapse_preload = None   # {filter_tuple: (paying, paidup)}
        self._beprd_preload = None   # {(filter_tuple, rsk_cat_val): beprd_array}
        self._skew_preload = None    # {filter_tuple: skew_array}
        self._preload_driver_tables()
        self._preload_cov_def_cd()

    # ------------------------------------------------------------------
    # IP_P_COV DEF_CD 사전 로드 (RSK_RT_DIV_VAL 동적 매핑)
    # ------------------------------------------------------------------
    def _preload_cov_def_cd(self):
        """IP_P_COV.RSK_RT_DIV_VAL_DEF_CD1~10 → {def_cd: rsk_div_index} 매핑.

        IR_RSKRT_CHR.DEF_CD[pos] = code일 때,
        IP_P_COV에서 같은 code가 DEF_CD[j]에 있으면 → II_INFRC.RSK_RT_DIV_VAL[j] 사용.
        """
        rows = self.conn.execute(f"""
            SELECT PROD_CD, CLS_CD, COV_CD,
                   RSK_RT_DIV_VAL_DEF_CD1, RSK_RT_DIV_VAL_DEF_CD2,
                   RSK_RT_DIV_VAL_DEF_CD3, RSK_RT_DIV_VAL_DEF_CD4,
                   RSK_RT_DIV_VAL_DEF_CD5, RSK_RT_DIV_VAL_DEF_CD6,
                   RSK_RT_DIV_VAL_DEF_CD7, RSK_RT_DIV_VAL_DEF_CD8,
                   RSK_RT_DIV_VAL_DEF_CD9, RSK_RT_DIV_VAL_DEF_CD10
            FROM {self.p}IP_P_COV
        """).fetchall()
        for r in rows:
            key = (str(r[0]), str(r[1]), str(r[2]))
            mapping = {}
            for i in range(10):
                def_cd = str(r[3 + i]) if r[3 + i] else None
                if def_cd:
                    mapping[def_cd] = i  # def_cd → rsk_divs[i] 인덱스
            self._cov_def_cd_map[key] = mapping

    def _get_def_cd_map(self, prod_cd, cls_cd, cov_cd):
        """(prod,cls,cov)의 DEF_CD → rsk_divs 인덱스 매핑 반환."""
        m = self._cov_def_cd_map.get((prod_cd, cls_cd, cov_cd))
        if m:
            return m
        return _FIXED_DEF_CD_MAP

    # ------------------------------------------------------------------
    # 계약 정보 로드
    # ------------------------------------------------------------------
    def preload_contracts(self, idnos=None):
        """II_INFRC 일괄 로드 → _contract_cache."""
        where = ""
        if idnos:
            id_list = ",".join(str(i) for i in idnos)
            where = f" AND INFRC_IDNO IN ({id_list})"
        rows = self.conn.execute(f"""
            SELECT INFRC_IDNO, PROD_CD, CLS_CD, COV_CD,
                   ISRD_JOIN_AGE, INSTRM_YYCNT, PAYPR_YYCNT,
                   PASS_YYCNT, PASS_MMCNT, CLOS_YM, CTR_DT,
                   MAIN_PAYPR_YYCNT, CTR_TPCD, PAY_STCD,
                   ASSM_DIV_VAL1, ASSM_DIV_VAL2, ASSM_DIV_VAL3,
                   ASSM_DIV_VAL4, ASSM_DIV_VAL5, ASSM_DIV_VAL6,
                   ASSM_DIV_VAL7, ASSM_DIV_VAL8, ASSM_DIV_VAL9,
                   ASSM_DIV_VAL10, ASSM_DIV_VAL11, ASSM_DIV_VAL12,
                   ASSM_DIV_VAL13, ASSM_DIV_VAL14, ASSM_DIV_VAL15,
                   RSK_RT_DIV_VAL1, RSK_RT_DIV_VAL2, RSK_RT_DIV_VAL3,
                   RSK_RT_DIV_VAL4, RSK_RT_DIV_VAL5, RSK_RT_DIV_VAL6,
                   RSK_RT_DIV_VAL7, RSK_RT_DIV_VAL8, RSK_RT_DIV_VAL9,
                   RSK_RT_DIV_VAL10
            FROM {self.p}II_INFRC WHERE INFRC_SEQ = 1{where}
        """).fetchall()
        for row in rows:
            ctr = self._row_to_contract(row)
            self._contract_cache[ctr.idno] = ctr

    # ------------------------------------------------------------------
    # 배치 전건 프리로드 (mn_chain DB I/O 제거)
    # ------------------------------------------------------------------
    def preload_data_tables(self):
        """배치 실행 시 3대 데이터 테이블을 메모리에 일괄 로드.

        이후 load_mortality_rates/load_lapse_rates/load_beprd/load_skew에서
        SQL 대신 인메모리 lookup으로 대체.
        """
        import time
        t0 = time.time()
        self._preload_mortality()
        t1 = time.time()
        self._preload_lapse()
        t2 = time.time()
        self._preload_beprd()
        t3 = time.time()
        self._preload_skew()
        t4 = time.time()
        print(f"  데이터 프리로드: MORT={t1-t0:.1f}s LAPSE={t2-t1:.1f}s "
              f"BEPRD={t3-t2:.1f}s SKEW={t4-t3:.1f}s (합계 {t4-t0:.1f}s)")
        print(f"    MORT={len(self._mort_preload):,}건 "
              f"LAPSE={len(self._lapse_preload):,}건 "
              f"BEPRD={len(self._beprd_preload):,}건 "
              f"SKEW={len(self._skew_preload):,}건")
        # 히트율 추적 카운터
        self._preload_stats = {
            "mort_hit": 0, "mort_miss": 0,
            "lapse_hit": 0, "lapse_miss": 0,
            "beprd_hit": 0, "beprd_miss": 0,
            "skew_hit": 0, "skew_miss": 0,
        }

    def print_preload_stats(self):
        """프리로드 히트율 출력."""
        if not hasattr(self, '_preload_stats'):
            return
        s = self._preload_stats
        for name in ["mort", "lapse", "beprd", "skew"]:
            h, m = s[f"{name}_hit"], s[f"{name}_miss"]
            total = h + m
            pct = h / total * 100 if total > 0 else 0
            print(f"    {name}: {h:,}/{total:,} ({pct:.1f}% hit)")

    def _preload_mortality(self):
        """IR_RSKRT_VAL 전건 → _mort_preload.

        키: (rsk_cd, (div_val1, div_val2, ..., div_val10))
        값: rate_by_age (numpy array, 인덱스=나이)
        """
        div_cols = [f"RSK_RT_DIV_VAL{i}" for i in range(1, 11)]
        df = self.conn.execute(f"""
            SELECT RSK_RT_CD, AGE, RSK_RT, {', '.join(div_cols)}
            FROM {self.p}IR_RSKRT_VAL
        """).fetchdf()

        self._mort_preload = {}
        if df.empty:
            return

        # 그룹화: (RSK_RT_CD, DIV_VAL1..10) → ages/rates
        group_cols = ["RSK_RT_CD"] + div_cols
        for key_vals, sub in df.groupby(group_cols, sort=False):
            rsk_cd = str(key_vals[0])
            div_vals = tuple(str(v) if pd.notna(v) else "" for v in key_vals[1:])

            ages = sub["AGE"].values.astype(int)
            vals = sub["RSK_RT"].values.astype(np.float64)

            if len(ages) == 0:
                continue
            max_age = int(ages.max())
            arr = np.zeros(max_age + 1, dtype=np.float64)
            arr[ages] = vals
            self._mort_preload[(rsk_cd, div_vals)] = arr

    def _preload_lapse(self):
        """IA_T_TRMNAT_RT 전건 → _lapse_preload.

        키: 행의 필터 컬럼 tuple
        값: (paying_array, paidup_array)
        """
        df = self.conn.execute(f"""
            SELECT * FROM {self.p}IA_T_TRMNAT_RT
        """).fetchdf()

        self._lapse_preload = {}
        if df.empty:
            return

        # 필터 컬럼 = ASSM_FILE_ID, PROD_GRP_CD, ASSM_GRP_CD*
        filter_cols = [c for c in df.columns
                       if c in ("ASSM_FILE_ID", "PROD_GRP_CD") or c.startswith("ASSM_GRP_CD")]
        val_cols = sorted(
            [c for c in df.columns if c.startswith("TRMNAT_RT") and c[9:].isdigit()],
            key=lambda x: int(x[9:])
        )
        max_years = 100

        # (filter_tuple) → {pay_dvcd: rate_array}
        temp = {}
        for _, row in df.iterrows():
            fkey = tuple(str(row[c]) if pd.notna(row[c]) else "" for c in filter_cols)
            pay_dvcd = int(row.get("PAY_DVCD", 1))
            rates = np.zeros(max_years, dtype=np.float64)
            for yr_idx, col in enumerate(val_cols):
                if yr_idx < max_years:
                    v = float(row[col]) if pd.notna(row[col]) else 0.0
                    rates[yr_idx] = v
            n_data = len(val_cols)
            if n_data < max_years:
                rates[n_data:] = rates[n_data - 1]
            temp.setdefault(fkey, {})[pay_dvcd] = rates

        for fkey, dvcd_map in temp.items():
            paying = dvcd_map.get(1, np.zeros(max_years, dtype=np.float64))
            paidup = dvcd_map.get(2, np.zeros(max_years, dtype=np.float64))
            self._lapse_preload[fkey] = (paying, paidup)

        # _lapse_filter_cols 저장 (lookup 시 사용)
        self._lapse_filter_cols = filter_cols

    def _preload_beprd(self):
        """IA_R_BEPRD_DEFRY_RT 전건 → _beprd_preload.

        키: (filter_tuple, rsk_cat_val)
        값: beprd_array
        """
        df = self.conn.execute(f"""
            SELECT * FROM {self.p}IA_R_BEPRD_DEFRY_RT
        """).fetchdf()

        self._beprd_preload = {}
        if df.empty:
            return

        filter_cols = [c for c in df.columns
                       if c in ("ASSM_FILE_ID", "PROD_GRP_CD") or c.startswith("ASSM_GRP_CD")]
        val_cols = sorted(
            [c for c in df.columns if c.startswith("BEPRD_DEFRY_RT")],
            key=lambda x: int(x.replace("BEPRD_DEFRY_RT", ""))
        )
        max_years = 100

        for _, row in df.iterrows():
            fkey = tuple(str(row[c]) if pd.notna(row[c]) else "" for c in filter_cols)
            rsk_cat_val = str(row.get("RSK_CAT_VAL", "")) if pd.notna(row.get("RSK_CAT_VAL")) else ""

            beprd = np.ones(max_years, dtype=np.float64)
            for yr_idx, col in enumerate(val_cols):
                if yr_idx < max_years:
                    v = float(row[col]) if pd.notna(row[col]) else 1.0
                    beprd[yr_idx] = v
            # 마지막 유효값 연장
            last_valid = 0
            for i in range(max_years):
                if beprd[i] != 1.0:
                    last_valid = i
            if last_valid > 0 and last_valid < max_years - 1:
                beprd[last_valid + 1:] = beprd[last_valid]

            self._beprd_preload[(fkey, rsk_cat_val)] = beprd

        self._beprd_filter_cols = filter_cols

    def _preload_skew(self):
        """IA_T_SKEW 전건 → _skew_preload.

        키: filter_tuple
        값: skew_array (max_months)
        """
        df = self.conn.execute(f"""
            SELECT * FROM {self.p}IA_T_SKEW
        """).fetchdf()

        self._skew_preload = {}
        if df.empty:
            return

        filter_cols = [c for c in df.columns
                       if c in ("ASSM_FILE_ID", "PROD_GRP_CD") or c.startswith("ASSM_GRP_CD")]
        max_months = 1200

        for _, row in df.iterrows():
            fkey = tuple(str(row[c]) if pd.notna(row[c]) else "" for c in filter_cols)
            skew = np.full(max_months, 1.0 / 12.0, dtype=np.float64)
            for mi in range(36):
                col = f"SKEW{mi + 1}"
                if col in df.columns:
                    v = float(row[col]) if pd.notna(row[col]) else 1.0 / 12.0
                    skew[mi] = v
            # 37개월 이후 = 마지막 값 유지
            if len(df.columns) > 0:
                skew[36:] = skew[35]
            self._skew_preload[fkey] = skew

        self._skew_filter_cols = filter_cols

    def _row_to_contract(self, row) -> ContractInfo:
        main_pterm = int(row[11]) if row[11] is not None else int(row[6])
        ctr_tpcd = str(row[12]) if row[12] is not None else "0"
        pay_stcd = str(row[13]) if row[13] is not None else "1"
        assm_divs = [str(v) if v is not None else "^" for v in row[14:29]]
        rsk_divs = [str(v) if v is not None else "^" for v in row[29:39]]
        return ContractInfo(
            idno=int(row[0]), prod_cd=str(row[1]),
            cls_cd=str(row[2]), cov_cd=str(row[3]),
            entry_age=int(row[4]), bterm_yy=int(row[5]),
            pterm_yy=int(row[6]), pass_yy=int(row[7]),
            pass_mm=int(row[8]), clos_ym=int(row[9]),
            ctr_dt=int(row[10]), assm_divs=assm_divs,
            rsk_divs=rsk_divs, main_pterm_yy=main_pterm,
            ctr_tpcd=ctr_tpcd, pay_stcd=pay_stcd,
        )

    def load_contract(self, idno: int, infrc_seq: int = 1) -> ContractInfo:
        if idno in self._contract_cache:
            return self._contract_cache[idno]
        row = self.conn.execute(f"""
            SELECT INFRC_IDNO, PROD_CD, CLS_CD, COV_CD,
                   ISRD_JOIN_AGE, INSTRM_YYCNT, PAYPR_YYCNT,
                   PASS_YYCNT, PASS_MMCNT, CLOS_YM, CTR_DT,
                   MAIN_PAYPR_YYCNT, CTR_TPCD, PAY_STCD,
                   ASSM_DIV_VAL1, ASSM_DIV_VAL2, ASSM_DIV_VAL3,
                   ASSM_DIV_VAL4, ASSM_DIV_VAL5, ASSM_DIV_VAL6,
                   ASSM_DIV_VAL7, ASSM_DIV_VAL8, ASSM_DIV_VAL9,
                   ASSM_DIV_VAL10, ASSM_DIV_VAL11, ASSM_DIV_VAL12,
                   ASSM_DIV_VAL13, ASSM_DIV_VAL14, ASSM_DIV_VAL15,
                   RSK_RT_DIV_VAL1, RSK_RT_DIV_VAL2, RSK_RT_DIV_VAL3,
                   RSK_RT_DIV_VAL4, RSK_RT_DIV_VAL5, RSK_RT_DIV_VAL6,
                   RSK_RT_DIV_VAL7, RSK_RT_DIV_VAL8, RSK_RT_DIV_VAL9,
                   RSK_RT_DIV_VAL10
            FROM {self.p}II_INFRC
            WHERE INFRC_IDNO = ? AND INFRC_SEQ = ?
        """, [idno, infrc_seq]).fetchone()
        if row is None:
            raise ValueError(f"IDNO {idno} not found")

        ctr = self._row_to_contract(row)
        self._contract_cache[idno] = ctr
        return ctr

    # ------------------------------------------------------------------
    # 위험률코드 매핑
    # ------------------------------------------------------------------
    def load_risk_codes(self, ctr: ContractInfo) -> List[RiskInfo]:
        """IP_R_RSKRT_C (전체 위험률코드) + IR_RSKRT_CHR (특성) 조인.

        IP_R_COV_RSKRT_C에는 일부 코드만 있고, IP_R_RSKRT_C에 전체가 있음.
        111018 같은 코드는 IP_R_COV_RSKRT_C에 없지만 IP_R_RSKRT_C에 존재.
        """
        key = (ctr.prod_cd, ctr.cls_cd, ctr.cov_cd)
        if key in self._cache_risk_codes:
            return self._cache_risk_codes[key]
        rows = self.conn.execute(f"""
            SELECT r.RSK_RT_CD,
                   chr.RSK_RT_CHR_CD,
                   chr.MM_TRF_WAY_CD,
                   chr.DEAD_RT_DVCD,
                   r.RSK_GRP_NO,
                   chr.RSK_RT_DIV_VAL_DEF_CD1,
                   chr.RSK_RT_DIV_VAL_DEF_CD2,
                   chr.RSK_RT_DIV_VAL_DEF_CD3,
                   chr.RSK_RT_DIV_VAL_DEF_CD4,
                   chr.RSK_RT_DIV_VAL_DEF_CD5,
                   chr.RSK_RT_DIV_VAL_DEF_CD6,
                   chr.RSK_RT_DIV_VAL_DEF_CD7,
                   chr.RSK_RT_DIV_VAL_DEF_CD8,
                   chr.RSK_RT_DIV_VAL_DEF_CD9,
                   chr.RSK_RT_DIV_VAL_DEF_CD10
            FROM {self.p}IP_R_RSKRT_C r
            JOIN {self.p}IR_RSKRT_CHR chr ON chr.RSK_RT_CD = r.RSK_RT_CD
            WHERE r.PROD_CD = ? AND r.CLS_CD = ? AND r.COV_CD = ?
            ORDER BY r.RSK_RT_CD
        """, [ctr.prod_cd, ctr.cls_cd, ctr.cov_cd]).fetchall()

        result = []
        seen = set()
        for r in rows:
            rsk_cd = str(r[0])
            if rsk_cd in seen:
                continue
            seen.add(rsk_cd)
            def_cds = [str(r[i]) if r[i] is not None else None for i in range(5, 15)]
            result.append(RiskInfo(
                risk_cd=rsk_cd,
                chr_cd=str(r[1]) if r[1] else "A",
                mm_trf_way_cd=int(r[2]) if r[2] else 1,
                dead_rt_dvcd=int(r[3]) if r[3] is not None else 1,
                rsk_grp_no=str(r[4]) if r[4] is not None else "0",
                def_cds=def_cds,
            ))
        self._cache_risk_codes[key] = result
        return result

    # ------------------------------------------------------------------
    # 위험률 원율 (IR_RSKRT_VAL) — DEF_CD 기반 DIV_VAL 필터링
    # ------------------------------------------------------------------
    def load_mortality_rates(
        self, risks: List[RiskInfo], ctr: ContractInfo
    ) -> Dict[str, np.ndarray]:
        """위험률코드별 원율 로드. DEF_CD 매핑으로 정확한 DIV_VAL 행 선택.

        IR_RSKRT_CHR의 RSK_RT_DIV_VAL_DEF_CD가 '49'이면
        IR_RSKRT_VAL.RSK_RT_DIV_VAL1 = 계약의 RSK_RT_DIV_VAL1 로 필터.

        Returns:
            {rsk_cd: rate_by_age or scalar}
        """
        if not risks:
            return {}

        # IP_P_COV 기반 동적 DEF_CD 매핑
        def_cd_map = self._get_def_cd_map(ctr.prod_cd, ctr.cls_cd, ctr.cov_cd)

        rates = {}
        for risk in risks:
            rsk_cd = risk.risk_cd

            # DEF_CD 매핑으로 DIV_VAL 결정
            div_vals_list = [""] * 10  # 10개 위치 기본값
            div_filters = []
            for pos_idx in range(10):
                def_cd = risk.def_cds[pos_idx] if risk.def_cds else None
                if def_cd and def_cd in def_cd_map:
                    ctr_idx = def_cd_map[def_cd]
                    ctr_val = ctr.rsk_divs[ctr_idx] if ctr_idx < len(ctr.rsk_divs) else "^"
                    div_vals_list[pos_idx] = ctr_val
                    div_filters.append(
                        f"RSK_RT_DIV_VAL{pos_idx + 1} = '{ctr_val}'"
                    )

            # 프리로드 모드: dict lookup
            if self._mort_preload is not None:
                div_key = tuple(div_vals_list)
                arr = self._mort_preload.get((rsk_cd, div_key))
                if arr is not None:
                    rates[rsk_cd] = arr[:1] if risk.chr_cd == "S" else arr
                    if hasattr(self, '_preload_stats'):
                        self._preload_stats["mort_hit"] += 1
                else:
                    rates[rsk_cd] = np.zeros(1, dtype=np.float64)
                    if hasattr(self, '_preload_stats'):
                        self._preload_stats["mort_miss"] += 1
                continue

            # SQL 모드: 기존 캐시 + DB 조회
            div_where = " AND " + " AND ".join(div_filters) if div_filters else ""
            cache_key = ("MORT", rsk_cd, div_where)
            if cache_key in self._data_cache:
                rates[rsk_cd] = self._data_cache[cache_key]
                continue

            df = self.conn.execute(f"""
                SELECT AGE, RSK_RT
                FROM {self.p}IR_RSKRT_VAL
                WHERE RSK_RT_CD = ?{div_where}
                ORDER BY AGE
            """, [rsk_cd]).fetchdf()

            if df.empty:
                rates[rsk_cd] = np.zeros(1, dtype=np.float64)
                self._data_cache[cache_key] = rates[rsk_cd]
                continue

            ages = df["AGE"].values.astype(int)
            vals = df["RSK_RT"].values.astype(np.float64)

            if risk.chr_cd == "S":
                rates[rsk_cd] = vals[:1]
            else:
                max_age = int(ages.max())
                arr = np.zeros(max_age + 1, dtype=np.float64)
                arr[ages] = vals
                rates[rsk_cd] = arr

            self._data_cache[cache_key] = rates[rsk_cd]

        return rates

    # ------------------------------------------------------------------
    # 드라이버 사전 로드 (일괄)
    # ------------------------------------------------------------------
    def _preload_driver_tables(self):
        """IA_M_ASSM_DRIV, IA_M_PROD_GRP, IA_M_ETC_ASSM_KEY, IA_M_RSK_CAT 일괄 로드."""
        # IA_M_ASSM_DRIV
        rows = self.conn.execute(f"""
            SELECT ASSM_KDCD, ASSM_FILE_ID, MAP_CRIT_SRNO,
                   PROD_DIV_GRP_CD_YN, COV_DIV_GRP_CD_YN, RSK_CAT_VAL_YN,
                   ASSM_DIV_VAL1_YN, ASSM_DIV_VAL2_YN, ASSM_DIV_VAL3_YN,
                   ASSM_DIV_VAL4_YN, ASSM_DIV_VAL5_YN, ASSM_DIV_VAL6_YN,
                   ASSM_DIV_VAL7_YN, ASSM_DIV_VAL8_YN, ASSM_DIV_VAL9_YN,
                   ASSM_DIV_VAL10_YN, ASSM_DIV_VAL11_YN, ASSM_DIV_VAL12_YN,
                   ASSM_DIV_VAL13_YN, ASSM_DIV_VAL14_YN, ASSM_DIV_VAL15_YN
            FROM {self.p}IA_M_ASSM_DRIV
        """).fetchall()
        for r in rows:
            self._driv_cache[int(r[0])] = r

        # IA_M_PROD_GRP (file_id를 str로 통일)
        rows = self.conn.execute(f"""
            SELECT ASSM_KDCD, ASSM_FILE_ID, MAP_CRIT_SRNO, PROD_CD, CLS_CD, PROD_GRP_CD
            FROM {self.p}IA_M_PROD_GRP
        """).fetchall()
        for r in rows:
            fid = str(r[1])
            mc = r[2]
            self._prod_grp_cache[(int(r[0]), fid, mc, str(r[3]), str(r[4]))] = str(r[5])
            fb_key = (int(r[0]), fid, mc, str(r[3]))
            if fb_key not in self._prod_grp_cache:
                self._prod_grp_cache[fb_key] = str(r[5])

        # IA_M_ETC_ASSM_KEY (file_id를 str로 통일)
        rows = self.conn.execute(f"""
            SELECT ASSM_KDCD, ASSM_FILE_ID, MAP_CRIT_SRNO, ASSM_KEY_NO, ASSM_DIV_VAL, ASSM_GRP_CD
            FROM {self.p}IA_M_ETC_ASSM_KEY
        """).fetchall()
        for r in rows:
            self._etc_key_cache[(int(r[0]), str(r[1]), r[2], int(r[3]), str(r[4]))] = str(r[5])

        # IA_M_RSK_CAT (ASSM_KDCD 포함, file_id를 str로 통일)
        rows = self.conn.execute(f"""
            SELECT ASSM_KDCD, ASSM_FILE_ID, RSK_RT_CD, RSK_CAT_VAL
            FROM {self.p}IA_M_RSK_CAT
        """).fetchall()
        for r in rows:
            self._rsk_cat_cache[(int(r[0]), str(r[1]), str(r[2]))] = str(r[3])

    # ------------------------------------------------------------------
    # 드라이버 기반 키매칭 (캐시 기반, SQL 없음)
    # ------------------------------------------------------------------
    def _resolve_assm_filter(
        self, kdcd: int, ctr: ContractInfo
    ) -> Optional[dict]:
        """드라이버 키매칭 (사전 로드된 캐시 사용, SQL 없음)."""
        # 캐시 키: (kdcd, prod, cls, 활성 dim 값들)
        drv = self._driv_cache.get(kdcd)
        if drv is None:
            return None

        file_id = str(drv[1])
        map_crit = drv[2]
        prod_yn = int(drv[3])
        div_yns = [int(v) for v in drv[6:]]

        # 캐시 키 구성 (활성 dim만)
        active_vals = []
        for i in range(15):
            if div_yns[i] != 0:
                active_vals.append((i, ctr.assm_divs[i] if i < len(ctr.assm_divs) else "^"))
        cache_key = (kdcd, ctr.prod_cd, ctr.cls_cd, tuple(active_vals))
        if cache_key in self._resolve_cache:
            return self._resolve_cache[cache_key]

        # 상품그룹 매핑 (인메모리)
        prod_grp = "^"
        if prod_yn:
            pg = self._prod_grp_cache.get(
                (kdcd, file_id, map_crit, ctr.prod_cd, ctr.cls_cd))
            if pg is None:
                pg = self._prod_grp_cache.get(
                    (kdcd, file_id, map_crit, ctr.prod_cd))
            if pg:
                prod_grp = pg

        # ASSM_GRP_CD 매핑 (인메모리)
        grp_filters = {}
        for dim_idx in range(15):
            driv_value = div_yns[dim_idx]
            if driv_value == 0:
                continue
            key_no = dim_idx + 1
            col_name = f"ASSM_GRP_CD{key_no}"
            div_val = ctr.assm_divs[dim_idx] if dim_idx < len(ctr.assm_divs) else "^"

            if driv_value == 2:
                grp_filters[col_name] = div_val if div_val else "^"
            elif driv_value == 1:
                grp_cd = self._etc_key_cache.get(
                    (kdcd, file_id, map_crit, key_no, div_val))
                grp_filters[col_name] = grp_cd if grp_cd else div_val

        result = {
            "file_id": file_id,
            "prod_grp": prod_grp,
            "grp_filters": grp_filters,
            "has_rsk": bool(drv[5]),
        }
        self._resolve_cache[cache_key] = result
        return result

    def _build_where(self, resolved: dict) -> str:
        """resolved 필터를 SQL WHERE 절로 변환."""
        parts = [
            f"ASSM_FILE_ID = '{resolved['file_id']}'",
            f"PROD_GRP_CD = '{resolved['prod_grp']}'",
        ]
        for col, val in resolved["grp_filters"].items():
            parts.append(f"{col} = '{val}'")
        return " AND ".join(parts)

    def _resolved_to_filter_key(self, resolved: dict, filter_cols: list) -> tuple:
        """resolved dict → 프리로드 키 변환.

        프리로드 시 filter_cols 순서로 행의 값을 tuple로 만들었으므로,
        같은 순서로 resolved의 값을 추출한다.
        """
        vals = []
        for col in filter_cols:
            if col == "ASSM_FILE_ID":
                vals.append(str(resolved["file_id"]))
            elif col == "PROD_GRP_CD":
                vals.append(str(resolved["prod_grp"]))
            else:
                vals.append(str(resolved["grp_filters"].get(col, "")))
        return tuple(vals)

    # ------------------------------------------------------------------
    # 해지율 (IA_T_TRMNAT_RT)
    # ------------------------------------------------------------------
    def load_lapse_rates(
        self, ctr: ContractInfo, max_years: int = 100
    ) -> Tuple[np.ndarray, np.ndarray]:
        """해지율 원율 조회: (paying_annual[year], paidup_annual[year]).

        Returns:
            paying: (max_years,) 납입중 연해지율 (index 0 = 1년차)
            paidup: (max_years,) 납입후 연해지율 (index 0 = 1년차)
        """
        resolved = self._resolve_assm_filter(12, ctr)
        if resolved is None:
            return np.zeros(max_years), np.zeros(max_years)

        # 프리로드 모드
        if self._lapse_preload is not None:
            fkey = self._resolved_to_filter_key(resolved, self._lapse_filter_cols)
            result = self._lapse_preload.get(fkey)
            if result is not None:
                if hasattr(self, '_preload_stats'):
                    self._preload_stats["lapse_hit"] += 1
                return result
            if hasattr(self, '_preload_stats'):
                self._preload_stats["lapse_miss"] += 1
            return np.zeros(max_years), np.zeros(max_years)

        where = self._build_where(resolved)
        cache_key = ("TRMNAT", where)
        if cache_key in self._data_cache:
            return self._data_cache[cache_key]

        df = self.conn.execute(f"""
            SELECT * FROM {self.p}IA_T_TRMNAT_RT WHERE {where}
        """).fetchdf()

        if df.empty:
            self._data_cache[cache_key] = (np.zeros(max_years), np.zeros(max_years))
            return self._data_cache[cache_key]

        val_cols = sorted(
            [c for c in df.columns if c.startswith("TRMNAT_RT") and c[9:].isdigit()],
            key=lambda x: int(x[9:])
        )

        paying = np.zeros(max_years, dtype=np.float64)
        paidup = np.zeros(max_years, dtype=np.float64)

        for _, row in df.iterrows():
            pay_dvcd = int(row.get("PAY_DVCD", 1))
            target = paying if pay_dvcd == 1 else paidup
            for yr_idx, col in enumerate(val_cols):
                if yr_idx < max_years:
                    v = float(row[col]) if pd.notna(row[col]) else 0.0
                    target[yr_idx] = v

        # 마지막 데이터 컬럼 값으로 연장 (RT20 이후 = RT20 값 유지)
        n_data = len(val_cols)
        if n_data < max_years:
            for arr in (paying, paidup):
                arr[n_data:] = arr[n_data - 1]

        self._data_cache[cache_key] = (paying, paidup)
        return paying, paidup

    # ------------------------------------------------------------------
    # 스큐 (IA_T_SKEW)
    # ------------------------------------------------------------------
    def load_skew(
        self, ctr: ContractInfo, max_months: int = 1200
    ) -> np.ndarray:
        """스큐 조회: (max_months,) — SKEW1~36은 월별 값.

        Returns:
            skew: (max_months,) 스큐 지수 (index 0 = 1개월차)
        """
        resolved = self._resolve_assm_filter(13, ctr)
        if resolved is None:
            return np.full(max_months, 1.0 / 12.0, dtype=np.float64)

        # 프리로드 모드
        if self._skew_preload is not None:
            fkey = self._resolved_to_filter_key(resolved, self._skew_filter_cols)
            result = self._skew_preload.get(fkey)
            if result is not None:
                if hasattr(self, '_preload_stats'):
                    self._preload_stats["skew_hit"] += 1
                return result
            if hasattr(self, '_preload_stats'):
                self._preload_stats["skew_miss"] += 1
            return np.full(max_months, 1.0 / 12.0, dtype=np.float64)

        where = self._build_where(resolved)
        cache_key = ("SKEW", where)
        if cache_key in self._data_cache:
            return self._data_cache[cache_key]

        df = self.conn.execute(f"""
            SELECT * FROM {self.p}IA_T_SKEW WHERE {where}
        """).fetchdf()

        if df.empty:
            result = np.full(max_months, 1.0 / 12.0, dtype=np.float64)
            self._data_cache[cache_key] = result
            return result

        val_cols = sorted(
            [c for c in df.columns if c.startswith("SKEW") and c[4:].isdigit()],
            key=lambda x: int(x[4:])
        )

        skew = np.full(max_months, 0.0, dtype=np.float64)
        row = df.iloc[0]
        for mm_idx, col in enumerate(val_cols):
            v = float(row[col]) if pd.notna(row[col]) else 0.0
            if mm_idx < max_months:
                skew[mm_idx] = v

        self._data_cache[cache_key] = skew
        return skew

    # ------------------------------------------------------------------
    # BEPRD (IA_R_BEPRD_DEFRY_RT)
    # ------------------------------------------------------------------
    def load_beprd(
        self, ctr: ContractInfo, risk_cds: List[str], max_years: int = 100
    ) -> Dict[str, np.ndarray]:
        """BEPRD 경과년도별 지급률: {rsk_cd: (max_years,)}.

        Returns:
            {rsk_cd: beprd[year]} — index 0 = 1년차
        """
        resolved = self._resolve_assm_filter(9, ctr)
        if resolved is None:
            return {rsk: np.ones(max_years) for rsk in risk_cds}

        # 프리로드 모드
        if self._beprd_preload is not None:
            fkey = self._resolved_to_filter_key(resolved, self._beprd_filter_cols)
            result = {}
            for risk_cd in risk_cds:
                rsk_cat_val = self._rsk_cat_cache.get((9, resolved["file_id"], risk_cd))
                if rsk_cat_val is None:
                    result[risk_cd] = np.ones(max_years, dtype=np.float64)
                else:
                    arr = self._beprd_preload.get((fkey, str(rsk_cat_val)))
                    if arr is not None:
                        result[risk_cd] = arr
                        if hasattr(self, '_preload_stats'):
                            self._preload_stats["beprd_hit"] += 1
                    else:
                        result[risk_cd] = np.ones(max_years, dtype=np.float64)
                        if hasattr(self, '_preload_stats'):
                            self._preload_stats["beprd_miss"] += 1
            return result

        result = {}
        for risk_cd in risk_cds:
            # RSK_CAT_VAL 매핑 (인메모리)
            rsk_cat_val = self._rsk_cat_cache.get((9, resolved["file_id"], risk_cd))

            if rsk_cat_val is None:
                result[risk_cd] = np.ones(max_years, dtype=np.float64)
                continue

            where_parts = [self._build_where(resolved), f"RSK_CAT_VAL = '{rsk_cat_val}'"]
            where_sql = " AND ".join(where_parts)

            cache_key = ("BEPRD", where_sql)
            if cache_key in self._data_cache:
                result[risk_cd] = self._data_cache[cache_key]
                continue

            try:
                df = self.conn.execute(f"""
                    SELECT * FROM {self.p}IA_R_BEPRD_DEFRY_RT WHERE {where_sql}
                """).fetchdf()
            except Exception:
                result[risk_cd] = np.ones(max_years, dtype=np.float64)
                continue

            if df.empty:
                result[risk_cd] = np.ones(max_years, dtype=np.float64)
                continue

            val_cols = sorted(
                [c for c in df.columns if c.startswith("BEPRD_DEFRY_RT")],
                key=lambda x: int(x.replace("BEPRD_DEFRY_RT", ""))
            )

            beprd = np.ones(max_years, dtype=np.float64)
            row = df.iloc[0]
            for yr_idx, col in enumerate(val_cols):
                if yr_idx < max_years:
                    v = float(row[col]) if pd.notna(row[col]) else 1.0
                    beprd[yr_idx] = v

            # 마지막 유효값으로 연장
            last_valid = 0
            for i in range(max_years):
                if beprd[i] != 1.0:
                    last_valid = i
            if last_valid > 0 and last_valid < max_years - 1:
                beprd[last_valid + 1:] = beprd[last_valid]

            self._data_cache[cache_key] = beprd
            result[risk_cd] = beprd

        return result

    # ------------------------------------------------------------------
    # 면책기간 (IP_R_INVLD_TRMNAT)
    # ------------------------------------------------------------------
    def load_invld_months(self, ctr: ContractInfo) -> Dict[str, int]:
        """위험률코드별 면책기간(월)."""
        key = (ctr.prod_cd, ctr.cls_cd, ctr.cov_cd)
        if key in self._cache_invld:
            return self._cache_invld[key]
        rows = self.conn.execute(f"""
            SELECT RSK_RT_CD, INVLD_TRMNAT_PRD_TPCD, INVLD_TRMNAT_PRD_CNT
            FROM {self.p}IP_R_INVLD_TRMNAT
            WHERE PROD_CD = ? AND CLS_CD = ? AND COV_CD = ?
        """, [ctr.prod_cd, ctr.cls_cd, ctr.cov_cd]).fetchall()

        result = {}
        for r in rows:
            rsk_cd = str(r[0])
            tpcd = str(r[1]) if r[1] else "M"
            cnt = int(r[2]) if r[2] else 0
            months = cnt * 12 if tpcd == "Y" else cnt
            result[rsk_cd] = months
        self._cache_invld[key] = result
        return result

    # ------------------------------------------------------------------
    # 중복제거용 exit flag (IP_R_COV_RSKRT_C, IP_R_BNFT_RSKRT_C)
    # ------------------------------------------------------------------
    def load_exit_flags(
        self, ctr: ContractInfo, risks: List[RiskInfo]
    ) -> Dict[str, Dict[str, int]]:
        """위험률코드별 탈퇴 플래그 로드.

        IP_R_COV_RSKRT_C / IP_R_BNFT_RSKRT_C의 모든 코드를 포함
        (C1/C2 등 IP_R_RSKRT_C에 없는 코드도 포함).

        Returns:
            {rsk_cd: {"rsvamt": 0|1, "bnft": 0|1, "pyexsp": 0|1}}
        """
        key = (ctr.prod_cd, ctr.cls_cd, ctr.cov_cd)
        if key in self._cache_exit_flags:
            # 캐시 결과 + risks에 있는 코드 보강
            cached = self._cache_exit_flags[key]
            rsk_cds = [r.risk_cd for r in risks]
            result = {cd: cached.get(cd, {"rsvamt": 0, "bnft": 0, "pyexsp": 0}) for cd in rsk_cds}
            return result

        rsk_cds = [r.risk_cd for r in risks]
        flags = {cd: {"rsvamt": 0, "bnft": 0, "pyexsp": 0} for cd in rsk_cds}

        # IP_R_COV_RSKRT_C → RSVAMT + PYEXSP
        rows = self.conn.execute(f"""
            SELECT RSK_RT_CD, RSVAMT_DEFRY_DRPO_RSKRT_YN, PYEXSP_DRPO_RSKRT_YN
            FROM {self.p}IP_R_COV_RSKRT_C
            WHERE PROD_CD = ? AND CLS_CD = ? AND COV_CD = ?
        """, [ctr.prod_cd, ctr.cls_cd, ctr.cov_cd]).fetchall()
        for r in rows:
            cd = str(r[0])
            if cd not in flags:
                flags[cd] = {"rsvamt": 0, "bnft": 0, "pyexsp": 0}
            flags[cd]["rsvamt"] = int(r[1]) if r[1] is not None else 0
            flags[cd]["pyexsp"] = int(r[2]) if r[2] is not None else 0

        # IP_R_BNFT_RSKRT_C → BNFT
        # 규칙: BNFT_DRPO_RSKRT_YN=1 AND MIN(BNFT_RSKRT_YN)=0
        #   → BNFT_RSKRT_YN=0인 BNFT_NO가 있어야 탈퇴위험으로 인정
        #   → 전부 BNFT_RSKRT_YN=1이면 급부산출 전용 (탈퇴에 미사용)
        rows = self.conn.execute(f"""
            SELECT RSK_RT_CD,
                   MAX(BNFT_DRPO_RSKRT_YN) as BNFT_YN,
                   MIN(BNFT_RSKRT_YN) as MIN_RSKRT_YN
            FROM {self.p}IP_R_BNFT_RSKRT_C
            WHERE PROD_CD = ? AND CLS_CD = ? AND COV_CD = ?
            GROUP BY RSK_RT_CD
        """, [ctr.prod_cd, ctr.cls_cd, ctr.cov_cd]).fetchall()
        for r in rows:
            cd = str(r[0])
            if cd not in flags:
                flags[cd] = {"rsvamt": 0, "bnft": 0, "pyexsp": 0}
            bnft_drpo = int(r[1]) if r[1] is not None else 0
            min_rskrt_yn = int(r[2]) if r[2] is not None else 1
            # BNFT exit = DRPO=1 AND 최소 하나의 BNFT_NO에서 RSKRT_YN=0
            flags[cd]["bnft"] = 1 if (bnft_drpo == 1 and min_rskrt_yn == 0) else 0

        self._cache_exit_flags[key] = flags
        return flags

    # ------------------------------------------------------------------
    # 추가 위험코드 로드 (C1/C2 등 IP_R_RSKRT_C에 없는 코드)
    # ------------------------------------------------------------------
    def load_extra_risk_codes(
        self, ctr: ContractInfo, existing_cds: set
    ) -> List[RiskInfo]:
        """IP_R_COV_RSKRT_C / IP_R_BNFT_RSKRT_C에만 있는 추가 위험코드 로드.

        C1/C2 등 IP_R_RSKRT_C에는 없지만 exit_flags에 필요한 코드를
        IR_RSKRT_CHR에서 메타 정보와 함께 로드한다.

        Args:
            ctr: 계약 정보
            existing_cds: 이미 load_risk_codes로 로드된 코드 집합

        Returns:
            추가 RiskInfo 리스트 (unique rsk_grp_no 부여)
        """
        cache_key = (ctr.prod_cd, ctr.cls_cd, ctr.cov_cd, frozenset(existing_cds))
        if cache_key in self._cache_extra_risks:
            return self._cache_extra_risks[cache_key]

        extra_cds = set()
        for tbl in ("IP_R_COV_RSKRT_C", "IP_R_BNFT_RSKRT_C"):
            rows = self.conn.execute(f"""
                SELECT DISTINCT RSK_RT_CD FROM {self.p}{tbl}
                WHERE PROD_CD = ? AND CLS_CD = ? AND COV_CD = ?
            """, [ctr.prod_cd, ctr.cls_cd, ctr.cov_cd]).fetchall()
            for r in rows:
                cd = str(r[0])
                if cd not in existing_cds:
                    extra_cds.add(cd)

        if not extra_cds:
            return []

        result = []
        for rsk_cd in sorted(extra_cds):
            chr_row = self.conn.execute(f"""
                SELECT RSK_RT_CHR_CD, MM_TRF_WAY_CD, DEAD_RT_DVCD,
                       RSK_RT_DIV_VAL_DEF_CD1, RSK_RT_DIV_VAL_DEF_CD2,
                       RSK_RT_DIV_VAL_DEF_CD3, RSK_RT_DIV_VAL_DEF_CD4,
                       RSK_RT_DIV_VAL_DEF_CD5, RSK_RT_DIV_VAL_DEF_CD6,
                       RSK_RT_DIV_VAL_DEF_CD7, RSK_RT_DIV_VAL_DEF_CD8,
                       RSK_RT_DIV_VAL_DEF_CD9, RSK_RT_DIV_VAL_DEF_CD10
                FROM {self.p}IR_RSKRT_CHR
                WHERE RSK_RT_CD = ?
            """, [rsk_cd]).fetchone()

            if chr_row:
                def_cds = [str(chr_row[i]) if chr_row[i] is not None else None
                           for i in range(3, 13)]
                result.append(RiskInfo(
                    risk_cd=rsk_cd,
                    chr_cd=str(chr_row[0]) if chr_row[0] else "A",
                    mm_trf_way_cd=int(chr_row[1]) if chr_row[1] else 1,
                    dead_rt_dvcd=int(chr_row[2]) if chr_row[2] is not None else 0,
                    rsk_grp_no=f"__{rsk_cd}__",
                    def_cds=def_cds,
                ))
            else:
                # IR_RSKRT_CHR에 없으면 사망위험 가상코드로 추가
                result.append(RiskInfo(
                    risk_cd=rsk_cd,
                    chr_cd="A", mm_trf_way_cd=1,
                    dead_rt_dvcd=0, rsk_grp_no=f"__{rsk_cd}__",
                ))

        self._cache_extra_risks[cache_key] = result
        return result

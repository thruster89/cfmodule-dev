"""
ETL: Legacy DB(VSOLN2.vdb) → v2 DuckDB 스키마 변환

사용법:
    from cf_module.v2.etl import migrate_legacy_db
    migrate_legacy_db("legacy.vdb", "v2.duckdb")

변환 핵심:
1. flat 테이블 → 정규화 star schema
2. 복합키 18개 필드 → assm_profile 사전계산
3. 3개 테이블 JOIN → map_contract_risk에 exit 플래그 통합
4. 문자열 rate dict → fact_mortality/lapse/skew에 행 단위 저장
"""

import sqlite3
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

from cf_module.v2.schema import create_schema


def migrate_legacy_db(
    legacy_path: str,
    v2_path: str,
    infrc_seq: int = 1,
    assm_ym: str = "202306",
    assm_grp_id: str = "AGR06328",
    idno_start: Optional[int] = None,
    idno_end: Optional[int] = None,
) -> duckdb.DuckDBPyConnection:
    """Legacy SQLite DB를 v2 DuckDB로 변환한다.

    Args:
        legacy_path: 원본 VSOLN2.vdb 경로
        v2_path: 출력 v2 DuckDB 경로
        infrc_seq: 계약 구분자 (기본 1)
        assm_ym: 가정년월
        assm_grp_id: 가정 그룹 ID
        idno_start: 계약 ID 범위 시작 (None=전체)
        idno_end: 계약 ID 범위 끝 (None=전체)

    Returns:
        v2 DuckDB 커넥션
    """
    # 1. v2 스키마 생성
    v2 = create_schema(v2_path)

    # 2. Legacy DB를 DuckDB에 attach
    v2.execute(f"ATTACH '{legacy_path}' AS legacy (TYPE sqlite, READ_ONLY)")

    # 3. 단계별 마이그레이션
    _migrate_products(v2)
    _migrate_risks(v2, infrc_seq, idno_start, idno_end, assm_ym)
    _migrate_contracts(v2, infrc_seq, idno_start, idno_end, assm_grp_id)
    _migrate_contract_risk_map(v2, infrc_seq, idno_start, idno_end)
    _migrate_mortality_rates(v2, infrc_seq, idno_start, idno_end)
    _migrate_lapse_rates(v2, infrc_seq, idno_start, idno_end)
    _migrate_skew_rates(v2, infrc_seq, idno_start, idno_end)
    _migrate_beprd_rates(v2, infrc_seq, idno_start, idno_end)
    _migrate_reserves(v2, infrc_seq, idno_start, idno_end)
    _update_profile_counts(v2)

    v2.execute("DETACH legacy")
    return v2


def _migrate_products(v2: duckdb.DuckDBPyConnection):
    """상품 차원 테이블 생성."""
    v2.execute("""
        INSERT OR IGNORE INTO dim_product (prod_cd, prod_grp, cls_cd, cov_cd)
        SELECT DISTINCT
            PROD_CD,
            COALESCE(PROD_CD, ''),
            CLS_CD,
            COV_CD
        FROM legacy.II_INFRC
    """)


def _migrate_risks(
    v2: duckdb.DuckDBPyConnection,
    infrc_seq: int,
    idno_start: Optional[int],
    idno_end: Optional[int],
    assm_ym: str,
):
    """위험률 차원 테이블: IR_RSKRT_CHR + IP_R_RSKRT_C 통합.

    IP_R_RSKRT_C는 PROD_CD/CLS_CD/COV_CD로 조인 (상품 수준 테이블).
    대상 계약의 상품 범위에 해당하는 위험률만 마이그레이션.
    """
    where_clause = _build_idno_where("i", infrc_seq, idno_start, idno_end)

    v2.execute(f"""
        INSERT OR IGNORE INTO dim_risk (
            risk_cd, risk_name, chr_cd, is_death, risk_group,
            mm_trf_way_cd, revi_ym
        )
        SELECT DISTINCT
            chr.RSK_RT_CD,
            chr.RSK_RT_NM,
            chr.RSK_RT_CHR_CD,
            CASE WHEN CAST(chr.DEAD_RT_DVCD AS VARCHAR) = '0' THEN true ELSE false END,
            COALESCE(CAST(grp.RSK_GRP_NO AS VARCHAR), '0'),
            COALESCE(CAST(chr.MM_TRF_WAY_CD AS INTEGER), 1),
            chr.REVI_YM
        FROM legacy.IR_RSKRT_CHR chr
        LEFT JOIN (
            SELECT DISTINCT p.RSK_RT_CD, p.RSK_GRP_NO
            FROM legacy.IP_R_RSKRT_C p
            WHERE EXISTS (
                SELECT 1 FROM legacy.II_INFRC i
                WHERE {where_clause}
                  AND i.PROD_CD = p.PROD_CD
                  AND i.CLS_CD = p.CLS_CD
                  AND i.COV_CD = p.COV_CD
            )
        ) grp ON chr.RSK_RT_CD = grp.RSK_RT_CD
        WHERE chr.RSK_RT_CD IN (
            SELECT DISTINCT p.RSK_RT_CD
            FROM legacy.IP_R_RSKRT_C p
            WHERE EXISTS (
                SELECT 1 FROM legacy.II_INFRC i
                WHERE {where_clause}
                  AND i.PROD_CD = p.PROD_CD
                  AND i.CLS_CD = p.CLS_CD
                  AND i.COV_CD = p.COV_CD
            )
        )
    """)


def _migrate_contracts(
    v2: duckdb.DuckDBPyConnection,
    infrc_seq: int,
    idno_start: Optional[int],
    idno_end: Optional[int],
    assm_grp_id: str,
):
    """계약 마스터 + assm_profile 사전계산.

    II_INFRC 실제 컬럼 매핑:
    - CTR_DT → CTR_YM (앞 6자리)
    - GRNTPT_GPREM → premium
    - GRNTPT_JOIN_AMT → sum_assured
    - SEX_CD 없음 → RSK_RT_DIV_VAL1 또는 빈 문자열
    """
    where_clause = _build_idno_where("i", infrc_seq, idno_start, idno_end)

    # 먼저 원본 데이터를 DataFrame으로 읽기
    df = v2.execute(f"""
        SELECT
            i.INFRC_IDNO AS contract_id,
            i.PROD_CD AS prod_cd,
            COALESCE(i.PROD_CD, '') AS prod_grp,
            COALESCE(i.RSK_RT_DIV_VAL1, '') AS sex,
            COALESCE(CAST(i.ISRD_JOIN_AGE AS INTEGER), 0) AS entry_age,
            COALESCE(SUBSTRING(CAST(i.CTR_DT AS VARCHAR), 1, 6), '') AS ctr_ym,
            COALESCE(CAST(i.INSTRM_YYCNT AS INTEGER), 0) AS bterm,
            COALESCE(CAST(i.PAYPR_YYCNT AS INTEGER), 0) AS pterm,
            COALESCE(CAST(i.GRNTPT_GPREM AS DOUBLE), 0) AS premium,
            COALESCE(CAST(i.GRNTPT_JOIN_AMT AS DOUBLE), 0) AS sum_assured,
            COALESCE(i.CLS_CD, '') AS cls_cd,
            COALESCE(i.COV_CD, '') AS cov_cd,
            COALESCE(i.ASSM_DIV_VAL1, '') AS assm_div_val1,
            COALESCE(i.ASSM_DIV_VAL2, '') AS assm_div_val2,
            COALESCE(i.ASSM_DIV_VAL3, '') AS assm_div_val3,
            COALESCE(i.ASSM_DIV_VAL4, '') AS assm_div_val4,
            COALESCE(i.ASSM_DIV_VAL5, '') AS assm_div_val5,
            COALESCE(i.ASSM_DIV_VAL6, '') AS assm_div_val6,
            COALESCE(i.ASSM_DIV_VAL7, '') AS assm_div_val7,
            COALESCE(i.ASSM_DIV_VAL8, '') AS assm_div_val8,
            COALESCE(i.ASSM_DIV_VAL9, '') AS assm_div_val9,
            COALESCE(i.ASSM_DIV_VAL10, '') AS assm_div_val10,
            COALESCE(i.ASSM_DIV_VAL11, '') AS assm_div_val11,
            COALESCE(i.ASSM_DIV_VAL12, '') AS assm_div_val12,
            COALESCE(i.ASSM_DIV_VAL13, '') AS assm_div_val13,
            COALESCE(i.ASSM_DIV_VAL14, '') AS assm_div_val14,
            COALESCE(i.ASSM_DIV_VAL15, '') AS assm_div_val15
        FROM legacy.II_INFRC i
        WHERE {where_clause}
    """).fetchdf()

    if df.empty:
        return

    # assm_profile 계산: prod_grp + 가정에 영향을 주는 차원들의 조합
    # 같은 프로파일 = 같은 위험률/해지율/스큐 테이블
    profile_cols = ["prod_grp", "sex", "cls_cd", "cov_cd"]
    df["assm_profile"] = df[profile_cols].apply(
        lambda row: "|".join(str(v) for v in row), axis=1
    )

    # dim_contract 컬럼 순서에 맞게 정렬 (assm_profile이 assm_div_val* 앞)
    ordered_cols = [
        "contract_id", "prod_cd", "prod_grp", "sex", "entry_age", "ctr_ym",
        "bterm", "pterm", "premium", "sum_assured", "cls_cd", "cov_cd",
        "assm_profile",
    ] + [f"assm_div_val{i}" for i in range(1, 16)]
    df = df[ordered_cols]

    v2.execute("INSERT INTO dim_contract SELECT * FROM df")

    # 프로파일 메타 등록
    profiles = df.groupby("assm_profile").agg(
        prod_grp=("prod_grp", "first"),
        n_contracts=("contract_id", "count"),
    ).reset_index()
    profiles["description"] = profiles.apply(
        lambda r: f"{r['prod_grp']} ({r['n_contracts']}건)", axis=1
    )

    v2.execute("""
        INSERT OR IGNORE INTO meta_assm_profile (assm_profile, prod_grp, description, n_contracts)
        SELECT assm_profile, prod_grp, description, n_contracts FROM profiles
    """)


def _migrate_contract_risk_map(
    v2: duckdb.DuckDBPyConnection,
    infrc_seq: int,
    idno_start: Optional[int],
    idno_end: Optional[int],
):
    """계약-위험률 매핑: 상품 수준 3개 테이블을 계약 수준 1개로 통합.

    실제 DB 구조:
    - IP_R_RSKRT_C: PROD_CD/CLS_CD/COV_CD/RSK_RT_CD (전체 위험률 목록)
    - IP_R_COV_RSKRT_C: PROD_CD/CLS_CD/COV_CD/RSK_RT_CD + RSVAMT_DEFRY_DRPO_RSKRT_YN, PYEXSP_DRPO_RSKRT_YN
    - IP_R_BNFT_RSKRT_C: PROD_CD/CLS_CD/COV_CD/BNFT_NO/RSK_RT_CD + BNFT_DRPO_RSKRT_YN, BNFT_RSKRT_YN

    IP_R_COV_RSKRT_C에 없는 위험률(111018 등)은 RSVAMT/PYEXSP=0으로 처리.
    IP_R_BNFT_RSKRT_C는 BNFT_NO가 여러 행 → MAX로 집계 (1개라도 Y면 Y).
    """
    where_clause = _build_idno_where("i", infrc_seq, idno_start, idno_end)

    v2.execute(f"""
        INSERT OR IGNORE INTO map_contract_risk (
            contract_id, risk_cd, is_exit_rsv, is_exit_bnft, is_exit_pay, is_bnft_risk
        )
        SELECT
            i.INFRC_IDNO,
            rsk.RSK_RT_CD,
            COALESCE(cov.RSVAMT_DEFRY_DRPO_RSKRT_YN = 1, false),
            COALESCE(bnft.BNFT_DRPO_YN, false),
            COALESCE(cov.PYEXSP_DRPO_RSKRT_YN = 1, false),
            COALESCE(bnft.BNFT_RSKRT_YN, false)
        FROM legacy.II_INFRC i
        JOIN legacy.IP_R_RSKRT_C rsk
            ON i.PROD_CD = rsk.PROD_CD
            AND i.CLS_CD = rsk.CLS_CD
            AND i.COV_CD = rsk.COV_CD
        LEFT JOIN legacy.IP_R_COV_RSKRT_C cov
            ON rsk.PROD_CD = cov.PROD_CD
            AND rsk.CLS_CD = cov.CLS_CD
            AND rsk.COV_CD = cov.COV_CD
            AND rsk.RSK_RT_CD = cov.RSK_RT_CD
        LEFT JOIN (
            SELECT
                PROD_CD, CLS_CD, COV_CD, RSK_RT_CD,
                MAX(CASE WHEN BNFT_DRPO_RSKRT_YN = 1 THEN true ELSE false END) AS BNFT_DRPO_YN,
                MAX(CASE WHEN BNFT_RSKRT_YN = 1 THEN true ELSE false END) AS BNFT_RSKRT_YN
            FROM legacy.IP_R_BNFT_RSKRT_C
            GROUP BY PROD_CD, CLS_CD, COV_CD, RSK_RT_CD
        ) bnft
            ON rsk.PROD_CD = bnft.PROD_CD
            AND rsk.CLS_CD = bnft.CLS_CD
            AND rsk.COV_CD = bnft.COV_CD
            AND rsk.RSK_RT_CD = bnft.RSK_RT_CD
        WHERE {where_clause}
    """)


def _migrate_mortality_rates(
    v2: duckdb.DuckDBPyConnection,
    infrc_seq: int,
    idno_start: Optional[int],
    idno_end: Optional[int],
):
    """위험률 값: IR_RSKRT_VAL → fact_mortality (risk_cd, age, rate).

    RSK_RT_DIV_VAL_DEF_CD 기반 키매칭 수행:
    - IR_RSKRT_CHR의 DEF_CD가 II_INFRC의 RSK_RT_DIV_VAL 위치를 지정
    - DIV_VAL_DEF_CD_MAP: {"49": 0, "21": 1, "22": 2, ...}
    - 계약의 RSK_RT_DIV_VAL[mapped_idx]를 rate 테이블 필터에 사용
    """
    # DEF_CD → RSK_RT_DIV_VAL 인덱스 매핑 (v1 assm_key_builder.py와 동일)
    DEF_CD_MAP = {"49": 0, "21": 1, "22": 2, "03": 3, "70": 4, "71": 5}

    where_clause = _build_idno_where("i", infrc_seq, idno_start, idno_end)

    # 1. 대상 계약의 RSK_RT_DIV_VAL 읽기 (대표 1건)
    contract_divs = v2.execute(f"""
        SELECT
            RSK_RT_DIV_VAL1, RSK_RT_DIV_VAL2, RSK_RT_DIV_VAL3,
            RSK_RT_DIV_VAL4, RSK_RT_DIV_VAL5, RSK_RT_DIV_VAL6,
            RSK_RT_DIV_VAL7, RSK_RT_DIV_VAL8, RSK_RT_DIV_VAL9,
            RSK_RT_DIV_VAL10
        FROM legacy.II_INFRC i
        WHERE {where_clause}
        LIMIT 1
    """).fetchone()

    if contract_divs is None:
        return

    div_vals = [str(v) if v is not None else "" for v in contract_divs]

    # 2. 대상 위험률코드의 CHR 메타데이터 읽기
    risk_cds_df = v2.execute(f"""
        SELECT DISTINCT p.RSK_RT_CD
        FROM legacy.IP_R_RSKRT_C p
        WHERE EXISTS (
            SELECT 1 FROM legacy.II_INFRC i
            WHERE {where_clause}
              AND i.PROD_CD = p.PROD_CD AND i.CLS_CD = p.CLS_CD AND i.COV_CD = p.COV_CD
        )
    """).fetchdf()

    if risk_cds_df.empty:
        return

    risk_cds = risk_cds_df["RSK_RT_CD"].values

    # 3. 각 위험률코드별로 키매칭된 rate 추출
    for risk_cd in risk_cds:
        chr_row = v2.execute("""
            SELECT RSK_RT_CHR_CD, REVI_YM,
                   RSK_RT_DIV_VAL_DEF_CD1, RSK_RT_DIV_VAL_DEF_CD2,
                   RSK_RT_DIV_VAL_DEF_CD3, RSK_RT_DIV_VAL_DEF_CD4,
                   RSK_RT_DIV_VAL_DEF_CD5, RSK_RT_DIV_VAL_DEF_CD6,
                   RSK_RT_DIV_VAL_DEF_CD7, RSK_RT_DIV_VAL_DEF_CD8,
                   RSK_RT_DIV_VAL_DEF_CD9, RSK_RT_DIV_VAL_DEF_CD10
            FROM legacy.IR_RSKRT_CHR
            WHERE RSK_RT_CD = ?
        """, [str(risk_cd)]).fetchone()

        if chr_row is None:
            continue

        chr_cd = chr_row[0]
        revi_ym = chr_row[1]
        def_cds = chr_row[2:]  # 10개 DEF_CD

        # DIV_VAL 필터 구축: DEF_CD → 계약의 DIV_VAL 매핑
        div_filters = []
        for pos, def_cd in enumerate(def_cds):
            if def_cd is None or str(def_cd).strip() in ("", "None"):
                break
            def_cd_str = str(def_cd).strip()
            mapped_idx = DEF_CD_MAP.get(def_cd_str)
            if mapped_idx is not None and mapped_idx < len(div_vals):
                col_name = f"RSK_RT_DIV_VAL{pos + 1}"
                div_filters.append((col_name, div_vals[mapped_idx]))

        # WHERE 절 구축
        where_parts = [f"RSK_RT_CD = '{risk_cd}'", f"REVI_YM = '{revi_ym}'"]
        for col_name, val in div_filters:
            where_parts.append(f"{col_name} = '{val}'")
        where_sql = " AND ".join(where_parts)

        # rate 추출
        rates_df = v2.execute(f"""
            SELECT
                CASE WHEN '{chr_cd}' = 'S' THEN -1 ELSE COALESCE(CAST(AGE AS SMALLINT), -1) END AS age,
                CAST(RSK_RT AS DOUBLE) AS rate
            FROM legacy.IR_RSKRT_VAL
            WHERE {where_sql}
        """).fetchdf()

        if rates_df.empty:
            continue

        # fact_mortality에 삽입
        rates_df.insert(0, "risk_cd", str(risk_cd))
        v2.execute("INSERT OR IGNORE INTO fact_mortality SELECT risk_cd, age, rate FROM rates_df")


def _resolve_assm_filter(
    v2: duckdb.DuckDBPyConnection,
    kdcd: int,
    contract_assm_divs: list,
    prod_cd: str,
    cls_cd: str,
) -> dict:
    """드라이버 기반 키매칭: KDCD별 ASSM_GRP_CD 필터 생성.

    IA_M_ASSM_DRIV → IA_M_ETC_ASSM_KEY → IA_M_PROD_GRP 참조하여
    계약의 ASSM_DIV_VAL → 해당 가정 테이블의 필터 조건을 구축.

    Returns:
        {"file_id": str, "prod_grp": str, "grp_filters": {col: val}, "has_rsk": bool}
    """
    # 1. 드라이버 조회
    drv = v2.execute("""
        SELECT ASSM_FILE_ID, MAP_CRIT_SRNO, PROD_DIV_GRP_CD_YN, COV_DIV_GRP_CD_YN,
               RSK_CAT_VAL_YN,
               ASSM_DIV_VAL1_YN, ASSM_DIV_VAL2_YN, ASSM_DIV_VAL3_YN,
               ASSM_DIV_VAL4_YN, ASSM_DIV_VAL5_YN, ASSM_DIV_VAL6_YN,
               ASSM_DIV_VAL7_YN, ASSM_DIV_VAL8_YN, ASSM_DIV_VAL9_YN,
               ASSM_DIV_VAL10_YN, ASSM_DIV_VAL11_YN, ASSM_DIV_VAL12_YN,
               ASSM_DIV_VAL13_YN, ASSM_DIV_VAL14_YN, ASSM_DIV_VAL15_YN
        FROM legacy.IA_M_ASSM_DRIV
        WHERE ASSM_KDCD = ?
    """, [kdcd]).fetchone()

    if drv is None:
        return None

    file_id = drv[0]
    map_crit = drv[1]
    prod_yn = drv[2]
    cov_yn = drv[3]
    rsk_yn = drv[4]
    div_yns = drv[5:]  # 15개

    # 2. 상품그룹 매핑
    prod_grp = "^"
    if prod_yn:
        pg_row = v2.execute("""
            SELECT PROD_GRP_CD FROM legacy.IA_M_PROD_GRP
            WHERE ASSM_KDCD = ? AND ASSM_FILE_ID = ? AND MAP_CRIT_SRNO = ?
              AND PROD_CD = ? AND CLS_CD = ?
        """, [kdcd, file_id, map_crit, prod_cd, cls_cd]).fetchone()
        if pg_row is None:
            # CLS_CD 없이 재시도
            pg_row = v2.execute("""
                SELECT PROD_GRP_CD FROM legacy.IA_M_PROD_GRP
                WHERE ASSM_KDCD = ? AND ASSM_FILE_ID = ? AND MAP_CRIT_SRNO = ?
                  AND PROD_CD = ?
                ORDER BY CLS_CD LIMIT 1
            """, [kdcd, file_id, map_crit, prod_cd]).fetchone()
        if pg_row:
            prod_grp = str(pg_row[0])

    # 3. ASSM_GRP_CD 매핑: 활성 차원별로 ETC_ASSM_KEY 조회
    grp_filters = {}
    for dim_idx in range(15):
        if div_yns[dim_idx]:
            key_no = dim_idx + 1
            div_val = str(contract_assm_divs[dim_idx]) if dim_idx < len(contract_assm_divs) else "^"

            grp_row = v2.execute("""
                SELECT ASSM_GRP_CD FROM legacy.IA_M_ETC_ASSM_KEY
                WHERE ASSM_KDCD = ? AND ASSM_FILE_ID = ? AND MAP_CRIT_SRNO = ?
                  AND ASSM_KEY_NO = ? AND ASSM_DIV_VAL = ?
            """, [kdcd, file_id, map_crit, key_no, div_val]).fetchone()

            col_name = f"ASSM_GRP_CD{key_no}"
            grp_filters[col_name] = str(grp_row[0]) if grp_row else "^"

    return {
        "file_id": file_id,
        "prod_grp": prod_grp,
        "grp_filters": grp_filters,
        "has_rsk": bool(rsk_yn),
    }


def _migrate_lapse_rates(
    v2: duckdb.DuckDBPyConnection,
    infrc_seq: int,
    idno_start: Optional[int],
    idno_end: Optional[int],
):
    """해지율: 드라이버 기반 키매칭 → fact_lapse.

    IA_M_ASSM_DRIV(KDCD=12) → IA_M_ETC_ASSM_KEY → IA_T_TRMNAT_RT 매칭.
    assm_profile별로 루프하여 각 프로파일의 해지율을 저장.
    """
    where_clause = _build_idno_where("i", infrc_seq, idno_start, idno_end)

    # 프로파일별 대표 계약 1건 조회
    profiles = v2.execute(f"""
        SELECT d.assm_profile, FIRST(i.PROD_CD), FIRST(i.CLS_CD),
               FIRST(i.ASSM_DIV_VAL1), FIRST(i.ASSM_DIV_VAL2), FIRST(i.ASSM_DIV_VAL3),
               FIRST(i.ASSM_DIV_VAL4), FIRST(i.ASSM_DIV_VAL5), FIRST(i.ASSM_DIV_VAL6),
               FIRST(i.ASSM_DIV_VAL7), FIRST(i.ASSM_DIV_VAL8), FIRST(i.ASSM_DIV_VAL9),
               FIRST(i.ASSM_DIV_VAL10), FIRST(i.ASSM_DIV_VAL11), FIRST(i.ASSM_DIV_VAL12),
               FIRST(i.ASSM_DIV_VAL13), FIRST(i.ASSM_DIV_VAL14), FIRST(i.ASSM_DIV_VAL15)
        FROM legacy.II_INFRC i
        JOIN dim_contract d ON d.contract_id = i.INFRC_IDNO
        WHERE {where_clause}
        GROUP BY d.assm_profile
    """).fetchdf()

    all_rows = []

    for _, prof_row in profiles.iterrows():
        assm_profile = prof_row.iloc[0]
        prod_cd, cls_cd = prof_row.iloc[1], prof_row.iloc[2]
        assm_divs = [str(v) if v is not None else "^"
                     for v in prof_row.iloc[3:].values]

        resolved = _resolve_assm_filter(v2, 12, assm_divs, prod_cd, cls_cd)
        if resolved is None:
            continue

        where_parts = [f"ASSM_FILE_ID = '{resolved['file_id']}'"]
        where_parts.append(f"PROD_GRP_CD = '{resolved['prod_grp']}'")
        for col, val in resolved["grp_filters"].items():
            where_parts.append(f"{col} = '{val}'")
        where_sql = " AND ".join(where_parts)

        df = v2.execute(f"""
            SELECT * FROM legacy.IA_T_TRMNAT_RT WHERE {where_sql}
        """).fetchdf()

        if df.empty:
            continue

        val_cols = sorted(
            [c for c in df.columns if c.startswith("TRMNAT_RT") and c[9:].isdigit()],
            key=lambda x: int(x[9:])
        )
        for _, row in df.iterrows():
            pay_dvcd = int(row.get("PAY_DVCD", 1))
            pay_phase = "paying" if pay_dvcd == 1 else "paidup"
            for yr_idx, col in enumerate(val_cols):
                annual_rate = float(row[col]) if pd.notna(row[col]) else 0.0
                if annual_rate != 0:
                    for m in range(12):
                        month_dur = yr_idx * 12 + m + 1
                        all_rows.append({
                            "assm_profile": assm_profile,
                            "pay_phase": pay_phase,
                            "duration": month_dur,
                            "rate": annual_rate,
                        })

    if all_rows:
        lapse_df = pd.DataFrame(all_rows)
        v2.execute("INSERT INTO fact_lapse SELECT * FROM lapse_df")


def _migrate_skew_rates(
    v2: duckdb.DuckDBPyConnection,
    infrc_seq: int,
    idno_start: Optional[int],
    idno_end: Optional[int],
):
    """스큐: 드라이버 기반 키매칭 → fact_skew. assm_profile별 루프."""
    where_clause = _build_idno_where("i", infrc_seq, idno_start, idno_end)

    profiles = v2.execute(f"""
        SELECT d.assm_profile, FIRST(i.PROD_CD), FIRST(i.CLS_CD),
               FIRST(i.ASSM_DIV_VAL1), FIRST(i.ASSM_DIV_VAL2), FIRST(i.ASSM_DIV_VAL3),
               FIRST(i.ASSM_DIV_VAL4), FIRST(i.ASSM_DIV_VAL5), FIRST(i.ASSM_DIV_VAL6),
               FIRST(i.ASSM_DIV_VAL7), FIRST(i.ASSM_DIV_VAL8), FIRST(i.ASSM_DIV_VAL9),
               FIRST(i.ASSM_DIV_VAL10), FIRST(i.ASSM_DIV_VAL11), FIRST(i.ASSM_DIV_VAL12),
               FIRST(i.ASSM_DIV_VAL13), FIRST(i.ASSM_DIV_VAL14), FIRST(i.ASSM_DIV_VAL15)
        FROM legacy.II_INFRC i
        JOIN dim_contract d ON d.contract_id = i.INFRC_IDNO
        WHERE {where_clause}
        GROUP BY d.assm_profile
    """).fetchdf()

    all_rows = []

    for _, prof_row in profiles.iterrows():
        assm_profile = prof_row.iloc[0]
        prod_cd, cls_cd = prof_row.iloc[1], prof_row.iloc[2]
        assm_divs = [str(v) if v is not None else "^"
                     for v in prof_row.iloc[3:].values]

        resolved = _resolve_assm_filter(v2, 13, assm_divs, prod_cd, cls_cd)
        if resolved is None:
            continue

        where_parts = [f"ASSM_FILE_ID = '{resolved['file_id']}'"]
        where_parts.append(f"PROD_GRP_CD = '{resolved['prod_grp']}'")
        for col, val in resolved["grp_filters"].items():
            where_parts.append(f"{col} = '{val}'")
        where_sql = " AND ".join(where_parts)

        df = v2.execute(f"SELECT * FROM legacy.IA_T_SKEW WHERE {where_sql}").fetchdf()

        if df.empty:
            continue

        val_cols = sorted(
            [c for c in df.columns if c.startswith("SKEW") and c[4:].isdigit()],
            key=lambda x: int(x[4:])
        )
        for _, row in df.iterrows():
            for month_idx, col in enumerate(val_cols):
                factor = float(row[col]) if pd.notna(row[col]) else 1.0 / 12.0
                all_rows.append({
                    "assm_profile": assm_profile,
                    "duration": month_idx + 1,
                    "factor": factor,
                })

    if all_rows:
        skew_df = pd.DataFrame(all_rows)
        v2.execute("INSERT INTO fact_skew SELECT * FROM skew_df")


def _migrate_beprd_rates(
    v2: duckdb.DuckDBPyConnection,
    infrc_seq: int,
    idno_start: Optional[int],
    idno_end: Optional[int],
):
    """BEPRD: 드라이버 기반 키매칭 → fact_beprd. assm_profile별 루프.

    BEPRD는 RSK_CAT_VAL로 위험률코드를 구분:
    - IA_M_RSK_CAT: RSK_RT_CD → RSK_CAT_VAL 매핑
    - IA_R_BEPRD_DEFRY_RT: RSK_CAT_VAL + 기타 키 → 경과년도별 지급률
    """
    where_clause = _build_idno_where("i", infrc_seq, idno_start, idno_end)

    profiles = v2.execute(f"""
        SELECT d.assm_profile, FIRST(i.PROD_CD), FIRST(i.CLS_CD),
               FIRST(i.ASSM_DIV_VAL1), FIRST(i.ASSM_DIV_VAL2), FIRST(i.ASSM_DIV_VAL3),
               FIRST(i.ASSM_DIV_VAL4), FIRST(i.ASSM_DIV_VAL5), FIRST(i.ASSM_DIV_VAL6),
               FIRST(i.ASSM_DIV_VAL7), FIRST(i.ASSM_DIV_VAL8), FIRST(i.ASSM_DIV_VAL9),
               FIRST(i.ASSM_DIV_VAL10), FIRST(i.ASSM_DIV_VAL11), FIRST(i.ASSM_DIV_VAL12),
               FIRST(i.ASSM_DIV_VAL13), FIRST(i.ASSM_DIV_VAL14), FIRST(i.ASSM_DIV_VAL15)
        FROM legacy.II_INFRC i
        JOIN dim_contract d ON d.contract_id = i.INFRC_IDNO
        WHERE {where_clause}
        GROUP BY d.assm_profile
    """).fetchdf()

    all_rows = []

    for _, prof_row in profiles.iterrows():
        assm_profile = prof_row.iloc[0]
        prod_cd, cls_cd = prof_row.iloc[1], prof_row.iloc[2]
        assm_divs = [str(v) if v is not None else "^"
                     for v in prof_row.iloc[3:].values]

        resolved = _resolve_assm_filter(v2, 9, assm_divs, prod_cd, cls_cd)
        if resolved is None:
            continue

        base_parts = [f"ASSM_FILE_ID = '{resolved['file_id']}'"]
        base_parts.append(f"PROD_GRP_CD = '{resolved['prod_grp']}'")
        for col, val in resolved["grp_filters"].items():
            base_parts.append(f"{col} = '{val}'")

        # 이 프로파일에 속한 위험률코드 목록
        risk_cds = v2.execute("""
            SELECT DISTINCT m.risk_cd FROM map_contract_risk m
            JOIN dim_contract d ON d.contract_id = m.contract_id
            WHERE d.assm_profile = ?
        """, [assm_profile]).fetchdf()["risk_cd"].values

        for risk_cd in risk_cds:
            rsk_cat = v2.execute("""
                SELECT RSK_CAT_VAL FROM legacy.IA_M_RSK_CAT
                WHERE ASSM_KDCD = 9 AND ASSM_FILE_ID = ? AND RSK_RT_CD = ?
            """, [resolved["file_id"], str(risk_cd)]).fetchone()

            if rsk_cat is None:
                continue

            rsk_cat_val = str(rsk_cat[0])
            where_parts = base_parts + [f"RSK_CAT_VAL = '{rsk_cat_val}'"]
            where_sql = " AND ".join(where_parts)

            try:
                df = v2.execute(f"SELECT * FROM legacy.IA_R_BEPRD_DEFRY_RT WHERE {where_sql}").fetchdf()
            except Exception:
                continue

            if df.empty:
                continue

            val_cols = [c for c in df.columns if c.startswith("BEPRD_DEFRY_RT")]
            for _, row in df.iterrows():
                for dur_idx, col in enumerate(val_cols):
                    rate = float(row[col]) if pd.notna(row[col]) else 1.0
                    all_rows.append({
                        "assm_profile": assm_profile,
                        "risk_cd": str(risk_cd),
                        "duration": dur_idx + 1,
                        "rate": rate,
                    })

    if all_rows:
        beprd_df = pd.DataFrame(all_rows)
        v2.execute("INSERT OR IGNORE INTO fact_beprd SELECT * FROM beprd_df")


def _migrate_reserves(
    v2: duckdb.DuckDBPyConnection,
    infrc_seq: int,
    idno_start: Optional[int],
    idno_end: Optional[int],
):
    """준비금: II_RSVAMT_BAS → fact_reserve."""
    where_clause = _build_idno_where("r", infrc_seq, idno_start, idno_end)

    try:
        df = v2.execute(f"""
            SELECT * FROM legacy.II_RSVAMT_BAS r
            WHERE {where_clause}
        """).fetchdf()
    except Exception:
        return

    if df.empty:
        return

    # vend_rsvamt1 ~ vend_rsvamt120 같은 컬럼을 unpivot
    id_col = None
    for c in df.columns:
        if "IDNO" in c.upper():
            id_col = c
            break

    if id_col is None:
        return

    rsvamt_cols = [c for c in df.columns if "RSVAMT" in c.upper() and c != id_col]
    rows = []
    for _, row in df.iterrows():
        contract_id = int(row[id_col])
        for yr_idx, col in enumerate(rsvamt_cols):
            val = row[col]
            if pd.notna(val) and val != 0:
                rows.append({
                    "contract_id": contract_id,
                    "duration_year": yr_idx + 1,
                    "reserve_amt": float(val),
                })

    if rows:
        reserve_df = pd.DataFrame(rows)
        v2.execute("INSERT INTO fact_reserve SELECT * FROM reserve_df")


def _update_profile_counts(v2: duckdb.DuckDBPyConnection):
    """프로파일별 계약 수 갱신."""
    v2.execute("""
        UPDATE meta_assm_profile
        SET n_contracts = (
            SELECT COUNT(*)
            FROM dim_contract c
            WHERE c.assm_profile = meta_assm_profile.assm_profile
        )
    """)


def _build_idno_where(
    alias: str,
    infrc_seq: int,
    idno_start: Optional[int],
    idno_end: Optional[int],
) -> str:
    """INFRC_IDNO 범위 WHERE 절 생성."""
    parts = [f"{alias}.INFRC_SEQ = {infrc_seq}"]
    if idno_start is not None:
        parts.append(f"{alias}.INFRC_IDNO >= {idno_start}")
    if idno_end is not None:
        parts.append(f"{alias}.INFRC_IDNO <= {idno_end}")
    return " AND ".join(parts) if parts else "1=1"

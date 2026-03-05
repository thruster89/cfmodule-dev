"""
DuckDB 스키마 정의 — 정규화된 Star Schema

설계 원칙:
1. 차원 테이블: 이름 있는 컬럼, 비즈니스 의미 명확
2. 팩트 테이블: FK 기반 조인, SQL에서 필터링
3. assm_profile: 가정 조회 좌표를 사전 계산하여 런타임 키 조립 제거
4. 메타데이터 통합: 3개 테이블 JOIN → 1개 행
"""

import duckdb

# ============================================================
# DDL 정의
# ============================================================

SCHEMA_DDL = """
-- ============================================================
-- A. 계약/상품 차원 (Dimension Tables)
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_product (
    prod_cd       VARCHAR PRIMARY KEY,
    prod_grp      VARCHAR NOT NULL,
    cls_cd        VARCHAR,
    cov_cd        VARCHAR,
    prod_name     VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_contract (
    contract_id   INTEGER PRIMARY KEY,   -- INFRC_IDNO
    prod_cd       VARCHAR NOT NULL,
    prod_grp      VARCHAR NOT NULL,      -- 비정규화: JOIN 절약
    sex           VARCHAR,               -- 'M'/'F'
    entry_age     SMALLINT NOT NULL,
    ctr_ym        VARCHAR,               -- 계약년월
    bterm         SMALLINT NOT NULL,     -- 보장기간(년)
    pterm         SMALLINT NOT NULL,     -- 납입기간(년)
    premium       DOUBLE,
    sum_assured   DOUBLE,
    cls_cd        VARCHAR,
    cov_cd        VARCHAR,
    -- 가정 조회용 사전계산 필드
    assm_profile  VARCHAR NOT NULL,      -- 그룹핑 키
    -- 원본 가정 분류값 (비정규화)
    assm_div_val1  VARCHAR, assm_div_val2  VARCHAR, assm_div_val3  VARCHAR,
    assm_div_val4  VARCHAR, assm_div_val5  VARCHAR, assm_div_val6  VARCHAR,
    assm_div_val7  VARCHAR, assm_div_val8  VARCHAR, assm_div_val9  VARCHAR,
    assm_div_val10 VARCHAR, assm_div_val11 VARCHAR, assm_div_val12 VARCHAR,
    assm_div_val13 VARCHAR, assm_div_val14 VARCHAR, assm_div_val15 VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_contract_profile
    ON dim_contract(assm_profile);

CREATE INDEX IF NOT EXISTS idx_contract_prod
    ON dim_contract(prod_cd);


-- ============================================================
-- B. 위험률 차원 (Risk Dimension)
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_risk (
    risk_cd       VARCHAR PRIMARY KEY,
    risk_name     VARCHAR,
    chr_cd        VARCHAR NOT NULL,      -- 'S'(단일률) / 'A'(연령별)
    is_death      BOOLEAN NOT NULL,      -- true=사망위험, false=비사망
    risk_group    VARCHAR,               -- 동일위험그룹 번호
    mm_trf_way_cd SMALLINT DEFAULT 1,    -- 월변환방식: 1=1-(1-q)^(1/12), 2=q/12
    revi_ym       VARCHAR                -- 적용 리비전 년월
);


-- ============================================================
-- C. 계약-위험률 매핑 (탈퇴 조건 통합)
-- ============================================================

CREATE TABLE IF NOT EXISTS map_contract_risk (
    contract_id   INTEGER NOT NULL,
    risk_cd       VARCHAR NOT NULL,
    is_exit_rsv   BOOLEAN DEFAULT FALSE, -- 준비금 탈퇴 (RSVAMT_DEFRY_DRPO_RSKRT_YN)
    is_exit_bnft  BOOLEAN DEFAULT FALSE, -- 급부 탈퇴 (BNFT_DRPO_RSKRT_YN)
    is_exit_pay   BOOLEAN DEFAULT FALSE, -- 납면 탈퇴 (PYEXSP_DRPO_RSKRT_YN)
    is_bnft_risk  BOOLEAN DEFAULT FALSE, -- 급부 위험률 여부 (BNFT_RSKRT_YN)
    PRIMARY KEY (contract_id, risk_cd)
);

CREATE INDEX IF NOT EXISTS idx_map_cr_contract
    ON map_contract_risk(contract_id);


-- ============================================================
-- D. 팩트: 위험률 값
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_mortality (
    risk_cd       VARCHAR NOT NULL,
    age           SMALLINT NOT NULL,     -- S타입: -1 (단일), A타입: 실제 연령
    rate          DOUBLE NOT NULL,
    PRIMARY KEY (risk_cd, age)
);

CREATE INDEX IF NOT EXISTS idx_mort_risk
    ON fact_mortality(risk_cd);


-- ============================================================
-- E. 팩트: 해지율
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_lapse (
    assm_profile  VARCHAR NOT NULL,
    pay_phase     VARCHAR NOT NULL,      -- 'paying' / 'paidup'
    duration      SMALLINT NOT NULL,     -- 경과월 (1-based)
    rate          DOUBLE NOT NULL,
    PRIMARY KEY (assm_profile, pay_phase, duration)
);

CREATE INDEX IF NOT EXISTS idx_lapse_profile
    ON fact_lapse(assm_profile, pay_phase);


-- ============================================================
-- F. 팩트: 스큐
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_skew (
    assm_profile  VARCHAR NOT NULL,
    duration      SMALLINT NOT NULL,     -- 경과월 (1-based)
    factor        DOUBLE NOT NULL,
    PRIMARY KEY (assm_profile, duration)
);


-- ============================================================
-- G. 팩트: BEPRD 경과년도별 지급률
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_beprd (
    assm_profile  VARCHAR NOT NULL,
    risk_cd       VARCHAR NOT NULL,
    duration      SMALLINT NOT NULL,     -- 경과월
    rate          DOUBLE NOT NULL,
    PRIMARY KEY (assm_profile, risk_cd, duration)
);


-- ============================================================
-- H. 팩트: 준비금 (계약별)
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_reserve (
    contract_id   INTEGER NOT NULL,
    duration_year SMALLINT NOT NULL,     -- 경과연도
    reserve_amt   DOUBLE NOT NULL,
    PRIMARY KEY (contract_id, duration_year)
);


-- ============================================================
-- I. 메타: 가정 프로파일 정의
-- ============================================================

CREATE TABLE IF NOT EXISTS meta_assm_profile (
    assm_profile  VARCHAR PRIMARY KEY,
    prod_grp      VARCHAR NOT NULL,
    description   VARCHAR,               -- 사람이 읽을 수 있는 설명
    n_contracts   INTEGER DEFAULT 0      -- 소속 계약 수 (ETL 시 갱신)
);


-- ============================================================
-- J. 팩트: 금리 커브
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_interest (
    curve_id      VARCHAR NOT NULL,      -- 'BASE', 'KICS_UP', etc.
    term_month    SMALLINT NOT NULL,
    spot_rate     DOUBLE NOT NULL,
    forward_rate  DOUBLE,
    PRIMARY KEY (curve_id, term_month)
);
"""


def create_schema(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """DuckDB에 v2 스키마를 생성하고 커넥션을 반환한다."""
    conn = duckdb.connect(db_path)
    conn.execute("BEGIN TRANSACTION")
    for stmt in SCHEMA_DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.execute("COMMIT")
    return conn


def attach_readonly(db_path: str) -> duckdb.DuckDBPyConnection:
    """기존 v2 DB를 읽기 전용으로 연다."""
    conn = duckdb.connect(db_path, read_only=True)
    return conn

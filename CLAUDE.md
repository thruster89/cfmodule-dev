# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

- 모든 응답은 한국어로 작성할 것

## Project Overview

보험 Cash Flow 프로젝션 엔진. `duckdb_transform.duckdb`(75개 raw 테이블)에서 보험상품 가정을 직접 읽어, 계리 계산(사망률, 해약률, 스큐, 중복제거 위험률)을 수행하여 CF를 산출한다.

**핵심 원칙**: v2 ETL(v2/etl.py)을 거치지 않고, input DB(duckdb_transform.duckdb)의 raw 테이블에서 legacy 드라이버 기반 키매칭을 직접 수행하여 PROJ_O2.vdb와 동일한 결과 테이블을 구현한다. v2 Star Schema(fact 테이블)는 사용하지 않음.

## Key Dependencies

- Python 3, pandas, numpy (core)
- sqlite3 (built-in, v1 primary database)
- duckdb (raw 테이블 직접 조회)

## Running

```bash
# OD_RSK_RT / OD_LAPSE_RT 단건 검증 (IDNO=760397)
python test_rsk_lapse_rt.py

# OD_LAPSE_RT 전건 검증 (42,001건)
python test_lapse_rt_all.py

# OD_TRAD_PV 전건 검증 (42,000건 × 43컬럼)
python test_trad_pv_all.py

# OD_TBL_BN 전건 검증 (32,962건 × 72,797 BNFT)
python test_tbl_bn.py

# v1 단건 테스트 (기본 IDNO=8833)
python test_single_contract.py

# v1 특정 계약 테스트
python test_single_contract.py --idno 760397

# v1 메인 파이프라인
python -m cf_module.main --sample 500
```

## Architecture — 산출 모듈 (현재 활성)

### OD_RSK_RT (위험률 산출) — 42,001건 ALL PASS

| 파일 | 역할 |
|------|------|
| `data/rsk_lapse_loader.py` | RawAssumptionLoader: 드라이버 키매칭 (15차원 ASSM_DIV_VAL) |
| `calc/tbl_rsk_rt.py` | compute_rsk_rt: 원율 → BEPRD → 월변환 → 면책 적용 |
| `test_rsk_lapse_rt.py` | PROJ_O2.vdb 기대값 비교 (7 risk × 9 col × 345 steps) |

**핵심 공식**:
- 경과년수: `ceil(months/12)` = `max((duration_months - 1) // 12 + 1, 1)`
- 연령: `entry_age + max(duration_months - 1, 0) // 12`
- BF_YR = RSK_RT × LOSS_RT × MTH_EFECT × BEPRD × TRD × ARVL
- 월변환(mm_trf_way_cd=1): `1 - (1 - bf_yr)^(1/12)`
- 월변환(mm_trf_way_cd=2): `bf_yr / 12`
- 면책: `duration_months < invld_mm` → 0

### OD_LAPSE_RT (해지율 산출) — 42,001건 × 3컬럼 ALL PASS

| 파일 | 역할 |
|------|------|
| `data/rsk_lapse_loader.py` | load_lapse_rates: KDCD=12(납입중)/13(납입후) 드라이버 매칭 |
| `calc/tbl_lapse_rt.py` | compute_lapse_rt: 납입중/납입후 선택 → SKEW(1/12) → APLY |
| `test_lapse_rt_all.py` | duckdb OD_LAPSE_RT 기대값 42,001건 전건 비교 |

**핵심 규칙**:
- SKEW = 1/12 상수 (IA_T_SKEW 월별 가중치와 무관 — OD_LAPSE_RT는 단순 월환산만)
- 납입중/납입후 전환: **MAIN_PAYPR_YYCNT**(주계약 납입기간) 기준, PAYPR_YYCNT(특약) 아님
- `is_paying = duration_months <= main_pterm_months` (pterm월 포함 = 납입중)
- 납입후 경과: `paidup_months = duration_months - main_pterm_months` (납입후 기준 상대 경과)
- Paying 연장: 마지막 데이터 컬럼(RT20) 값 유지 (RT20=0이면 0 연장)
- 만기도래: `elapsed >= bterm_months` → TRMNAT=0, SKEW=0, APLY=0 (9건)
- APLY = `1 - (1 - TRMNAT_RT)^(1/12)`

### OD_TRAD_PV (전통형 현가) — 42,000건 × 43컬럼 ALL PASS

| 파일 | 역할 |
|------|------|
| `calc/trad_pv.py` | 7단계 산출 (보험료→PRPD→이율→적립금→환급금→KICS→약관대출) |
| `data/trad_pv_loader.py` | TradPVDataCache: 12개 테이블 일괄 로드 |
| `pipeline.py` | run_trad_pv_pipeline: 배치 처리 (98개 PROD_CD/CLS_CD 그룹) |
| `test_trad_pv_all.py` | 전건 검증 |

### OD_TBL_BN (급부 테이블) — 32,962건 × 16컬럼 Phase 1 PASS

| 파일 | 역할 |
|------|------|
| `calc/tbl_bn.py` | Per-BNFT 독립 exit rate → tpx → 탈퇴자/발생건 → PYAMT |
| `data/bn_loader.py` | 6개 참조테이블 로드 |
| `test_tbl_bn.py` | Phase 1 검증 |

### RawAssumptionLoader 드라이버 키매칭

**데이터 흐름**:
```
II_INFRC (계약 정보, ASSM_DIV_VAL1~15, RSK_RT_DIV_VAL1~10)
  → IA_M_ASSM_DRIV (활성 차원: ASSM_DIV_VAL_YN 0=무시, 1=ETC매핑, 2=원본유지)
    → IA_M_ETC_ASSM_KEY (ASSM_GRP_CD 매핑)
    → IA_M_PROD_GRP (상품그룹 매핑)
  → IA_T_TRMNAT (해지율), IR_RSKRT_VAL (위험률), IA_T_SKEW (스큐) 등
```

**ContractInfo 주요 필드**:
- `pterm_yy`: PAYPR_YYCNT (특약 납입기간)
- `main_pterm_yy`: MAIN_PAYPR_YYCNT (주계약 납입기간) — **해지율 paying/paidup 판정용**
- `bterm_yy`: INSTRM_YYCNT (보험기간)

## Architecture (ver1.1) — 기존 v1 프레임워크

### Data Flow

```
CFConfig (config.py)
  → DataReader (io/reader.py) + queries/*.sql (60개 개별 SQL)
    → ModelPointSet (data/model_point.py)         ← II_INFRC
    → AssumptionLoader (data/assumptions.py)      ← IR_RSKRT_*, IA_*, IP_R_*
      → AssumptionKeyBuilder (data/assm_key_builder.py)  ← 복합키 매칭
    → TimingResult (calc/timing.py)
    → DecrementResult (calc/decrement.py)         ← 중복제거 위험률
    → run_projection (projection/projector.py)    ← CF 산출
```

### 중복제거 위험률 (calc/decrement.py)

**공식**: `q'ᵢ = qᵢ × (1 - Σⱼ(qⱼ × Cᵢⱼ) / 2)`

C행렬 조건 (Cᵢⱼ = 0):
1. 자기자신 (i = j)
2. 동일위험그룹 (RSK_GRP_NO 동일)
3. j가 사망위험 (DEAD_RT_DVCD = 0)

**탈퇴 위험률 분류**:
- CTR (유지자): is_exit = RSVAMT_YN | BNFT_YN
- PAY (납입자): is_exit = RSVAMT_YN | BNFT_YN | PYEXSP_YN

### 검증 데이터

- `duckdb_transform.duckdb`: 42,001건 raw 데이터 + OD_RSK_RT, OD_LAPSE_RT 기대값
- `PROJ_O2.vdb` (`C:\python\cf_module\PROJ_O2.vdb`): INFRC_IDNO=760397 기대값
  - OD_RSK_RT, OD_LAPSE_RT, OD_TBL_MN (CTR_* / PAY_* 컬럼, 345행)

## Current Status

### 완료 — 전건 검증 PASS

- [x] **OD_RSK_RT**: 42,001건 × 9컬럼 ALL PASS (드라이버 키매칭 → 원율 → BEPRD → 월변환 → 면책)
- [x] **OD_LAPSE_RT**: 42,001건 × 3컬럼 ALL PASS (SKEW=1/12, MAIN_PAYPR 기준 전환)
- [x] **OD_TRAD_PV**: 42,000건 × 43컬럼 ALL PASS (보험료→PRPD→이율→적립금→환급금→KICS→대출)
- [x] **OD_TBL_BN Phase 1**: 32,962건 × 16컬럼 (15/16 PASS, PYAMT float precision 1.49e-6)
- [x] **OD_TBL_MN**: 42,001건 × 18컬럼 ALL PASS (OD_RSK_RT/OD_LAPSE_RT 입력 → 중복제거 → tpx → 탈퇴자 분해)

### 미구현 (다음 작업)

- [ ] **BN Phase 2**: Per-BNFT 독립 중복제거 엔진 (driver 기반 가정 매칭)
- [ ] **BN Phase 2**: DEFRY_RT/PRTT_RT/GRADIN_RT 자체 산출
- [ ] **Premium/Benefit/Expense/Reserve/Discount/PV 단계**: projector.py 8단계 중 3~8단계

---

<details>
<summary>v2 아키텍처 (현재 비활성 — 참고용)</summary>

v2는 초기 프로토타입으로, 현재는 raw 테이블 직접 조회 방식으로 전환됨. 코드는 `v2/` 디렉토리에 남아있으나 활성 개발 대상 아님.

| 모듈 | 역할 |
|------|------|
| `v2/schema.py` | DuckDB Star Schema DDL |
| `v2/etl.py` | Legacy SQLite → DuckDB 변환 |
| `v2/engine.py` | 중복제거 + tpx + 탈퇴자 분해 |

테스트: `test_v2_real.py`, `test_v2_vs_proj_o2.py`, `python -m cf_module.v2.test_v2`

</details>

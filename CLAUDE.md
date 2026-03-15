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

# OD_TBL_BN Phase 2 전건 검증 (32,963건 × 72,798 BNFT)
python test_tbl_bn_phase2.py --all

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
| `test_trad_pv_all.py` | 전건 검증 |

### OD_TBL_BN (급부 테이블) — 32,963건 × 72,798 BNFT (16/16 PASS)

| 파일 | 역할 |
|------|------|
| `calc/tbl_bn.py` | Per-BNFT 독립 C행렬 dedup → tpx → 탈퇴자/발생건 → PYAMT |
| `data/bn_loader.py` | 9개 참조테이블 로드 (risk_meta, rsvamt_flags, expct_inrt_prtt) |
| `test_tbl_bn_phase2.py` | Phase 2 전건 검증 (raw OD_RSK_RT/OD_LAPSE_RT 입력) |

**PRTT_RT 산출 (ann_due 기반)**:
- `PRTT_RT = DEFRY_RT × ann_due(TOT, rate, CYC) × (CD==1 ? v² : 1)`
- CD=1: EXPCT_INRT×v²(2개월이연), CD=3: AVG_PUBANO_INRT, CD=4: min(EXPCT, AVG_PUBANO)

### OD_EXP (사업비) — 13/16 PASS (760397 단건)

| 파일 | 역할 |
|------|------|
| `calc/exp.py` | DRVR별 사업비 산출 (GPREM/절대금액/BNFT/LOAN/KICS 기반) |
| `data/exp_loader.py` | IA_E_ACQSEXP_DR/MNTEXP_DR/LOSS_SVYEXP + 드라이버 키매칭 |

### OD_CF (캐시플로우) — 21/26 PASS (760397 단건)

| 파일 | 역할 |
|------|------|
| `calc/cf.py` | MN×TRAD_PV(보험료) + BN(보험금) + MN×EXP(사업비) 결합 |

### OD_DC_RT (할인율) — 2/2 ALL PASS

| 파일 | 역할 |
|------|------|
| `calc/dc_rt.py` | IE_DC_RT 커브 → v=(1+r)^(-1/12) → 기시(TRMO)/기말(TRME) 누적곱 |

### OD_PVCF (현가 캐시플로우) — 22/27 PASS

| 파일 | 역할 |
|------|------|
| `calc/pvcf.py` | CF × DC_RT (기시: 보험료/사업비, 기말: 보험금/해약) |

### OP_BEL (최선추정부채) — 21/26 PASS

| 파일 | 역할 |
|------|------|
| `calc/bel.py` | PVCF 전 시점 합산 → BEL |

### 전체 파이프라인

| 파일 | 역할 |
|------|------|
| `pipeline.py` | run_pipeline: RSK→LAPSE→MN→PV→BN 전체 배치 |
| `run.py` | run_single/run_batch: 단건/다건 파이프라인 + CSV 출력 |

**파이프라인 체인**: `RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN → EXP → CF → DC_RT → PVCF → BEL`

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

- [x] **OD_RSK_RT**: 42,001건 × 9컬럼 ALL PASS
- [x] **OD_LAPSE_RT**: 42,001건 × 3컬럼 ALL PASS
- [x] **OD_TRAD_PV**: 42,000건 × 43컬럼 ALL PASS
- [x] **OD_TBL_MN**: 42,001건 × 18컬럼 ALL PASS
- [x] **OD_TBL_BN**: 16/16컬럼 PASS (PRTT_RT CD=1/3/4 구현 완료)
- [x] **OD_DC_RT**: 2/2컬럼 ALL PASS

### 완료 — 760397 단건 검증 (구조 완성)

- [x] **OD_EXP**: 13/16 PASS (DRVR=1/2/4 구현, DRVR=9/10 CNCTTP 이슈)
- [x] **OD_CF**: 21/26 PASS (MN×PV+BN+EXP 결합)
- [x] **OD_PVCF**: 22/27 PASS (CF×DC_RT 기시/기말)
- [x] **OP_BEL**: 21/26 PASS (PVCF 합산)

### 잔여 이슈 (FAIL 항목)

- [ ] **EXP DRVR=9/10**: CNCTTP_ACUMAMT_KICS가 LTRMNAT_TMRFND에 물려있어 불일치 → MNT KD3/5/15
- [ ] **ACQS KD2**: t=14 근처 rate 전환 시 ~251 차이
- [ ] **CF TMRFND_INPAY/PYEX**: 공식 미확인
- [ ] **전건 테스트**: EXP~BEL 단계 전건 검증 미실시 (760397 단건만)

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

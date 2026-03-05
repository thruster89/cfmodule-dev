# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

- 모든 응답은 한국어로 작성할 것

## Project Overview

보험 Cash Flow 프로젝션 엔진. SQLite DB에서 보험상품 가정을 추출하고, 계리 계산(사망률, 해약률, 스큐, 중복제거 위험률)을 수행하여 CF를 산출한다.

## Key Dependencies

- Python 3, pandas, numpy (core)
- sqlite3 (built-in, v1 primary database)
- duckdb (v2 Star Schema)

## Running

```bash
# v1 단건 테스트 (기본 IDNO=8833)
python test_single_contract.py

# v1 특정 계약 테스트
python test_single_contract.py --idno 760397

# v1 메인 파이프라인
python -m cf_module.main --sample 500

# v2 실제 DB 연동 테스트 (t=1 검증)
python test_v2_real.py

# v2 OD_TBL_MN 전체 비교 (344개월 × 12항목)
python test_v2_vs_proj_o2.py
python test_v2_vs_proj_o2.py --csv   # 비교 결과 CSV 저장
python test_v2_vs_proj_o2.py --keep-db  # DuckDB 파일 보존

# v2 합성 데이터 단위 테스트
python -m cf_module.v2.test_v2
```

## Architecture (ver1.1)

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

### Key Modules

| 모듈 | 역할 |
|------|------|
| `config.py` | CFConfig, RunsetParams, DBConfig 등 설정 |
| `io/reader.py` | DataReader — sqlite3/DuckDB, named params (:name) |
| `data/model_point.py` | MP 로딩 (II_INFRC) |
| `data/assumptions.py` | 가정 로딩: MortalityTable, LapseTable, SkewTable + 중복제거 메타 |
| `data/assm_key_builder.py` | 복합키 빌더 (위험률, 해지율, 스큐, BEPRD) |
| `calc/timing.py` | 시간축: duration, age, is_in_force |
| `calc/decrement.py` | 탈퇴율: qx/wx 매핑 + 중복제거 + tpx 산출 |
| `projection/projector.py` | 8단계 프로젝션 (timing→decrement→premium→benefit→expense→reserve→discount→PV) |

### 중복제거 위험률 (calc/decrement.py)

**공식**: `q'ᵢ = qᵢ × (1 - Σⱼ(qⱼ × Cᵢⱼ) / 2)`

C행렬 조건 (Cᵢⱼ = 0):
1. 자기자신 (i = j)
2. 동일위험그룹 (RSK_GRP_NO 동일)
3. j가 사망위험 (DEAD_RT_DVCD = 0)

**DB 메타데이터 출처**:
- `IR_RSKRT_CHR` → DEAD_RT_DVCD (0=사망, 1=비사망)
- `IP_R_RSKRT_C` → RSK_GRP_NO (동일위험그룹)
- `IP_R_COV_RSKRT_C` → RSVAMT_DEFRY_DRPO_RSKRT_YN (준비금탈퇴), PYEXSP_DRPO_RSKRT_YN (납입면제탈퇴)
- `IP_R_BNFT_RSKRT_C` → BNFT_DRPO_RSKRT_YN (급부탈퇴)

**탈퇴 위험률 분류**:
- CTR (유지자): is_exit = RSVAMT_YN | BNFT_YN
- PAY (납입자): is_exit = RSVAMT_YN | BNFT_YN | PYEXSP_YN ← **v1/v2 모두 구현 완료**

### queries/ 디렉토리

60개 `.sql` 파일. `:named` 파라미터 스타일. DataReader가 디렉토리에서 자동 로딩.

### 검증 데이터

- `PROJ_O2.vdb` (`C:\python\cf_module\PROJ_O2.vdb`): INFRC_IDNO=760397 기대값
  - OD_RSK_RT, OD_LAPSE_RT, OD_TBL_MN (CTR_* / PAY_* 컬럼, 345행)
- Legacy DB: `C:\Users\thrus\Downloads\VSOLN2\VSOLN2.vdb`

## Architecture (v2)

### Data Flow

```
VSOLN2.vdb (Legacy SQLite)
  → migrate_legacy_db() (v2/etl.py)     ← 드라이버 기반 키매칭
    → DuckDB Star Schema (v2/schema.py)
      - dim_contract, dim_risk, map_contract_risk
      - fact_mortality, fact_lapse, fact_skew, fact_beprd, fact_reserve
  → load_group_assumptions() (v2/engine.py)  ← GroupAssumptions
  → project_group() (v2/engine.py)           ← ProjectionResultV2
```

### v2 Key Modules

| 모듈 | 역할 |
|------|------|
| `v2/schema.py` | DuckDB Star Schema DDL (10개 테이블) |
| `v2/etl.py` | Legacy SQLite → DuckDB 변환 (키매칭, 드라이버 해석) |
| `v2/engine.py` | 중복제거 + CTR/PAY tpx + 탈퇴자 분해 프로젝션 |

### v2 프로젝션 엔진 핵심 로직 (engine.py)

**단계**:
1. 위험률(qx_raw) 로드: 연령별/단일률 + BEPRD 적용 (연도 단위)
2. 해지율(wx_raw) 로드: paying/paidup 구분 + 월률 변환 `1-(1-q)^(1/12)`
3. 중복제거: CTR (RSVAMT|BNFT) / PAY (RSVAMT|BNFT|PYEXSP)
4. tpx 산출: CTR tpx, PAY pay_tpx (cumprod)
5. 탈퇴자 분해: d_rsvamt, d_bnft (CTR tpx_bot 기준), d_pyexsp (PAY pay_tpx_bot 기준)

**주요 공식**:
- Duration years: `month // 12 + 1` (PROJ_O2 기준)
- Age: `entry_age + month // 12`
- Elapsed: `CLOS_YM - ctr_ym + 1` (계약월 포함)
- is_paying: `duration_months < pterm_months` (strict less)
- Paidup lapse: 납입후 시작 기준 상대 경과 (계약 시작 아님)
- BEPRD: 데이터 범위 초과 시 마지막 값 연장

### OD_TBL_MN 비교 항목 (test_v2_vs_proj_o2.py)

| # | 항목 | v2 출처 | 기준 |
|---|------|---------|------|
| 1 | CTR_TRME (유지자수) | tpx | CTR tpx |
| 2 | CTR_TRMNAT_RT (해약률) | wx_ctr | CTR 중복제거 |
| 3 | CTR_RSVAMT_DRPO_RSKRT | d_rsvamt / tpx_bot | CTR tpx_bot |
| 4 | CTR_BNFT_DRPO_RSKRT | d_bnft / tpx_bot | CTR tpx_bot |
| 5 | CTR_TRMPSN (해약자수) | tpx_bot × wx_ctr | CTR tpx_bot |
| 6 | CTR_RSVAMT_DRPSN | d_rsvamt | CTR tpx_bot |
| 7 | CTR_BNFT_DRPSN | d_bnft | CTR tpx_bot |
| 8 | PAY_TRME (납입자수) | pay_tpx | PAY tpx |
| 9 | PAY_TRMNAT_RT (납입해약률) | wx_pay | PAY 중복제거 |
| 10 | PYEXSP_DRPO_RSKRT | d_pyexsp / pay_tpx_bot | PAY pay_tpx_bot |
| 11 | PAY_TRMPSN (납입해약자수) | pay_tpx_bot × wx_pay | PAY pay_tpx_bot |
| 12 | PYEXSP_DRPSN | d_pyexsp | PAY pay_tpx_bot |

## Current Status (ver1.1)

### 완료 (v1)

- [x] queries.json → queries/ 마이그레이션 (60개 SQL)
- [x] DataReader: 디렉토리 로딩 + named params + DuckDB
- [x] ContractParams → RunsetParams 리네이밍
- [x] 위험률 DB키매칭: MortalityKeyBuilder + BEPRD + 월변환
- [x] 해약률 DB키매칭: AssumptionKeyBuilder + 납입기간/납입후
- [x] 스큐 DB키매칭
- [x] CTR 중복제거 위험률 (RSVAMT + BNFT)
- [x] CTR tpx (유지자수) 검증: 8833, 760397 모두 PROJ_O2.vdb 일치
- [x] pyexsp_drpo_yn 로딩 (IP_R_COV_RSKRT_C에서)
- [x] DEBUG 로깅 (timing, decrement, assumptions, projector)
- [x] test_single_contract.py --idno 인자
- [x] PAY 중복제거 + 납입자수(pay_tpx): v1 decrement.py 구현
- [x] debug CSV: 02_decrement.csv에 PAY 컬럼 포함
- [x] PAY 결과 검증 (v1): test_single_contract.py에 760397 PROJ_O2.vdb 기대값 비교 추가

### 완료 (v2)

- [x] v2 Star Schema 설계: 10개 테이블 (dim 3 + fact 5 + meta 1 + interest 1)
- [x] v2 ETL: VSOLN2.vdb → DuckDB (드라이버 기반 키매칭 — 위험률/해지율/스큐/BEPRD/준비금)
- [x] v2 engine: 중복제거 + CTR/PAY tpx + 탈퇴자 분해 프로젝션
- [x] 해지율 월변환: `1-(1-q)^(1/12)` (skew 미적용, v1과 동일)
- [x] BEPRD 연도 단위 인덱싱 + 마지막 값 연장
- [x] Paidup lapse: 납입후 시작 기준 상대 경과
- [x] d_pyexsp: PAY pay_tpx_bot 기준 (CTR tpx_bot 아님)
- [x] v2 합성 데이터 단위 테스트: test_v2.py (C행렬, einsum, 파이프라인)
- [x] v2 실제 DB 검증 (t=1): test_v2_real.py — PROJ_O2.vdb 기대값 전항목 PASS (diff < 1e-10)
- [x] v2 OD_TBL_MN 전체 비교: test_v2_vs_proj_o2.py — **344개월 × 12항목 전부 PASS (diff < 1e-15)**

### 미구현 (다음 작업)

- [ ] **v2 오케스트레이터**: 대량 계약 청크 처리 + 병렬화
- [ ] **Premium/Benefit/Expense/Reserve/Discount/PV 단계**: projector.py 8단계 중 3~8단계

### PAY 구현 참고사항

760397 계약 기준:
- 7개 위험률코드 중 CTR exit: 2개 (111018-BNFT, 212015-RSVAMT)
- PAY exit: 7개 전부 (PYEXSP 5개 추가: 241208, 121108, 241171, 211024, 221139)
- PAY C행렬: 8×8 (wx + 7 risks), CTR보다 큰 adjustment → pay_tpx < ctr_tpx
- 기대값: PAY_TRMNAT_RT=0.0063239731, PYEXSP_DRPO_RSKRT=0.0010430155, PAY_TRME(t=1)=0.9925024849

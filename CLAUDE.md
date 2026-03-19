# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

- 모든 응답은 한국어로 작성할 것

## Project Overview

보험 Cash Flow 프로젝션 엔진. `duckdb_transform.duckdb`(75개 raw 테이블)에서 보험상품 가정을 직접 읽어, 계리 계산(사망률, 해약률, 스큐, 중복제거 위험률)을 수행하여 CF → PV → BEL을 산출한다.

**핵심 원칙**: v2 ETL(v2/etl.py)을 거치지 않고, input DB의 raw 테이블에서 legacy 드라이버 기반 키매칭을 직접 수행하여 모델 산출값과 동일한 결과를 구현한다.

## Key Dependencies

- Python 3, pandas, numpy (core)
- duckdb (raw 테이블 직접 조회)

## Running

```bash
# 단건 전체 파이프라인 (BEL만 출력 — 기본)
python -m cf_module.run --idno 760397

# 특정 단계까지만
python -m cf_module.run --idno 760397 --table MN
python -m cf_module.run --idno 760397 --table CF

# 디버그: 전체 중간테이블 CSV 출력 + 요약
python -m cf_module.run --idno 760397 --debug

# 디버그: 특정 테이블만 선택 저장
python -m cf_module.run --idno 760397 --debug --save RSK_RT,CF,BEL

# 전건 배치 BEL 산출 (42,001건 → DuckDB)
python run_batch_bel.py
python run_batch_bel.py --n 1000 -o result.duckdb

# 전건 검증 테스트
python test_bel_prem_base.py --all        # OP_BEL PREM_BASE 42,001건
python test_lapse_rt_all.py               # OD_LAPSE_RT 42,001건
python test_trad_pv_all.py                # OD_TRAD_PV 42,000건
python test_tbl_bn_phase2.py --all        # OD_TBL_BN 32,963건
```

## Architecture — 파이프라인 (10단계)

```
RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN → EXP → CF → DC_RT → PVCF → BEL
```

### 단계별 모듈

| 단계 | calc/ | data/ | 입력 | 전건 PASS율 |
|------|-------|-------|------|-----------|
| RSK_RT | tbl_rsk_rt.py | rsk_lapse_loader.py | IR_RSKRT_VAL, IA_M_ASSM_DRIV | 100% |
| LAPSE_RT | tbl_lapse_rt.py | rsk_lapse_loader.py | IA_T_TRMNAT, IA_T_SKEW | 100% |
| TBL_MN | tbl_mn.py | — | RSK_RT, LAPSE_RT | 100% |
| TRAD_PV | trad_pv.py | trad_pv_loader.py | TBL_MN, II_RSVAMT_BAS, IP_P_* | 100% |
| TBL_BN | tbl_bn.py | bn_loader.py | RSK_RT, LAPSE_RT, TRAD_PV | 100% (16컬럼) |
| EXP | exp.py | exp_loader.py | IA_E_ACQSEXP/MNTEXP/LOSS, TRAD_PV | 79.7% |
| CF | cf.py | — | TBL_MN, TRAD_PV, TBL_BN, EXP | 83.8% |
| DC_RT | dc_rt.py | — | IE_DC_RT | 100% |
| PVCF | pvcf.py | — | CF, DC_RT | — |
| BEL | bel.py | — | PVCF | — |

### 파이프라인 실행 (run.py)

| 파일 | 역할 |
|------|------|
| `run.py` | run_single: 단건 전체 파이프라인 + CSV 출력 |
| `run_batch_bel.py` | 전건 BEL 배치 산출 → DuckDB 출력 |
| `pipeline.py` | run_pipeline: RSK→LAPSE→MN→PV→BN 배치 (레거시) |

### 데이터 로더 + 캐시 최적화

| 로더 | 캐시 전략 | 단건/배치 |
|------|----------|----------|
| RawAssumptionLoader | 드라이버 사전로드 + resolve/data 캐시, preload_contracts() | 건당 2.9ms |
| TradPVDataCache | 단건: idno_filter(POLNO그룹만), 배치: 전건 | 1.1s/0.05s |
| BNDataCache | 단건: pcv_filter(해당 상품만), 배치: 전건 | 20s/0.2s |
| ExpDataCache | 사전 인덱스(_prod_cls_grp, _dim5_map, _items_cache) | <0.1s |

### RSK_RT DIV_VAL 매핑

**IP_P_COV 기반 동적 매핑** (하드코딩 아님):
```
IP_P_COV.RSK_RT_DIV_VAL_DEF_CD[i] = code → II_INFRC.RSK_RT_DIV_VAL[i] = value
IR_RSKRT_CHR.DEF_CD[pos] = code → IP_P_COV에서 code 위치 찾기 → INFRC 값 사용
```
- 1~6: 고정 (49,21,22,03,70,71)
- 7~10: 상품별 가변 (45 등)

### PRTT_RT 산출

```
PRTT_RT = DEFRY_RT × ann_due(TOT, rate, CYC) × (CD==1 ? v² : 1)
```
- CD=1: EXPCT_INRT × v²(2개월이연), CD=3: AVG_PUBANO_INRT, CD=4: min(EXPCT, AVG_PUBANO)
- 소스: IP_B_PRTT_BNFT_RT (파라미터), IP_P_EXPCT_INRT (이율)

### CF 공식

| 컬럼 | 공식 |
|------|------|
| PREM_BASE | CTR_TRMO × ORIG_PREM × PREM_PAY_YN |
| PREM_PYEX | (CTR_TRME[s-1] - PAY_TRME[s-1]) × ORIG_PREM × PAY_YN |
| DRPO_PYRV | CTR_RSVAMT_DEFRY_DRPSN × APLY_PREM_ACUMAMT_BNFT |
| INSUAMT_GEN | Σ BN.BNFT_INSUAMT |
| TMRFND | CTR_TRMPSN × CNCTTP_ACUMAMT_KICS |
| ACQSEXP_DR | Σ(ACQS_item × TRMO) — PAY_DVCD로 CTR/PAY 선택 |
| MNTEXP_DR | Σ(MNT_item × TRMO) |
| LOSS_SVYEXP | LSVY_rate × Σ BNFT_INSUAMT |

### EXP DRVR 코드

| DRVR | 기초금액 | 비고 |
|------|---------|------|
| 1 | RATE[t] × GPREM | t=CTR_AFT_PASS_MMCNT 월 인덱스 |
| 2 | AMOUNT[t] × 물가상승 | PRCE_ASC=1일 때 IE_INFL 적용 |
| 4 | 고정값 | BNFT_INSUAMT 대비 |
| 6 | RATE[t] × LOAN_REMAMT | |
| 9 | RATE[t] × CNCTTP_ACUMAMT_KICS | CNCTTP 이슈 있음 |
| 10 | RATE[t] × (CNCTTP - LOAN) | |

### PVCF 기시/기말 할인

- **기시(TRMO)**: PREM, PYEX, ACQSEXP, MNTEXP
- **기말(TRME)**: TMRFND, DRPO, INSUAMT, LOSS_SVYEXP

## Current Status

### 전건 검증 결과 (42,000건 OP_BEL 비교)

**100% PASS (11컬럼)**: PREM_ADD, INSUAMT_PENS, ACQSEXP_INDR, ACQSEXP_REDEM, MNTEXP_INDR, IV_MGMEXP_MNTEXP_CCRFND, HAFWDR, LOAN_NEW, LOAN_RPAY_MATU, PREM_ACUM_RSVAMT_ALTER, PREM_ADD_ACUMAMT_DEPL

**99%+ PASS (4컬럼)**: INSUAMT_GEN(99.6%), DRPO_PYRV(99.6%), LOSS_SVYEXP(99.9%), INSUAMT_HAFWAY(99.7%)

**80%+ PASS (4컬럼)**: PREM_BASE(83.8%), PREM_PYEX(84.5%), INSUAMT_MATU(78.5%), ACQSEXP_DR(79.7%)

**미구현 (7컬럼)**: TMRFND(4.8%), MNTEXP_DR(0%), BEL(0%), LOAN_ASET(0%), LOAN_INT(90.4%), LOAN_RPAY_HAFWAY(90.4%), IV_MGMEXP_CL_REMAMT(91.6%)

### 잔여 이슈

| 순위 | 항목 | 영향 | FAIL 건수 |
|------|------|------|----------|
| 1 | CNCTTP 산출 분리 (LTRMNAT 의존 제거) | TMRFND+MNTEXP_DR+BEL | 40,000+ |
| 2 | INSUAMT_MATU 구현 | 만기보험금 | 9,033 |
| 3 | PAY_STCD=3 보험료 처리 | PREM_BASE/PYEX | 6,803 |
| 4 | ACQSEXP_DR 매칭 확대 | 사업비 | 8,533 |
| 5 | 대출 CF (LOAN_INT 등) | 대출 관련 | 4,042 |

### 성능

| 구분 | 시간 |
|------|------|
| 단건 전체 파이프라인 | 0.3s |
| 건당 (배치, 캐시 후) | 2.9ms |
| 42,001건 배치 | ~3분 |

## 문서

- `docs/pipeline_flow.md`: 파이프라인 상세 흐름도
- `docs/dev_guide.md`: 개발 가이드 (미구현 항목 해결 방법)
- `docs/bel_comparison_report.md`: OP_BEL 전건 비교 검증 보고서

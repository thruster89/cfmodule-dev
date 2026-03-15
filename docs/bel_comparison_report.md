# OP_BEL 전건 비교 검증 보고서

## 개요

| 항목 | 값 |
|------|------|
| 기대값 | duckdb_transform.duckdb OP_BEL (42,000건) |
| 산출값 | output_bel_v2.duckdb (42,001건) |
| 파이프라인 | RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN → EXP → CF → DC_RT → PVCF → BEL |
| 산출 시간 | ~3분 (42,001건 배치) |
| 비교 기준 | ABS(기대-산출) < 1.0 → PASS |

---

## 컬럼별 전건 비교 결과 (42,000건)

### 구현 완료 — 높은 일치율

| 컬럼 | PASS | FAIL | PASS율 | 비고 |
|------|------|------|--------|------|
| PREM_ADD | 42,000 | 0 | 100.0% | |
| INSUAMT_PENS | 42,000 | 0 | 100.0% | |
| ACQSEXP_INDR | 42,000 | 0 | 100.0% | |
| ACQSEXP_REDEM | 42,000 | 0 | 100.0% | |
| MNTEXP_INDR | 42,000 | 0 | 100.0% | |
| IV_MGMEXP_MNTEXP_CCRFND | 42,000 | 0 | 100.0% | |
| HAFWDR | 42,000 | 0 | 100.0% | |
| LOAN_NEW | 42,000 | 0 | 100.0% | |
| LOAN_RPAY_MATU | 42,000 | 0 | 100.0% | |
| PREM_ACUM_RSVAMT_ALTER | 42,000 | 0 | 100.0% | |
| PREM_ADD_ACUMAMT_DEPL | 42,000 | 0 | 100.0% | |
| **LOSS_SVYEXP** | **41,944** | **56** | **99.9%** | BN 미매핑 56건 |
| **INSUAMT_GEN** | **41,852** | **148** | **99.6%** | BN 미매핑 일부 |
| **DRPO_PYRV** | **41,831** | **169** | **99.6%** | DEF_CD 영향 |
| **INSUAMT_HAFWAY** | **41,864** | **136** | **99.7%** | 미구현 (산출=0) |

### 부분 구현 — 미구현 항목 포함

| 컬럼 | PASS | FAIL | PASS율 | 원인 |
|------|------|------|--------|------|
| PREM_BASE | 35,197 | 6,803 | 83.8% | PAY_STCD=3 적립형 미처리 |
| PREM_PYEX | 35,502 | 6,498 | 84.5% | PREM_BASE와 동일 원인 |
| INSUAMT_MATU | 32,967 | 9,033 | 78.5% | 만기보험금 미구현 (산출=0) |
| ACQSEXP_DR | 33,467 | 8,533 | 79.7% | EXP 드라이버 매칭 범위 |
| IV_MGMEXP_MNTEXP_CL_REMAMT | 38,470 | 3,530 | 91.6% | 대출잔액 관련 미구현 |
| LOAN_INT | 37,980 | 4,020 | 90.4% | 대출이자 미구현 |
| LOAN_RPAY_HAFWAY | 37,958 | 4,042 | 90.4% | 대출중도상환 미구현 |

### 미구현 — 전건 불일치

| 컬럼 | PASS | FAIL | PASS율 | 원인 |
|------|------|------|--------|------|
| TMRFND | 2,035 | 39,965 | 4.8% | CNCTTP_ACUMAMT_KICS 산출 불일치 |
| MNTEXP_DR | 1 | 41,999 | 0.0% | DRVR=9/10(CNCTTP) 미구현 |
| BEL | 0 | 42,000 | 0.0% | 위 항목 차이 누적 |
| LOAN_ASET | 0 | 42,000 | 0.0% | BEL과 동일 (대출 미분리) |

---

## FAIL 원인 분석

### 1. PREM_BASE 6,803건 (83.8%)

| 원인 | 건수 | 상세 |
|------|------|------|
| TPCD=9 + PAY_STCD=3 (적립형 납입면제) | ~520 | calc=0, 기대값>0. 납입면제에도 보험료 산출 필요 |
| TPCD=9 + 산출>기대 | 6,460 | 적립형에서 산출값이 기대보다 큼 |
| TPCD=1 + PAY_STCD=3 | 156 | 비적립+납입면제. PROD=LA0217Y 등 |

**핵심**: PAY_STCD=3(납입면제) 계약에서 PREM_PAY_YN 처리 로직이 기대와 다름. 납입면제 시에도 일부 보험료가 산출되어야 하나 현재 0 처리.

### 2. INSUAMT_MATU 9,033건 (78.5%)

만기보험금 미구현. 기대값에는 9,033건이 비영이나 산출값은 전부 0.
- 주로 TPCD=9(적립형) 상품
- `calc/cf.py`의 `insuamt_matu` 항목 구현 필요

### 3. TMRFND 39,965건 (4.8%)

`TMRFND = CTR_TRMPSN × CNCTTP_ACUMAMT_KICS`에서 CNCTTP 산출값이 기대와 불일치.
- CNCTTP가 LTRMNAT_TMRFND에 의존적으로 산출
- 독립 KICS 경로 분리 필요

### 4. MNTEXP_DR 41,999건 (0.0%)

EXP DRVR=9/10 미구현으로 거의 전건 불일치.
- DRVR=9: RATE × CNCTTP_ACUMAMT_KICS
- DRVR=10: RATE × (CNCTTP - LOAN_REMAMT)
- CNCTTP 해결 시 자동 해결

### 5. ACQSEXP_DR 8,533건 (79.7%)

EXP 드라이버 매칭 범위 밖 상품이 많음.
- PROD_GRP 미등록 상품 → EXP 항목 없음 → 산출=0
- 760397 단건에서는 0.6% 차이 (KD2 edge case)

### 6. 대출 관련 (LOAN_INT, LOAN_RPAY_HAFWAY, IV_MGMEXP_CL_REMAMT)

약관대출 관련 CF 컬럼 미구현. 4,000~4,042건 FAIL.

---

## 성능

| 구분 | 시간 |
|------|------|
| 캐시 로드 (전건) | ~25s |
| 42,001건 배치 산출 | ~3분 |
| 건당 | ~3ms |

### 최적화 이력

| 단계 | Before | After |
|------|--------|-------|
| MN 체인 (건당) | 54ms | 2.9ms |
| BNDataCache (단건) | 20s | 0.2s |
| TradPVDataCache (단건) | 1.1s | 0.05s |
| DEF_CD 매핑 | 하드코딩 6개 | IP_P_COV 동적 (비결정성 해결) |

---

## 우선순위별 개선 로드맵

| 순위 | 항목 | 영향 컬럼 | FAIL 건수 | 난이도 |
|------|------|----------|----------|--------|
| 1 | CNCTTP 산출 분리 | TMRFND, MNTEXP_DR, BEL | 40,000+ | 중 |
| 2 | INSUAMT_MATU 구현 | INSUAMT_MATU, BEL | 9,033 | 중 |
| 3 | PAY_STCD=3 보험료 처리 | PREM_BASE, PREM_PYEX, BEL | 6,803 | 낮음 |
| 4 | ACQSEXP_DR 매칭 확대 | ACQSEXP_DR, BEL | 8,533 | 중 |
| 5 | 대출 CF (LOAN_INT 등) | 3개 컬럼, BEL | 4,042 | 중 |
| 6 | INSUAMT_HAFWAY 구현 | INSUAMT_HAFWAY, BEL | 136 | 낮음 |

### 1~3번 해결 시 예상 PASS율

| 컬럼 | 현재 | 예상 |
|------|------|------|
| PREM_BASE | 83.8% | ~100% |
| INSUAMT_MATU | 78.5% | ~100% |
| TMRFND | 4.8% | ~100% |
| MNTEXP_DR | 0.0% | ~100% |
| BEL | 0.0% | ~80%+ |

---

## 전건 검증 현황 요약

| 단계 | 전건 건수 | PASS율 | 기대값 소스 |
|------|----------|--------|------------|
| OD_RSK_RT | 42,001 | **100%** | duckdb_transform.duckdb |
| OD_LAPSE_RT | 42,001 | **100%** | duckdb_transform.duckdb |
| OD_TBL_MN | 42,001 | **100%** | duckdb_transform.duckdb |
| OD_TRAD_PV | 42,000 | **100%** (43컬럼) | duckdb_transform.duckdb |
| OD_TBL_BN | 32,963 | **100%** (16컬럼) | duckdb_transform.duckdb |
| OP_BEL | 42,000 | **11/26 컬럼 100%** | duckdb_transform.duckdb |
| | | PREM_BASE 83.8% | |
| | | INSUAMT_GEN 99.6% | |
| | | BEL 0% (파생) | |

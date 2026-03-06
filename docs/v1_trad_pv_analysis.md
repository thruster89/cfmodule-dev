# v1 OD_TRAD_PV 분석 결과

## 개요

- **기대값 DB**: `proj_o.duckdb` (42,000 IDNOs)
- **Legacy DB**: `VSOLN.vdb`
- **테스트 스크립트**: `test_v1_trad_pv_vs_proj_o.py`
- **비교 대상**: OD_TRAD_PV (50개 컬럼, 전통 준비금/보험료/환급금)

## OD_TRAD_PV 컬럼 구조 (50개)

| # | 컬럼 | 설명 | Phase |
|---|------|------|-------|
| 1 | INFRC_SEQ | 계약순번 | - |
| 2 | INFRC_IDNO | 계약ID | - |
| 3 | SETL_AFT_PASS_MMCNT | 결산후경과월 | - |
| 4 | CTR_AFT_PASS_MMCNT | 계약후경과월 | 1 |
| 5 | APLY_PUBANO_INRT | 적용공시이율 | 2 |
| 6 | PREM_PAY_YN | 납입여부 | 1 |
| 7 | ORIG_PREM | 원수보험료 | 1 |
| 8 | DC_PREM | 할인보험료 | 1* |
| 9 | ACUM_NPREM | 적립순보험료 | 1 |
| 10 | ACUM_NPREM_PRPD | 선납순보험료 | 3 |
| 11 | PRPD_MMCNT | 선납월수 | 3 |
| 12 | PRPD_PREM | 선납보험료 | 3 |
| 13 | PAD_PREM | 기납입보험료 | 1 |
| 14 | ADD_ACCMPT_GPREM | 추가적립영업보험료 | 3 |
| 15 | ADD_ACCMPT_NPREM | 추가적립순보험료 | 3 |
| 16 | ACQSEXP1_BIZEXP | 신계약비1 | 1 |
| 17 | ACQSEXP2_BIZEXP | 신계약비2 | 3 |
| 18 | AFPAY_MNTEXP | 납입후유지비 | 3 |
| 19 | LUMPAY_BIZEXP | 일시납사업비 | 3 |
| 20 | PAY_GRCPR_BIZEXP | 납입유예사업비 | 3 |
| 21 | YSTR_RSVAMT | 연시준비금 | 1 |
| 22 | YYEND_RSVAMT | 연말준비금 | 1 |
| 23 | YSTR_RSVAMT_TRM | 연시준비금(해지) | 1 |
| 24 | YYEND_RSVAMT_TRM | 연말준비금(해지) | 1 |
| 25-27 | PENS_* | 연금 관련 | 3 |
| 28 | HAFWAY_WDAMT | 중도인출금 | 3 |
| 29 | APLY_ADINT_TGT_AMT | 적용부리대상금액 | 2 |
| 30 | APLY_PREM_ACUMAMT_BNFT | 적용보험료적립금(급부) | 1 |
| 31 | APLY_PREM_ACUMAMT_EXP | 적용보험료적립금(사업비) | 1* |
| 32-33 | LWST_* | 최저보증 | 3 |
| 34 | LTRMNAT_TMRFND | 해지환급금 | 2 |
| 35 | SOFF_BF_TMRFND | 소멸전환급금 | 1* |
| 36 | SOFF_AF_TMRFND | 소멸후환급금 | 1* |
| 37-43 | *_ADD | 추가적립 관련 | 3 |
| 44 | CNCTTP_ACUMAMT_KICS | KICS적립금 | 2 |
| 45-49 | LOAN_* | 대출 관련 | 3 |
| 50 | MATU_MAINT_BNS_ACUM_AMT | 만기유지보너스적립금 | 3 |

> Phase 1: 확정 공식, Phase 2: 이자율 의존, Phase 3: 미분석
> 1*: 조건부 — 상품 유형에 따라 예외 있음

## Phase 1 확정 공식

### 기본 정보 출처

| 항목 | 출처 | 공식 |
|------|------|------|
| CTR_AFT_PASS_MMCNT | II_INFRC | PASS_YYCNT × 12 + PASS_MMCNT + SETL |
| PREM_PAY_YN | II_INFRC | CTR_MM ≤ PAYPR_YYCNT × 12 ? 1 : 0 |
| ORIG_PREM | II_INFRC | GRNTPT_GPREM (상수) |
| DC_PREM | II_INFRC | = ORIG_PREM (대부분 상품) |
| ACUM_NPREM | II_RSVAMT_BAS | NPREM × multiplier (상수) |
| PAD_PREM | 누적 | 초기값 = GPREM × CTR_MM[0], 이후 += GPREM × PREM_PAY_YN |
| ACQSEXP1_BIZEXP | II_INFRC | TOT_TRMNAT_DDCT_AMT (상수) |
| APLY_PREM_ACUMAMT_BNFT | II_RSVAMT_BAS | 정수월 보간 (아래 참조) |

### 승수 (Multiplier)

```
multiplier = II_INFRC.GRNTPT_JOIN_AMT / II_RSVAMT_BAS.CRIT_JOIN_AMT
```

예: IDNO=46017 → 100,000,000 / 1,000,000 = 100

### PAD_PREM 계산 (수정됨)

```python
PAD_PREM[0] = GPREM × CTR_MM[0]
PAD_PREM[t] = PAD_PREM[t-1] + GPREM × PREM_PAY_YN[t]   (t ≥ 1)
```

- 납입 중 (PREM_PAY_YN=1): 매월 GPREM만큼 증가
- 납입 후 (PREM_PAY_YN=0): 초기값에서 동결
- 주의: `GPREM × min(CTR_MM, PTERM_MM)` 아님! CTR_MM이 PTERM 초과해도 초기값 = GPREM × CTR_MM[0]

### 준비금 보간 (YSTR_RSVAMT / YYEND_RSVAMT / APLY_PREM_ACUMAMT_BNFT)

**II_RSVAMT_BAS 테이블**: YSTR_RSVAMT1~120, YYEND_RSVAMT1~120 (연도별)

```python
insurance_year = (CTR_MM - 1) // 12 + 1
month_in_year = CTR_MM - (insurance_year - 1) * 12

YSTR_RSVAMT = II_RSVAMT_BAS.YSTR_RSVAMTn × multiplier  (n = insurance_year)
YYEND_RSVAMT = II_RSVAMT_BAS.YYEND_RSVAMTn × multiplier

# 정수월 보간
APLY_PREM_ACUMAMT_BNFT = YSTR + (YYEND - YSTR) × month_in_year / 12
```

검증: **389/389 (100%)** — 500건 테스트에서 BAS 보유 계약 전부 PASS

## SOFF_BF_TMRFND 상품별 차이 (핵심 발견)

### 전체 구조

SOFF_BF_TMRFND = APLY_PREM_ACUMAMT_BNFT - [상품별 차감]

차감 적용 여부가 상품/담보/계약 조건에 따라 다름.

### 유형 A: TP2=4 (LA0201J, LA02079, LA0203C 등) — SOFF = 보간값

```
SOFF_BF_TMRFND = APLY_PREM_ACUMAMT_BNFT  (동일)
```

### 유형 B: SOFF_DEDUCT_PRODS — 7년(84개월) 정액상각

검증된 상품: LA0211Z, LA0215R, LA0215X, LA0216R, LA0216W, LA0217W

```
# 7년 정액상각 공식 (전 SETL 적용):
deduction = ACQSEXP1_BIZEXP × max(84 - CTR_MM, 0) / 84
SOFF_BF_TMRFND = APLY_PREM_ACUMAMT_BNFT - deduction

# CTR_MM ≥ 84 (7년 경과 후): deduction = 0 → SOFF = APLY
```

**8,605건, 3,811,670 SETL — 전부 PASS (오차 0)**

### 유형 C: LA0217Y (II_RSVAMT_TMRFND 있음) — 별도 계산

- 납입기간 동안 SOFF=0 (SETL 0~285)
- 납입 후: SOFF = APLY_PREM_ACUMAMT_BNFT

### 차감 적용 규칙 (확정)

**8,605건, 3,811,670 SETL 전부 PASS (LA0217Y 제외)**

```
차감 적용 = (PROD_CD ∈ SOFF_DEDUCT_PRODS) AND (PTERM > 5yr)
차감 공식 = ACQSEXP1 × max(84 - CTR_MM, 0) / 84

SOFF_DEDUCT_PRODS = {LA0211Z, LA0215R, LA0215X, LA0216R, LA0216W, LA0217W}
```

- PTERM ≤ 5yr: 위 상품이라도 차감 없음
- 7년(84개월) 정액상각: 경과 7년 초과 시 차감 = 0

### LA0217Y (Type C) — 미구현

- II_RSVAMT_TMRFND 데이터 보유, 복잡한 SOFF 계산
- 납입기간 동안 대부분 SOFF = 0 또는 별도 보간
- 일부 담보(CLA20429): SOFF = APLY_PREM_ACUMAMT_BNFT
- Phase 2에서 구현 필요

## 알려진 예외 사항

### PREM_PAY_YN 예외 (5/389, 1.3%)

CLA10007 특약에서 PREM_PAY_YN=0 (납입종료)인데 CTR_MM < PTERM_MM인 경우.
이 특약은 주계약과 다른 납입기간 판정 로직을 사용하는 것으로 추정.

### DC_PREM 예외 (8/389, 2.1%)

LA02158, LA02155 상품에서 DC_PREM < ORIG_PREM (약 2% 할인).
이들 상품은 보험료 할인이 적용되어 DC_PREM ≠ ORIG_PREM.

### II_RSVAMT_BAS 미보유 (111/500, 22.2%)

BSCTR_DVCD=0(특약)의 일부에 II_RSVAMT_BAS 데이터 없음.
BSCTR_DVCD=1(주계약)은 100% 보유.
BAS 미보유 계약: YSTR/YYEND=0이지만 SOFF>0 (이자율 기반 적립금 계산 사용 추정).

## Phase 1 테스트 결과 (500건 샘플)

| 구분 | 건수 | 비율 |
|------|------|------|
| PASS | 154 | 30.8% |
| FAIL | 235 | 47.0% (대부분 SOFF) |
| ERROR | 111 | 22.2% (BAS 없음) |

### 항목별 결과 (BAS 보유 389건 기준)

| 항목 | PASS | FAIL | 비고 |
|------|------|------|------|
| CTR_AFT_PASS_MMCNT | 389 | 0 | **100%** |
| ORIG_PREM | 389 | 0 | **100%** |
| ACUM_NPREM | 389 | 0 | **100%** |
| YSTR_RSVAMT | 389 | 0 | **100%** |
| YYEND_RSVAMT | 389 | 0 | **100%** |
| ACQSEXP1_BIZEXP | 389 | 0 | **100%** |
| APLY_PREM_ACUMAMT_BNFT | 389 | 0 | **100%** (max diff 3.73e-9) |
| PREM_PAY_YN | 384 | 5 | 98.7% (CLA10007 특약) |
| DC_PREM | 381 | 8 | 97.9% (할인 상품) |
| PAD_PREM | 384 | 5 | 98.7% (PREM_PAY_YN 연쇄) |
| SOFF_BF_TMRFND | 233 | 0 | **100%** (LA0217Y 제외) |

LA0217Y 156건 별도: SOFF_BF_TMRFND 3 PASS / 153 FAIL (Phase 2).

## 다음 단계

1. **SOFF 차감 조건 분석**: 차감 적용/미적용을 결정하는 DB 필드 또는 조건 탐색
2. **BAS 미보유 계약 처리**: 이자율 기반 적립금 계산 (Phase 2와 연관)
3. **CLA10007 납입기간**: 특약 고유 납입기간 판정 로직 확인
4. **DC_PREM 할인율**: LA02158/LA02155 할인 테이블 조사
5. **APLY_PUBANO_INRT 분석**: 공시이율 산출 공식 역산 (Phase 2)

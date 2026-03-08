# v1 OD_TRAD_PV 분석 결과

## 개요

- **기대값 DB**: `proj_o.duckdb` (42,000 IDNOs)
- **Legacy DB**: `VSOLN.vdb`
- **테스트 스크립트**: `test_trad_pv_full.py` (CLA00500 9,038건 전체 검증)
- **비교 대상**: OD_TRAD_PV (43개 비교 컬럼, 전통 준비금/보험료/환급금)

## 최종 검증 결과 (2026-03-08, CLA00500 9,038건 x 43컬럼 x 전시점)

### 완전 PASS (41개 컬럼)

| 컬럼 | 설명 | PASS |
|------|------|------|
| CTR_AFT_PASS_MMCNT | 계약후경과월 | 9038 |
| PREM_PAY_YN | 납입여부 | 9038 |
| ORIG_PREM | 원수보험료 | 9038 |
| DC_PREM | 할인보험료 | 9038 |
| **ACUM_NPREM** | 적립순보험료 | **9038** |
| ACUM_NPREM_PRPD | 미경과적립순보험료 | 9038 |
| PRPD_MMCNT | 미경과월수 | 9038 |
| PRPD_PREM | 미경과보험료 | 9038 |
| PAD_PREM | 기납입보험료 | 9038 |
| ADD_ACCMPT_GPREM | 추가적립영업보험료 | 9038 |
| ADD_ACCMPT_NPREM | 추가적립순보험료 | 9038 |
| ACQSEXP1_BIZEXP | 신계약비1 | 9038 |
| ACQSEXP2_BIZEXP | 신계약비2 | 9038 |
| AFPAY_MNTEXP | 납입후유지비 | 9038 |
| LUMPAY_BIZEXP | 일시납사업비 | 9038 |
| PAY_GRCPR_ACQSEXP | 납입유예사업비 | 9038 |
| YSTR_RSVAMT | 연시준비금 | 9038 |
| YYEND_RSVAMT | 연말준비금 | 9038 |
| YSTR_RSVAMT_TRM | 연시준비금(해지) | 9038 |
| YYEND_RSVAMT_TRM | 연말준비금(해지) | 9038 |
| PENS_INRT | 연금이율 | 9038 |
| PENS_DEFRY_RT | 연금부담율 | 9038 |
| PENS_ANNUAL_SUM | 연금연액 | 9038 |
| HAFWAY_WDAMT | 중도인출금 | 9038 |
| **APLY_PUBANO_INRT** | 적용공시이율 | **9038** |
| **APLY_ADINT_TGT_AMT** | 적용부리대상금액 | **9038** |
| **APLY_PREM_ACUMAMT_BNFT** | 적용적립금(급부) | **9038** |
| **APLY_PREM_ACUMAMT_EXP** | 적용적립금(사업비) | **9038** |
| **LWST_ADINT_TGT_AMT** | 최저부리대상금액 | **9038** |
| **LWST_PREM_ACUMAMT** | 최저적립금 | **9038** |
| **SOFF_BF_TMRFND** | 소멸전환급금 | **9038** |
| LTRMNAT_TMRFND | 해지환급금 | 9038 |
| HAFWAY_WDAMT_ADD | 중도인출금(추가) | 9038 |
| SOFF_BF_TMRFND_ADD | 소멸전환급금(추가) | 9038 |
| SOFF_AF_TMRFND_ADD | 소멸후환급금(추가) | 9038 |
| **LOAN_INT** | 대출이자 | **9038** |
| **LOAN_REMAMT** | 대출잔액 | **9038** |
| **LOAN_RPAY_HAFWAY** | 중도상환 | **9038** |
| LOAN_NEW | 신규대출 | 9038 |
| LOAN_RPAY_MATU | 만기상환 | 9038 |
| MATU_MAINT_BNS_ACUM_AMT | 만기유지보너스적립금 | 9038 |

### 남은 FAIL (2개 컬럼)

| 컬럼 | PASS | FAIL | 원인 |
|------|------|------|------|
| SOFF_AF_TMRFND | 9013 | 25 | CTR_MM < ~80 구간 0 처리 조건 미구현 |
| CNCTTP_ACUMAMT_KICS | 9012 | 26 | SOFF_AF 연쇄 |

---

## 핵심 공식 (구현 완료)

### 1. ACUM_NPREM (적립순보험료, 9038/9038 PASS)

**BAS 보유**: `NPREM × (JOIN_AMT / CRIT_JOIN_AMT)` (상수)

**BAS 미보유**: 사업비 키매칭 + 상각기간 기반

```python
# IP_P_EXPCT_BIZEXP_CMPT_CRIT 플래그 기반 동적 키매칭
# ETC_KEY 첫글자 분기
if ETC_KEY[0] == '1':
    NPREM = ACCMPT_GPREM × (1 - LOSS)
else:  # '9'
    NPREM_new = ACCMPT × (1 - MNT - LOSS)
    NPREM_old = ACCMPT × (1 - alpha/ann - MNT - LOSS)

# 상각기간: amort_mm = min(PTERM, ACQSEXP_ADDL_PRD) × 12
# CTR_AFT_PASS_MMCNT <= amort_mm → NPREM_old, 이후 → NPREM_new
```

### 2. APLY_PUBANO_INRT (적용공시이율, 9038/9038 PASS)

```python
if APLY_INRT_CD == '00':
    INRT = EXPCT_INRT  # 확정형
else:
    INRT[0] = inrt_lookup[CD]["0"]
    INRT[t>=1] = (EXT_WGHT*EXT_ITR + (DC_RT[t-1]-IV_ADEXP_RT)*(1-EXT_WGHT)) * ADJ_RT
# floor: INRT = max(INRT, LWST_GRNT_INRT_arr)
```

### 3. ADINT / ACUM (이율 기반 부리, 9038/9038 PASS)

```python
# V<0 (음수 적립금): 부리 없음
#   t=0: ADINT=V, ACUM=V
#   t>=1: ADINT=0, ACUM=V 고정

# V>=0 정상 부리:
# ADINT[0] = V (= ACCMPT_RSPB_RSVAMT)
# 연도 내: ADINT[t] = ADINT[t-1] + NPREM × (PAY_TRMO[t] / CTR_TRMO[t])
# 연도 경계: ADINT[t] = ACUM[t-1] + NPREM × ratio
# cum_int 연도경계 리셋, cum_int += ADINT[t] × INRT[t] / 12
# ACUM[t] = ADINT[t] + cum_int
# prem_pay_yn 배열로 실제 납입월만 P 추가 (비납입월=0)
```

### 4. PRPD (미경과보험료, 9038/9038 PASS)

```python
# PRPD_MMCNT: paycyc>1 && 납입중 → paycyc-(CM%paycyc), CM%paycyc==0이면 0
# PRPD_PREM: paycyc=12(연납)만 → ORIG_PREM * PRPD_MMCNT / 12
# ACUM_NPREM_PRPD: paycyc>1이면 nprem 상수, paycyc<=1이면 0
```

### 5. LOAN (약관대출, 9038/9038 PASS)

```python
# LOAN_REMAMT[0] = CTR_LOAN_REMAMT (초기 대출잔액)
# LOAN_REMAMT[t>=1] = 0 (즉시 전액 상환)
# LOAN_INT[1] = REMAMT[0]/2 × ((1+PUBANO[1])^(1/12)-1)  ← ADINTR 미포함!
# LOAN_RPAY_HAFWAY[1] = REMAMT[0]
# 키매칭: IA_M_PROD_GRP(FILE='2303066401') → PROD_GRP_CD + ITR_DVCD → IA_A_CTR_LOAN
```

### 6. SOFF_BF_TMRFND / LTRMNAT_TMRFND (9038/9038 PASS)

```python
DEDUCT_PRODS = {LA0211Z, LA0215R, LA0215X, LA0216R, LA0216W, LA0217W}
if (PROD in DEDUCT_PRODS and PTERM>5) or (TPCD=='0' and ACQSEXP>0):
    deduction = ACQSEXP × max(84-CM, 0) / 84
SOFF_BF = ACUM × rate - deduction
# rate: TPCD별 (3→0.3, 5→0.5, 1+CLS01/02→0납입중/1납입후, else→1)

# LTRMNAT: TPCD='9' → 0, else → max(0, ACUM - deduction)
```

### 7. CNCTTP_ACUMAMT_KICS

```
KICS = SOFF_AF_TMRFND × CTR_TRME_MTNPSN_CNT (from OD_TBL_MN)
```

---

## SOFF_AF 25건 FAIL 분석 (미해결)

- DB에서 SOFF_BF≠SOFF_AF인 IDNO: 153건 (대부분 SOFF_DEDUCT_PRODS)
- 패턴: CTR_MM < ~80 (약 6.5년) 구간에서 기대값 SOFF_AF=0
- CTR_MM ≥ 80부터 SOFF_AF = SOFF_BF
- 84개월(7년) 신계약비 상각기간과 관련 추정

---

## 구현 이력

| 날짜 | 변경 내용 |
|------|----------|
| 2026-03-07 | 초기 500건 검증, BAS 보유 233건 100% PASS |
| 2026-03-07 | SOFF 상품별 규칙 구현, LTRMNAT 통합 공식, ADINT+ACUM 구현 |
| 2026-03-07 | APLY_PUBANO_INRT DB 공식 구현 (inrt_lookup.json 캐시) |
| 2026-03-08 | 사업비 키매칭, ETC_KEY, PAYCYC, LWST floor, OD_TBL_MN 연동 |
| 2026-03-08 | amort_mm 기준 nprem_old/new 전환, CNCTTP_ACUMAMT_KICS |
| 2026-03-08 | V<0 음수 적립금 처리 (부리 없음) |
| 2026-03-08 | PRPD 3개 컬럼 구현 (미경과보험료) |
| 2026-03-08 | LOAN 즉시상환 공식 구현 (PUBANO only, ADINTR 미포함) |
| 2026-03-08 | **최종: 41/43컬럼 완전PASS, SOFF_AF 25건 + KICS 26건만 FAIL** |

---

## 파일 위치

| 파일 | 역할 |
|------|------|
| `cf_module/calc/trad_pv.py` | OD_TRAD_PV 산출 엔진 |
| `cf_module/data/trad_pv_loader.py` | DB 데이터 로딩 (II_INFRC, BAS, 사업비, 이율 등) |
| `test_trad_pv_full.py` | CLA00500 9,038건 전체 검증 스크립트 |
| `test_trad_pv_single.py` | 단건 검증 스크립트 (--idno, --detail, --cols) |
| `inrt_lookup.json` | APLY_INRT_CD별 SETL=0 이율 캐시 |
| `proj_o.duckdb` | 기대값 DB |

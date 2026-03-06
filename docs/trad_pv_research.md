# TRAD_PV 상세 리서치 노트

## 1. 데이터 출처 및 테이블 관계

### II_INFRC (계약 기본)

```sql
-- TRAD_PV 관련 주요 컬럼
SELECT INFRC_IDNO, PROD_CD, COV_CD, CLS_CD, CTR_DT,
       INSTRM_YYCNT,       -- 보험기간(년)
       PAYPR_YYCNT,        -- 납입기간(년)
       PAYPR_DVCD,         -- 납입기간구분 (Y010, Y015, Y020, Y030 등)
       PASS_YYCNT,         -- 경과년수
       PASS_MMCNT,         -- 경과월수
       GRNTPT_GPREM,       -- 보장파트 영업보험료
       GRNTPT_JOIN_AMT,    -- 보장파트 가입금액
       TOT_TRMNAT_DDCT_AMT,-- 총해지공제금액 = ACQSEXP1_BIZEXP
       STD_TRMNAT_DDCT_AMT,-- 기준해지공제금액
       ETC_EXPCT_BIZEXP_KEY_VAL, -- 기타예정사업비키값 (예: '9_99_00_9')
       ISR_OBJV_DVCD,      -- 보험목적구분 (1=피보험자)
       CTR_TPCD,           -- 계약유형코드
       PAY_STCD,           -- 납입상태코드 (1=납입중)
       RENW_DVCD           -- 갱신구분코드
FROM II_INFRC
```

### II_RSVAMT_BAS (준비금 기본)

```sql
-- 120년분 연시/연말 준비금 (CRIT_JOIN_AMT 기준 단위)
SELECT INFRC_IDNO, CRIT_JOIN_AMT, NPREM,
       YSTR_RSVAMT1, YYEND_RSVAMT1,  -- 1차년도
       YSTR_RSVAMT2, YYEND_RSVAMT2,  -- 2차년도
       ...
       YSTR_RSVAMT120, YYEND_RSVAMT120 -- 120차년도
FROM II_RSVAMT_BAS
```

- `multiplier = GRNTPT_JOIN_AMT / CRIT_JOIN_AMT`
- 실제 준비금 = raw값 × multiplier
- 42,000 IDNO 중 일부(약 12%)는 II_RSVAMT_BAS 데이터 없음 → ERROR

### II_RSVAMT_TMRFND (해지환급금)

- II_RSVAMT_BAS와 동일 구조 (YSTR/YYEND × 120년)
- CRIT_JOIN_AMT 없음 (raw값이 해지환급금)
- 일부 상품만 데이터 존재 (LA0217Y, LA0218M 등)
- OD_TRAD_PV에 있는 IDNO와 II_RSVAMT_TMRFND의 IDNO는 거의 겹치지 않음

### IP_P_COV (담보 상품 설정)

```sql
SELECT PROD_CD, COV_CD, CLS_CD,
       TMRFND_CALC_TP1_CD,  -- 환급금계산유형1 (1 or 2)
       TMRFND_CALC_TP2_CD,  -- 환급금계산유형2 (1 or 4)
       TMRFND_CALC_TP3_CD   -- 환급금계산유형3 (1 or 2)
FROM IP_P_COV
```

### IP_P_EXPCT_BIZEXP_RT (예정사업비율)

```sql
SELECT PROD_CD, CLS_CD, COV_CD, PAYPR_DVCD,
       ETC_EXPCT_BIZEXP_KEY_VAL,
       FRYY_GPREM_VS_ACQSEXP_RT,    -- 초년도 보험료 대비 신계약비율
       INPAY_GPREM_VS_MNTEXP_RT1,    -- 납입중 보험료 대비 유지비율
       AFPAY_GPREM_VS_MNTEXP_RT,     -- 납입후 보험료 대비 유지비율
       INPAY_GPREM_VS_LOSS_SVYEXP_RT -- 손해조사비율
FROM IP_P_EXPCT_BIZEXP_RT
```

- PAYPR_DVCD로 매칭 (Y010, Y015, Y020, Y025, Y030)
- ETC_EXPCT_BIZEXP_KEY_VAL로 추가 매칭

### IP_P_EXPCT_INRT (예정이율)

```sql
SELECT PROD_CD, CLS_CD, COV_CD,
       EXPCT_INRT1,          -- 예정이율1 (보통 0.0375)
       EXPCT_INRT2, EXPCT_INRT3,
       EXPCT_INRT_CHNG_YYCNT1, -- 이율변경 경과년수
       EXPCT_INRT_CHNG_YYCNT2,
       STD_INRT1,            -- 기준이율 (보통 0.04)
       STD_INRT2,
       AVG_PUBANO_INRT       -- 평균공시이율
FROM IP_P_EXPCT_INRT
```

- CLS_CD 01~05: 동일 상품/담보에 5개 분류
- LA0201J: 모든 CLS에서 EXPCT_INRT1=0.0375, STD_INRT1=0.04

### IE_PUBANO_INRT (공시이율 기본 설정)

```
ASSM_FILE_ID | PUBANO_INRT_CD | ADJ_RT | IV_ADEXP_RT | EXTER_INDT_ITR_WGHT_VAL | EXTER_INDT_ITR
202309_00    | 00             | 1.0    | 0.0         | 0.0                     | 0.0
202309_00    | 01             | 1.25   | 0.00004     | 1.0                     | 0.0364009
202309_00    | 06             | 0.80132| 0.00004     | 0.5                     | 0.0400524
...          | 29             | 0.48428| 0.00004     | 0.4                     | 0.0364009
```

21개 PUBANO_INRT_CD. 각 코드별 조정계수(ADJ_RT), 투자부가비율(IV_ADEXP_RT),
외부지표금리가중치(EXTER_INDT_ITR_WGHT_VAL), 외부지표금리(EXTER_INDT_ITR).

### IP_P_LTRMNAT (해지율)

```sql
SELECT PROD_CD, CLS_CD, CTR_TPCD_YN, CTR_TPCD, PAY_STCD,
       TMRFND_RT1 ~ TMRFND_RT20  -- 경과년수별 해지환급률
FROM IP_P_LTRMNAT
```

- 일부 상품만 데이터 존재 (LA0217Y, LA0218M 등 160개 이상 상품)
- LA0201J, LA0216W 등은 데이터 없음

## 2. 필드별 공식 상세

### CTR_AFT_PASS_MMCNT (계약후경과월수)

```python
elapsed = PASS_YYCNT * 12 + PASS_MMCNT  # SETL=0의 값
CTR_MM = elapsed + SETL
```

예: IDNO=46017, CTR_DT=20110801, CLOS_YM=202309
- PASS_YYCNT=12, PASS_MMCNT=2 → elapsed=146
- 검증: (2023-2011)*12 + (9-8) + 1 = 144+1+1 = 146 ✓

### PREM_PAY_YN (납입여부)

```python
PREM_PAY_YN = 1 if CTR_MM <= PAYPR_YYCNT * 12 else 0
```

예: IDNO=46017, PAYPR_YYCNT=30 → PTERM_MM=360
- CTR_MM=360 → PAY_YN=1 (마지막 납입월)
- CTR_MM=361 → PAY_YN=0 (납입종료)

### PAD_PREM (기납입보험료)

```python
PAD_PREM = ORIG_PREM × min(CTR_MM, PAYPR_YYCNT * 12)
```

- 납입중: PAD_PREM = ORIG_PREM × CTR_MM (매월 증가)
- 납입후: PAD_PREM = ORIG_PREM × PTERM_MM (고정)
- 예: IDNO=46017, SETL=214 → CTR_MM=360 → PAD=9400×360=3,384,000
- SETL=215 → CTR_MM=361 → PAD=9400×360=3,384,000 (고정)

### ACUM_NPREM (적립순보험료)

```python
ACUM_NPREM = II_RSVAMT_BAS.NPREM × multiplier
```

상수값 (시간 불변). 예: 72 × 100 = 7,200

### ACQSEXP1_BIZEXP (신계약비)

```python
ACQSEXP1 = II_INFRC.TOT_TRMNAT_DDCT_AMT  # 저장된 값 직접 사용
# 또는 계산:
# ACQSEXP1 = GPREM × 12 × FRYY_GPREM_VS_ACQSEXP_RT (IP_P_EXPCT_BIZEXP_RT)
```

상수값. 매칭 키: PROD_CD + CLS_CD + COV_CD + PAYPR_DVCD + ETC_EXPCT_BIZEXP_KEY_VAL

검증 (IDNO=46017):
- GPREM=9,400, PAYPR_DVCD=Y030
- IP_P_EXPCT_BIZEXP_RT에서 Y030의 FRYY_GPREM_VS_ACQSEXP_RT = 1.34
- 9,400 × 12 × 1.34 = 151,152 = TOT_TRMNAT_DDCT_AMT ✓

### YSTR_RSVAMT / YYEND_RSVAMT (연시/연말 준비금)

```python
insurance_year = (CTR_MM - 1) // 12 + 1  # 보험연도 (1-based)
YSTR = II_RSVAMT_BAS.YSTR_RSVAMTn × multiplier  # n = insurance_year
YYEND = II_RSVAMT_BAS.YYEND_RSVAMTn × multiplier
```

- 보험연도 1: CTR_MM 1~12
- 보험연도 2: CTR_MM 13~24
- ...
- 보험연도 n: CTR_MM (n-1)*12+1 ~ n*12

예: CTR_MM=146 → year=(146-1)//12+1=13 → YSTR_RSVAMT13=3630, ×100=363,000

### APLY_PREM_ACUMAMT_BNFT / EXP (적용보험료적립금)

```python
month_in_year = CTR_MM - (insurance_year - 1) * 12  # 연도 내 경과월 (1~12)
APLY_PREM_ACUMAMT = YSTR + (YYEND - YSTR) × month_in_year / 12
```

= 정수월 선형 보간. 모든 상품에서 동일 공식 사용 (검증 완료).

### SOFF_BF_TMRFND (소멸전환급금)

**상품 유형별 분기 — 핵심 발견:**

#### 유형 A: TMRFND_CALC_TP2=4, TP3=1

대상: LA0201J, LA02079, LA0203C, LA02125, LA02029 등

```python
SOFF_BF_TMRFND = APLY_PREM_ACUMAMT  # 정수월 보간 그대로
```

#### 유형 B: TMRFND_CALC_TP2=1, TP3=2 (TMRFND 없음)

대상: LA0216W, LA0217W, LA0214W 등

```python
year_0 = (CTR_MM_at_SETL0 - 1) // 12 + 1  # SETL=0의 보험연도
year_t = (CTR_MM - 1) // 12 + 1             # 현재 보험연도
year_end_CM = year_0 * 12                    # SETL=0 보험연도의 마지막 CTR_MM

if year_t == year_0:  # 초기 보험연도
    remaining = year_end_CM - CTR_MM
    deduction = ACQSEXP1 × remaining / (year_0 × 12)
    SOFF_BF_TMRFND = APLY_PREM_ACUMAMT - deduction
else:  # 이후 보험연도
    SOFF_BF_TMRFND = APLY_PREM_ACUMAMT
```

검증 (IDNO=596401, LA0216W, CTR_MM=82, year=7, ACQSEXP1=46,276):

| SETL | CTR_MM | Year | Remaining | Deduction | ACUMAMT | Calc SOFF | Exp SOFF | Match |
|------|--------|------|-----------|-----------|---------|-----------|----------|-------|
| 0 | 82 | 7 | 2 | 46276×2/84=1101.81 | 105432 | 104330.19 | 104330.19 | ✓ |
| 1 | 83 | 7 | 1 | 46276×1/84=550.90 | 106825 | 106274.10 | 106274.10 | ✓ |
| 2 | 84 | 7 | 0 | 0 | 108218 | 108218 | 108218 | ✓ |
| 3 | 85 | 8 | - | 0 | 109649.33 | 109649.33 | 109649.33 | ✓ |

검증 (IDNO=648895, LA0217W, CTR_MM=77, year=7, ACQSEXP1=18,540):

| SETL | CTR_MM | Remaining | Deduction | ACUMAMT | Calc SOFF | Exp SOFF | Match |
|------|--------|-----------|-----------|---------|-----------|----------|-------|
| 0 | 77 | 7 | 18540×7/84=1545.00 | 69682.92 | 68137.92 | 68137.92 | ✓ |
| 1 | 78 | 6 | 18540×6/84=1324.29 | 70662.50 | 69338.21 | 69338.21 | ✓ |
| 7 | 84 | 0 | 0 | 76540.00 | 76540.00 | 76540.00 | ✓ |
| 8 | 85 | - | 0 | 77544.17 | 77544.17 | 77544.17 | ✓ |

#### 유형 C: TMRFND_CALC_TP1=1, TP2=1, TP3=2 (TMRFND 있음)

대상: LA0217Y 등 (II_RSVAMT_TMRFND에 데이터 있는 경우)

```python
SOFF_BF_TMRFND = 0  # 초기~납입기간 동안
# 납입후 특정 시점부터: SOFF = APLY_PREM_ACUMAMT = LTRMNAT_TMRFND
```

- IDNO=1029166: SETL 0~285에서 SOFF=0
- SETL 286 (CTR_MM=361) 이후: SOFF=ACUMAMT=LTRMNAT_TMRFND
- LTRMNAT_TMRFND는 SETL=0부터 비zero (별도 보간 공식)
  - 분석 필요: 아마 II_RSVAMT_TMRFND × IP_P_LTRMNAT.TMRFND_RT

## 3. APLY_PUBANO_INRT 분석

### 관찰

SETL=0 값은 상품그룹별 고유:
- LA0201J 전체: 0.020464189
- LA02079 전체: 0.027500
- LA0216W/CLA10523: 0.010506361

SETL≥1 값은 시간에 따라 변화하며, 모든 상품이 비슷한 범위로 수렴 (0.030~0.033).

### SETL=0 값의 출처 추정

아직 미확인. 가설:
1. II_INFRC에 직접 저장되지 않음 (컬럼 없음)
2. IP_P_EXPCT_INRT의 EXPCT_INRT1(0.0375)이나 STD_INRT1(0.04)과 직접 매칭 안됨
3. IE_PUBANO_INRT의 EXTER_INDT_ITR과 일부 관련 가능성
   - PUBANO_INRT_CD=01: EXTER_ITR=0.036401
   - 0.020464와의 관계 불명
4. ITR_DVCD='S' (정률형)에서의 공시이율 산출 로직 조사 필요

### 매핑 경로 미확인

PUBANO_INRT_CD가 II_INFRC에 없음 → 계약과 IE_PUBANO_INRT를 연결하는 경로 불명.
가능한 경로:
- IP_P_PROD → CLS_CD → ??? → PUBANO_INRT_CD
- 또는 프로그래밍 로직에서 PROD_CD/ITR_DVCD 기반 결정

### SETL≥1 값의 추정

시나리오 기반 금리 전망값으로 보임:
- 시간에 따라 점진적 변화
- 모든 상품이 유사한 값으로 수렴
- 금리 시나리오 테이블이 별도 존재할 가능성

## 4. II_RSVAMT_BAS 누락 (111/500, 22.2%)

500건 샘플 중 111건 ERROR (II_RSVAMT_BAS not found).

### 원인 분석

```
BSCTR_DVCD=1 (주계약): 305/305 has BAS (100%)
BSCTR_DVCD=0 (특약):   442/695 has BAS (63.6%)
```

- BSCTR_DVCD=1은 항상 BAS 보유
- BSCTR_DVCD=0 중 36.4%가 BAS 미보유
- BAS 미보유 특약: YSTR/YYEND=0이지만 SOFF>0
  - 이자율 기반 적립금 계산 사용 (Phase 2)
  - APLY_PUBANO_INRT에 의존하는 재귀적 적립 공식 추정

## 5. DC_PREM (할인보험료)

대부분 DC_PREM = ORIG_PREM (할인 없음). 500건 테스트에서 8건(2.1%) 할인 적용.

### 할인 적용 상품

| 상품 | 할인율 | 비고 |
|------|--------|------|
| LA02158/CLA10007 | ~2.0% | 5건 |
| LA02155/CLA10007 | ~2.0% | 3건 |

IP_P_COV에 `PREM_DC_RT_TP1_CD` ~ `PREM_DC_RT_TP5_CD` 컬럼 존재:
- 할인율 유형 코드 + 적용 시작/종료 연차
- 할인율 테이블 조사 필요

## 5-1. PREM_PAY_YN 예외 (CLA10007)

500건 테스트에서 5건(1.3%) FAIL. 모두 COV=CLA10007.

```
CTR_MM < PAYPR_YYCNT × 12 인데 PREM_PAY_YN = 0 (납입종료)
```

- 특약 고유의 납입기간 판정 로직이 PAYPR_YYCNT와 다를 수 있음
- MN 테스트에서도 CLA10007 PAY 초기화 오류 확인 (유형 D)
- 별도 납입기간 필드 또는 주계약 연동 로직 조사 필요

## 5-2. PAD_PREM 공식 수정

**기존 (잘못된 공식)**: `PAD_PREM = GPREM × min(CTR_MM, PTERM_MM)`

**수정 (정확한 공식)**:
```python
PAD_PREM[0] = GPREM × CTR_MM[0]              # 초기값 = 경과월수 × 보험료
PAD_PREM[t] = PAD_PREM[t-1] + GPREM × PREM_PAY_YN[t]  # 납입 시 누적
```

- 납입기간 종료 후에도 초기값은 GPREM × CTR_MM[0] (cap 없음)
- 이후 PREM_PAY_YN=0이면 동결
- 검증: 500건 384/389 PASS (FAIL 5건은 PREM_PAY_YN 연쇄)

## 6. YSTR_RSVAMT_TRM vs YSTR_RSVAMT

현재 관찰된 모든 IDNO에서 YSTR_RSVAMT_TRM = YSTR_RSVAMT (동일).
별도 로직이 필요한 경우는 미발견.

## 7. 보험연도 체계 정리

```
CTR_DT = 20161209 (Dec 9, 2016)
CLOS_YM = 202309

elapsed = (2023-2016)*12 + (9-12) + 1 = 82
→ CTR_MM = 82 at SETL=0

insurance_year = (82-1)//12 + 1 = 7
year 7: CTR_MM 73~84 (Dec 2022 ~ Nov 2023)

month_in_year = 82 - 72 = 10
→ 보간: YSTR + (YYEND-YSTR) × 10/12

year_end_CM = 84
remaining_to_year_end = 84 - 82 = 2
```

## 8. 날짜 관련 참고사항

SOFF_BF_TMRFND 유형 B에서 CTR_DT의 일(day)은 보간에 영향을 미치지 않음.
- PASS: CTR_DT day=1,20,21,23,28,31 → 정수월 보간 정확 일치
- FAIL (유형 B): day=4,9,21 → ACQSEXP 차감 때문이지 day 때문이 아님
- 모든 경우 정수월 기반 보간 사용 (날짜 비례 보간 아님)

## 9. CNCTTP_ACUMAMT_KICS (KICS적립금)

SETL=0: 일부 상품에서 SOFF_BF_TMRFND와 동일, 다른 상품에서 다름.

IDNO=46017:
- SETL=0: CNCTTP=369166.67 = SOFF (동일)
- SETL=1: CNCTTP=370960.22 ≠ SOFF=372250 (차이 발생)
- CNCTTP는 APLY_PUBANO_INRT로 할인/부리한 값으로 추정

IDNO=596401:
- SETL=0: CNCTTP=104330.19 = SOFF (동일)
- SETL=1: CNCTTP와 SOFF 차이 발생 예상

→ CNCTTP = 이전기 KICS적립금 × (1 + monthly_rate) + 보험료 - 비용 등의 재귀 공식 추정
→ Phase 2에서 APLY_PUBANO_INRT 확정 후 분석

## 10. SOFF 차감 대량 분석 결과

### 차감 적용 통계

proj_o.duckdb 42,000 IDNOs 기반 분석:

```
SOFF ≠ APLY_PREM_ACUMAMT_BNFT: 8,427건 (20%)
SOFF = APLY_PREM_ACUMAMT_BNFT: 나머지 (80%)
```

### 차감 적용 상품 (42개 PROD/COV 조합)

| 상품 | 건수 | 비고 |
|------|------|------|
| LA0217W | ~6,000 | 대부분의 COV에서 차감 |
| LA0217Y | ~750 | Type C (TMRFND 데이터 기반) |
| LA0216W | ~500 | 일부 COV에서 차감 |
| LA0211Z | ~20 | |
| LA0215R | ~10 | |

### 핵심 발견: 동일 TP 코드, 다른 차감 동작

```
LA0216W/CLA10523 (IDNO=596401): TP2=1 → 차감 적용 ✓
LA0216W/CLA10363 (IDNO=252):    TP2=1 → 차감 미적용 ✗
LA0214W/CLA10359 (IDNO=1126584): TP2=1 → 차감 미적용 ✗
LA0217W/CLA10359 (IDNO=648895): TP2=1 → 차감 적용 ✓
```

- TMRFND_CALC_TP1/2/3_CD만으로는 차감 여부 결정 불가
- PROD_CD, COV_CD, BSCTR_DVCD, CLS_CD 조합도 결정적이지 않음
- 레거시 시스템의 복합 조건 로직으로 추정

### APLY_PREM_ACUMAMT_BNFT = 정수월 보간 검증

**389/389 (100%) PASS** — 차감과 관계없이 보간 자체는 완벽히 일치.
SOFF 문제는 순수하게 "차감 적용 여부 및 금액" 문제.

## 11. 테스트 실행 방법

```bash
# Phase 1 기본 테스트 (10건)
python test_v1_trad_pv_vs_proj_o.py

# 500건 대량 테스트
python test_v1_trad_pv_vs_proj_o.py --n 500

# 단건 상세 디버그
python test_v1_trad_pv_vs_proj_o.py --debug 46017

# 특정 IDNO 테스트
python test_v1_trad_pv_vs_proj_o.py --idno 596401,648895

# CSV 저장
python test_v1_trad_pv_vs_proj_o.py --n 50 --csv
```

## 12. 500건 테스트 최종 결과 요약

| 범주 | 필드 | PASS/총 | 비율 |
|------|------|---------|------|
| **100% 완벽** | CTR_AFT_PASS_MMCNT | 389/389 | 100% |
| | ORIG_PREM | 389/389 | 100% |
| | ACUM_NPREM | 389/389 | 100% |
| | YSTR_RSVAMT | 389/389 | 100% |
| | YYEND_RSVAMT | 389/389 | 100% |
| | ACQSEXP1_BIZEXP | 389/389 | 100% |
| | APLY_PREM_ACUMAMT_BNFT | 389/389 | 100% |
| **98%+ 예외** | PREM_PAY_YN | 384/389 | 98.7% |
| | DC_PREM | 381/389 | 97.9% |
| | PAD_PREM | 384/389 | 98.7% |
| **미구현** | SOFF_BF_TMRFND | 167/389 | 42.9% |
| **Phase 2** | APLY_PUBANO_INRT | - | 미구현 |
| | CNCTTP_ACUMAMT_KICS | - | 미구현 |
| | LTRMNAT_TMRFND | - | 미구현 |

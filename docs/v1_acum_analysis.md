# v1 CLA00500 적립금 부리 공식 분석

## 개요

- BAS 미보유 계약 9,038건 → 전부 CLA00500 (적립부분)
- II_RSVAMT_BAS 없으므로 이율 기반 부리로 APLY_PREM_ACUMAMT_BNFT 계산
- CTR_TPCD=9인 경우 (IDNO=17): II_RSVAMT_TMRFND 데이터 없음

## 확정된 공식

### ACUM = ADINT_TGT + 누적이자 (100% 검증)

```python
# 보험연도 내에서:
cum_interest = 0
for m in range(year_start_setl, year_end_setl + 1):
    cum_interest += ADINT_TGT[m] * INRT[m] / 12
    ACUM[m] = ADINT_TGT[m] + cum_interest

# 보험연도 경계에서 cum_interest 리셋 (= 0)
```

**검증**: IDNO=17, 563 SETL 전부 오차 0 (모든 연도 경계 포함)

### ACUM ≠ 복리

표준 복리 공식 `(ACUM[t-1] + NPREM) * (1 + INRT[t]/12)`은 **불일치**:
- SETL=1: 오차 0 (첫 단계만 일치)
- SETL=22: 오차 257
- SETL=563 (최종): 오차 11,482,185 (실질 2배)

→ "연도 내 단리, 연도 간 복리" 방식

### ADINT_TGT 구조

보험연도 내에서 ADINT_TGT는 **2차 함수**:

```
ADINT[s] = V_year + s*P - correction(s)
```

- `s`: 연도 내 월 (1~12 또는 부분연도)
- `P`: ACUM_NPREM (정수)
- `V_year`: 연시 기준값
- `correction(s)`: 2차 보정항, 단리-복리 차이 반영

#### 연도 경계 리셋

```
V_{year+1} = ACUM[year_end] - accumulated_correction
```

- Year 13→14: correction = 45.77
- Year 14→15: correction = 102.11

### 2차 보정 상수 C (second difference)

| 연도 | C값 | C/NPREM |
|------|-----|---------|
| 13 | 4.58 | 0.000382 |
| 14 | 4.71 | 0.000393 |
| 15 | 4.81 | 0.000401 |

C는 연도마다 소폭 증가 (장기금리 수렴과 연관 추정)

## 미해결: APLY_PUBANO_INRT 도출

### 알려진 사실

1. **동일 APLY_INRT_CD → 동일 INRT**: IDNO=17,50,329 (모두 CD='08') 동일값 확인
2. **매우 smooth**: 월별 변동 소폭 (0.030~0.033 범위), SETL=0만 예외적 저값 (0.020464)
3. **장기 수렴**: INRT → 0.031869 (≈ STD_INRT1 × ADJ_RT = 0.04 × 0.80157 = 0.032063)

### 관련 테이블

| 테이블 | 주요 컬럼 | 값 (CD=08) |
|--------|-----------|------------|
| IE_PUBANO_INRT | ADJ_RT | 0.80157 |
| | IV_ADEXP_RT | 4e-05 |
| | EXTER_INDT_ITR_WGHT_VAL | 0.7 |
| | EXTER_INDT_ITR | 0.036489 |
| IP_P_ACUM_COV | APLY_INRT_CD | 08 |
| | LWST_GRNT_INRT1 | 0.02 |
| | INRT_CHG_CYC_CD | 1 (월별) |
| IP_P_EXPCT_INRT | EXPCT_INRT1 | 0.0375 |
| | STD_INRT1 | 0.04 |
| IE_DC_RT | 1440 spot rates | 할인율 커브 |

### 시도한 접근 (모두 불일치)

1. 월별 forward rate × ADJ_RT: 변동성 과다
2. 12개월 평균 forward rate: 수준 불일치 (~0.038 vs 0.031)
3. par yield: 수준 불일치 (~0.035)
4. 남은 만기 평균 forward rate: 불일치
5. fwd_12m/fwd_12m(0) × STD_INRT: 변동성 과다
6. EXTER_WGHT blending: 수준 불일치

### 역산된 input_rate

```
input_rate = (INRT + IV_ADEXP_RT) / ADJ_RT
```

- SETL=0: 0.025580 (예외적 저값)
- SETL=1~10: 0.038~0.041
- 장기: 0.039808 (≈ STD_INRT1 - 0.0002)

→ **INRT 공식 미파악**: 규제 문서 또는 원본 코드 참조 필요

## IDNO=17 기본 정보

| 항목 | 값 |
|------|-----|
| PROD_CD | LA0201J |
| COV_CD | CLA00500 |
| CTR_DT | 20110805 |
| INSTRM_YY | 59 |
| PAYPR_YY | 20 |
| PASS_YY/MM | 12/2 |
| CTR_TPCD | 9 |
| GPREM | 0 |
| JOIN_AMT | 0 |
| ACUM_NPREM | 11990.12 |
| APLY_INRT_CD | 08 |

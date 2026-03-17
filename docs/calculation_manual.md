# CF 모듈 계산 매뉴얼

보험 Cash Flow 프로젝션 엔진의 계산 항목별 상세 설계서.
유입(수입)과 유출(지출) 각 항목의 계산 로직, 입력 데이터, 산출 공식을 정리한다.

---

## 목차

1. [파이프라인 개요](#1-파이프라인-개요)
2. [기초 산출 단계](#2-기초-산출-단계)
   - 2.1 [RSK_RT (위험률)](#21-rsk_rt-위험률)
   - 2.2 [LAPSE_RT (해지율)](#22-lapse_rt-해지율)
   - 2.3 [TBL_MN (주테이블 - 유지자수)](#23-tbl_mn-주테이블---유지자수)
   - 2.4 [TRAD_PV (보험료/적립금)](#24-trad_pv-보험료적립금)
   - 2.5 [TBL_BN (급부별 계산)](#25-tbl_bn-급부별-계산)
   - 2.6 [EXP (사업비)](#26-exp-사업비)
3. [CF (캐시플로우) 산출](#3-cf-캐시플로우-산출)
   - 3.1 [유입 항목 (Inflow)](#31-유입-항목-inflow)
   - 3.2 [유출 항목 (Outflow)](#32-유출-항목-outflow)
   - 3.3 [미구현 항목](#33-미구현-항목)
4. [DC_RT (할인율)](#4-dc_rt-할인율)
5. [PVCF (현가 캐시플로우)](#5-pvcf-현가-캐시플로우)
6. [BEL (최선추정부채)](#6-bel-최선추정부채)
7. [핵심 개념 정리](#7-핵심-개념-정리)

---

## 1. 파이프라인 개요

```
RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN → EXP → CF → DC_RT → PVCF → BEL
```

| 단계 | 모듈 | 역할 | 소스 |
|------|------|------|------|
| RSK_RT | `calc/tbl_rsk_rt.py` | 위험률 산출 (월환산, 면책 적용) | `data/rsk_lapse_loader.py` |
| LAPSE_RT | `calc/tbl_lapse_rt.py` | 해지율 산출 (월환산) | `data/rsk_lapse_loader.py` |
| TBL_MN | `calc/tbl_mn.py` | 중복제거 + 유지자수/탈퇴자 산출 | — |
| TRAD_PV | `calc/trad_pv.py` | 보험료, 적립금, 환급금, 대출 산출 | `data/trad_pv_loader.py` |
| TBL_BN | `calc/tbl_bn.py` | 급부별 보험금 산출 | `data/bn_loader.py` |
| EXP | `calc/exp.py` | 사업비 산출 | `data/exp_loader.py` |
| CF | `calc/cf.py` | 캐시플로우 조합 | — |
| DC_RT | `calc/dc_rt.py` | 할인율 커브 산출 | — |
| PVCF | `calc/pvcf.py` | 현가 캐시플로우 산출 | — |
| BEL | `calc/bel.py` | 최선추정부채 산출 | — |

---

## 2. 기초 산출 단계

### 2.1 RSK_RT (위험률)

**모듈**: `calc/tbl_rsk_rt.py`
**입력**: `IR_RSKRT_VAL` (위험률 원율), `IA_M_ASSM_DRIV` (드라이버 가정)

#### 산출 과정

**① 원율 로딩 (RSK_RT)**

| 조건 | 산출식 |
|------|--------|
| 고정위험률 (`chr_cd == "S"`) | `RSK_RT = rate_arr[0]` (전 시점 동일) |
| 연령별위험률 | `RSK_RT = rate_arr[entry_age + max(duration_months - 1, 0) // 12]` |

**② 연 단위 위험률 (BF_YR)**

```
BF_YR = RSK_RT × LOSS_RT × MTH_EFECT_COEF × BEPRD_DEFRY_RT × TRD_COEF × ARVL_AGE_COEF
```

- `LOSS_RT` = 1.0 (상수)
- `MTH_EFECT_COEF` = 1.0 (상수)
- `BEPRD_DEFRY_RT`: `IA_T_BEPRD` 테이블 기반 경과년수별 면책비율
- `TRD_COEF`, `ARVL_AGE_COEF` = 1.0 (상수)

**③ 월환산 (BF_MM)**

| 월환산 방식 (`mm_trf_way_cd`) | 공식 |
|-------------------------------|------|
| 1 (Additive) | `BF_MM = 1 - (1 - BF_YR)^(1/12)` |
| 2 (Division) | `BF_MM = BF_YR / 12` |

**④ 면책기간 적용 (AF)**

```
AF = BF_MM
AF[duration_months < invld_mm] = 0    ← 면책기간 내 위험률 0
```

#### 출력 (OD_RSK_RT)

| 컬럼 | 설명 |
|------|------|
| `RSK_RT` | 연 단위 원율 |
| `BEPRD_DEFRY_RT` | 경과년별 면책비율 |
| `INVLD_TRMNAT_BF_MM_RSK_RT` | 월환산값 (면책 전) |
| `INVLD_TRMNAT_AF_APLY_RSK_RT` | **최종 위험률** (면책 적용 후) → TBL_MN에서 사용 |

---

### 2.2 LAPSE_RT (해지율)

**모듈**: `calc/tbl_lapse_rt.py`
**입력**: `IA_T_TRMNAT` (해지율 테이블), `IA_T_SKEW` (스큐 테이블)

#### 산출 과정

**① 연 단위 해지율 선택**

| 기간 구분 | 조건 | 산출식 |
|-----------|------|--------|
| 납입중 | `duration_months ≤ pterm_months` | `TRMNAT_RT = lapse_paying[proj_year_idx]` |
| 납입후 | `duration_months > pterm_months` | `TRMNAT_RT = lapse_paidup[paidup_year_idx]` |
| 만기도래 | `elapsed ≥ bterm_months` | `TRMNAT_RT = 0` |

**② 월환산**

```
APLY_TRMNAT_RT = 1 - (1 - TRMNAT_RT)^(1/12)
```

#### 출력 (OD_LAPSE_RT)

| 컬럼 | 설명 |
|------|------|
| `TRMNAT_RT` | 연 단위 해지율 |
| `APLY_TRMNAT_RT` | **월환산 해지율** → TBL_MN에서 사용 |

---

### 2.3 TBL_MN (주테이블 - 유지자수)

**모듈**: `calc/tbl_mn.py`
**입력**: RSK_RT 결과, LAPSE_RT 결과

#### 핵심: C행렬 중복제거

동일 계약에 여러 위험이 중복 적용되는 경우 과대 산정을 방지하기 위한 중복제거 로직.

**C행렬 규칙** (Cij = 0이면 중복제거 안 함):

| 조건 | Cij 값 | 설명 |
|------|--------|------|
| `i == j` (대각선) | 0 | 자기 자신 |
| `risk_grp[i] == risk_grp[j]` | 0 | 동일 위험그룹 |
| `dead_rt_dvcd == 0` (사망위험 열) | 0 | 사망위험은 중복제거 제외 |
| 기타 | 1 | **중복제거 적용** |

**중복제거 공식**:

```
q'ᵢ = qᵢ × (1 - Σⱼ(qⱼ × Cᵢⱼ) / 2)
```

#### Exit Set 구성

| Exit Set | 포함 조건 | 용도 |
|----------|----------|------|
| CTR (계약자) | `rsvamt = 1` 또는 `bnft = 1` | 계약 기준 유지자 |
| PAY (납입자) | `rsvamt = 1` 또는 `bnft = 1` 또는 `pyexsp = 1` | 납입 기준 유지자 |

#### 유지자수 산출

```
dx = clip(Σ(qx_dedup) + wx_dedup, 0, 1)       ← 총 탈퇴율
dx *= is_in_force                                ← 만기 전까지만

tpx[0] = 1
tpx[t] = cumprod(1 - dx[0:t+1])                ← 기말 유지자

tpx_bot[0] = 0
tpx_bot[t≥1] = tpx[t-1]                        ← 기시 유지자
```

#### 탈퇴자 분해

```
CTR_TRMPSN     = tpx_bot × wx_dedup_ctr         ← 해지자
CTR_RSVAMT_DRPSN = tpx_bot × ctr_rsvamt_rt      ← 적립금 탈퇴자
CTR_BNFT_DRPSN = tpx_bot × ctr_bnft_rt          ← 급부 탈퇴자
```

#### 납입면제 (PAY_STCD = 3) 처리

```
PAY_STCD == "3" 인 경우:
  PAY_TRME = 0, PAY_TRMO = 0
  모든 PAY 관련 변수 = 0
```

#### 출력 (OD_TBL_MN - 18컬럼)

**CTR 기준 (계약자)**:

| 컬럼 | 설명 |
|------|------|
| `CTR_TRMO_MTNPSN_CNT` | 기시 유지자수 |
| `CTR_TRME_MTNPSN_CNT` | 기말 유지자수 |
| `CTR_TRMNAT_RT` | 해지율 |
| `CTR_RSVAMT_DEFRY_DRPO_RSKRT` | 적립금 탈퇴율 |
| `CTR_BNFT_DRPO_RSKRT` | 급부 탈퇴율 |
| `CTR_TRMPSN_CNT` | 해지자수 |
| `CTR_RSVAMT_DEFRY_DRPSN_CNT` | 적립금 탈퇴자수 |
| `CTR_BNFT_DEFRY_DRPSN_CNT` | 급부 탈퇴자수 |

**PAY 기준 (납입자)**: `PAY_`로 시작하는 동일 구조 8컬럼

---

### 2.4 TRAD_PV (보험료/적립금)

**모듈**: `calc/trad_pv.py`
**입력**: `II_RSVAMT_BAS`, `IP_P_COV`, `IP_P_ACUM_COV`, `IP_P_EXPCT_INRT` 등

#### STEP 1: 보험료 (PREM)

**납입여부 (PREM_PAY_YN)**:

| 조건 | PREM_PAY_YN |
|------|-------------|
| `pay_stcd ≠ 1` (납입면제 등) | 0 (전 시점) |
| 납입기간 내 + 월납 (`paycyc == 1`) | 1 |
| 납입기간 내 + 연납/반기납 | `ctr_mm % cyc == 1` 인 월만 1 |
| 납입기간 후 | 0 |

**원수보험료**: `ORIG_PREM = gprem` (상수)

**적립순보험료 (ACUM_NPREM)**:

| 조건 | 산출식 |
|------|--------|
| BAS 보유 | `ACUM_NPREM = bas_nprem × (join_amt / bas_crit_join_amt)` |
| BAS 미보유, 상각기간 내 | `ACUM_NPREM = acum_nprem_old × PREM_PAY_YN` |
| BAS 미보유, 상각기간 후 | `ACUM_NPREM = acum_nprem_nobas × PREM_PAY_YN` |

#### STEP 2: 미경과보험료 (PRPD)

| 항목 | 조건 | 산출식 |
|------|------|--------|
| PRPD_MMCNT | `paycyc > 1` | `paycyc - (ctr_mm % paycyc)` (나머지 ≠ 0일 때) |
| PRPD_PREM | `paycyc == 12` | `ORIG_PREM × PRPD_MMCNT / 12` |

#### STEP 3: 이율 배열

**최저보증이율 (LWST_GRNT_INRT)**:

| chng_cd | 산출식 |
|---------|--------|
| 0 | `lwst = lwst1` (고정) |
| 그 외 | 경과년수(ctr_yy)에 따라 lwst1 → lwst2 → lwst3 단계적 적용 |

**공시이율 (PUBANO_INRT)**:

| APLY_INRT_CD | 산출식 |
|--------------|--------|
| '00' | `EXPCT_INRT` (고정) |
| 그 외 | `pubano[t] = max((ew × ei + (dc[t-1] - iv) × (1-ew)) × adj, lwst)` |

#### STEP 4: 적립금 (APLY_PREM_ACUMAMT_BNFT)

**BAS 보유 계약** (예정 준비금 선형보간):

```
ins_year = (ctr_mm - 1) // 12 + 1
month_in_year = ctr_mm - (ins_year - 1) × 12

ystr_rsvamt  = bas_ystr[ins_year - 1] × mult
yyend_rsvamt = bas_yyend[ins_year - 1] × mult

APLY_PREM_ACUMAMT_BNFT = ystr_rsvamt + (yyend_rsvamt - ystr_rsvamt) × month_in_year / 12
```

**BAS 미보유 계약** (이율 부리):

```
[연도 경계: ctr_mm % 12 == 1]
  base = ACUM[t-1], cum_int = 0

[연도 내]
  base = ADINT[t-1]

ADINT[t] = base + nprem × (pay_trmo / ctr_trmo)
cum_int += ADINT[t] × inrt[t] / 12
ACUM[t]  = ADINT[t] + cum_int
```

#### STEP 5: 환급금

**해지환급금 (LTRMNAT_TMRFND)**:

| 조건 | 산출식 |
|------|--------|
| `ctr_tpcd == "9"` | 0 |
| `acqsexp1 > 0` & `pay_stcd ≠ 3` | `max(0, APLY_PREM_ACUMAMT_BNFT - acqsexp1 × max(84 - ctr_mm, 0) / 84)` |
| 기타 | `max(0, APLY_PREM_ACUMAMT_BNFT)` |

#### STEP 6: KICS 적립금

```
CNCTTP_ACUMAMT_KICS = (SOFF_AF_TMRFND + PRPD_PREM) × CTR_TRME
```

#### STEP 7: 약관대출

| 조건 | 처리 |
|------|------|
| 대출잔액 = 0 또는 대출유형 = 0 | 대출 관련 변수 모두 0 |
| 대출 존재 시 | `loan_remamt[0] = ctr_loan_remamt`, t=1에서 전액 상환 |

**대출이자**: `loan_int[1] = loan_remamt[0] / 2 × ((1 + pub_rate)^(1/12) - 1)`

#### 출력 (OD_TRAD_PV - 주요 50컬럼)

| 컬럼 | 설명 | CF에서 사용 |
|------|------|------------|
| `PREM_PAY_YN` | 납입여부 | PREM_BASE |
| `ORIG_PREM` | 원수보험료 | PREM_BASE |
| `ACUM_NPREM` | 적립순보험료 | — |
| `APLY_PREM_ACUMAMT_BNFT` | 적립금(급부용) | DRPO_PYRV, BN |
| `CNCTTP_ACUMAMT_KICS` | KICS 적립금 | TMRFND, EXP |
| `LOAN_REMAMT` | 대출잔액 | EXP, LOAN CF |
| `LTRMNAT_TMRFND` | 해지환급금 | — |

---

### 2.5 TBL_BN (급부별 계산)

**모듈**: `calc/tbl_bn.py`
**입력**: RSK_RT, LAPSE_RT, TRAD_PV (APLY_PREM_ACUMAMT_BNFT), `IA_A_BNFT_INFO`

#### 급부별 독립 중복제거

각 급부(BNFT_NO)별로 독립적인 exit set을 구성하여 별도 중복제거 수행.

**C행렬 규칙** (TBL_MN과 유사 + 추가 규칙):

| 조건 | Cij 값 | 설명 |
|------|--------|------|
| 대각선 | 0 | 자기 자신 |
| 동일 `RSK_GRP_NO` | 0 | 동일 위험그룹 |
| `DEAD_RT_DVCD == 0` 열 | 0 | 사망위험 제외 |
| **RSKRT-only risk** 열 | 0 | DRPO에 없는 RSKRT → 수동적 dedup |

#### 급부 보험금 산출

```
① 부담보(NCOV) 판정
   in_coverage = (ctr_mm ≥ ncov_months)

② 중복제거
   r_dedup = _bn_dedup(wx, qx_rates, exit_cds, ...)

③ exit rate 산출
   bn_exit = clip(trmnat + rsvamt_drpo + bnft_drpo, 0, 1)

④ 급부별 유지자
   trmo[0] = 1, trme[0] = 1
   trmo[t] = trme[t-1]
   trme[t] = trmo[t] × (1 - bn_exit[t])

⑤ PRTT/DEFRY 선택
   if has_prtt:  pyamt = crit_amt × prtt_rt
   else:         pyamt = crit_amt × defry_rt

⑥ 보험금 산출
   BNFT_OCURPE = trmo × bnft_rskrt          ← 급부 발생자
   BNFT_INSUAMT = BNFT_OCURPE × PYAMT       ← 최종 보험금
```

#### 출력 (OD_TBL_BN - 급부별)

| 컬럼 | 설명 |
|------|------|
| `TRMO_MTNPSN_CNT` | 급부 기시 유지자 |
| `TRME_MTNPSN_CNT` | 급부 기말 유지자 |
| `BNFT_RSKRT` | 급부 위험률 |
| `BNFT_OCURPE_CNT` | 급부 발생자 |
| `DEFRY_RT` | 면책비율 |
| `PRTT_RT` | 적립배당률 |
| `PYAMT` | 지급금액 |
| `BNFT_INSUAMT` | **최종 보험금** → CF의 INSUAMT_GEN으로 합산 |

---

### 2.6 EXP (사업비)

**모듈**: `calc/exp.py`
**입력**: `IA_E_ACQSEXP`, `IA_E_MNTEXP`, `IA_E_LOSS`, `IE_EXP_DIM`, `IE_INFL`

#### 드라이버별 기초금액 산출

| DRVR | 기초금액 산출식 | 비고 |
|------|----------------|------|
| 1 | `RATE[t] × GPREM` | t = CTR_AFT_PASS_MMCNT 월 인덱스 |
| 2 | `AMOUNT[t]` (절대금액) | PRCE_ASC=1일 때 IE_INFL 적용 |
| 4 | `고정값` | BNFT_INSUAMT 대비 비율 |
| 6 | `RATE[t] × LOAN_REMAMT` | 대출잔액 기반 |
| 9 | `RATE[t] × CNCTTP_ACUMAMT_KICS` | KICS 적립금 기반 |
| 10 | `RATE[t] × (CNCTTP - LOAN)` | KICS - 대출잔액 |

#### 물가상승 보정

```
PRCE_ASC == 1 & monthly_esc > 1 & t ≥ 2 인 경우:
  base_val *= monthly_esc^(t-1)
```

#### 시간 제약

| 유형 | 시작 조건 | 종료 조건 |
|------|----------|----------|
| ACQS (신계약비) | step ≥ 1 | `ctr_mm ≥ eprd` 또는 (`pay_dvcd == 0` & `t > pterm_mm`) |
| MNT (유지비) | — | `eym_yn == 1` & `yyyymm > eym` 또는 (`pay_dvcd == 0` & `t > pterm_mm`) |
| LSVY (손해조사비) | — | — |

#### 출력 (OD_EXP)

| 필드 | 설명 |
|------|------|
| `tpcd` | "ACQS" / "MNT" / "LSVY" |
| `kdcd` | 사업비 종류 코드 |
| `d_ind` | 직접(1) / 간접(0) |
| `values` | (n_steps,) 배열 - 월별 사업비 금액 |

---

## 3. CF (캐시플로우) 산출

**모듈**: `calc/cf.py`

CF 단계에서 기초 산출 결과를 조합하여 유입/유출 항목을 산출한다.

### 3.1 유입 항목 (Inflow)

#### PREM_BASE (기본보험료)

```
PREM_BASE[t] = CTR_TRMO[t] × ORIG_PREM × PREM_PAY_YN[t]
```

| 입력 | 출처 | 설명 |
|------|------|------|
| `CTR_TRMO` | TBL_MN | 계약자 기시 유지자수 |
| `ORIG_PREM` | TRAD_PV | 원수보험료 (상수) |
| `PREM_PAY_YN` | TRAD_PV | 납입여부 (0 또는 1) |

**의미**: 월초(기시) 시점에 유지 중인 계약자가 납입하는 보험료

---

#### PREM_PYEX (납입면제보험료)

```
PREM_PYEX[t] = (CTR_TRME[t-1] - PAY_TRME[t-1]) × ORIG_PREM[t] × PREM_PAY_YN[t]
```

| 입력 | 출처 | 설명 |
|------|------|------|
| `CTR_TRME[t-1]` | TBL_MN | 전월 기말 계약자 유지자 |
| `PAY_TRME[t-1]` | TBL_MN | 전월 기말 납입자 유지자 |
| `ORIG_PREM` | TRAD_PV | 원수보험료 |
| `PREM_PAY_YN` | TRAD_PV | 납입여부 |

**의미**: 납입면제 상태인 계약자(계약 유지 중이나 납입의무 면제)의 보험료.
계약자 유지자 - 납입자 유지자 = 납입면제 계약자수. 1월 지연(lag) 적용.

---

#### PREM_ADD (추가보험료)

```
PREM_ADD = 0    ← 현재 미구현 (전건 100% PASS)
```

---

### 3.2 유출 항목 (Outflow)

#### TMRFND (해지환급금)

```
TMRFND[t] = CTR_TRMPSN[t] × CNCTTP_ACUMAMT_KICS[t]
```

| 입력 | 출처 | 설명 |
|------|------|------|
| `CTR_TRMPSN` | TBL_MN | 해지자수 (기시 유지자 × 해지율) |
| `CNCTTP_ACUMAMT_KICS` | TRAD_PV | KICS 기준 적립금 |

**의미**: 해지한 계약자에게 돌려주는 환급금. KICS 적립금 기준으로 산출.

**KICS 적립금 구조**:
```
CNCTTP_ACUMAMT_KICS = (SOFF_AF_TMRFND + PRPD_PREM) × CTR_TRME
```

---

#### DRPO_PYRV (적립금탈퇴 지급금)

```
DRPO_PYRV[t] = CTR_RSVAMT_DEFRY_DRPSN[t] × APLY_PREM_ACUMAMT_BNFT[t]
```

| 입력 | 출처 | 설명 |
|------|------|------|
| `CTR_RSVAMT_DEFRY_DRPSN` | TBL_MN | 적립금 탈퇴자수 |
| `APLY_PREM_ACUMAMT_BNFT` | TRAD_PV | 적용보험료적립금(급부용) |

**의미**: 적립금 고갈(소멸) 등으로 탈퇴하는 계약자에 대한 지급금

---

#### INSUAMT_GEN (일반보험금)

```
INSUAMT_GEN[t] = Σ(BNFT[k].BNFT_INSUAMT[t])    ← 전 급부 합산
```

| 입력 | 출처 | 설명 |
|------|------|------|
| `BNFT_INSUAMT` | TBL_BN | 각 급부별 최종 보험금 |

**의미**: 사망, 진단, 수술 등 각종 급부 사유 발생 시 지급하는 보험금의 합계

**급부별 보험금 산출 구조** (TBL_BN에서):
```
BNFT_INSUAMT = BNFT_OCURPE × PYAMT
             = (trmo × bnft_rskrt) × (crit_amt × defry_rt 또는 prtt_rt)
```

---

#### INSUAMT_MATU (만기보험금)

```
INSUAMT_MATU = 0    ← 현재 미구현 (PASS율 78.5%)
```

**잔여 이슈**: 만기보험금 로직 구현 필요 (9,033건 FAIL)

---

#### INSUAMT_HAFWAY (중도보험금)

```
INSUAMT_HAFWAY = 0    ← 현재 미구현 (PASS율 99.7%)
```

---

#### INSUAMT_PENS (연금보험금)

```
INSUAMT_PENS = 0    ← 현재 미구현 (전건 100% PASS)
```

---

#### LOSS_SVYEXP (손해조사비)

```
LOSS_SVYEXP[t] = LSVY_RATE × Σ(BNFT[k].BNFT_INSUAMT[t])
```

| 입력 | 출처 | 설명 |
|------|------|------|
| `LSVY_RATE` | EXP | 손해조사비율 (DRVR=4 고정값) |
| `BNFT_INSUAMT` | TBL_BN | 급부별 보험금 합계 |

**의미**: 보험금 지급 시 발생하는 손해조사 관련 비용. 보험금에 일정 비율을 곱하여 산출.

---

#### ACQSEXP_DR (직접 신계약비)

```
ACQSEXP_DR[t] = Σ(EXP_ACQS_item[t] × trmo[t])
```

| 입력 | 출처 | 설명 |
|------|------|------|
| `EXP_ACQS_item` | EXP | 신계약비 항목별 금액 |
| `trmo` | TBL_MN | PAY_DVCD에 따라 CTR_TRMO 또는 PAY_TRMO 선택 |

**trmo 선택 기준**:

| PAY_DVCD | trmo |
|----------|------|
| 0 (계약 기준) | CTR_TRMO |
| 1 (납입 기준) | PAY_TRMO |

**의미**: 계약 체결 시 발생하는 직접 사업비 (설계사 수당, 심사비 등).
드라이버 코드에 따라 보험료 비례, 절대금액, KICS 적립금 비례 등 다양한 산출 방식 적용.

---

#### ACQSEXP_INDR (간접 신계약비)

```
ACQSEXP_INDR = 0    ← 현재 미구현 (전건 100% PASS)
```

---

#### ACQSEXP_REDEM (상환 신계약비)

```
ACQSEXP_REDEM = 0    ← 현재 미구현 (전건 100% PASS)
```

---

#### MNTEXP_DR (직접 유지비)

```
MNTEXP_DR[t] = Σ(EXP_MNT_item[t] × trmo[t])
```

| 입력 | 출처 | 설명 |
|------|------|------|
| `EXP_MNT_item` | EXP | 유지비 항목별 금액 |
| `trmo` | TBL_MN | PAY_DVCD에 따라 CTR_TRMO 또는 PAY_TRMO 선택 |

**의미**: 계약 유지 기간 중 발생하는 직접 사업비 (유지관리비, 수금비 등)

---

#### MNTEXP_INDR (간접 유지비)

```
MNTEXP_INDR = 0    ← 현재 미구현 (전건 100% PASS)
```

---

#### IV_MGMEXP_MNTEXP_CCRFND (투자관리비 - 유지비충당금)

```
IV_MGMEXP_MNTEXP_CCRFND = 0    ← 현재 미구현 (전건 100% PASS)
```

---

#### IV_MGMEXP_CL_REMAMT (투자관리비 - 대출잔액)

```
IV_MGMEXP_CL_REMAMT = 0    ← 현재 미구현 (PASS율 91.6%)
```

---

#### HAFWDR (중도인출)

```
HAFWDR = 0    ← 현재 미구현 (전건 100% PASS)
```

---

#### LOAN 관련 항목

| 항목 | 현재 상태 | PASS율 |
|------|----------|--------|
| `LOAN_NEW` (신규대출) | 미구현 (= 0) | 100% |
| `LOAN_INT` (대출이자) | 미구현 (= 0) | 90.4% |
| `LOAN_RPAY_HAFWAY` (중도상환) | 미구현 (= 0) | 90.4% |
| `LOAN_RPAY_MATU` (만기상환) | 미구현 (= 0) | 100% |
| `LOAN_ASET` (대출자산) | 미구현 (= 0) | 0% |

---

### 3.3 CF 항목 요약표

#### 유입 (Inflow)

| 항목 | 공식 | 구현 | PASS율 |
|------|------|------|--------|
| **PREM_BASE** | `CTR_TRMO × ORIG_PREM × PREM_PAY_YN` | ✅ | 83.8% |
| **PREM_PYEX** | `(CTR_TRME[t-1] - PAY_TRME[t-1]) × ORIG_PREM × PAY_YN` | ✅ | 84.5% |
| PREM_ADD | — | ⬜ | 100% |

#### 유출 (Outflow) - 보험금

| 항목 | 공식 | 구현 | PASS율 |
|------|------|------|--------|
| **TMRFND** | `CTR_TRMPSN × CNCTTP_ACUMAMT_KICS` | ✅ | 4.8% |
| **DRPO_PYRV** | `CTR_RSVAMT_DEFRY_DRPSN × APLY_PREM_ACUMAMT_BNFT` | ✅ | 99.6% |
| **INSUAMT_GEN** | `Σ BNFT_INSUAMT` | ✅ | 99.6% |
| INSUAMT_MATU | — | ⬜ | 78.5% |
| INSUAMT_HAFWAY | — | ⬜ | 99.7% |
| INSUAMT_PENS | — | ⬜ | 100% |

#### 유출 (Outflow) - 사업비

| 항목 | 공식 | 구현 | PASS율 |
|------|------|------|--------|
| **ACQSEXP_DR** | `Σ(EXP_ACQS × trmo)` | ✅ | 79.7% |
| **MNTEXP_DR** | `Σ(EXP_MNT × trmo)` | ✅ | 0% |
| **LOSS_SVYEXP** | `LSVY_RATE × Σ BNFT_INSUAMT` | ✅ | 99.9% |
| ACQSEXP_INDR | — | ⬜ | 100% |
| ACQSEXP_REDEM | — | ⬜ | 100% |
| MNTEXP_INDR | — | ⬜ | 100% |
| IV_MGMEXP_CCRFND | — | ⬜ | 100% |
| IV_MGMEXP_CL_REMAMT | — | ⬜ | 91.6% |

#### 유출 (Outflow) - 기타

| 항목 | 공식 | 구현 | PASS율 |
|------|------|------|--------|
| HAFWDR | — | ⬜ | 100% |
| LOAN_NEW | — | ⬜ | 100% |
| LOAN_INT | — | ⬜ | 90.4% |
| LOAN_RPAY_HAFWAY | — | ⬜ | 90.4% |
| LOAN_RPAY_MATU | — | ⬜ | 100% |
| LOAN_ASET | — | ⬜ | 0% |

---

## 4. DC_RT (할인율)

**모듈**: `calc/dc_rt.py`
**입력**: `IE_DC_RT` (할인율 커브)

#### 산출 과정

```
dc_rt[0] = 0

for s = 1 to n_steps:
    rate = dc_rt_curve[s-1]              ← IE_DC_RT 커브값
    v[s] = 1 / (1 + rate)^(1/12)         ← 월할인계수

TRME_MM_DC_RT = cumprod(v)               ← 기말 누적할인계수
TRMO_MM_DC_RT[0] = 1
TRMO_MM_DC_RT[s≥1] = TRME_MM_DC_RT[s-1] ← 기시 누적할인계수
```

#### 출력 (OD_DC_RT)

| 컬럼 | 설명 | 사용처 |
|------|------|--------|
| `DC_RT` | 할인율 커브 원값 | — |
| `TRMO_MM_DC_RT` | 기시 누적할인계수 | PVCF에서 보험료/사업비 할인 |
| `TRME_MM_DC_RT` | 기말 누적할인계수 | PVCF에서 보험금/환급금 할인 |

---

## 5. PVCF (현가 캐시플로우)

**모듈**: `calc/pvcf.py`

CF의 각 항목에 할인계수를 적용하여 현재가치로 변환한다.

### 기시(TRMO) 할인 항목 — 월초 발생

월초에 발생하는 항목은 **기시 누적할인계수(TRMO_MM_DC_RT)**로 할인:

| PVCF 항목 | 산출식 |
|-----------|--------|
| PAY_PREM (PREM_BASE) | `CF.PREM_BASE × TRMO_MM_DC_RT` |
| PYEX_BNAMT (PREM_PYEX) | `CF.PREM_PYEX × TRMO_MM_DC_RT` |
| ACQSEXP_DR | `CF.ACQSEXP_DR × TRMO_MM_DC_RT` |
| MNTEXP_DR | `CF.MNTEXP_DR × TRMO_MM_DC_RT` |

### 기말(TRME) 할인 항목 — 월말 발생

월말에 발생하는 항목은 **기말 누적할인계수(TRME_MM_DC_RT)**로 할인:

| PVCF 항목 | 산출식 |
|-----------|--------|
| TMRFND | `CF.TMRFND × TRME_MM_DC_RT` |
| DRPO_PYRV | `CF.DRPO_PYRV × TRME_MM_DC_RT` |
| INSUAMT_GEN | `CF.INSUAMT_GEN × TRME_MM_DC_RT` |
| LOSS_SVYEXP | `CF.LOSS_SVYEXP × TRME_MM_DC_RT` |

### NET_CF 산출

```
수입(IN) = PAY_PREM + ADD_PAY_PREM + PYEX_BNAMT

지출(OUT) = TMRFND + DRPO_PYRV
          + INSUAMT_GEN + INSUAMT_HAFWAY + INSUAMT_MATU + INSUAMT_PENS
          + ACQSEXP_DR + ACQSEXP_INDR + ACQSEXP_REDEM
          + MNTEXP_DR + MNTEXP_INDR
          + IV_MGMEXP_CCRFND + IV_MGMEXP_CL_REMAMT
          + LOSS_SVYEXP + HAFWDR

NET_CF = IN - OUT

ICL_NET_CF = NET_CF - LOAN_NEW - LOAN_INT + LOAN_RPAY_HAFWAY + LOAN_RPAY_MATU
```

---

## 6. BEL (최선추정부채)

**모듈**: `calc/bel.py`

PVCF의 전 시점을 합산하여 최선추정부채를 산출한다.

```
BEL_항목 = Σ(PVCF[항목][0 : n_steps])
```

| BEL 항목 | 산출식 |
|----------|--------|
| BEL_PREM_BASE | `Σ PVCF.PAY_PREM` |
| BEL_PREM_PYEX | `Σ PVCF.PYEX_BNAMT` |
| BEL_TMRFND | `Σ PVCF.TMRFND` |
| BEL_DRPO_PYRV | `Σ PVCF.DRPO_PYRV` |
| BEL_INSUAMT_GEN | `Σ PVCF.INSUAMT_GEN` |
| BEL_ACQSEXP_DR | `Σ PVCF.ACQSEXP_DR` |
| BEL_MNTEXP_DR | `Σ PVCF.MNTEXP_DR` |
| BEL_LOSS_SVYEXP | `Σ PVCF.LOSS_SVYEXP` |
| **BEL** | `Σ PVCF.NET_CF_AMT` |
| **LOAN_ASET** | `Σ PVCF.ICL_NET_CF_AMT` |

#### BEL 부호 해석

| BEL 값 | 의미 |
|--------|------|
| BEL > 0 | 유출 > 유입 → 부채 (추가 준비금 필요) |
| BEL < 0 | 유입 > 유출 → 자산 (수익 발생) |
| BEL = 0 | 유입 = 유출 → 균형 |

---

## 7. 핵심 개념 정리

### 유지자수 체계

| 항목 | 기호 | 의미 | 산출식 |
|------|------|------|--------|
| 기말 유지자 | TRME | t월말 생존자 | `cumprod(1 - 탈퇴율)` |
| 기시 유지자 | TRMO | t월초 생존자 | `TRME[t-1]`, TRMO[0]=0 |
| 해지자 | TRMPSN | t월 해지 건수 | `TRMO[t] × 해지율[t]` |

### 탈퇴 분해

| 유형 | 설명 | CF 항목 |
|------|------|---------|
| 해지 (TRMNAT) | 계약자 자발적 해지 | TMRFND |
| 적립금 탈퇴 (RSVAMT_DRPSN) | 적립금 고갈로 소멸 | DRPO_PYRV |
| 급부 탈퇴 (BNFT_DRPSN) | 보험사고로 보험금 지급 후 소멸 | INSUAMT_GEN |

### 기시 vs 기말 할인

| 구분 | 발생 시점 | 할인계수 | 해당 항목 |
|------|----------|----------|----------|
| 기시 (TRMO) | 월초 | TRMO_MM_DC_RT | 보험료, 사업비 |
| 기말 (TRME) | 월말 | TRME_MM_DC_RT | 보험금, 환급금, 손해조사비 |

### 데이터 흐름도

```
┌─────────────────────────────────────────────────────────────────┐
│                        입력 데이터 (DuckDB)                       │
├────────────┬────────────┬────────────┬────────────┬─────────────┤
│ IR_RSKRT   │ IA_T_      │ IP_P_COV   │ IA_A_BNFT  │ IE_EXP_DIM  │
│ _VAL       │ TRMNAT     │ IP_P_ACUM  │ _INFO      │ IA_E_*      │
│ (위험률)    │ (해지율)    │ (보험료)    │ (급부정보)  │ (사업비)     │
└─────┬──────┴─────┬──────┴─────┬──────┴─────┬──────┴──────┬──────┘
      │            │            │            │             │
      ▼            ▼            │            │             │
  ┌────────┐  ┌──────────┐     │            │             │
  │ RSK_RT │  │ LAPSE_RT │     │            │             │
  │ (위험률)│  │ (해지율)  │     │            │             │
  └───┬────┘  └────┬─────┘     │            │             │
      │            │            │            │             │
      ▼            ▼            │            │             │
  ┌──────────────────────┐     │            │             │
  │      TBL_MN          │     │            │             │
  │ (유지자수/탈퇴자)      │     │            │             │
  └──────────┬───────────┘     │            │             │
             │                  │            │             │
             ▼                  ▼            │             │
         ┌──────────────────────────┐       │             │
         │       TRAD_PV            │       │             │
         │ (보험료/적립금/환급금)      │       │             │
         └──────────┬───────────────┘       │             │
                    │                        │             │
                    ▼                        ▼             ▼
              ┌───────────┐          ┌────────────┐ ┌──────────┐
              │  TBL_BN   │          │    EXP     │ │  DC_RT   │
              │(급부보험금) │          │  (사업비)   │ │ (할인율)  │
              └─────┬─────┘          └─────┬──────┘ └────┬─────┘
                    │                      │              │
                    ▼                      ▼              │
              ┌──────────────────────────────────┐       │
              │              CF                   │       │
              │     (캐시플로우 조합)               │       │
              │  유입: PREM_BASE, PREM_PYEX       │       │
              │  유출: TMRFND, DRPO, INSUAMT,     │       │
              │        ACQSEXP, MNTEXP, LOSS      │       │
              └───────────────┬──────────────────┘       │
                              │                           │
                              ▼                           ▼
                        ┌──────────────────────────────────┐
                        │            PVCF                   │
                        │    (현가 캐시플로우)                │
                        │  기시 할인: 보험료, 사업비           │
                        │  기말 할인: 보험금, 환급금           │
                        └───────────────┬──────────────────┘
                                        │
                                        ▼
                                  ┌───────────┐
                                  │    BEL    │
                                  │(최선추정부채)│
                                  └───────────┘
```

---

## 부록: 소스 파일 목록

| 파일 | 역할 |
|------|------|
| `cf_module/calc/tbl_rsk_rt.py` | 위험률 산출 |
| `cf_module/calc/tbl_lapse_rt.py` | 해지율 산출 |
| `cf_module/calc/tbl_mn.py` | 주테이블 (유지자수/탈퇴자) |
| `cf_module/calc/trad_pv.py` | 보험료/적립금/환급금 |
| `cf_module/calc/tbl_bn.py` | 급부별 보험금 |
| `cf_module/calc/exp.py` | 사업비 |
| `cf_module/calc/cf.py` | 캐시플로우 조합 |
| `cf_module/calc/dc_rt.py` | 할인율 |
| `cf_module/calc/pvcf.py` | 현가 캐시플로우 |
| `cf_module/calc/bel.py` | 최선추정부채 |
| `cf_module/run.py` | 단건 파이프라인 실행 |
| `run_batch_bel.py` | 전건 배치 BEL 산출 |

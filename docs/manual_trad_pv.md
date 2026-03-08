# OD_TRAD_PV 산출 엔진 매뉴얼

> 최종 갱신: 2026-03-08
> 검증 결과: **42,000건 × 43컬럼 ALL PASS** (전체 CTR_TPCD: 0, 1, 3, 5, 9)

---

## 목차

1. [개요](#1-개요)
2. [아키텍처](#2-아키텍처)
3. [데이터 캐시 (TradPVDataCache)](#3-데이터-캐시-tradpvdatacache)
4. [ContractInfo 구조체](#4-contractinfo-구조체)
5. [계산 흐름 (STEP 1~7)](#5-계산-흐름-step-17)
6. [STEP 1: 보험료](#6-step-1-보험료)
7. [STEP 2: 미경과보험료 (PRPD)](#7-step-2-미경과보험료-prpd)
8. [STEP 3: 이율 배열](#8-step-3-이율-배열)
9. [STEP 4: 적립금](#9-step-4-적립금)
10. [STEP 5: 환급금 (SOFF / LTRMNAT)](#10-step-5-환급금-soff--ltrmnat)
11. [STEP 6: KICS](#11-step-6-kics)
12. [STEP 7: 약관대출](#12-step-7-약관대출)
13. [후처리: SOFF_AF Netting](#13-후처리-soff_af-netting)
14. [배치 파이프라인](#14-배치-파이프라인)
15. [성능 최적화](#15-성능-최적화)
16. [사용법](#16-사용법)
17. [파일 구조](#17-파일-구조)

---

## 1. 개요

OD_TRAD_PV는 전통형 보험상품의 **준비금·보험료·환급금** 시계열을 산출하는 엔진이다.

- **입력**: `duckdb_transform.duckdb` (75개 테이블, 42,000건)
- **출력**: 계약별 43개 컬럼 × n_steps 시점 시계열
- **의존**: OD_TBL_MN (유지자수/납입자수) — Phase B에서 사전 산출

### 산출 테이블 매핑

| OD_TRAD_PV 컬럼 | 산출 함수 | STEP |
|-----------------|----------|------|
| CTR_AFT_PASS_MMCNT | `_calc_premium` | 1 |
| PREM_PAY_YN | `_calc_premium` | 1 |
| ORIG_PREM, DC_PREM | `_calc_premium` | 1 |
| ACUM_NPREM | `_calc_premium` | 1 |
| PAD_PREM | `_calc_premium` | 1 |
| ACQSEXP1_BIZEXP, ACQSEXP2_BIZEXP | `_calc_premium` | 1 |
| AFPAY_MNTEXP, LUMPAY_BIZEXP, PAY_GRCPR_ACQSEXP | `_calc_premium` | 1 |
| ACUM_NPREM_PRPD | `_calc_prpd_acum` | 2 |
| PRPD_MMCNT | `_calc_prpd_mmcnt` | 2 |
| PRPD_PREM | `_calc_prpd_prem` | 2 |
| APLY_PUBANO_INRT | `_build_pubano_inrt_arr` | 3 |
| YSTR_RSVAMT, YYEND_RSVAMT | `_calc_accumulation` | 4 |
| YSTR_RSVAMT_TRM, YYEND_RSVAMT_TRM | = YSTR_RSVAMT, YYEND_RSVAMT | 4 |
| APLY_PREM_ACUMAMT_BNFT, APLY_PREM_ACUMAMT_EXP | `_calc_accumulation` | 4 |
| APLY_ADINT_TGT_AMT | `_calc_accumulation` | 4 |
| LWST_ADINT_TGT_AMT, LWST_PREM_ACUMAMT | `_calc_accumulation` | 4 |
| SOFF_BF_TMRFND | `_calc_surrender` | 5 |
| SOFF_AF_TMRFND | `_calc_surrender` + netting | 5 |
| LTRMNAT_TMRFND | `_calc_surrender` | 5 |
| CNCTTP_ACUMAMT_KICS | `compute_trad_pv` | 6 |
| LOAN_INT, LOAN_REMAMT, LOAN_RPAY_HAFWAY | `_calc_loan` | 7 |
| 0-컬럼 13개 (ADD_*, PENS_*, HAFWAY_*, etc.) | — | — |

---

## 2. 아키텍처

```
┌───────────────────────────────────────────────────────────────┐
│                   duckdb_transform.duckdb                      │
│                                                                │
│  소스 테이블 (12개)          산출 테이블                        │
│  ────────────────           ────────────                       │
│  II_INFRC                   OD_TBL_MN  (Phase B)               │
│  II_RSVAMT_BAS              OD_TRAD_PV (Phase C) ← 본 엔진     │
│  IP_P_ACUM_COV                                                 │
│  IP_P_EXPCT_INRT                                               │
│  IP_P_EXPCT_BIZEXP_CMPT_CRIT                                   │
│  IP_P_EXPCT_BIZEXP_RT                                          │
│  IP_P_PROD                                                     │
│  IP_P_LTRMNAT                                                  │
│  IE_PUBANO_INRT                                                │
│  IE_DC_RT                                                      │
│  IA_A_CTR_LOAN                                                 │
│  IA_M_PROD_GRP                                                 │
└───────────────────────────────────────────────────────────────┘

Phase A: ETL → Phase B: OD_TBL_MN → Phase C: OD_TRAD_PV
                (v2 engine.py)         (trad_pv.py) ← 본 문서
```

---

## 3. 데이터 캐시 (TradPVDataCache)

**파일**: `cf_module/data/trad_pv_loader.py`
**클래스**: `TradPVDataCache`

DB에서 12개 테이블을 메모리에 일괄 로드한다 (약 1.3초).

| # | 메서드 | 테이블 | 속성 | 키 구조 |
|---|--------|--------|------|---------|
| 1 | `_load_infrc` | II_INFRC | `self.infrc` | `{idno: dict}` |
| 2 | `_load_rsvamt_bas` | II_RSVAMT_BAS | `self.rsvamt_bas` | `{idno: {crit, nprem, ystr[120], yyend[120]}}` |
| 3 | `_load_acum_cov` | IP_P_ACUM_COV | `self.acum_cov_exact`, `self.acum_cov_fallback` | `{(prod,cov,cls): dict}` / `{(prod,cls): dict}` |
| 4 | `_load_expct_inrt` | IP_P_EXPCT_INRT | `self.expct_inrt` | `{(prod,cov,cls): {expct_inrt, std_inrt}}` |
| 5 | `_load_bizexp_cmpt_crit` | IP_P_EXPCT_BIZEXP_CMPT_CRIT | `self.bizexp_cmpt_crit` | `{(prod,cov,cls): {5개 YN 플래그}}` |
| 6 | `_load_bizexp_rt` | IP_P_EXPCT_BIZEXP_RT | `self.bizexp_rt` | `{(prod,cov,cls): [dict, ...]}` |
| 7 | `_load_pubano_inrt` | IE_PUBANO_INRT | `self.pubano_inrt` | `{inrt_cd: {adj_rt, iv_adexp_rt, ...}}` |
| 8 | `_load_dc_rt` | IE_DC_RT | `self.dc_rt_curve` | `np.ndarray[1440]` |
| 9 | `_load_loan_tables` | IA_M_PROD_GRP + IA_A_CTR_LOAN | `self.prod_grp`, `self.ctr_loan` | 계층 lookup |
| 10 | `_load_prod_loan_tpcd` | IP_P_PROD | `self.prod_loan_tpcd` | `{(prod,cls): loan_tpcd}` |
| 11 | `_load_ltrmnat` | IP_P_LTRMNAT | `self.ltrmnat` | `{(prod,cls,tpcd,pay_stcd): np.ndarray[20]}` |

### IP_P_LTRMNAT 상세

환급금 비율 테이블. 경과년 1~20별 비율을 저장.

```sql
SELECT PROD_CD, CLS_CD, CTR_TPCD, PAY_STCD, TMRFND_RT1, ..., TMRFND_RT20
FROM IP_P_LTRMNAT WHERE CTR_TPCD_YN = 1
```

**키**: `(PROD_CD, CLS_CD(zfill2), CTR_TPCD(str), PAY_STCD)`
**값**: `np.float64[20]` (경과년별 SOFF 비율)

**TPCD별 비율 예시**:

| CTR_TPCD | PAY_STCD=1 (납입중) | PAY_STCD=2 (납입후) | 설명 |
|----------|-------------------|-------------------|------|
| 0 | 1.0 | 1.0 | 일반형 |
| 1 | **0.0** (무저해지) | 1.0 | 무저해지 상품 |
| 3 | 0.3 | 1.0 | 30% 환급형 |
| 5 | 0.5 | 1.0 | 50% 환급형 |
| 미등록 | 1.0 (기본값) | 1.0 (기본값) | IP_P_LTRMNAT에 없는 경우 |

### `get_soff_rate(prod_cd, cls_cd, ctr_tpcd, pay_stcd)`

4-튜플로 조회. 없으면 `None` 반환 → 호출자가 기본값(1.0) 사용.

### `build_contract_info_cached(cache, idno)`

캐시에서 `ContractInfo` 구조체를 조립. 주요 분기:

1. **BAS 보유**: `rsvamt_bas[idno]` 존재 → `info.bas` 설정
2. **BAS 미보유**: 사업비율(`bizexp_rt`)로 `acum_nprem_nobas` 산출
   - `ETC_KEY` 시작='1': LOSS만 차감
   - `brt_full` 존재: `ACCMPT × (1 - MNTEXP - LOSS)` + alpha 상각
   - 없음: ACCMPT 그대로
3. **SOFF 비율**: `get_soff_rate(prod, cls, tpcd, 1)` / `..., 2)` → paying/paidup 배열

---

## 4. ContractInfo 구조체

**파일**: `cf_module/calc/trad_pv.py` (L47-82)

```python
@dataclass
class ContractInfo:
    # === 필수 ===
    idno: int               # INFRC_IDNO
    prod_cd: str            # 상품코드
    cov_cd: str             # 담보코드
    cls_cd: str             # 종코드
    ctr_tpcd: str           # 계약유형 ('0','1','3','5','9')
    pass_yy: int            # 경과년수
    pass_mm: int            # 경과월수 (연 내)
    bterm_yy: int           # 보장기간(년)
    pterm_yy: int           # 납입기간(년)
    gprem: float            # 영업보험료
    join_amt: float         # 가입금액

    # === 선택 ===
    pay_stcd: int = 1               # 1=납입중, 2=완납, 3=면제
    paycyc: int = 1                 # 1=월납, 3=분기, 6=반기, 12=연납
    prem_dc_rt: float = 0.0         # 보험료 할인율
    acqsexp1: float = 0.0           # 신계약비 (TOT_TRMNAT_DDCT_AMT)

    # === BAS 보유 시 ===
    bas: Optional[dict] = None      # {crit_join_amt, nprem, ystr[120], yyend[120]}

    # === BAS 미보유 시 (이율 기반 부리) ===
    acum_nprem_nobas: float = 0.0   # 적립순보험료
    acum_nprem_old: float = 0.0     # 상각기간 내 NPREM
    amort_mm: int = 0               # 상각기간(월)
    accmpt_rspb_rsvamt: float = 0.0 # 적립책임준비금 초기값
    acum_cov: Optional[dict] = None # IP_P_ACUM_COV 참조
    expct_inrt_data: Optional[dict] = None  # 예정이율
    pubano_params: Optional[dict] = None    # 공시이율 파라미터
    dc_rt_curve: Optional[np.ndarray] = None  # 할인율 커브

    # === 약관대출 ===
    ctr_loan_remamt: float = 0.0
    ctr_loan_tpcd: int = 1          # 0=대출불가
    loan_params: Optional[dict] = None

    # === SOFF 비율 (IP_P_LTRMNAT) ===
    soff_rates_paying: Optional[np.ndarray] = None   # PAY_STCD=1
    soff_rates_paidup: Optional[np.ndarray] = None   # PAY_STCD=2
```

---

## 5. 계산 흐름 (STEP 1~7)

**파일**: `cf_module/calc/trad_pv.py` — `compute_trad_pv()` (L611)

```
입력: ContractInfo + n_steps + OD_TBL_MN(pay_trmo, ctr_trmo, ctr_trme)

STEP 1: 보험료           ← 독립
STEP 2: 미경과보험료      ← STEP 1
STEP 3: 이율 배열        ← 독립
STEP 4: 적립금           ← STEP 1 + STEP 3 + OD_TBL_MN
STEP 5: 환급금           ← STEP 1 + STEP 4
STEP 6: KICS             ← STEP 2 + STEP 5
STEP 7: 약관대출          ← STEP 3

후처리: CTR_POLNO netting  ← STEP 5 + STEP 6
```

**의존 그래프**:

```
     STEP 1 (보험료)
    ╱     ╲
STEP 2    STEP 4 ← STEP 3 (이율)
    ╲     ╱             ╲
    STEP 5             STEP 7 (대출)
      │
    STEP 6 (KICS)
      │
    netting (후처리)
```

---

## 6. STEP 1: 보험료

**함수**: `_calc_premium(info, n_steps)` (L200)

### 산출 항목

| 변수 | 공식 |
|------|------|
| `ctr_mm` | `경과년×12 + 경과월 + t` (1-based 계약후경과월) |
| `prem_pay_yn` | `1 if ctr_mm ≤ pterm_mm else 0` (pay_stcd≠1이면 보정) |
| `orig_prem` | `gprem × prem_pay_yn` |
| `dc_prem` | `gprem × prem_dc_rt × prem_pay_yn` |
| `pad_prem` | `gprem × cumsum(prem_pay_yn)` + 초기납입분 |

### ACUM_NPREM (적립순보험료)

- **BAS 보유**: `bas["nprem"] / bas["crit_join_amt"] × join_amt × prem_pay_yn`
- **BAS 미보유 + 상각**: `CTR_MM ≤ amort_mm → nprem_old`, `else → nprem_new` × prem_pay_yn
- **BAS 미보유 (기본)**: `nprem_nobas × prem_pay_yn`

### 사업비

사업비 키매칭: `BIZEXP_CMPT_CRIT` 5개 YN 플래그 기반 동적 매칭.

| 컬럼 | 내용 |
|------|------|
| ACQSEXP1_BIZEXP | 신계약비 1 (연도 기반 조건부) |
| ACQSEXP2_BIZEXP | 신계약비 2 |
| AFPAY_MNTEXP | 납입후 유지비 |
| LUMPAY_BIZEXP | 일시납 사업비 |
| PAY_GRCPR_ACQSEXP | 유예 신계약비 |

---

## 7. STEP 2: 미경과보험료 (PRPD)

**함수**: `_calc_prpd_mmcnt`, `_calc_prpd_prem`, `_calc_prpd_acum` (L443~)

- **paycyc=12(연납)만** 해당, 나머지 0
- `prpd_mmcnt`: 연납 시 미경과월수 (최대 11)
- `prpd_prem`: `orig_prem × prpd_mmcnt / 12`
- `acum_nprem_prpd`: `acum_nprem × prpd_mmcnt / 12`

---

## 8. STEP 3: 이율 배열

### APLY_PUBANO_INRT (적용공시이율)

**함수**: `_build_pubano_inrt_arr(info, n_steps)` (L390)

```
if APLY_INRT_CD == '00':
    pubano = EXPCT_INRT  (예정이율 고정)
elif acum_cov 없음:
    pubano = EXPCT_INRT  (폴백)
else:
    pubano[t] = (EXT_WGHT × EXT_ITR
                 + (DC_RT[t] - IV_ADEXP_RT) × (1 - EXT_WGHT))
                × ADJ_RT
    pubano = max(pubano, LWST_GRNT_INRT)  (최저보증 floor)
```

### LWST_GRNT_INRT (최저보증이율)

**함수**: `_build_lwst_grnt_inrt_arr(info, n_steps)` (L357)

경과년수 기반 변동 (chng_cd별):
- `chng_cd=1`: 연도별 단계적 변동 (chng_yycnt 기준)
- `chng_cd=2`: 일시 변동
- 기타: 단일값

---

## 9. STEP 4: 적립금

**함수**: `_calc_accumulation(info, n_steps, ...)` (L277)

### 경로 A: BAS 보유 (32,962건)

**선형 보간**:

```python
ins_year = (CTR_MM - 1) // 12 + 1      # 보험연도 (1-based)
month_in_year = CTR_MM - (ins_year - 1) * 12  # 연 내 월 (1~12)
yr_idx = ins_year - 1                    # 0-based 인덱스

mult = join_amt / crit_join_amt          # 배수

ystr_rsvamt  = bas_ystr[yr_idx] × mult
yyend_rsvamt = bas_yyend[yr_idx] × mult

ACUM = ystr + (yyend - ystr) × month_in_year / 12
```

- `yr_idx ≥ 120`: 0 처리
- `APLY_PREM_ACUMAMT_BNFT = APLY_PREM_ACUMAMT_EXP = ACUM`
- `YSTR_RSVAMT_TRM = YSTR_RSVAMT`, `YYEND_RSVAMT_TRM = YYEND_RSVAMT`

### 경로 B: BAS 미보유 — 이율 기반 부리 (9,038건)

**함수**: `_compute_acum_interest_based(...)` (L733)

```python
V = ACCMPT_RSPB_RSVAMT  # 초기값

ADINT[0] = V
ACUM[0]  = V

for t in range(1, n_steps):
    cm = ctr_mm[t]

    if V < 0:
        # 음수 적립금: 부리 없음
        ADINT[t] = V
        ACUM[t]  = V
        continue

    if cm % 12 == 1:  # 연도 경계
        base = ACUM[t-1]
        cum_int = 0
    else:
        base = ADINT[t-1]

    # NPREM 비율 (OD_TBL_MN)
    ratio = PAY_TRMO[t] / CTR_TRMO[t]  if CTR_TRMO[t] > 0

    is_pay_month = (PAY_STCD == 1) and (cm ≤ pterm_mm)
    if is_pay_month:
        ADINT[t] = base + NPREM × ratio
    else:
        ADINT[t] = base

    cum_int += ADINT[t] × INRT / 12
    ACUM[t] = ADINT[t] + cum_int
```

**핵심 규칙**:
- `V < 0`: 부리 없음 (ADINT=V, ACUM=V 고정)
- 연도 경계(`cm%12==1`): base = prev_ACUM, cum_int 리셋
- 연도 내: base = prev_ADINT
- APLY/LWST 이율 각각 독립 산출

---

## 10. STEP 5: 환급금 (SOFF / LTRMNAT)

**함수**: `_calc_surrender(info, n_steps, ctr_mm, prem_pay_yn, aply_prem_acumamt_bnft)` (L482)

### 10.1 SOFF 비율 결정 (IP_P_LTRMNAT 기반)

```python
pterm_mm = pterm_yy × 12

if soff_rates_paying 또는 soff_rates_paidup 존재:
    # PAY_STCD 판정: cm 기반 (prem_pay_yn 아님!)
    in_pay_period = (ctr_mm ≤ pterm_mm)

    # 경과년 인덱스 (0~19)
    ins_year = ((ctr_mm - 1) // 12).clip(0, 19)

    rate = where(in_pay_period,
                 soff_rates_paying[ins_year],   # 납입중 비율
                 soff_rates_paidup[ins_year])    # 납입후 비율
else:
    rate = 1.0  # IP_P_LTRMNAT 미등록 → 기본값
```

> **중요**: PAY_STCD 판정은 `ctr_mm ≤ pterm_mm` 기준이다. `prem_pay_yn`이 아님.
> PAY_STCD=3(면제) 계약은 prem_pay_yn=0이지만 cm 기준으로는 납입기간 내이므로 올바른 비율이 적용됨.

### 10.2 SOFF_BF (소멸전환급금)

```python
SOFF_BF = ACUM × rate
```

**ACQSEXP 차감 조건** (PAY_STCD ≠ 3일 때만):

```python
apply_deduction = (
    pay_stcd ≠ 3
    AND (
        (PROD_CD ∈ DEDUCT_PRODS AND PTERM > 5)
        OR (CTR_TPCD == '0' AND ACQSEXP1 > 0)
    )
)

DEDUCT_PRODS = {LA0211Z, LA0215R, LA0215X, LA0216R, LA0216W, LA0217W}

if apply_deduction:
    remaining = max(84 - CTR_MM, 0)
    SOFF_BF -= ACQSEXP1 × remaining / 84
```

### 10.3 SOFF_AF (소멸후환급금)

```python
SOFF_AF = SOFF_BF.copy()
# → 후처리 netting에서 수정될 수 있음
```

### 10.4 LTRMNAT (해지환급금)

```python
if CTR_TPCD == '9':
    LTRMNAT = 0  (적립형 상품: 미지급)
elif ACQSEXP1 > 0 AND pay_stcd ≠ 3:
    deduction = ACQSEXP1 × max(84 - CTR_MM, 0) / 84
    LTRMNAT = max(0, ACUM - deduction)
else:
    LTRMNAT = max(0, ACUM)
```

> **변경 이력**: 기존에는 DEDUCT_PRODS 또는 TPCD='0'에만 차감 적용했으나,
> 전건 검증 결과 **모든 non-'9' TPCD**에서 ACQSEXP>0이면 차감 적용하는 것이 올바름.
> 단, PAY_STCD=3(면제)은 차감 미적용.

---

## 11. STEP 6: KICS

```python
CNCTTP_ACUMAMT_KICS = (SOFF_AF + PRPD_PREM) × CTR_TRME
```

- `CTR_TRME`: OD_TBL_MN에서 로드 (기말 유지자수)
- netting 후 SOFF_AF가 변경되면 KICS도 재산출

---

## 12. STEP 7: 약관대출

**함수**: `_calc_loan(info, n_steps, aply_pubano_inrt)` (L570)

**전제 조건**: `CTR_LOAN_TPCD ≠ 0` (대출불가 상품은 스킵)

```python
loan_remamt[0] = ctr_loan_remamt  # 초기 대출잔액
monthly_rate = pubano_inrt[0] + adintr_sum
loan_int[1]  = loan_remamt[0] / 2 × monthly_rate
loan_rpay[1] = loan_remamt[0]     # 즉시 전액 상환
```

---

## 13. 후처리: SOFF_AF Netting

**함수**: `apply_soff_af_netting(results, polno_to_idnos, ctr_trme_map, idno_to_cov)` (L830)

동일 CTR_POLNO(증번) 그룹 내에서 SOFF_BF 합이 음수인 시점에 대해 AF를 조정.

### 규칙

| 조건 | 주계약 (CLA00500) | 특약 |
|------|------------------|------|
| 그룹 SOFF_BF 합 ≥ 0 | AF = BF (변동없음) | AF = BF (변동없음) |
| 그룹 SOFF_BF 합 < 0 | **AF = 0** | **AF = max(0, BF)** |

```python
for polno, idno_list in polno_to_idnos.items():
    bf_sum = Σ(r.soff_bf_tmrfnd for r in group)
    neg_mask = (bf_sum < 0)

    for idno, r in group:
        if is_main_contract:
            r.soff_af_tmrfnd[neg_mask] = 0.0
        else:
            r.soff_af_tmrfnd[neg_mask] = max(0, r.soff_bf_tmrfnd[neg_mask])

        # KICS 재산출
        r.cncttp_acumamt_kics = (r.soff_af_tmrfnd + r.prpd_prem) × ctr_trme
```

---

## 14. 배치 파이프라인

**파일**: `cf_module/pipeline.py`
**함수**: `run_trad_pv_pipeline(con, tpcd_filter, ...)`

### 실행 흐름

```
1. TradPVDataCache 로드 (12개 테이블, ~1.3초)
2. 대상 계약 필터링 (CTR_TPCD, COV_CD 등)
3. (PROD_CD, CLS_CD) 기준 그룹핑 → 98개 그룹
4. OD_TBL_MN 일괄 로드 (SQL 2회)
5. 그룹별 계산:
   ├─ build_contract_info_cached()
   ├─ compute_trad_pv()
   └─ apply_soff_af_netting() (CTR_POLNO별 인라인)
6. 결과 반환: {idno: TradPVResult}
```

### 그룹핑 근거

- 동일 CTR_POLNO는 100% 같은 (PROD_CD, CLS_CD) 그룹에 속함
- → netting을 그룹 내에서 즉시 처리 가능
- → 그룹 간 의존 없음

---

## 15. 성능 최적화

### fast_mode (zero-copy)

`compute_trad_pv(info, n_steps, ..., fast_mode=True)`

- 0-배열 16개를 공유 참조 (`.copy()` 생략)
- `APLY_PREM_ACUMAMT_EXP = APLY_PREM_ACUMAMT_BNFT` 동일 참조
- `YSTR_RSVAMT_TRM = YSTR_RSVAMT` 동일 참조

### 벡터화 (NumPy)

5개 함수에서 Python for-loop → NumPy 벡터 연산 전환:

| 함수 | 기법 |
|------|------|
| `_calc_premium` | `np.where(ctr_mm ≤ amort_mm, old, new)` |
| `_calc_surrender` | `np.where(in_pay_period, pay_rate, paidup_rate)` |
| `_build_pubano_inrt_arr` | 배열 인덱싱 + broadcasting |
| `_build_lwst_grnt_inrt_arr` | 배열 인덱싱 + broadcasting |
| `_calc_accumulation` (BAS) | 배열 슬라이싱 + 선형보간 |

### 성능 결과

| 단계 | before | after | 개선 |
|------|--------|-------|------|
| 계산 | 52초 | 8.2초 | **-84%** |
| 캐시 | 1.3초 | 1.3초 | — |
| MN로드 | 4초 | 4초 | — |
| **총** | **57초** | **13.5초** | **-76%** |

### 향후 과제

- ProcessPoolExecutor / multiprocessing: GIL 우회 진정한 병렬화
- ThreadPoolExecutor는 GIL로 CPU-bound numpy에 비효과적 (기본 비활성)

---

## 16. 사용법

### 전체 실행

```python
import duckdb
from cf_module.pipeline import run_trad_pv_pipeline

con = duckdb.connect('duckdb_transform.duckdb', read_only=True)

# TPCD 0,9 (기본)
results, stats = run_trad_pv_pipeline(con, tpcd_filter=('0', '9'))

# 전체 TPCD
results, stats = run_trad_pv_pipeline(
    con, tpcd_filter=('0', '1', '3', '5', '9')
)

# 특정 상품만
results, stats = run_trad_pv_pipeline(
    con, prod_cls_filter=('LA0201J', '01')
)

# fast_mode
results, stats = run_trad_pv_pipeline(
    con, tpcd_filter=('0', '1', '3', '5', '9'), fast_mode=True
)

print(f"완료: {stats.computed:,}건, {stats.elapsed_total:.1f}s")
con.close()
```

### 단건 디버그

```python
from cf_module.pipeline import compute_single

result = compute_single(con, idno=625683)
d = result.to_dict()
print(d['SOFF_BF_TMRFND'][:5])
print(d['APLY_PREM_ACUMAMT_BNFT'][:5])
```

### 검증 실행

```bash
# 전체 42K건 검증
python test_trad_pv_all.py

# 특정 COV만
python test_trad_pv_all.py --cov CLA10007

# 결과 파일 저장
python test_trad_pv_all.py --save
```

---

## 17. 파일 구조

```
cf_module/
├── pipeline.py                 # 통합 파이프라인 (run_trad_pv_pipeline)
├── calc/
│   └── trad_pv.py             # 핵심 산출 (compute_trad_pv, STEP 1~7)
│                              #   L47: ContractInfo
│                              #   L85: TradPVResult
│                              #   L200: _calc_premium
│                              #   L277: _calc_accumulation
│                              #   L357: _build_lwst_grnt_inrt_arr
│                              #   L390: _build_pubano_inrt_arr
│                              #   L443: _calc_prpd_mmcnt
│                              #   L460: _calc_prpd_prem
│                              #   L471: _calc_prpd_acum
│                              #   L482: _calc_surrender ★
│                              #   L570: _calc_loan
│                              #   L611: compute_trad_pv
│                              #   L733: _compute_acum_interest_based
│                              #   L830: apply_soff_af_netting
├── data/
│   └── trad_pv_loader.py      # 데이터 캐시 (TradPVDataCache)
│                              #   L36: __init__ (12개 테이블 로드)
│                              #   L307: _load_ltrmnat ★
│                              #   L327: get_soff_rate ★
│                              #   L359: build_contract_info_cached
├── v2/
│   ├── engine.py              # OD_TBL_MN 산출 (Phase B)
│   ├── etl.py                 # Legacy DB → DuckDB 변환
│   └── schema.py              # Star Schema DDL
└── utils/
    └── logger.py

테스트/
├── test_trad_pv_all.py        # 전체 42K건 검증
├── test_trad_pv_single.py     # 단건 상세
├── test_trad_pv_full.py       # CLA00500 전수
└── test_trad_pv_module.py     # 모듈 테스트
```

★ = TPCD 0,9 이외 케이스의 핵심 로직

# CF 프로젝션 계산 프로세스

## 전체 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                    duckdb_transform.duckdb                      │
│                                                                 │
│  소스 테이블           참조 테이블            산출 테이블        │
│  ─────────           ─────────            ─────────            │
│  II_INFRC            IP_P_ACUM_COV        OD_TBL_MN            │
│  II_RSVAMT_BAS       IP_P_EXPCT_INRT      OD_TRAD_PV           │
│  IR_RSKRT_CHR        IP_P_EXPCT_BIZEXP_RT OD_TBL_BN            │
│  IR_RSKRT_VAL        IP_P_PROD                                 │
│  IA_T_TRMNAT_RT      IE_PUBANO_INRT                            │
│  IA_T_SKEW           IE_DC_RT                                  │
│  IA_R_BEPRD_DEFRY_RT IA_A_CTR_LOAN                             │
│  IP_R_RSKRT_C        IA_M_PROD_GRP                             │
│  IP_R_COV_RSKRT_C                                              │
│  IP_R_BNFT_RSKRT_C                                             │
└─────────────────────────────────────────────────────────────────┘
```

### 계산 파이프라인 (3단계)

```
Phase A: ETL                    Phase B: OD_TBL_MN              Phase C: OD_TRAD_PV
─────────────                   ──────────────                  ──────────────
VSOLN*.vdb (Legacy)             v2 Star Schema                  OD_TBL_MN + 참조 테이블
   ↓ migrate_legacy_db()           ↓ project_group()               ↓ compute_trad_pv()
duckdb_transform.duckdb         ProjectionResultV2              TradPVResult
                                   ↓                               ↓
                                OD_TBL_MN 저장                  OD_TRAD_PV 저장
```

---

## Phase B: OD_TBL_MN 산출 (v2 프로젝션 엔진)

### 모듈: `cf_module/v2/engine.py`

위험률 중복제거 + CTR/PAY 유지자수 + 탈퇴자 분해를 벡터 연산으로 수행.

### 계산 흐름

```
1. 시간축 준비
   duration_years = month // 12 + 1
   age = entry_age + month // 12
   is_paying = duration_months < pterm_months  (strict <)

2. 위험률 추출 (n_risks × n_contracts × max_t)
   S타입: 스칼라 확장
   A타입: 연령 인덱싱 (age clipping)
   BEPRD: 연도 단위 인덱싱, 범위 초과 시 마지막 값 연장
   월변환: 1-(1-q)^(1/12)

3. 해지율 로드 (n_contracts × max_t)
   paying: 계약 시작 기준 경과연수
   paidup: 납입후 시작 기준 상대 경과
   월변환: 1-(1-q)^(1/12)

4. 중복제거 (einsum 기반)
   C행렬: Cij = 0 if (i=j) or (동일위험그룹) or (j=사망위험)
   공식: q'ᵢ = qᵢ × (1 - Σⱼ(qⱼ × Cᵢⱼ) / 2)
   CTR/PAY 분리 적용

5. tpx 계산
   dx = Σ(qx_adjusted) + wx
   tpx = cumprod(1 - dx)
   tpx_bot[t] = tpx[t-1]  (기시 유지자)

6. 탈퇴자 분해
   d_rsvamt = tpx_bot × Σ(qx_rsv)       (CTR 기시 기준)
   d_bnft   = tpx_bot × Σ(qx_bnft)      (CTR 기시 기준)
   d_pyexsp = pay_tpx_bot × Σ(qx_pyexsp) (PAY 기시 기준)
```

### OD_TBL_MN 산출 컬럼 매핑

| OD_TBL_MN 컬럼 | v2 변수 | 설명 |
|----------------|---------|------|
| CTR_TRME_MTNPSN_CNT | tpx | CTR 기말 유지자수 |
| CTR_TRMO_MTNPSN_CNT | tpx_bot | CTR 기시 유지자수 |
| PAY_TRME_MTNPSN_CNT | pay_tpx | PAY 기말 유지자수 |
| PAY_TRMO_MTNPSN_CNT | pay_tpx_bot | PAY 기시 유지자수 |
| CTR_TRMNAT_RT | wx_ctr | CTR 중복제거 해약률 |
| PAY_TRMNAT_RT | wx_pay | PAY 중복제거 해약률 |
| CTR_RSVAMT_DEFRY_DRPO_RSKRT | d_rsvamt/tpx_bot | 준비금 탈퇴율 |
| CTR_BNFT_DRPO_RSKRT | d_bnft/tpx_bot | 급부 탈퇴율 |
| PYEXSP_DRPO_RSKRT | d_pyexsp/pay_tpx_bot | 납면 탈퇴율 |
| CTR_TRMPSN_CNT | tpx_bot × wx_ctr | CTR 해약자수 |
| CTR_RSVAMT_DEFRY_DRPSN_CNT | d_rsvamt | 준비금 탈퇴자수 |
| CTR_BNFT_DEFRY_DRPSN_CNT | d_bnft | 급부 탈퇴자수 |
| PAY_TRMPSN_CNT | pay_tpx_bot × wx_pay | PAY 해약자수 |
| PYEXSP_DRPSN_CNT | d_pyexsp | 납면 탈퇴자수 |

### 중복제거 위험률 (핵심)

**탈퇴 위험률 분류:**
- CTR exit: `is_exit = RSVAMT_YN | BNFT_YN`
- PAY exit: `is_exit = RSVAMT_YN | BNFT_YN | PYEXSP_YN`

**C행렬 규칙 (Cij = 0인 경우):**
1. 자기자신 (i = j)
2. 동일위험그룹 (RSK_GRP_NO 동일)
3. j가 사망위험 (DEAD_RT_DVCD = 0)

---

## Phase C: OD_TRAD_PV 산출 (전통 준비금 엔진)

### 모듈: `cf_module/calc/trad_pv.py`

### 계산 흐름 (STEP 1~7, 의존성 순서)

```
입력: ContractInfo + OD_TBL_MN (pay_trmo, ctr_trmo, ctr_trme)

STEP 1: 보험료                ← 독립
  ├─ ctr_mm (계약후경과월)
  ├─ prem_pay_yn (납입여부)
  ├─ orig_prem, dc_prem (원수/할인보험료)
  ├─ acum_nprem (적립순보험료)
  ├─ pad_prem (기납입보험료, 누적)
  └─ acqsexp1 (신계약비)

STEP 2: 미경과보험료           ← STEP 1, KICS에 선행
  ├─ prpd_mmcnt (미경과월수)
  ├─ prpd_prem (미경과보험료)
  └─ acum_nprem_prpd

STEP 3: 이율 배열              ← 독립, 적립금 부리에 선행
  ├─ pubano_inrt (적용공시이율)
  │  ├─ acum_cov 있음: INRT_CD별 공식 or 예정이율
  │  └─ acum_cov 없음: EXPCT_INRT 폴백
  └─ lwst_grnt_inrt (최저보증이율)
     └─ 경과년수 기반 변동 (chng_cd)

STEP 4: 적립금                 ← STEP 1 + STEP 3
  ├─ BAS 보유: 연시/연말 선형보간
  │  └─ ACUM = ystr + (yyend - ystr) × month_in_year / 12
  ├─ BAS 미보유: 이율 기반 부리
  │  ├─ t=0: ADINT = V, ACUM = V
  │  ├─ 연도 경계: base = prev_ACUM, cum_int = 0
  │  ├─ 연도 내: base = prev_ADINT
  │  ├─ ADINT = base + NPREM × (PAY_TRMO/CTR_TRMO)  ← OD_TBL_MN 사용
  │  └─ ACUM = ADINT + cum_int
  ├─ ystr_rsvamt, yyend_rsvamt
  ├─ aply_prem_acumamt_bnft
  ├─ aply_adint_tgt_amt (부리대상금액)
  └─ lwst_prem_acumamt (최저보험료적립금)

STEP 5: 환급금                 ← STEP 1 + STEP 4
  ├─ soff_bf_tmrfnd (소멸전환급금)
  │  ├─ CTR_TPCD별 납입중 비율: '3'→0.3, '5'→0.5, else→1.0
  │  └─ ACQSEXP 차감 (DEDUCT_PRODS + PTERM>5, 또는 TPCD='0')
  ├─ soff_af_tmrfnd (소멸후환급금)
  │  └─ 초기값 = soff_bf (netting 후 변경)
  └─ ltrmnat_tmrfnd (해지환급금)
     └─ TPCD='9' → 0, else → max(0, ACUM - deduction)

STEP 6: KICS                  ← STEP 2 + STEP 5
  └─ cncttp_acumamt_kics = (SOFF_AF + PRPD_PREM) × CTR_TRME

STEP 7: 약관대출               ← STEP 3
  ├─ CTR_LOAN_TPCD=0 → 대출불가 (스킵)
  ├─ loan_remamt[0] = 초기잔액, [t>=1] = 0
  ├─ loan_int[1] = remamt/2 × monthly_rate
  └─ loan_rpay[1] = remamt (즉시 전액 상환)

후처리: CTR_POLNO netting       ← STEP 5 + STEP 6
  ├─ 동일 증번(CTR_POLNO) 그룹 내 SOFF_BF 합산
  ├─ 합산 < 0인 시점:
  │  ├─ 주계약(CLA00500): SOFF_AF = 0
  │  └─ 특약: SOFF_AF = max(0, SOFF_BF)
  └─ KICS 재산출: (새 SOFF_AF + PRPD_PREM) × CTR_TRME
```

### 핵심 공식

#### BAS 미보유 적립금 (이율 기반 부리)

```python
# 초기값
V = ACCMPT_RSPB_RSVAMT  # 책임준비금 초기값
ADINT[0] = V
ACUM[0] = V

# t >= 1
if 연도경계 (CTR_MM % 12 == 1):
    base = ACUM[t-1]
    cum_int = 0
else:
    base = ADINT[t-1]

ratio = PAY_TRMO[t] / CTR_TRMO[t]  # OD_TBL_MN에서
ADINT[t] = base + NPREM × ratio
cum_int += ADINT[t] × INRT[t] / 12
ACUM[t] = ADINT[t] + cum_int
```

#### SOFF 차감 규칙

```python
# 차감 적용 조건
apply_deduction = (
    PAY_STCD != 3  # 납입면제 아님
    AND (
        (PROD_CD in DEDUCT_PRODS AND PTERM > 5)
        OR (CTR_TPCD == '0' AND ACQSEXP > 0)
    )
)

# 차감 공식
deduction = ACQSEXP × max(84 - CTR_MM, 0) / 84
SOFF_BF = ACUM × rate - deduction  # rate: TPCD별 비율
```

#### PUBANO_INRT 산출

```python
if APLY_INRT_CD == '00':
    pubano = EXPCT_INRT  # 예정이율 고정
else:
    # IE_PUBANO_INRT × IE_DC_RT 공식
    pubano[t] = (EXT_WGHT × EXT_ITR + (DC_RT[t] - IV_ADEXP_RT) × (1-EXT_WGHT)) × ADJ_RT
    pubano = max(pubano, LWST_GRNT_INRT)  # 최저보증
```

---

## 배치 처리 구조

### (PROD_CD, CLS_CD) 기준 그룹핑

```
전체 42,000건 (TPCD 0,9: 28,211건)
  ↓ (PROD_CD, CLS_CD) 기준 98개 그룹
  ↓ 그룹별 계산 + CTR_POLNO netting 인라인

장점:
  1. 동일 CTR_POLNO가 100% 같은 그룹에 속함 → netting 즉시 처리
  2. 동일 그룹 내 가정 데이터 (bizexp_rt, acum_cov) 공유
  3. SQL I/O: MN 전체 1회 로드 (120회 → 1회)
```

### 데이터 로드 전략

```
TradPVDataCache (1.3초, 1회)
  ├─ II_INFRC: 42,000건 → dict (O(1) lookup)
  ├─ II_RSVAMT_BAS: 32,962건 → dict + numpy (ystr/yyend 120년)
  ├─ IP_P_ACUM_COV: 107건 → (prod,cov,cls) / (prod,cls) 2단계 lookup
  ├─ IP_P_EXPCT_INRT: 2,998건 → (prod,cov,cls) lookup
  ├─ IP_P_EXPCT_BIZEXP_RT: 7,569건 → 키매칭 (CMPT_CRIT 플래그)
  ├─ IE_PUBANO_INRT: 21건 → INRT_CD lookup
  ├─ IE_DC_RT: 1,440건 → numpy 배열 (할인율 커브)
  ├─ IA_A_CTR_LOAN: 12건 → (prod_grp, assm_grp) lookup
  ├─ IP_P_PROD: 5,724건 → (prod,cls) → CTR_LOAN_TPCD
  └─ polno_to_idnos: CTR_POLNO 역매핑

OD_TBL_MN 일괄 로드 (2,134만행)
  ├─ CTR_TRMO, PAY_TRMO, CTR_TRME 3개 컬럼만
  └─ groupby INFRC_IDNO → dict
```

---

## 통합 파이프라인 사용법

### 모듈: `cf_module/pipeline.py`

```python
import duckdb
from cf_module.pipeline import run_trad_pv_pipeline, compute_single

con = duckdb.connect('duckdb_transform.duckdb', read_only=True)

# 전체 실행
results, stats = run_trad_pv_pipeline(con, tpcd_filter=('0', '9'))
print(f"완료: {stats.computed:,}건, {stats.elapsed_total:.1f}s")

# 특정 상품만
results, stats = run_trad_pv_pipeline(con, prod_cls_filter=('LA0201J', '01'))

# 단건 디버그
result = compute_single(con, idno=625683)
d = result.to_dict()
print(d['SOFF_BF_TMRFND'][:5])

con.close()
```

---

## 검증 체계

### 검증 계층

| 레벨 | 테스트 | 대상 | 결과 |
|------|--------|------|------|
| 1 | `cf_module/v2/test_v2.py` | C행렬, einsum, 합성데이터 | PASS |
| 2 | `test_v2_real.py` | OD_TBL_MN t=1 (760397) | PASS (< 1e-10) |
| 3 | `test_v2_vs_proj_o2.py` | OD_TBL_MN 344개월 × 12항목 | ALL PASS (< 1e-15) |
| 3 | `test_v2_vs_proj_o_201j.py` | OD_TBL_MN 562개월 × 2계약 | ALL PASS (< 1e-15) |
| 4 | `test_trad_pv_all.py` | OD_TRAD_PV 28,211건 × 43컬럼 | **ALL PASS** |
| 4 | `test_trad_pv_single.py` | OD_TRAD_PV 단건 상세 | 디버그용 |

### 검증 기준

- 허용 오차: `1e-6` (OD_TRAD_PV), `1e-15` (OD_TBL_MN)
- 기대값 출처: `duckdb_transform.duckdb` 내 OD_TRAD_PV / OD_TBL_MN 테이블
- 비교 방식: `max(|computed - expected|) < threshold`

---

## 주요 설계 결정

| 결정 | 선택 | 근거 |
|------|------|------|
| 해지율 월변환 | `1-(1-q)^(1/12)` | PROJ_O2 기대값 일치 |
| BEPRD 인덱싱 | `duration_years - 1` (연도 단위) | 월 단위 금지 |
| PAY d_pyexsp | PAY pay_tpx_bot 기준 | CTR tpx_bot 아님 |
| 적립금 NPREM 비율 | `PAY_TRMO/CTR_TRMO` (MN에서) | 선형 근사 대비 정확 |
| SOFF_AF netting | CTR_POLNO 그룹 단위 | 주계약=0, 특약=max(0,BF) |
| KICS 공식 | `(SOFF_AF + PRPD_PREM) × CTR_TRME` | netting 후 재산출 |
| LOAN 조건 | CTR_LOAN_TPCD=0 → 스킵 | 약관대출 불가 상품 |
| PAY_STCD=3 | ACQSEXP 상각 미적용 | 납입면제 계약 |
| 배치 키 | (PROD_CD, CLS_CD) | POLNO 100% 포함, 가정 공유 |

---

## 파일 구조

```
cf_module/
├── pipeline.py              # 통합 파이프라인 (이 문서의 핵심)
├── calc/
│   └── trad_pv.py          # OD_TRAD_PV 산출 (STEP 1~7)
├── data/
│   └── trad_pv_loader.py   # 참조 데이터 캐시 (TradPVDataCache)
├── v2/
│   ├── engine.py           # OD_TBL_MN 산출 (중복제거 + tpx)
│   ├── etl.py              # Legacy DB → DuckDB 변환
│   └── schema.py           # DuckDB Star Schema DDL
└── utils/
    └── logger.py

테스트/
├── test_trad_pv_all.py     # OD_TRAD_PV 전체 검증 (28,211건)
├── test_trad_pv_single.py  # OD_TRAD_PV 단건 상세
├── test_trad_pv_full.py    # OD_TRAD_PV CLA00500 전수
├── test_v2_vs_proj_o2.py   # OD_TBL_MN 전체 비교 (760397)
├── test_v2_vs_proj_o_201j.py  # OD_TBL_MN 201J 비교
└── test_v2_real.py         # OD_TBL_MN t=1 검증
```

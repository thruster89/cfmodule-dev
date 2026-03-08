# 계산 모듈 가이드

계산 흐름 순서대로 정리. 각 단계의 입력→출력 의존성 포함.

---

## 전체 구조 요약

```
┌─────────────────────────────────────────────────────────┐
│  v2 Engine (OD_TBL_MN)                                  │
│  v2/etl.py → v2/engine.py                               │
│  위험률 중복제거 + CTR/PAY tpx + 탈퇴자 분해            │
└──────────────────────┬──────────────────────────────────┘
                       ↓
┌──────────────────────┴──────────────────────────────────┐
│  OD_TBL_BN (급부별 산출)                                │
│  data/bn_loader.py → calc/tbl_bn.py                     │
│  Per-BNFT 독립 exit rate → tpx → PYAMT/BNFT_INSUAMT    │
└──────────────────────┬──────────────────────────────────┘
                       ↓
┌──────────────────────┴──────────────────────────────────┐
│  OD_TRAD_PV (적립금/환급금/KICS)                        │
│  data/trad_pv_loader.py → calc/trad_pv.py               │
│  보험료 → 미경과 → 이율 → 적립금 → 환급금 → KICS → 대출│
└──────────────────────┬──────────────────────────────────┘
                       ↓
              pipeline.py (통합 오케스트레이션)
```

---

## STEP 1. 데이터 로딩

### 1-1. Legacy DB → DuckDB 변환 (1회성)

| 파일 | 줄수 | 역할 |
|------|------|------|
| `v2/schema.py` | 202 | DuckDB Star Schema DDL (10개 테이블) |
| `v2/etl.py` | 762 | VSOLN2.vdb(SQLite) → DuckDB 변환, 드라이버 기반 키매칭 |

### 1-2. 가정 로딩 (v1 경로)

| 파일 | 줄수 | 역할 |
|------|------|------|
| `data/model_point.py` | 267 | II_INFRC → ModelPointSet (계약 기본정보) |
| `data/assumptions.py` | 871 | MortalityTable, LapseTable, SkewTable, InterestRate 등 |
| `data/assm_key_builder.py` | 523 | 복합키 빌더 (드라이버 패턴, ASSM_GRP_CD1..15) |
| `io/reader.py` | 167 | DataReader — SQL 파일 로딩, named params |

---

## STEP 2. OD_TBL_MN — 유지자/납입자 산출

> MN = 계약 전체 수준의 tpx, 탈퇴자수, 위험률

| 파일 | 줄수 | 역할 |
|------|------|------|
| `v2/engine.py` | 605 | **핵심 엔진** — 중복제거 + CTR/PAY tpx + 탈퇴자 분해 |

**계산 순서** (engine.py 내부):

```
1. qx_raw 로드       연령별/단일률 위험률 + BEPRD 적용
2. wx_raw 로드       해약률 (paying/paidup) + 월변환 1-(1-q)^(1/12)
3. C행렬 구성        중복제거 계수 (CTR용 / PAY용 각각)
4. 중복제거 qx       q'ᵢ = qᵢ × (1 - Σⱼ(qⱼ×Cᵢⱼ)/2)
5. CTR tpx           cumprod(1 - exit_rate_ctr)
6. PAY tpx           cumprod(1 - exit_rate_pay)
7. 탈퇴자 분해       d_rsvamt, d_bnft (CTR), d_pyexsp (PAY)
```

**출력 컬럼** (12개):

| CTR (유지자) | PAY (납입자) |
|-------------|-------------|
| CTR_TRME (유지자수) | PAY_TRME (납입자수) |
| CTR_TRMNAT_RT (해약률) | PAY_TRMNAT_RT (납입해약률) |
| CTR_RSVAMT_DRPO_RSKRT | PYEXSP_DRPO_RSKRT |
| CTR_BNFT_DRPO_RSKRT | PAY_TRMPSN (납입해약자수) |
| CTR_TRMPSN (해약자수) | PYEXSP_DRPSN |
| CTR_RSVAMT_DRPSN | |
| CTR_BNFT_DRPSN | |

**v1 대응 모듈** (동일 로직, 단건 처리):

| 파일 | 줄수 | 역할 |
|------|------|------|
| `calc/timing.py` | 230 | 시간축 (경과월/년, 나이, 납입기간 판정) |
| `calc/decrement.py` | 859 | 중복제거 탈퇴율 + tpx + 탈퇴자 분해 |

---

## STEP 3. OD_TBL_BN — 급부별 산출

> BN = BNFT_NO별 독립 tpx 및 지급금액

| 파일 | 줄수 | 역할 |
|------|------|------|
| `data/bn_loader.py` | 180 | 6개 참조테이블 로드 (BNFT_RSKRT_C, DEFRY_RT, NCOV 등) |
| `calc/tbl_bn.py` | 232 | **BN 엔진** — Per-BNFT exit rate → tpx → 지급금액 |

**계산 순서** (tbl_bn.py 내부):

```
1. risk_to_dedup 매핑   MN dedup qx를 risk_cd별로 인덱싱
2. rsvamt_drpo 합산     is_exit_rsv 위험들의 dedup qx 합
3. 급부별 루프 시작     for bnft_no in bnft_mapping:
   3a. NCOV 마스크      부담보 기간 판정 (ctr_mm >= ncov_mm)
   3b. bnft_drpo 합산   이 급부의 DRPO 위험률 합
   3c. bnft_rskrt 합산  이 급부의 RSKRT 위험률 합
   3d. bn_exit 산출     trmnat + rsvamt_drpo + bnft_drpo (clip 0~1)
   3e. tpx 루프         t=0: TRMO=1,TRME=1 / t≥1: TRMO→TRME→counts
   3f. DEFRY_RT         경과연수별 지급률
   3g. PRTT_RT          분담률 × 적립금/가입금액
   3h. PYAMT            PRTT≠0 → CRIT×PRTT, else CRIT×DEFRY
   3i. BNFT_INSUAMT     BNFT_OCURPE × PYAMT
```

**출력 컬럼** (16개):

```
TRMNAT_RT, RSVAMT_DEFRY_DRPO_RSKRT, BNFT_DRPO_RSKRT, BNFT_RSKRT,
TRMO_MTNPSN_CNT, TRMPSN_CNT, RSVAMT_DEFRY_DRPSN_CNT, DEFRY_DRPSN_CNT,
TRME_MTNPSN_CNT, BNFT_OCURPE_CNT, CRIT_AMT,
DEFRY_RT, PRTT_RT, GRADIN_RT, PYAMT, BNFT_INSUAMT
```

---

## STEP 4. OD_TRAD_PV — 적립금/환급금/KICS

> TRAD_PV = 보험료 → 적립금 → 환급금 → KICS → 대출

| 파일 | 줄수 | 역할 |
|------|------|------|
| `data/trad_pv_loader.py` | 962 | 12개 참조테이블 일괄 로드 (TradPVDataCache) |
| `calc/trad_pv.py` | 880 | **TRAD_PV 엔진** — 7단계 순차 산출 |

**계산 순서** (trad_pv.py 내부):

```
STEP 1  _calc_premium()          보험료 배열 (orig/dc/nprem_old/nprem_new/pad)
STEP 2  _calc_prpd()             미경과보험료 (연납 paycyc=12만)
STEP 3  _build_pubano_inrt_arr() 공시이율 + 최저보증이율 배열
STEP 4  _calc_accumulation()     적립금 (BAS경로 or 이율부리)
STEP 5  _calc_surrender()        환급금 (SOFF_BF → netting → SOFF_AF)
STEP 6  KICS 산출                (SOFF_AF + PRPD_PREM) × CTR_TRME
STEP 7  약관대출                 IA_A_CTR_LOAN 기반
```

**주요 규칙**:
- V<0: 부리 없음 (ADINT=0, ACUM=V)
- SOFF netting: CTR_POLNO 그룹 BF합<0 → 주계약=0, 특약=max(0,BF)
- Old/New NPREM 전환: `CTR_MM <= amort_mm` 기준
- LTRMNAT 차감: 모든 non-'9' TPCD (ACQSEXP>0 AND PAY_STCD≠3)

**출력**: TradPVResult (50개 컬럼)

---

## STEP 5. 통합 파이프라인

| 파일 | 줄수 | 역할 |
|------|------|------|
| `pipeline.py` | 352 | MN→BN→TRAD_PV 전체 흐름 오케스트레이션 |
| `projection/projector.py` | 985 | v1 8단계 프로젝션 (timing→discount→PV) |
| `projection/batch.py` | 177 | 배치 청크 처리 |

**pipeline.py 흐름**:

```python
run_trad_pv_pipeline(con, tpcd_filter)
  → (PROD_CD, CLS_CD) 98개 그룹 루프
    → compute_single(idno)
      → v2 engine (MN)
      → tbl_bn (BN)
      → trad_pv (PV)
```

---

## STEP 6. v1 전용 — 보험료/급부/사업비/할인/PV

> v1 projector.py의 STEP 3~8 (v2에서는 trad_pv.py가 대체)

| 순서 | 파일 | 줄수 | 역할 |
|------|------|------|------|
| 3 | `calc/premium.py` | 91 | 보험료 CF (gross/net/risk/saving) |
| 4 | `calc/benefit.py` | 105 | 급부 CF (사망/만기/생존/해약) |
| 5 | `calc/expense.py` | 175 | 사업비 CF (신계약/유지/수금) |
| 6 | `calc/reserve.py` | 124 | 준비금(V) → 환급금(W) |
| 7 | `calc/discount.py` | 144 | 할인계수 (금리커브 → 월별 선도금리) |
| 8 | projector.py 내 | — | PV = Σ(CF × discount_factor) |

---

## 테스트 파일

| 파일 | 대상 | 검증 규모 |
|------|------|----------|
| `test_v2_vs_proj_o2.py` | OD_TBL_MN | 344개월 × 12컬럼, diff < 1e-15 |
| `test_tbl_bn.py` | OD_TBL_BN | 32,962건 × 72,797 BNFT, 15/16 PASS |
| `test_trad_pv_all.py` | OD_TRAD_PV | 42,000건 × 43컬럼 ALL PASS |
| `test_trad_pv_single.py` | TRAD_PV 단건 | 상세 디버깅용 |
| `test_v2_real.py` | v2 engine t=1 | PROJ_O2.vdb 기대값 |
| `test_single_contract.py` | v1 전체 | 760397 / 8833 |

---

## 모듈 크기 순위 (계산 관련만)

| # | 파일 | 줄수 |
|---|------|------|
| 1 | `projection/projector.py` | 985 |
| 2 | `data/trad_pv_loader.py` | 962 |
| 3 | `calc/trad_pv.py` | 880 |
| 4 | `data/assumptions.py` | 871 |
| 5 | `calc/decrement.py` | 859 |
| 6 | `v2/etl.py` | 762 |
| 7 | `v2/engine.py` | 605 |
| 8 | `data/assm_key_builder.py` | 523 |
| 9 | `pipeline.py` | 352 |
| 10 | `data/model_point.py` | 267 |
| 11 | `calc/tbl_bn.py` | 232 |
| 12 | `calc/timing.py` | 230 |
| 13 | `v2/schema.py` | 202 |
| 14 | `data/bn_loader.py` | 180 |
| 15 | `projection/batch.py` | 177 |
| 16 | `calc/expense.py` | 175 |
| 17 | `io/reader.py` | 167 |
| 18 | `config.py` | 145 |
| 19 | `calc/discount.py` | 144 |
| 20 | `calc/reserve.py` | 124 |
| 21 | `calc/benefit.py` | 105 |
| 22 | `calc/premium.py` | 91 |

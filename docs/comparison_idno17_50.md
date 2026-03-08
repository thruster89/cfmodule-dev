# IDNO 17, 50 전체 테이블 비교 결과

기준 DB: `PROJ_O_201J20110004359.vdb` (EXE_HIST_NO='D000000001')
Legacy DB: `VSOLN.vdb`
검증일: 2026-03-07

## 요약

| 구분 | PASS | FAIL | SKIP(미구현) |
|------|------|------|-------------|
| 합계 | **221** | **11** | **113** |

---

## 1. OD_TRAD_PV (보험료/적립금/환급금)

### IDNO=17 (NoBAS, 563 steps) — PASS 35 / FAIL 9 / SKIP 3

| 컬럼 | 상태 | max_diff | 원인 |
|------|------|----------|------|
| APLY_PUBANO_INRT | FAIL | 3.31e-02 | **Phase 2 미구현** (stub=0) |
| APLY_ADINT_TGT_AMT | FAIL | 1.15e+07 | **Phase 2 미구현** (stub=0) |
| APLY_PREM_ACUMAMT_BNFT | FAIL | 7.87e+04 | **ADINT alpha 공식 미도출** (선형근사 오차) |
| APLY_PREM_ACUMAMT_EXP | FAIL | 7.87e+04 | 위와 동일 (BNFT = EXP) |
| LWST_ADINT_TGT_AMT | FAIL | 6.82e+06 | **Phase 2 미구현** (stub=0) |
| LWST_PREM_ACUMAMT | FAIL | 6.96e+06 | **Phase 2 미구현** (stub=0) |
| CNCTTP_ACUMAMT_KICS | FAIL | 2.51e+06 | **Phase 2 미구현** (stub=0) |
| SOFF_BF_TMRFND | FAIL | 7.87e+04 | ACUMAMT 오차 전파 |
| SOFF_AF_TMRFND | FAIL | 7.87e+04 | ACUMAMT 오차 전파 |
| LOAN_INT | SKIP | - | **미구현** (nonzero 562건) |
| LOAN_REMAMT | SKIP | - | **미구현** (nonzero 561건) |
| LOAN_RPAY_HAFWAY | SKIP | - | **미구현** (nonzero 562건) |

**PASS 컬럼 (35개)**: CTR_AFT_PASS_MMCNT, PREM_PAY_YN, ORIG_PREM, DC_PREM, ACUM_NPREM,
PAD_PREM, ACQSEXP1_BIZEXP, YSTR_RSVAMT, YYEND_RSVAMT, LTRMNAT_TMRFND + zero 컬럼 25개

### IDNO=50 (BAS, 563 steps) — PASS 42 / FAIL 2 / SKIP 3

| 컬럼 | 상태 | max_diff | 원인 |
|------|------|----------|------|
| APLY_PUBANO_INRT | FAIL | 3.31e-02 | **Phase 2 미구현** (stub=0) |
| CNCTTP_ACUMAMT_KICS | FAIL | 4.37e+05 | **Phase 2 미구현** (stub=0) |
| LOAN_INT | SKIP | - | **미구현** |
| LOAN_REMAMT | SKIP | - | **미구현** |
| LOAN_RPAY_HAFWAY | SKIP | - | **미구현** |

**PASS 컬럼 (42개)**: 보험료/적립금/환급금/준비금 전부 PASS (BAS 계약은 이율 부리 불필요)

---

## 2. OD_TBL_MN (유지자/탈퇴자) — v1 pipeline

### IDNO=17 — PASS 37 / FAIL 0 / SKIP 4

모든 CTR/PAY 핵심 12개 항목 **완전 일치** (diff < 1e-8)

| SKIP 컬럼 | 사유 |
|-----------|------|
| CTR_AFT_PASS_MMCNT | 단순 시간축 (미매핑) |
| PASS_AGE | 단순 시간축 (미매핑) |
| PAY_RSVAMT_DEFRY_DRPO_RSKRT | PAY 기준 d_rsvamt ratio 미산출 |
| PAY_RSVAMT_DEFRY_DRPSN_CNT | PAY 기준 d_rsvamt 미산출 |

### IDNO=50 — PASS 35 / FAIL 0 / SKIP 6

| 추가 SKIP (50만) | 사유 |
|------------------|------|
| PAY_BNFT_DRPO_RSKRT | PAY 기준 d_bnft ratio 미산출 |
| PAY_BNFT_DEFRY_DRPSN_CNT | PAY 기준 d_bnft 미산출 |

**참고**: PAY_RSVAMT/PAY_BNFT 분해는 CTR 기준과 다른 PAY tpx_bot 기준 필요. 현재 engine에서 d_rsvamt, d_bnft는 CTR 기준만 산출.

---

## 3. OD_TBL_BN (급부 테이블)

- IDNO=17: **데이터 없음** (주계약 CLA00500은 BN 테이블 없음)
- IDNO=50: 1,689행 (3개 급부 × 563 시점) — **전부 미구현** (SKIP 14)
  - 주요 미구현: BNFT_RSKRT, BNFT_OCURPE_CNT, CRIT_AMT, DEFRY_RT, PYAMT, BNFT_INSUAMT
  - TRMNAT_RT, TRMO/TRME_MTNPSN_CNT 등 (MN에서 파생 가능)

---

## 4. OD_RSK_RT (위험률)

- IDNO=17: 2,815행 (5개 RSK_RT_CD × 563 시점)
- IDNO=50: 3,378행 (6개 RSK_RT_CD × 563 시점)
- **전부 미구현** (SKIP 9 × 2)
- 주요 컬럼: RSK_RT, LOSS_RT, MTH_EFECT_COEF, BEPRD_DEFRY_RT, TRD_COEF, ARVL_AGE_COEF

---

## 5. OD_LAPSE_RT (해지율) — PASS 3 × 2 = 6

- IDNO=17, 50 각 563행 — **3개 컬럼 전부 완벽 일치** (diff=0)
- `TRMNAT_RT`: 연 해지율 (paying=경과연수 기반, paidup=납입후경과 기반)
- `SKEW`: 1/12 고정 (월변환 계수)
- `APLY_TRMNAT_RT`: `1-(1-TRMNAT_RT)^SKEW` (연율→월율)
- **핵심 발견**: paying/paidup 경계에서 paying 마지막 유효값 연장 필요, paidup 인덱싱은 `(paidup_mm-1)//12+1`

---

## 6. OD_CF (Cash Flow)

- IDNO=17: SKIP 9 / PASS 17 (zero 컬럼)
- IDNO=50: SKIP 13 / PASS 13
- 주요 미구현: PREM_BASE, TMRFND, DRPO_PYRV, INSUAMT_GEN/MATU, MNTEXP_DR, ACQSEXP_DR, LOSS_SVYEXP, LOAN_*

---

## 7. OD_PVCF (PV Cash Flow)

- IDNO=17: SKIP 10 / PASS 18
- IDNO=50: SKIP 12 / PASS 16
- 주요 미구현: PAY_PREM, TMRFND, DRPO_PYRV, INSUAMT, MNTEXP_DR, ACQSEXP_DR, NET_CF_AMT, ICL_*

---

## 8. OD_DC_RT (할인율)

- IDNO=17, 50 각 563행 — **전부 미구현** (SKIP 6 × 2)
- 컬럼: DC_RT, TRME_MM_DC_RT, NDSCRT_PUBANO_INRT, DSCRT_PUBANO_INRT, LWST_GRNT_INRT, APLY_PUBANO_INRT

---

## 9. OP_BEL (BEL 요약, 각 1행)

| 항목 | IDNO=17 | IDNO=50 |
|------|---------|---------|
| PREM_BASE | 931,064 | 387,047 |
| PREM_PYEX | - | 6,393 |
| TMRFND | 1,322,466 | 191,222 |
| DRPO_PYRV | 523,941 | 24,793 |
| INSUAMT_GEN | - | 256,159 |
| INSUAMT_MATU | 37,684 | - |
| ACQSEXP_DR | 3.54 | 1.45 |
| MNTEXP_DR | 85,320 | 187,610 |
| LOSS_SVYEXP | - | 8,375 |
| LOAN_INT | 232,588 | 30,157 |
| LOAN_RPAY_HAFWAY | -226,558 | -29,539 |
| **BEL** | **1,032,319** | **286,889** |

---

## 10. OS_EXP_ACVAL_* (사업비 현가)

- **ACQS**: ACQSEXP_2만 nonzero (17: 3.54, 50: 1.45)
- **MNT**: MNTEXP_1~6,14,15 nonzero
- **LSVY**: IDNO=50만, LOSS_SVYEXP_1 = 8,374.5

---

## 구현 우선순위 (프로젝션 파이프라인)

### Phase 1: 기반 데이터 출력 (OD_RSK_RT, OD_LAPSE_RT)
- v2 engine이 이미 내부적으로 계산하지만 테이블 형태로 출력하지 않음
- OD_RSK_RT: 위험률코드별 RSK_RT, LOSS_RT, MTH_EFECT_COEF, BEPRD 등
- OD_LAPSE_RT: TRMNAT_RT, SKEW, APLY_TRMNAT_RT

### Phase 2: OD_DC_RT + APLY_PUBANO_INRT
- 할인율 곡선 (DC_RT, TRME_MM_DC_RT)
- 공시이율 (APLY_PUBANO_INRT) — inrt_lookup.json 기반
- 이것이 해결되면 TRAD_PV의 APLY_PUBANO_INRT, ADINT_TGT, LWST, KICS도 연쇄 해결

### Phase 3: OD_TBL_BN (급부 테이블)
- BNFT_RSKRT, BNFT_OCURPE_CNT 산출 (MN에서 파생)
- CRIT_AMT, DEFRY_RT, PRTT_RT, PYAMT, BNFT_INSUAMT

### Phase 4: OD_CF (Cash Flow)
- PREM_BASE, TMRFND, DRPO_PYRV (MN × TRAD_PV 조합)
- INSUAMT_GEN/MATU (BN 급부금)
- MNTEXP_DR, ACQSEXP_DR, LOSS_SVYEXP (사업비)
- LOAN_* (대출)

### Phase 5: OD_PVCF (PV CF) + OP_BEL
- OD_PVCF = OD_CF × DC_RT (할인)
- OP_BEL = sum(OD_PVCF) 각 항목별

### Phase 6: TRAD_PV NoBAS alpha 공식
- ADINT_TGT geometric decay 파라미터
- 원본 소스코드 참조 필요

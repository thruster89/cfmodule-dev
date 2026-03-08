# 잔여 과제 (Remaining Issues)

작성일: 2026-03-07

## 1. OD_TBL_MN (유지자/탈퇴자) — 13/50건 FAIL

테스트: `test_v1_vs_proj_o.py`, 50건 샘플, seed=42

### 유형 A: RSVAMT 플래그 매칭 오류 (5건)
- **상품**: LA0217W (CLA10561 등)
- **IDNO**: 298518, 564996, 580578, 690827, 710450
- **증상**: CTR_TRMNAT_RT=PASS이나 CTR_RSVAMT_DRPO ~6배 과대
- **원인**: IP_R_COV_RSKRT_C에서 RSVAMT_DEFRY_DRPO_RSKRT_YN 플래그 매칭 오류
  - 예: RSK=157015가 COV 테이블에 없어 RSVAMT=0 처리되어야 하는데, 탈퇴자 분해 비율이 기대값과 다름
- **영향**: CTR_TRME 누적 오차 → PAY 연쇄 FAIL

### 유형 B: C-matrix/BNFT 플래그 차이 (5건)
- **상품**: LA0217Y (CLA20274 등)
- **IDNO**: 1028627, 1044253, 1082272, 1292545, 1348448
- **증상**: CTR_TRMNAT_RT 미세 오차 ~8.1e-07, CTR_BNFT_DRPO max diff 2.14e-04
- **원인**: 9개 위험률, GRP=90 동일위험그룹 C-matrix 계산 미세 차이
- **영향**: 장기 tpx 누적 → CTR_TRME 최대 5.16e-02

### 유형 C: 기타 상품 RSVAMT 과대 (2건)
- **상품**: LA02079 (CLA40097), LA02058 (CLA40006)
- **IDNO**: 1121188, 1221360
- **증상**: 유형 A와 동일 패턴, 오차 규모 작음 (CTR_TRME max ~1e-03)
- **원인**: 유형 A와 동일 (exit 플래그 분해 비율)

### 유형 D: PAY PTERM 판정 오류 (1건)
- **상품**: LA0201J (CLA10007 특약)
- **IDNO**: 1368409
- **증상**: PAY_TRME SETL=1에서 v1=1.0, 기대값=0.0 (납입완료인데 납입중으로 판정)
- **원인**: 특약의 납입기간(PTERM) vs 경과기간 비교 로직 오류

---

## 2. OD_TBL_BN (급부 테이블) — 11/30건 FAIL

테스트: `test_v1_bn_vs_proj_o.py`, 30건 샘플

### Phase 1 (BNFT_RSKRT, TRME, BNFT_OCURPE) — 구현 완료
- **BNFT_RSKRT dedup 미세 차이** (7건): ~4e-8 수준, 연율 vs 월율 dedup 순서 차이
- **MN FAIL 연쇄** (4건): MN 유형 A/B 수정 시 자동 해결 예상

### Phase 2 (PYAMT, BNFT_INSUAMT) — 미구현
- **PRTT_RT (분담률)**: IP_B_PRTT_BNFT_RT 테이블에서 로드 필요
- **PYAMT**: CRIT_AMT × DEFRY_RT (× PRTT_RT 일부 상품)
- **BNFT_INSUAMT**: BNFT_OCURPE × PYAMT
- **GRADIN_RT**: 계단비율 (미분석)
- 관련 컬럼: TRMPSN_CNT, RSVAMT_DEFRY_DRPSN_CNT, DEFRY_DRPSN_CNT 등

---

## 3. OD_TRAD_PV (전통 준비금/보험료) — 84/500건 FAIL

테스트: `test_v1_trad_pv_vs_proj_o.py --n 500`, seed=42

### ACUM (이율 기반 부리) — 82건
- **대상**: BAS 미보유 계약 (CLA00500 + CTR_TPCD='9', 약 9,038건)
- **증상**: APLY_PREM_ACUMAMT_BNFT 오차 → SOFF_BF/AF_TMRFND 연쇄
- **원인**: ADINT_TGT의 alpha(geometric decay) 공식 미도출
  - alpha는 이율 무관 (APLY/LWST 동일), (remain_bterm, remain_paypr, expct_inrt)에 의존
  - 연도 내 일정, 연도 간 소폭 변동
  - 선형 근사(alpha=1) 사용 중 → 장기 누적 오차 발생
- **해결 방안**: 원본 소스코드 또는 규제 문서 참조 필요

### SOFF_AF_TMRFND — 2건
- **LA0217W 계열**: SOFF_AF와 SOFF_BF 미세 차이 (0.003% 미만)
- **원인**: SOFF_AF 계산에 추가 조정항 존재 가능

### ACUM_NPREM 구형상품 — 19건
- **LA0217W** (16건): 사업비 공제율이 다른 공식 사용 (IP_P_BIZEXP_RT_V2 등)
- **LA0216W** (3건): 미상 공식

### 미구현 컬럼 (Phase 2/3)
| 구분 | 컬럼 | 설명 |
|------|------|------|
| Phase 2 | APLY_PUBANO_INRT | 적용공시이율 시계열 |
| Phase 2 | APLY_ADINT_TGT_AMT / LWST_ADINT_TGT_AMT | 부리대상금액 |
| Phase 2 | CNCTTP_ACUMAMT_KICS | KICS 적립금 |
| Phase 3 | DC_PREM | 할인보험료 (일부 상품) |
| Phase 3 | ACUM_NPREM_PRPD / PRPD_MMCNT / PRPD_PREM | 선납 관련 |
| Phase 3 | ACQSEXP2_BIZEXP / AFPAY_MNTEXP / LUMPAY_BIZEXP | 사업비 상세 |
| Phase 3 | PENS_* (3개) | 연금 관련 |
| Phase 3 | HAFWAY_WDAMT / *_ADD (7개) | 중도인출/추가적립 |
| Phase 3 | LWST_* (2개) | 최저보증 |
| Phase 3 | LOAN_* (5개) | 대출 관련 (4,043건) |
| Phase 3 | MATU_MAINT_BNS_ACUM_AMT | 만기유지보너스 |

---

## 4. APLY_PUBANO_INRT (적용공시이율) — 미도출

- **inrt_lookup.json**: CD별 시계열 (부분적)
- **등록된 CD**: 06, 08, 09, 18, 21, 24, 27
- **미등록 CD**: 00(고정=EXPCT), 19, 22, 25, 28, 29
- **CD='00'**: EXPCT_INRT 고정 (구현 완료)
- **기타 미등록**: 도출 공식 미파악 (IE_DC_RT, IE_PUBANO_INRT 기반 추정)
- 상세: `docs/v1_acum_analysis.md`

---

## 5. v2 엔진 미구현

| 항목 | 설명 |
|------|------|
| v2 오케스트레이터 | 대량 계약 청크 처리 + 병렬화 |
| Premium 단계 | 보험료 산출 |
| Benefit 단계 | 급부금 산출 |
| Expense 단계 | 사업비 산출 |
| Reserve 단계 | 준비금 산출 |
| Discount 단계 | 할인 |
| PV 단계 | 현재가치 산출 |

---

## 우선순위 제안

1. **MN 유형 A/C (exit 플래그 수정)** — 근본 원인 1개, 7건 해결 + BN 4건 자동 해결
2. **MN 유형 D (PTERM 판정)** — 1건, 비교적 단순
3. **BN Phase 2 (PYAMT/BNFT_INSUAMT)** — 새 컬럼 구현
4. **MN 유형 B (C-matrix GRP=90)** — 5건, 미세 오차
5. **TRAD_PV alpha 공식** — 원본 소스 없이는 진행 불가

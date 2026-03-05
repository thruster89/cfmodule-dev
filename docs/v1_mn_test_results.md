# v1 OD_TBL_MN 대량 비교 테스트 결과

## 테스트 개요

- **테스트 스크립트**: `test_v1_vs_proj_o.py`
- **Legacy DB**: `VSOLN.vdb` (v1 엔진 입력)
- **기대값 DB**: `proj_o.duckdb` (42,000 IDNOs, OD_TBL_MN)
- **비교 항목**: 12개 (CTR 7개 + PAY 5개)
- **허용 오차**: 1e-8
- **비교 시작점**: SETL=1 (SETL=0은 초기행, CTR_TRMO=0)

## 테스트 결과 요약 (50건 랜덤 샘플, seed=42)

| 구분 | 건수 | 비율 |
|------|------|------|
| **PASS** | 37 | 74% |
| **FAIL** | 13 | 26% |

### 항목별 통계

| 항목 | PASS | FAIL | Max Diff |
|------|------|------|----------|
| CTR_TRME (유지자수) | 38 | 12 | 1.15e-01 |
| CTR_TRMNAT_RT (해약률) | 45 | 5 | 8.10e-07 |
| CTR_RSVAMT_DRPO (준비금탈퇴율) | 38 | 12 | 1.22e-03 |
| CTR_BNFT_DRPO (급부탈퇴율) | 45 | 5 | 2.14e-04 |
| CTR_TRMPSN (해약자수) | 38 | 12 | 5.81e-04 |
| CTR_RSVAMT_DRPSN (준비금탈퇴자수) | 38 | 12 | 1.12e-03 |
| CTR_BNFT_DRPSN (급부탈퇴자수) | 45 | 5 | 2.06e-04 |
| PAY_TRME (납입자수) | 37 | 13 | 1.00e+00 |
| PAY_TRMNAT_RT (납입해약률) | 40 | 10 | 8.10e-07 |
| PYEXSP_DRPO (납면탈퇴율) | 40 | 10 | 1.09e-05 |
| PAY_TRMPSN (납입해약자수) | 37 | 13 | 7.34e-03 |
| PYEXSP_DRPSN (납면탈퇴자수) | 39 | 11 | 5.55e-04 |

## FAIL 유형 분류

### 유형 A: CTR RSVAMT 탈퇴율 과대 (LA0217W — 5건)

**IDNO**: 298518, 564996, 580578, 690827, 710450

| 속성 | 값 |
|------|-----|
| 상품코드 | LA0217W |
| 계약일 | 2017.03~05 |
| 경과기간 | 약 6년 |
| 담보코드 | CLA10561 등 |

**FAIL 항목**: CTR_TRME, CTR_RSVAMT_DRPO, CTR_TRMPSN, CTR_RSVAMT_DRPSN + PAY 연쇄

**증상**:
- CTR_TRMNAT_RT (전체 해약률) = PASS (diff = 0)
- CTR_RSVAMT_DRPO (준비금 탈퇴율) = FAIL (v1이 기대값 대비 ~6배 과대)
  - 예: SETL=18에서 v1=0.000714, 기대값=0.000109
- CTR_BNFT_DRPO = PASS
- tpx 누적 오차: SETL=215에서 최대 4.49e-02

**추정 원인**: RSVAMT_DEFRY_DRPO_RSKRT_YN 플래그 매칭 오류. 특정 위험률(157015 등)이 v1에서 RSVAMT exit으로 잘못 분류되어 d_rsvamt가 과대 계산됨. 전체 wx는 맞지만 탈퇴자 분해 비율이 다름.

**위험률 구조** (IDNO 298518, CLA10561):
```
RSK=111016 GRP=1 DEAD=0  (사망)
RSK=121093 GRP=2 DEAD=1  (비사망)
RSK=157015 GRP=3 DEAD=1  (비사망) ← 이 위험률의 RSVAMT 플래그 확인 필요
RSK=212003 GRP=4 DEAD=0  (해지)
```

---

### 유형 B: CTR TRMNAT_RT 미세 차이 + BNFT 불일치 (LA0217Y — 5건)

**IDNO**: 1028627, 1044253, 1082272, 1292545, 1348448

| 속성 | 값 |
|------|-----|
| 상품코드 | LA0217Y |
| 계약일 | 2017.06~07 |
| 경과기간 | 약 6년 |
| 담보코드 | CLA20274 등 |

**FAIL 항목**: CTR_TRME, CTR_TRMNAT_RT, CTR_RSVAMT_DRPO + PAY 연쇄

**증상**:
- CTR_TRMNAT_RT 미세 오차: max diff ~8.1e-07
- 오차가 누적되어 CTR_TRME에서 SETL=284~661 부근 최대 5.16e-02 차이
- CTR_BNFT_DRPO도 FAIL (max diff 2.14e-04)

**추정 원인**: 9개 위험률 보유 상품으로, GRP=90 (동일위험그룹) 처리에서 C-matrix 계산 미세 차이 가능. 또는 특정 위험률의 BNFT exit 플래그 불일치.

**위험률 구조** (IDNO 1028627, CLA20274):
```
RSK=111016 GRP=1  DEAD=0
RSK=212003 GRP=2  DEAD=0
RSK=221008 GRP=3  DEAD=1
RSK=227003 GRP=4  DEAD=1
RSK=241001 GRP=5  DEAD=1
RSK=241002 GRP=6  DEAD=1
RSK=221007 GRP=90 DEAD=1  ← 동일위험그룹
RSK=221016 GRP=90 DEAD=1
RSK=221017 GRP=90 DEAD=1
```

---

### 유형 C: CTR RSVAMT 과대 — 기타 상품 (LA02079, LA02058 — 각 1건)

**IDNO**: 1121188 (LA02079), 1221360 (LA02058)

**증상**: 유형 A와 동일 패턴 (CTR_RSVAMT_DRPO 과대, CTR_TRMNAT_RT PASS)

**특징**:
- LA02079: CLA40097 (4개 위험률, RSK=520040 포함)
- LA02058: CLA40006 (4개 위험률, RSK=410002 포함)
- 오차 규모가 유형 A보다 작음 (CTR_TRME max diff ~1e-03)

---

### 유형 D: PAY 초기화 오류 (LA0201J 특약 — 1건)

**IDNO**: 1368409

| 속성 | 값 |
|------|-----|
| 상품코드 | LA0201J |
| 담보코드 | CLA10007 (특약) |
| 경과기간 | 12년 2개월 |

**FAIL 항목**: PAY_TRME, PAY_TRMPSN, PYEXSP_DRPSN

**증상**:
- PAY_TRME: SETL=1에서 v1=1.0, 기대값=0.0
- 기대값에서는 납입기간 종료 후 PAY=0이어야 하는데, v1은 PAY=1.0으로 시작
- PAY_TRMPSN, PYEXSP_DRPSN 연쇄 FAIL

**추정 원인**: 특약(CLA10007)의 납입기간(PTERM) 판정 오류. 경과 12년 2개월인데 납입기간이 종료되었는지 여부 판정에서 v1이 아직 납입 중으로 간주.

---

## 공통 분석

### FAIL의 연쇄 구조

1. **근본 원인**: 특정 위험률의 exit 플래그 (RSVAMT/BNFT/PYEXSP) 불일치 또는 C-matrix 계산 오류
2. **1차 영향**: CTR_RSVAMT_DRPO 또는 CTR_TRMNAT_RT 오차
3. **2차 영향**: tpx 누적 곱에서 오차 축적 → CTR_TRME drift
4. **3차 영향**: PAY도 CTR 기반이므로 PAY_TRME 연쇄 영향

### 상품코드별 분포

| 상품코드 | FAIL 건수 | 유형 |
|----------|-----------|------|
| LA0217W | 5 | A (RSVAMT 과대) |
| LA0217Y | 5 | B (TRMNAT_RT 미세 + BNFT) |
| LA02079 | 1 | C (RSVAMT 과대, 소규모) |
| LA02058 | 1 | C (RSVAMT 과대, 소규모) |
| LA0201J | 1 | D (PAY 초기화) |

### 다음 단계 (우선순위)

1. **유형 A/C 디버그**: IP_R_COV_RSKRT_C / IP_R_BNFT_RSKRT_C에서 RSVAMT exit 플래그 매칭 로직 점검
2. **유형 B 디버그**: GRP=90 동일위험그룹 C-matrix 처리 및 BNFT exit 플래그 확인
3. **유형 D 디버그**: 특약의 PTERM 계산 로직 점검 (경과기간 vs 납입기간 비교)
4. **대량 테스트**: 500건+ 샘플로 추가 유형 발굴

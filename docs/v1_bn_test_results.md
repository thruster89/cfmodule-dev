# v1 OD_TBL_BN (급부 테이블) 비교 테스트 결과

## 테스트 개요

- **테스트 스크립트**: `test_v1_bn_vs_proj_o.py`
- **Legacy DB**: `VSOLN.vdb`
- **기대값 DB**: `proj_o.duckdb` (32,962 BN IDNOs)
- **비교 항목 (Phase 1)**: BNFT_RSKRT, TRME, BNFT_OCURPE (decrement 기반)
- **Phase 2 (미구현)**: PYAMT, BNFT_INSUAMT (TRAD_PV 의존)
- **허용 오차**: 1e-8

## BN 테이블 구조

OD_TBL_BN은 급부번호(BNFT_NO)별 세부 테이블:

| 컬럼 | 설명 | 산출 공식 |
|------|------|-----------|
| BNFT_RSKRT | 급부 위험률 (월) | qx_dedup[risk_cd] (C-matrix 적용) |
| TRME_MTNPSN_CNT | 급부별 기말유지자수 | cumprod(1 - bn_exit) |
| TRMO_MTNPSN_CNT | 급부별 기시유지자수 | TRME[t-1] |
| BNFT_OCURPE_CNT | 급부 발생건수 | TRMO × BNFT_RSKRT |
| CRIT_AMT | 기준금액 | II_INFRC.GRNTPT_JOIN_AMT |
| DEFRY_RT | 지급률 | IP_B_BNFT_DEFRY_RT (경과연수별) |
| PRTT_RT | 분담률 | 준비금 기반 계산 (Phase 2) |
| PYAMT | 지급금액 | CRIT_AMT × DEFRY_RT × PRTT_RT |
| BNFT_INSUAMT | 급부보험금 | BNFT_OCURPE × PYAMT |

### 급부별 tpx 계산 핵심

각 급부는 고유한 tpx를 가짐:
- `bn_exit = wx_ctr + rsvamt_drpo + bnft_drpo_k`
- `bnft_drpo_k` = 해당 급부의 BNFT_DRPO_RSKRT_YN=1인 위험률 합 (급부마다 다름)
- BNFT_NO=0: 계약수준 (MN CTR_BNFT_DRPO에 사용)
- BNFT_NO=1,2,...: 급부별 (BN TRME에 사용)

### DB 테이블 참조

| 테이블 | 역할 |
|--------|------|
| IP_B_BNFT_BAS | 급부 기본 정의 (BNFT_NO, BNFT_CHRT_DVCD) |
| IP_R_BNFT_RSKRT_C | 급부-위험률 매핑 (BNFT_RSKRT_YN, BNFT_DRPO_RSKRT_YN) |
| IP_B_BNFT_DEFRY_RT | 지급률 (경과연수별) |
| IP_B_PRTT_BNFT_RT | 분담률 (경과연수별) |

## Phase 1 테스트 결과 (30건 샘플)

| 구분 | 건수 | 비율 |
|------|------|------|
| **PASS** | 19 | 63% |
| **FAIL** | 11 | 37% |

### 항목별 통계

| 항목 | PASS | FAIL | Max Diff |
|------|------|------|----------|
| BNFT_RSKRT | 41 | 9 | 1.60e-03 |
| TRME | 39 | 11 | 4.64e-02 |
| BNFT_OCURPE | 41 | 9 | 1.37e-03 |

## FAIL 유형 분류

### 유형 1: BNFT_RSKRT dedup 미세 차이 (~4e-8)

**해당**: LA0201J, LA02125 등 (7건)

- 모든 급부에서 BF_MM(pre-dedup) 대비 동일한 비율의 차이
- v1은 월율로 C-matrix dedup 적용, 기대값은 연율 기반 dedup 후 월변환 가능성
- **TRME는 PASS** — tpx 계산 자체는 정확
- 실질적 영향 미미 (4-5e-8 수준)

### 유형 2: MN FAIL과 동일 원인 (TRME diff > 1e-6)

**해당**: LA0217W (700869), LA0217Y (1021684, 1308843), LA0216W (910014)

- MN에서 이미 FAIL인 상품과 동일한 근본 원인
- RSVAMT/BNFT exit 플래그 불일치 → CTR tpx 오차 → BN TRME 연쇄 영향
- **MN 수정 시 자동 해결 예상**

## Phase 2 미구현 사항 (PYAMT / BNFT_INSUAMT)

PYAMT 계산에 PRTT_RT(분담률)가 필요한 경우 있음:
- DEFRY_RT=0, PRTT_RT>0인 상품: PYAMT = CRIT_AMT × PRTT_RT
- PRTT_RT는 준비금/적립금 기반 계산값으로, TRAD_PV 단계 구현 후 검증 가능
- 예: IDNO 141683 (LA0217W) PRTT=106.44 → 준비금 기반 산출

## 다음 단계

1. **MN FAIL 수정**: exit 플래그 로직 정리 → BN도 자동 해결
2. **BNFT_RSKRT dedup 차이 조사**: 연율 vs 월율 dedup 순서 확인
3. **TRAD_PV 구현**: 보험료, 준비금, 해약환급금 단계 → PRTT_RT 산출 → PYAMT 검증

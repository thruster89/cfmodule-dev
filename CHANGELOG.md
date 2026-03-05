# Changelog

## [1.2.1] - 2026-03-02

### Changed
- **영업보험료(G) α 처리**: 확정연금 `ä_{m|}=1+v+...+v^{m-1}` 기준 연간비율 환산 (`α/ä_{m|}`)
- **debug CSV**: 10_gross_premium.csv 새 공식 반영, 11_pricing_pv.csv 컬럼명 정리

## [1.2.0] - 2026-03-02

### Added
- **Pricing 모드 엔진** (`commutation.py`): 계산기수(Dx,Nx,Cx,Mx) 기반 순보험료/준비금/영업보험료 산출
- **감액률** (`IP_B_REDUC_RT`): BNFT_NO별 감액률 로딩 및 1차년도 qx_benefit 조정
- **면책기간** (`IP_R_INVLD_TRMNAT`): 면책월수 기반 첫해 qx_benefit 조정 `(12-k)/12`
- **REVI_YM 분기** (`IR_RSKRT_CHR_PRICING.sql`): pricing 모드에서 CTR_YM 기준 위험률 필터
- **배치 검증** (`test_pricing_batch.py`): CTR_DT 2023%, CTR_TPCD 0/9 대상 일괄 검증
- **debug CSV**: qx 코드별 상세(02c~02f), 기수표+V(t)(11), 영업보험료(10,12)
- **예정이율/예정사업비율 로딩**: `IP_P_EXPCT_INRT`, `IP_P_EXPCT_BIZEXP_RT` 매칭

### Changed
- **V(t) 산출**: P_annual(연납 순보험료) + 연 단위 연금현가 사용 (Woolhouse 미적용)
- **영업보험료(G)**: α(초년도 신계약비) 1회성 PV 처리 (`-α` not `-α×ä`)
- **qx_exit**: BNFT_DRPO=1 코드만 사용 (RSVAMT 제외)
- **VERSION 파일 도입**: `x.x.x` 관리 체계

## [1.1.0] - 2026-02-22

### Added
- 중복제거 위험률 + DB키매칭 파이프라인
- queries.json → 개별 .sql 마이그레이션 (60개)
- DataReader: 디렉토리 로딩 + named params + DuckDB
- MortalityKeyBuilder + BEPRD + 월변환
- CTR 중복제거 (RSVAMT + BNFT), tpx 검증

## [1.0.0] - 2026-02-22

### Added
- cf_module 보험 Cash Flow 프로젝션 엔진 초기 버전

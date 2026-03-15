# CF 파이프라인 상세 흐름

## 실행

```bash
python -m cf_module.run --idno 760397
```

진입점: `cf_module/run.py` → `main()` → `run_single(con, idno)`

---

## 전체 흐름도

```
[II_INFRC]  계약 정보 로드
     │
     ▼
[STEP 1] RSK_RT ─── 위험률 산출
     │  loader.load_contract(idno)
     │  loader.load_risk_codes(ctr)       ← IP_R_RSKRT_C
     │  loader.load_mortality_rates()     ← IR_RSKRT_VAL (드라이버 키매칭)
     │  loader.load_beprd()               ← IA_T_BEPRD
     │  loader.load_invld_months()        ← IP_R_INVLD_TRMNAT
     │  compute_rsk_rt()                  → {risk_cd: {9개 컬럼 배열}}
     │
     ▼
[STEP 2] LAPSE_RT ─── 해약률 산출
     │  loader.load_lapse_rates()         ← IA_T_TRMNAT (KDCD=12/13)
     │  loader.load_skew()                ← IA_T_SKEW
     │  compute_lapse_rt()                → {3개 컬럼 배열}
     │
     ▼
[STEP 3] TBL_MN ─── 중복제거 위험률 + tpx + 탈퇴자 분해
     │  입력: RSK_RT[INVLD_TRMNAT_AF_APLY_RSK_RT], LAPSE_RT[APLY_TRMNAT_RT]
     │  compute_tbl_mn()
     │    - C행렬 구성 (dedup)
     │    - CTR/PAY tpx 계산
     │    - TRMPSN, DRPSN, PYEXSP 분해
     │  출력: {18개 컬럼 배열}
     │    CTR_TRMO, CTR_TRME, CTR_TRMPSN, CTR_RSVAMT_DEFRY_DRPSN, ...
     │    PAY_TRMO, PAY_TRME, PAY_TRMPSN, PYEXSP_DRPSN, ...
     │
     ▼
[STEP 4] TRAD_PV ─── 전통형 현가 (보험료/적립금/환급금)
     │  TradPVDataCache 생성 (단건: POLNO 그룹만 로드)
     │    ← II_INFRC, II_RSVAMT_BAS, IP_P_ACUM_COV, IP_P_EXPCT_INRT,
     │       IP_P_EXPCT_BIZEXP_*, IE_PUBANO_INRT, IE_DC_RT, IP_P_LTRMNAT, ...
     │  build_contract_info_cached()
     │  compute_trad_pv(info, n_steps, pay_trmo, ctr_trmo, ctr_trme)
     │    - STEP 4.1: 보험료 (ORIG_PREM, DC_PREM, PREM_PAY_YN)
     │    - STEP 4.2: 미경과보험료 (PRPD)
     │    - STEP 4.3: 이율배열 (PUBANO/LWST)
     │    - STEP 4.4: 적립금 (YSTR/YYEND_RSVAMT)
     │    - STEP 4.5: 환급금 (SOFF_BF/AF_TMRFND)
     │    - STEP 4.6: KICS (CNCTTP_ACUMAMT_KICS)
     │    - STEP 4.7: 약관대출 (LOAN_REMAMT)
     │  apply_soff_af_netting() (CTR_POLNO 그룹 netting)
     │  출력: TradPVResult.to_dict() → {43개 컬럼 배열}
     │
     ▼
[STEP 5] TBL_BN ─── 급부 테이블
     │  BNDataCache 생성 (단건: pcv_filter로 해당 상품만 로드)
     │    ← IP_R_BNFT_RSKRT_C, IP_B_BNFT_BAS, IP_B_BNFT_DEFRY_RT,
     │       IP_B_PRTT_BNFT_RT, IP_P_EXPCT_INRT, IP_R_RSKRT_C, ...
     │  compute_bn()
     │    - Per-BNFT 독립 dedup (C행렬)
     │    - tpx → TRMO/TRME
     │    - 탈퇴자/발생건 (TRMPSN, DRPSN, OCURPE)
     │    - DEFRY_RT / PRTT_RT (ann_due 기반)
     │    - PYAMT = CRIT_AMT × (PRTT or DEFRY)
     │    - BNFT_INSUAMT = OCURPE × PYAMT
     │  출력: BNResult.bnft_results → {bnft_no: BNFTResult(16개 배열)}
     │
     ▼
[STEP 6] EXP ─── 사업비
     │  ExpDataCache 생성
     │    ← IA_E_ACQSEXP_DR, IA_E_MNTEXP_DR, IA_E_LOSS_SVYEXP,
     │       IA_M_PROD_GRP, IA_M_ETC_ASSM_KEY, IE_INFL
     │  드라이버 키매칭: PROD→PROD_GRP, ASSM_DIV_VAL5→ASSM_GRP_CD5
     │  compute_exp()
     │    - DRVR=1: RATE[t] × GPREM
     │    - DRVR=2: AMOUNT[t] × 물가상승(IE_INFL)
     │    - DRVR=4: 고정값
     │    - DRVR=6: RATE[t] × LOAN_REMAMT       ← TRAD_PV
     │    - DRVR=9: RATE[t] × CNCTTP_ACUMAMT    ← TRAD_PV
     │    - DRVR=10: RATE[t] × (CNCTTP - LOAN)  ← TRAD_PV
     │    - PAY_MTNPSN_DVCD=0: pterm까지만 적용
     │    - ACQS: step=0 제외
     │    - EYM 시간 제한 (BIZEXP_OCUR_EYM)
     │  출력: [ExpResult(tpcd, kdcd, d_ind, values), ...]
     │
     ▼
[STEP 7] CF ─── 캐시플로우 결합
     │  입력: TBL_MN + TRAD_PV + TBL_BN + EXP
     │  compute_cf()
     │    - PREM_BASE  = CTR_TRMO × ORIG_PREM × PREM_PAY_YN
     │    - PREM_PYEX  = (CTR_TRME[s-1] - PAY_TRME[s-1]) × ORIG × PAY_YN
     │    - DRPO_PYRV  = CTR_RSVAMT_DEFRY_DRPSN × APLY_PREM_ACUMAMT_BNFT
     │    - INSUAMT_GEN = Σ BN.BNFT_INSUAMT
     │    - TMRFND     = CTR_TRMPSN × CNCTTP_ACUMAMT_KICS
     │    - ACQSEXP_DR = Σ(ACQS_item × TRMO)  ← PAY_DVCD로 CTR/PAY 선택
     │    - MNTEXP_DR  = Σ(MNT_item × TRMO)
     │    - LOSS_SVYEXP = LSVY_rate × Σ BNFT_INSUAMT
     │  출력: CFResult.to_dict() → {26개 컬럼 배열}
     │
     ▼
[STEP 8] DC_RT ─── 할인율
     │  입력: IE_DC_RT 커브 (TradPVDataCache.dc_rt_curve)
     │  compute_dc_rt()
     │    - v[s] = 1 / (1 + IE_DC_RT[s])^(1/12)
     │    - TRME_MM_DC_RT = cumprod(v)           기말 할인계수
     │    - TRMO_MM_DC_RT[s] = TRME[s-1]         기시 할인계수
     │  출력: DCRTResult → {DC_RT, TRMO_MM_DC_RT, TRME_MM_DC_RT}
     │
     ▼
[STEP 9] PVCF ─── 현가 캐시플로우
     │  입력: CF + DC_RT
     │  compute_pvcf()
     │    - 기시(TRMO) 할인: PREM, PYEX, ACQSEXP, MNTEXP  ← 기초 발생
     │    - 기말(TRME) 할인: TMRFND, DRPO, INSUAMT, LSVY   ← 기말 발생
     │    - NET_CF = 수입(PREM+PYEX) - 지출(TMRFND+DRPO+INSUAMT+EXP+LSVY)
     │  출력: PVCFResult.to_dict() → {27개 컬럼 배열}
     │
     ▼
[STEP 10] BEL ─── 최선추정부채
     │  입력: PVCF
     │  compute_bel()
     │    - 각 컬럼 = PVCF 전 시점 합산 (Σ over all steps)
     │    - BEL = Σ NET_CF_AMT
     │  출력: BELResult → {26개 스칼라값} (단일 행)
     │
     ▼
[CSV 출력]
     output/{idno}_RSK_RT.csv      2415행 (7 risks × 345 steps)
     output/{idno}_LAPSE_RT.csv     345행
     output/{idno}_TBL_MN.csv       345행 × 18컬럼
     output/{idno}_TRAD_PV.csv      345행 × 43컬럼
     output/{idno}_TBL_BN.csv       345행 × 16컬럼 (BNFT별)
     output/{idno}_EXP.csv         5520행 (16 items × 345 steps)
     output/{idno}_CF.csv           345행 × 26컬럼
     output/{idno}_DC_RT.csv        345행 × 3컬럼
     output/{idno}_PVCF.csv         345행 × 27컬럼
     output/{idno}_BEL.csv            1행 × 26컬럼
```

---

## 데이터 흐름 요약

```
                    ┌─────────┐
                    │ II_INFRC │ 계약 정보
                    └────┬────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         ┌─────────┐ ┌────────┐ ┌─────────┐
         │ RSK_RT  │ │LAPSE_RT│ │ TRAD_PV │
         │ 위험률  │ │ 해약률 │ │ 현가    │
         └────┬────┘ └───┬────┘ └────┬────┘
              │          │           │
              └─────┬────┘           │
                    ▼                │
              ┌──────────┐           │
              │  TBL_MN  │           │
              │ 중복제거 │           │
              └────┬─────┘           │
                   │                 │
              ┌────┴─────┬───────────┤
              ▼          ▼           ▼
         ┌─────────┐ ┌──────┐  ┌────────┐
         │ TBL_BN  │ │ EXP  │  │ DC_RT  │
         │ 급부    │ │사업비│  │ 할인율 │
         └────┬────┘ └──┬───┘  └───┬────┘
              │         │          │
              └────┬────┘          │
                   ▼               │
              ┌──────────┐         │
              │    CF    │         │
              │ 캐시플로우│         │
              └────┬─────┘         │
                   │               │
                   └───────┬───────┘
                           ▼
                     ┌──────────┐
                     │   PVCF   │
                     │ 현가 CF  │
                     └────┬─────┘
                          ▼
                     ┌──────────┐
                     │   BEL    │
                     │최선추정  │
                     └──────────┘
```

---

## 캐시 생성 타이밍

단건 모드(`run_single`)에서는 해당 계약에 필요한 데이터만 로드:

| 캐시 | 필터 | 생성 시점 |
|------|------|-----------|
| RawAssumptionLoader | 없음 (쿼리 시 계약별) | STEP 1 전 |
| TradPVDataCache | idno_filter={POLNO 그룹} | STEP 4 전 |
| BNDataCache | pcv_filter=[(prod, cov)] | STEP 5 전 |
| ExpDataCache | 없음 (전건 로드, 소량) | STEP 6 전 |

배치 모드(`run_batch`)에서는 캐시를 외부에서 전건 로드 후 공유.

---

## 주요 배열 인덱싱

```
경과 0Y4M 계약 (elapsed=4, n_steps=345):

step:    0    1    2    3    ...  344
CTR_MM:  4    5    6    7    ...  348
의미:    현재  1개월후  2개월후  ...  만기

step=0 (t=4): 프로젝션 시작점
  - RSK_RT, LAPSE_RT, TBL_MN: step 0부터 산출
  - TRAD_PV: step 0부터 산출
  - TBL_BN: step 0부터 산출 (TRMO[0]=1, counts=0)
  - EXP: ACQS는 step 1부터 (step 0 제외), MNT/LSVY는 step 0부터
  - CF: step 0부터
  - PREM_PYEX: lag 사용 (step 0 = 0, step 1부터 유효)
```

---

## 컬럼 수 요약

| 테이블 | 컬럼 수 | 비고 |
|--------|---------|------|
| RSK_RT | 9 × risk | risk별 독립 |
| LAPSE_RT | 3 | TRMNAT, SKEW, APLY |
| TBL_MN | 18 | CTR 8 + PAY 8 + PYEXSP 2 |
| TRAD_PV | 43 | 보험료~대출 |
| TBL_BN | 16 × bnft | BNFT별 독립 |
| EXP | 4 × item | (tpcd, kdcd, d_ind, val) |
| CF | 26 | 보험료+보험금+사업비 |
| DC_RT | 3 | DC_RT, TRMO, TRME |
| PVCF | 27 | CF×DC + NET_CF |
| BEL | 26 | PVCF 합산 (1행) |

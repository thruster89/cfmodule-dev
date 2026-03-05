# CF Module - 보험 Cash Flow 프로젝션 엔진

생명보험/손해보험 범용 Cash Flow Module.
IFRS17 (BEL/RA/CSM), K-ICS (지급여력), Pricing (적정보험료) 산출을 지원한다.

---

## 실행 방법

```bash
# 샘플 데이터로 전체 실행 (기본 100건)
python -m cf_module.main

# 샘플 500건
python -m cf_module.main --sample 500

# IFRS17만 실행
python -m cf_module.main --target ifrs17

# K-ICS + Pricing만 연별 실행
python -m cf_module.main --target kics pricing --time-step yearly

# 실제 DB 연결
python -m cf_module.main --db-path "C:/path/to/VSOLN2.vdb" --target ifrs17 kics pricing
```

### CLI 옵션 전체

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--target` | ifrs17 kics pricing | 실행할 산출물 (복수 선택 가능) |
| `--time-step` | monthly | 시간축 단위 (`monthly` \| `yearly`) |
| `--db-path` | _(없음→샘플)_ | DB 파일 경로 |
| `--db-type` | sqlite | DB 유형 (`sqlite` \| `duckdb`) |
| `--base-date` | 202309 | 결산기준일 (yyyymm) |
| `--scenario` | BASE | 시나리오 ID |
| `--chunk-size` | 10000 | 배치 청크 크기 |
| `--workers` | 4 | 병렬 워커 수 |
| `--output-dir` | ./output | 결과 저장 디렉토리 |
| `--output-format` | csv | 출력 형식 (`csv` \| `excel`) |
| `--sample` | _(없음)_ | 샘플 모드. 건수 지정 가능 (`--sample 500`) |

---

## 프로젝트 구조

```
cf_module/
├── __init__.py                 # 패키지 초기화
├── main.py                     # 실행 진입점 + CLI
├── config.py                   # 전역 설정 (DB, 프로젝션, 배치, 시나리오, 출력)
│
├── io/                         # I/O 계층
│   ├── reader.py               # 데이터 읽기 (SQLite, DuckDB, CSV)
│   └── writer.py               # 결과 쓰기 (CSV, Excel, DB)
│
├── data/                       # 데이터 로딩 & 전처리
│   ├── model_point.py          # Model Point 로딩/검증/정규화
│   └── assumptions.py          # 가정 로딩 (위험률, 해약률, 사업비, 금리 등)
│
├── calc/                       # 계산 엔진 (핵심)
│   ├── timing.py               # 시간축 생성 (t, 경과월, 경과년, 나이)
│   ├── decrement.py            # 탈퇴율 (사망률 qx, 해약률 wx, 생존확률 tpx)
│   ├── premium.py              # 보험료 CF (영업보험료, 순보험료, 위험보험료)
│   ├── benefit.py              # 급부 CF (사망보험금, 만기보험금, 해약환급금)
│   ├── expense.py              # 사업비 CF (신계약비, 유지비, 수금비)
│   ├── reserve.py              # 준비금 (순보험료식, 해약환급금)
│   └── discount.py             # 할인 (현가, 금리 시나리오)
│
├── projection/                 # CF 프로젝션 통합
│   ├── projector.py            # 메인 프로젝션 루프 (단일 청크)
│   └── batch.py                # 대규모 배치 처리 (청크 분할, 병렬)
│
├── output/                     # 목적별 산출물
│   ├── ifrs17.py               # IFRS17 (BEL, RA, CSM, Loss Component)
│   ├── kics.py                 # K-ICS (요구자본, 가용자본, 지급여력비율)
│   └── pricing.py              # Pricing (손해율, 사업비율, 적정보험료)
│
└── utils/
    └── logger.py               # 로깅
```

---

## 데이터 흐름

```
┌─────────────────────────────────────────────────────────────┐
│  config.py                                                  │
│  (DB경로, 결산일, 시나리오, time_step, run_targets)          │
└────────┬────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐    ┌─────────────────┐
│  io/reader.py   │    │  io/reader.py   │
│  → model_point  │    │  → assumptions  │
│  MP 로딩/정규화  │    │  가정 로딩       │
└────────┬────────┘    └────────┬────────┘
         │                      │
         ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│  calc/timing.py                                             │
│  시간축 생성: t, 경과월, 경과년, 나이, 납입여부, 보장여부     │
│  shape: (n_points, n_steps)                                 │
└────────┬────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  calc/decrement.py                                          │
│  탈퇴율: qx(사망), wx(해약) → tpx(생존확률) → 탈퇴자비율     │
└────────┬────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  premium.py  │  │  benefit.py  │  │  expense.py  │
│  보험료 CF   │  │  급부 CF     │  │  사업비 CF    │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                  │
       ▼                 ▼                  ▼
┌─────────────────────────────────────────────────────────────┐
│  calc/discount.py  →  현가(PV) 계산                         │
└────────┬────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  projection/projector.py                                    │
│  파이프라인 통합: timing → decrement → CF → discount → PV   │
│                                                             │
│  projection/batch.py                                        │
│  대규모 MP 분할 → 병렬 프로젝션 → 결과 병합                  │
└────────┬────────────────────────────────────────────────────┘
         │
         ▼  (run_targets에 따라 선택 실행)
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  ifrs17.py   │  │  kics.py     │  │  pricing.py  │
│  BEL/RA/CSM  │  │  SCR/지급여력 │  │  손해율/적정P │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                  │
       ▼                 ▼                  ▼
┌─────────────────────────────────────────────────────────────┐
│  io/writer.py → CSV / Excel / DB 저장                       │
└─────────────────────────────────────────────────────────────┘
```

---

## 핵심 설계 원칙

### 1. 벡터화 (numpy 2D 배열)
- 모든 계산 배열의 shape: `(n_model_points, n_time_steps)`
- Python for문 대신 numpy 브로드캐스팅으로 다건 MP를 한번에 계산
- 예: 100,000건 × 360시점 = 3600만 셀을 단일 연산으로 처리

### 2. 월별/연별 전환
- `--time-step monthly` (기본): 월 단위 프로젝션
- `--time-step yearly`: 연 단위 프로젝션 (12개월 단위 추출)
- 계산 로직은 동일. 시간축(`timing.py`)에서 분기

### 3. 런셋 선택 실행
- `--target ifrs17`: IFRS17만 실행
- `--target kics pricing`: K-ICS + Pricing만 실행
- 프로젝션은 항상 실행. 산출물 가공 단계에서 분기

### 4. 배치 처리 (대규모 MP 대응)
- Model Point를 chunk 단위로 분할 (기본 10,000건)
- `ProcessPoolExecutor`로 청크별 병렬 프로젝션
- `--chunk-size`, `--workers`로 조정 가능

---

## 산출물별 주요 지표

### IFRS17
| 지표 | 설명 |
|------|------|
| BEL | Best Estimate Liability. 미래 CF 현가 |
| RA | Risk Adjustment. 비재무위험 보상 |
| CSM | Contractual Service Margin. 미실현이익 |
| Loss Component | 부담계약 손실요소 |

### K-ICS
| 지표 | 설명 |
|------|------|
| SCR (Life) | 생명보험 위험액 |
| SCR (Interest) | 금리 위험액 |
| Available Capital | 가용자본 |
| Solvency Ratio | 지급여력비율 (가용/요구) |

### Pricing
| 지표 | 설명 |
|------|------|
| Loss Ratio | 손해율 (급부/보험료) |
| Expense Ratio | 사업비율 (사업비/보험료) |
| Combined Ratio | 합산비율 |
| Breakeven Premium | 손익분기 보험료 |
| Adequate Premium | 적정보험료 (목표이익률 반영) |

---

## 의존성

| 패키지 | 용도 | 필수 |
|--------|------|------|
| numpy | 벡터 계산 | O |
| pandas | DataFrame 처리 | O |
| sqlite3 | SQLite DB (Python 내장) | O |
| duckdb | DuckDB 지원 | X (선택) |
| psycopg2 | PostgreSQL 출력 | X (선택) |
| openpyxl | Excel 출력 | X (선택) |

```bash
# 최소 설치 (numpy, pandas만 있으면 실행 가능)
pip install numpy pandas

# 선택적 설치
pip install duckdb             # DuckDB 사용 시
pip install psycopg2-binary    # PostgreSQL 출력 시
pip install openpyxl           # Excel 출력 시
```

---

## 코드에서 사용하기 (API)

```python
from cf_module.config import CFConfig, ProjectionConfig
from cf_module.main import run_full_pipeline

# 설정
config = CFConfig(
    projection=ProjectionConfig(time_step="monthly"),
    run_targets=["ifrs17"],
)

# 샘플 실행
results = run_full_pipeline(config=config, use_sample=True, sample_n=1000)

# 결과 접근
print(results["ifrs17"].head())
print(results["summary"].describe())
```

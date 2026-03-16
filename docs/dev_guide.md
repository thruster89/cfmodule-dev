# CF Module 개발 가이드

## 실행 방법

```bash
# 전체 파이프라인 (RSK_RT → ... → BEL)
python -m cf_module.run --idno 760397

# 특정 단계까지만
python -m cf_module.run --idno 760397 --table MN      # RSK_RT → LAPSE_RT → MN
python -m cf_module.run --idno 760397 --table CF       # ... → CF까지

# PV 이후 제외 (RSK_RT → LAPSE_RT → MN만)
python -m cf_module.run --idno 760397 --no-pv

# 상세 출력
python -m cf_module.run --idno 760397 --debug

# 출력 경로 변경
python -m cf_module.run --idno 760397 -o my_output
```

출력: `output/{idno}_{테이블}.csv` (RSK_RT, LAPSE_RT, TBL_MN, TRAD_PV, TBL_BN, EXP, CF, DC_RT, PVCF, BEL)

### 배치 실행

```bash
# 전건 BEL 산출 → DuckDB
python run_batch_bel.py                    # 42,001건 → output_bel.duckdb (~3분)
python run_batch_bel.py --n 1000           # 1000건만
python run_batch_bel.py -o result.duckdb   # 출력 경로 지정

# 전건 검증
python test_bel_prem_base.py --all         # PREM_BASE 42,001건
```

---

## 파이프라인 구조

```
RSK_RT → LAPSE_RT → TBL_MN → TRAD_PV → TBL_BN → EXP → CF → DC_RT → PVCF → BEL
 100%     100%      100%     100%     100%      79.7% 83.8% 100%   —     —
```

### 전건 검증 결과 (42,000건 OP_BEL 기대값 비교)

- **11컬럼 100% PASS**: PREM_ADD, INSUAMT_PENS, ACQSEXP_INDR/REDEM, MNTEXP_INDR, HAFWDR 등
- **4컬럼 99%+**: INSUAMT_GEN(99.6%), DRPO_PYRV(99.6%), LOSS_SVYEXP(99.9%)
- **4컬럼 80%+**: PREM_BASE(83.8%), INSUAMT_MATU(78.5%), ACQSEXP_DR(79.7%)
- **7컬럼 미구현**: TMRFND(4.8%), MNTEXP_DR(0%), BEL(0%)

### 의존성

| 단계 | 입력 | 출력 파일 |
|------|------|-----------|
| RSK_RT | II_INFRC, IR_RSKRT_VAL, IA_M_ASSM_DRIV | calc/tbl_rsk_rt.py |
| LAPSE_RT | II_INFRC, IA_T_TRMNAT | calc/tbl_lapse_rt.py |
| TBL_MN | RSK_RT, LAPSE_RT | calc/tbl_mn.py |
| TRAD_PV | TBL_MN, II_RSVAMT_BAS, IP_P_* | calc/trad_pv.py |
| TBL_BN | RSK_RT, LAPSE_RT, TRAD_PV, IP_B_* | calc/tbl_bn.py |
| EXP | IA_E_ACQSEXP_DR/MNTEXP_DR/LOSS_SVYEXP, TRAD_PV | calc/exp.py |
| CF | TBL_MN, TRAD_PV, TBL_BN, EXP | calc/cf.py |
| DC_RT | IE_DC_RT | calc/dc_rt.py |
| PVCF | CF, DC_RT | calc/pvcf.py |
| BEL | PVCF | calc/bel.py |

### 데이터 로더

| 로더 | 역할 | 파일 |
|------|------|------|
| RawAssumptionLoader | 드라이버 키매칭 (15차원) | data/rsk_lapse_loader.py |
| TradPVDataCache | 12개 참조테이블 일괄 로드 | data/trad_pv_loader.py |
| BNDataCache | 9개 참조테이블 (risk_meta, prtt) | data/bn_loader.py |
| ExpDataCache | 사업비 3개 테이블 + 드라이버 매칭 | data/exp_loader.py |

---

## 검증 방법

### 기대값 위치 (duckdb_transform.duckdb)

| 테이블 | IDNO 수 | 비고 |
|--------|---------|------|
| OD_RSK_RT | 42,001 | 전건 |
| OD_LAPSE_RT | 42,001 | 전건 |
| OD_TBL_MN | 42,001 | 전건 |
| OD_TRAD_PV | 42,001 | 전건 |
| OD_TBL_BN | 32,963 | BN 매핑 있는 계약만 |
| OD_EXP | 1 (760397) | 단건만 |
| OD_CF | 1 (760397) | 단건만 |
| OD_DC_RT | 1 (760397) | 단건만 |
| OD_PVCF | 1 (760397) | 단건만 |
| OP_BEL | 42,000 | 전건 (최종 결과) |

### 단건 검증 스크립트 패턴

```python
import duckdb, numpy as np
from cf_module.run import run_single

con = duckdb.connect('duckdb_transform.duckdb', read_only=True)
result = run_single(con, 760397)

# 기대값 로드
expected = con.execute("""
    SELECT CTR_AFT_PASS_MMCNT, COL1, COL2, ...
    FROM OD_XXX WHERE INFRC_IDNO=760397
    ORDER BY CTR_AFT_PASS_MMCNT
""").fetchall()

# 비교
elapsed = result.ctr.pass_yy * 12 + result.ctr.pass_mm
for row in expected:
    t = row[0]
    s = t - elapsed  # step index
    calc_val = result.xxx.to_dict()["COL1"][s]
    exp_val = row[1]
    diff = abs(exp_val - calc_val)
    # ...

con.close()
```

### 전건 테스트

```bash
python test_rsk_lapse_rt.py            # RSK_RT + LAPSE_RT 단건 (760397)
python test_lapse_rt_all.py            # OD_LAPSE_RT 42,001건 ALL PASS
python test_trad_pv_all.py             # OD_TRAD_PV 42,000건 ALL PASS
python test_tbl_bn_phase2.py --all     # OD_TBL_BN 32,963건 16/16 PASS
python test_bel_prem_base.py --all     # OP_BEL PREM_BASE 42,001건
python run_batch_bel.py                # OP_BEL 전건 산출 → output_bel.duckdb
```

---

## 미구현 항목 및 해결 가이드

### 1. EXP DRVR=9/10 (CNCTTP_ACUMAMT_KICS 불일치)

**현상**: MNT KD3(DRVR=9), KD5(DRVR=10), KD15(DRVR=9) FAIL

**원인**: CNCTTP_ACUMAMT_KICS가 LTRMNAT_TMRFND에 의존적으로 산출되어, 자체 산출값과 OD_EXP 기대값이 불일치

**파일**: `calc/exp.py` (76~82행 부근, DRVR=9/10 분기)

**해결 방향**:
1. `calc/trad_pv.py`에서 CNCTTP_ACUMAMT_KICS 산출 로직 확인
2. LTRMNAT_TMRFND 의존 제거 → 독립 KICS 경로 구현
3. 검증: OD_TRAD_PV.CNCTTP_ACUMAMT_KICS vs 자체산출값 비교

**검증 코드**:
```python
# OD_TRAD_PV vs 자체산출
pv_d = result.trad_pv.to_dict()
exp_kics = con.execute("""
    SELECT CTR_AFT_PASS_MMCNT, CNCTTP_ACUMAMT_KICS
    FROM OD_TRAD_PV WHERE INFRC_IDNO=760397
    ORDER BY CTR_AFT_PASS_MMCNT
""").fetchall()
for row in exp_kics:
    s = row[0] - 4
    print(f"t={row[0]} exp={row[1]:.4f} calc={pv_d['CNCTTP_ACUMAMT_KICS'][s]:.4f}")
```

### 2. ACQS KD2 전환 오차 (t=14, ~251)

**현상**: ACQSEXP_DR에서 t=14 근처 251 차이

**파일**: `calc/exp.py`, `data/exp_loader.py`

**조사 방향**:
```python
# ACQS KD2 rate 확인
con.execute("""
    SELECT ACQSEXP13, ACQSEXP14, ACQSEXP15
    FROM IA_E_ACQSEXP_DR
    WHERE PROD_GRP_CD=21 AND ASSM_GRP_CD5='05' AND ACQSEXP_KDCD=2
""").fetchone()

# OD_EXP 기대값과 비교
con.execute("""
    SELECT CTR_AFT_PASS_MMCNT, EXP_VAL FROM OD_EXP
    WHERE INFRC_IDNO=760397 AND EXP_TPCD='ACQS' AND EXP_KDCD=2
    AND CTR_AFT_PASS_MMCNT BETWEEN 12 AND 16
    ORDER BY CTR_AFT_PASS_MMCNT
""").fetchall()
```

### 3. CF TMRFND_INPAY / TMRFND_PYEX (미구현)

**현상**: 0으로 출력, 기대값은 비영

**파일**: `calc/cf.py` (CFResult 클래스, tmrfnd_inpay/tmrfnd_pyex 필드)

**조사 방향**:
```python
# 기대값 역산
con.execute("""
    SELECT CTR_AFT_PASS_MMCNT, TMRFND_INPAY, TMRFND_PYEX, TMRFND
    FROM OD_CF WHERE INFRC_IDNO=760397
    AND TMRFND_INPAY != 0
    ORDER BY CTR_AFT_PASS_MMCNT LIMIT 10
""").fetchall()
# TMRFND = TMRFND_INPAY + TMRFND_PYEX 인지 확인
# 납입중/납입후 구분 기준 확인 (pterm_mm 경계)
```

### 4. CF 미사용 컬럼 (760397에서 0)

아래 컬럼은 760397에서 전부 0이라 검증 불가. 다른 IDNO에서 테스트 필요:

| 컬럼 | 추정 공식 | 테스트 IDNO 탐색 SQL |
|------|-----------|---------------------|
| PREM_ADD | 추가보험료 | `SELECT DISTINCT INFRC_IDNO FROM OD_CF WHERE PREM_ADD != 0` |
| INSUAMT_HAFWAY | 중도보험금 | `... WHERE INSUAMT_HAFWAY != 0` |
| INSUAMT_MATU | 만기보험금 | `... WHERE INSUAMT_MATU != 0` |
| HAFWDR | 중도인출 | `... WHERE HAFWDR != 0` |
| LOAN_* | 약관대출 | `... WHERE LOAN_NEW != 0` |
| ACQSEXP_INDR | 간접신계약비 | `... WHERE ACQSEXP_INDR != 0` |
| MNTEXP_INDR | 간접유지비 | `... WHERE MNTEXP_INDR != 0` |

### 5. 전건 검증 확대

EXP~BEL 단계는 현재 760397 단건만 검증. 전건 테스트가 필요하면:

```python
# OD_EXP에 데이터가 있는 IDNO 확인
con.execute("SELECT DISTINCT INFRC_IDNO FROM OD_EXP").fetchall()
# → 현재 760397만 존재

# OD_CF 전건
con.execute("SELECT DISTINCT INFRC_IDNO FROM OD_CF").fetchall()
```

---

## 새 모듈 추가 패턴

### 1. 산출 모듈 (calc/)

```python
# calc/new_module.py
from dataclasses import dataclass
from typing import Dict
import numpy as np

@dataclass
class NewResult:
    n_steps: int
    col1: np.ndarray
    col2: np.ndarray

    def to_dict(self) -> Dict[str, np.ndarray]:
        return {"COL1": self.col1, "COL2": self.col2}

def compute_new(n_steps: int, ...) -> NewResult:
    ...
    return NewResult(n_steps=n_steps, col1=..., col2=...)
```

### 2. 데이터 로더 (data/)

```python
# data/new_loader.py
class NewDataCache:
    def __init__(self, conn, pcv_filter=None):
        # pcv_filter로 단건 최적화
        self._pcv_filter = pcv_filter
        self._load_table(conn)

    def _pcv_where(self, prefix="WHERE"):
        if not self._pcv_filter:
            return ""
        conds = " OR ".join(
            f"(PROD_CD = '{p}' AND COV_CD = '{c}')" for p, c in self._pcv_filter
        )
        return f" {prefix} ({conds})"
```

### 3. 파이프라인 통합 (run.py)

1. `SingleResult`에 필드 추가
2. `run_single()`에 산출 스텝 추가
3. `_save_csv()`에 CSV 출력 추가
4. `ALL_TABLES`에 테이블명 추가

---

## 디버깅 팁

### 단건 빠른 실행 (Python)

```python
import duckdb
from cf_module.run import run_single

con = duckdb.connect('duckdb_transform.duckdb', read_only=True)
r = run_single(con, 760397)

# 각 단계 결과 접근
r.rsk_rt          # {risk_cd: {col: array}}
r.lapse_rt        # {col: array}
r.tbl_mn          # {col: array}
r.trad_pv.to_dict()  # {col: array}
r.tbl_bn.bnft_results  # {bnft_no: BNFTResult}
r.exp_results     # [ExpResult, ...]
r.cf.to_dict()    # {col: array}
r.dc_rt.to_dict() # {col: array}
r.pvcf.to_dict()  # {col: array}
r.bel.to_dict()   # {col: float}

con.close()
```

### 특정 시점 값 확인

```python
s = 5  # step (0-based)
t = s + r.ctr.pass_yy * 12 + r.ctr.pass_mm  # CTR_AFT_PASS_MMCNT

print(f"CTR_TRMO={r.tbl_mn['CTR_TRMO_MTNPSN_CNT'][s]:.8f}")
print(f"ORIG_PREM={r.trad_pv.to_dict()['ORIG_PREM'][s]:.2f}")
print(f"PREM_BASE={r.cf.to_dict()['PREM_BASE'][s]:.2f}")
```

### DB 기대값 빠른 조회

```python
con.execute("""
    SELECT * FROM OD_CF WHERE INFRC_IDNO=760397
    AND CTR_AFT_PASS_MMCNT=10
""").fetchone()
```

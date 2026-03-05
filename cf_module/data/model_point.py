"""
Model Point 로딩, 검증, 정규화

보험 계약 정보를 읽어 프로젝션에 필요한 표준 컬럼 구조로 변환한다.
수십만 건 Model Point를 numpy 배열로 관리한다.
"""

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from cf_module.config import CFConfig
from cf_module.io.reader import DataReader
from cf_module.utils.logger import get_logger

logger = get_logger("model_point")

# Model Point 필수 컬럼 정의 (정규화된 이름)
MP_REQUIRED_COLUMNS = {
    "mp_id",           # Model Point 식별자
    "product_cd",      # 상품코드
    "sex_cd",          # 성별 (M/F)
    "age_at_entry",    # 가입연령
    "ctr_ym",          # 계약년월 (yyyymm)
    "bterm",           # 보장기간 (년)
    "pterm",           # 납입기간 (년)
    "premium",         # 보험료
    "sum_assured",     # 보험가입금액
}

# ASSM_DIV_VAL / RSK_RT_DIV_VAL 컬럼명 리스트
ASSM_DIV_VAL_COLS = [f"ASSM_DIV_VAL{i}" for i in range(1, 16)]
RSK_RT_DIV_VAL_COLS = [f"RSK_RT_DIV_VAL{i}" for i in range(1, 11)]

# 기존 DB 컬럼 → 정규화 컬럼 매핑
# 여러 대체 컬럼명을 지원: 우선순위가 높은 것이 먼저
COLUMN_MAPPING = {
    # II_INFRC 테이블 기준
    "INFRC_IDNO": "mp_id",
    "PROD_CD": "product_cd",
    "SEX_CD": "sex_cd",
    "ISR_OBJV_DVCD": "sex_cd",         # SEX_CD가 없을 때 대체
    "ISRD_JOIN_AGE": "age_at_entry",
    "CTR_YM": "ctr_ym",
    "INSTRM_YYCNT": "bterm",
    "PAYPR_YYCNT": "pterm",
    "PREM": "premium",
    "GRNTPT_GPREM": "premium",          # PREM이 없을 때 대체
    "INSAMT": "sum_assured",
    "GRNTPT_JOIN_AMT": "sum_assured",   # INSAMT이 없을 때 대체
    # 분류값 (위험률/해약률 키 생성에 사용)
    "CLS_CD": "cls_cd",
    "COV_CD": "cov_cd",
    # 해약공제액
    "TOT_TRMNAT_DDCT_AMT": "deductible",
    # 마감년월
    "CLOS_YM": "clos_ym",
}


@dataclass
class ModelPointSet:
    """Model Point 데이터셋

    Attributes:
        df: 원본 DataFrame (정규화된 컬럼명)
        n_points: Model Point 건수
        mp_ids: MP 식별자 배열
        age_at_entry: 가입연령 배열 (n_points,)
        bterm: 보장기간 배열 (n_points,)  [년]
        pterm: 납입기간 배열 (n_points,)  [년]
        premium: 보험료 배열 (n_points,)
        sum_assured: 보험가입금액 배열 (n_points,)
        sex_cd: 성별코드 배열 (n_points,)
        product_cd: 상품코드 배열 (n_points,)
        ctr_ym: 계약년월 배열 (n_points,)
        deductible: 해약공제액 배열 (n_points,) — optional, 없으면 0
        cls_cd: 분류코드 배열 (n_points,) — 가정 키 생성용
        cov_cd: 담보코드 배열 (n_points,) — 가정 키 생성용
        clos_ym: 마감년월 배열 (n_points,)
        assm_div_vals: 가정 분류값 (n_points, 15) ASSM_DIV_VAL1~15
        rsk_rt_div_vals: 위험률 분류값 (n_points, 10) RSK_RT_DIV_VAL1~10
    """
    df: pd.DataFrame
    n_points: int
    mp_ids: np.ndarray
    age_at_entry: np.ndarray
    bterm: np.ndarray
    pterm: np.ndarray
    premium: np.ndarray
    sum_assured: np.ndarray
    sex_cd: np.ndarray
    product_cd: np.ndarray
    ctr_ym: np.ndarray
    deductible: np.ndarray
    cls_cd: np.ndarray = field(default_factory=lambda: np.array([]))
    cov_cd: np.ndarray = field(default_factory=lambda: np.array([]))
    clos_ym: np.ndarray = field(default_factory=lambda: np.array([]))
    assm_div_vals: Optional[np.ndarray] = None   # (n_points, 15)
    rsk_rt_div_vals: Optional[np.ndarray] = None  # (n_points, 10)

    @property
    def extra_columns(self) -> pd.DataFrame:
        """정규화 필수 컬럼 외 나머지 컬럼"""
        extra_cols = [c for c in self.df.columns if c not in MP_REQUIRED_COLUMNS]
        return self.df[extra_cols]


def load_model_points(
    reader: DataReader,
    config: CFConfig,
    query_name: str = "II_INFRC",
    params: Optional[dict] = None,
    csv_path: Optional[str] = None,
) -> ModelPointSet:
    """Model Point를 로딩하고 정규화한다.

    Args:
        reader: 데이터 리더
        config: 전역 설정
        query_name: 쿼리 이름 (DB 로딩 시)
        params: 쿼리 파라미터
        csv_path: CSV 파일 경로 (CSV 로딩 시, 우선 적용)

    Returns:
        ModelPointSet
    """
    # 데이터 로딩
    if csv_path:
        logger.info(f"CSV에서 MP 로딩: {csv_path}")
        df = reader.read_csv(csv_path)
    else:
        logger.info(f"DB에서 MP 로딩: {query_name}")
        df = reader.fetch_data(query_name, params or {})

    logger.info(f"로딩 완료: {len(df)}건")

    # 컬럼명 정규화
    logger.debug("[MP] 원본 컬럼(%d개): %s", len(df.columns), list(df.columns))
    df = _normalize_columns(df)
    logger.debug("[MP] 정규화 후 컬럼: %s", list(df.columns))

    # 검증
    _validate(df)

    # 타입 변환
    df = _convert_types(df)

    # deductible: optional — 없으면 0으로 처리
    if "deductible" in df.columns:
        deductible = pd.to_numeric(df["deductible"], errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
    else:
        deductible = np.zeros(len(df), dtype=np.float64)

    # cls_cd, cov_cd: optional
    cls_cd = df["cls_cd"].to_numpy() if "cls_cd" in df.columns else np.full(len(df), "")
    cov_cd = df["cov_cd"].to_numpy() if "cov_cd" in df.columns else np.full(len(df), "")

    # clos_ym: optional
    clos_ym_arr = df["clos_ym"].to_numpy() if "clos_ym" in df.columns else np.full(len(df), "")

    # ASSM_DIV_VAL1~15 추출 (n_points, 15)
    assm_div_vals = _extract_div_vals(df, ASSM_DIV_VAL_COLS)

    # RSK_RT_DIV_VAL1~10 추출 (n_points, 10)
    rsk_rt_div_vals = _extract_div_vals(df, RSK_RT_DIV_VAL_COLS)

    mp = ModelPointSet(
        df=df,
        n_points=len(df),
        mp_ids=df["mp_id"].to_numpy(),
        age_at_entry=df["age_at_entry"].to_numpy(dtype=np.int32),
        bterm=df["bterm"].to_numpy(dtype=np.int32),
        pterm=df["pterm"].to_numpy(dtype=np.int32),
        premium=df["premium"].to_numpy(dtype=np.float64),
        sum_assured=df["sum_assured"].to_numpy(dtype=np.float64),
        sex_cd=df["sex_cd"].to_numpy(),
        product_cd=df["product_cd"].to_numpy(),
        ctr_ym=df["ctr_ym"].to_numpy(),
        deductible=deductible,
        cls_cd=cls_cd,
        cov_cd=cov_cd,
        clos_ym=clos_ym_arr,
        assm_div_vals=assm_div_vals,
        rsk_rt_div_vals=rsk_rt_div_vals,
    )

    # DEBUG: 단건 상세 정보
    for i in range(min(mp.n_points, 3)):
        logger.debug(
            "[MP] #%d  mp_id=%s  PROD=%s  SEX=%s  AGE=%d  BTERM=%d  PTERM=%d  "
            "PREM=%.0f  SA=%.0f  CLS=%s  COV=%s  CTR_YM=%s",
            i, mp.mp_ids[i], mp.product_cd[i], mp.sex_cd[i],
            mp.age_at_entry[i], mp.bterm[i], mp.pterm[i],
            mp.premium[i], mp.sum_assured[i], cls_cd[i], cov_cd[i], mp.ctr_ym[i],
        )
    if assm_div_vals is not None and mp.n_points > 0:
        logger.debug("[MP] ASSM_DIV_VAL[0]: %s", list(assm_div_vals[0]))
    if rsk_rt_div_vals is not None and mp.n_points > 0:
        logger.debug("[MP] RSK_RT_DIV_VAL[0]: %s", list(rsk_rt_div_vals[0]))

    return mp


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """DB 컬럼명을 정규화된 이름으로 변환한다."""
    # CTR_DT (yyyymmdd) → CTR_YM (yyyymm) 변환
    if "CTR_DT" in df.columns and "CTR_YM" not in df.columns:
        df["CTR_YM"] = df["CTR_DT"].astype(str).str[:6]

    rename_map = {}
    already_mapped = set()  # 이미 매핑된 정규화 이름 추적 (중복 방지)
    for old_name, new_name in COLUMN_MAPPING.items():
        if old_name in df.columns and new_name not in already_mapped:
            rename_map[old_name] = new_name
            already_mapped.add(new_name)

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _validate(df: pd.DataFrame) -> None:
    """필수 컬럼 존재 여부를 검증한다."""
    missing = MP_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"필수 컬럼이 누락되었습니다: {missing}")

    if df.empty:
        raise ValueError("Model Point가 비어 있습니다.")


def _convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """컬럼 타입을 변환한다."""
    int_cols = ["age_at_entry", "bterm", "pterm"]
    float_cols = ["premium", "sum_assured"]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def _extract_div_vals(df: pd.DataFrame, col_names: list) -> Optional[np.ndarray]:
    """DataFrame에서 DIV_VAL 컬럼들을 (n_points, n_cols) 배열로 추출한다.

    컬럼이 하나도 없으면 None 반환.
    """
    present = [c for c in col_names if c in df.columns]
    if not present:
        return None

    n = len(df)
    n_cols = len(col_names)
    result = np.full((n, n_cols), "", dtype=object)
    for idx, col in enumerate(col_names):
        if col in df.columns:
            result[:, idx] = df[col].fillna("").astype(str).to_numpy()
    return result

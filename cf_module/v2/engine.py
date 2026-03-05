"""
그룹 기반 벡터 연산 엔진

핵심 설계:
1. 같은 assm_profile = 같은 가정 테이블 → 1번만 로드
2. 그룹 내 N건을 numpy 브로드캐스트로 동시 처리
3. 중복제거: np.einsum으로 C행렬 곱 (Python 루프 0개)
4. 청크 단위 처리로 메모리 제어

성능 목표:
- 1억 건 × 120개월 → 수 시간 (8코어 기준)
- 청크당 50K건 × 120개월 → ~500MB 메모리
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np


# ============================================================
# 데이터 구조 (v2)
# ============================================================

@dataclass
class RiskMeta:
    """위험률코드 메타데이터 (그룹 내 공유)."""
    risk_cds: np.ndarray        # (n_risks,) 위험률코드
    chr_cd: np.ndarray          # (n_risks,) 'S'/'A'
    is_death: np.ndarray        # (n_risks,) bool
    risk_group: np.ndarray      # (n_risks,) 위험그룹 문자열
    mm_trf_way_cd: np.ndarray   # (n_risks,) 1 or 2
    is_exit_ctr: np.ndarray     # (n_risks,) bool — RSV | BNFT
    is_exit_pay: np.ndarray     # (n_risks,) bool — RSV | BNFT | PYEXSP
    is_exit_pyexsp: np.ndarray  # (n_risks,) bool — PYEXSP only
    is_exit_rsv: np.ndarray     # (n_risks,) bool
    is_exit_bnft: np.ndarray    # (n_risks,) bool


@dataclass
class GroupAssumptions:
    """한 그룹(assm_profile)에 대한 모든 가정 데이터."""
    # 위험률: {risk_cd: rate_by_age} — A타입은 age 인덱스, S타입은 스칼라
    mortality_rates: Dict[str, np.ndarray]
    risk_meta: RiskMeta

    # 해지율: (max_dur,) 벡터
    lapse_paying: np.ndarray     # 납입중 해지율
    lapse_paidup: np.ndarray     # 납입후 해지율

    # 스큐: (max_dur,) 벡터
    skew: np.ndarray

    # BEPRD: {risk_cd: (max_dur,)} 경과년도별 지급률
    beprd: Dict[str, np.ndarray]

    # C행렬 (사전계산, 그룹 내 불변)
    c_matrix_ctr: np.ndarray     # (n_exit_ctr+1, n_exit_ctr+1) — +1은 wx
    c_matrix_pay: np.ndarray     # (n_exit_pay+1, n_exit_pay+1)
    exit_idx_ctr: np.ndarray     # C행렬의 exit 인덱스 (risk_cds 기준)
    exit_idx_pay: np.ndarray


@dataclass
class ProjectionResultV2:
    """그룹 프로젝션 결과."""
    contract_ids: np.ndarray     # (n,)
    tpx: np.ndarray              # (n, max_t) 유지율
    pay_tpx: np.ndarray          # (n, max_t) 납입자 유지율
    qx_monthly: np.ndarray       # (n, max_t) 중복제거 후 총 사망률
    wx_monthly: np.ndarray       # (n, max_t) CTR 중복제거 후 해지율
    wx_pay_monthly: np.ndarray   # (n, max_t) PAY 중복제거 후 해지율
    d_death: np.ndarray          # (n, max_t) 사망탈퇴자
    d_lapse: np.ndarray          # (n, max_t) 해지탈퇴자
    d_rsvamt: np.ndarray         # (n, max_t) 준비금탈퇴자
    d_bnft: np.ndarray           # (n, max_t) 급부탈퇴자
    d_pyexsp: np.ndarray         # (n, max_t) 납면탈퇴자


# ============================================================
# 가정 로딩 (SQL → numpy, 그룹당 1회)
# ============================================================

def load_group_assumptions(
    conn: duckdb.DuckDBPyConnection,
    assm_profile: str,
    sample_contract_id: int,
    max_duration: int = 1200,
) -> GroupAssumptions:
    """한 그룹의 가정 데이터를 DuckDB에서 로드한다.

    Args:
        conn: v2 DuckDB 커넥션
        assm_profile: 가정 프로파일 키
        sample_contract_id: 이 그룹의 아무 계약 ID (위험률 매핑 조회용)
        max_duration: 최대 경과월
    """
    # 1. 위험률 메타 + exit 플래그
    risk_meta = _load_risk_meta(conn, sample_contract_id)

    # 2. 위험률 값
    mortality_rates = _load_mortality_rates(conn, risk_meta.risk_cds)

    # 3. 해지율 (paidup만 데이터 범위 이후 마지막 값 연장)
    lapse_paying = _load_rate_vector(
        conn, "fact_lapse", assm_profile,
        filter_col="pay_phase", filter_val="paying",
        max_dur=max_duration,
    )
    lapse_paidup = _load_rate_vector(
        conn, "fact_lapse", assm_profile,
        filter_col="pay_phase", filter_val="paidup",
        max_dur=max_duration, extend_last=True,
    )

    # 4. 스큐
    skew = _load_rate_vector(
        conn, "fact_skew", assm_profile,
        val_col="factor", max_dur=max_duration, default=1.0,
    )

    # 5. BEPRD
    beprd = _load_beprd(conn, assm_profile, risk_meta.risk_cds, max_duration)

    # 6. C행렬 사전계산
    c_ctr, exit_idx_ctr = build_c_matrix(risk_meta, exit_type="ctr")
    c_pay, exit_idx_pay = build_c_matrix(risk_meta, exit_type="pay")

    return GroupAssumptions(
        mortality_rates=mortality_rates,
        risk_meta=risk_meta,
        lapse_paying=lapse_paying,
        lapse_paidup=lapse_paidup,
        skew=skew,
        beprd=beprd,
        c_matrix_ctr=c_ctr,
        c_matrix_pay=c_pay,
        exit_idx_ctr=exit_idx_ctr,
        exit_idx_pay=exit_idx_pay,
    )


def _load_risk_meta(
    conn: duckdb.DuckDBPyConnection, contract_id: int
) -> RiskMeta:
    """계약의 위험률 메타데이터 로드 (dim_risk + map_contract_risk JOIN)."""
    df = conn.execute("""
        SELECT
            r.risk_cd,
            r.chr_cd,
            r.is_death,
            r.risk_group,
            r.mm_trf_way_cd,
            cr.is_exit_rsv,
            cr.is_exit_bnft,
            cr.is_exit_pay AS is_exit_pyexsp
        FROM map_contract_risk cr
        JOIN dim_risk r ON cr.risk_cd = r.risk_cd
        WHERE cr.contract_id = ?
        ORDER BY r.risk_cd
    """, [contract_id]).fetchdf()

    if df.empty:
        return RiskMeta(
            risk_cds=np.array([], dtype=str),
            chr_cd=np.array([], dtype=str),
            is_death=np.array([], dtype=bool),
            risk_group=np.array([], dtype=str),
            mm_trf_way_cd=np.array([], dtype=int),
            is_exit_ctr=np.array([], dtype=bool),
            is_exit_pay=np.array([], dtype=bool),
            is_exit_pyexsp=np.array([], dtype=bool),
            is_exit_rsv=np.array([], dtype=bool),
            is_exit_bnft=np.array([], dtype=bool),
        )

    is_exit_rsv = df["is_exit_rsv"].values.astype(bool)
    is_exit_bnft = df["is_exit_bnft"].values.astype(bool)
    is_exit_pyexsp = df["is_exit_pyexsp"].values.astype(bool)

    return RiskMeta(
        risk_cds=df["risk_cd"].values.astype(str),
        chr_cd=df["chr_cd"].values.astype(str),
        is_death=df["is_death"].values.astype(bool),
        risk_group=df["risk_group"].values.astype(str),
        mm_trf_way_cd=df["mm_trf_way_cd"].values.astype(int),
        is_exit_ctr=is_exit_rsv | is_exit_bnft,
        is_exit_pay=is_exit_rsv | is_exit_bnft | is_exit_pyexsp,
        is_exit_pyexsp=is_exit_pyexsp & ~is_exit_rsv & ~is_exit_bnft,
        is_exit_rsv=is_exit_rsv,
        is_exit_bnft=is_exit_bnft,
    )


def _load_mortality_rates(
    conn: duckdb.DuckDBPyConnection,
    risk_cds: np.ndarray,
) -> Dict[str, np.ndarray]:
    """위험률 값 로드: {risk_cd: rate_by_age}."""
    if len(risk_cds) == 0:
        return {}

    placeholders = ", ".join(["?"] * len(risk_cds))
    df = conn.execute(f"""
        SELECT risk_cd, age, rate
        FROM fact_mortality
        WHERE risk_cd IN ({placeholders})
        ORDER BY risk_cd, age
    """, list(risk_cds)).fetchdf()

    rates = {}
    for risk_cd, group in df.groupby("risk_cd"):
        if group["age"].iloc[0] == -1:
            # S타입: 스칼라
            rates[risk_cd] = np.array([group["rate"].iloc[0]])
        else:
            # A타입: age 인덱스 배열 (0~max_age)
            max_age = int(group["age"].max())
            arr = np.zeros(max_age + 1, dtype=np.float64)
            arr[group["age"].values.astype(int)] = group["rate"].values
            rates[risk_cd] = arr

    return rates


def _load_rate_vector(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    assm_profile: str,
    filter_col: Optional[str] = None,
    filter_val: Optional[str] = None,
    val_col: str = "rate",
    max_dur: int = 1200,
    default: float = 0.0,
    extend_last: bool = False,
) -> np.ndarray:
    """가정 팩트 테이블에서 경과월 벡터를 로드.

    extend_last=True이면 데이터 범위 이후를 마지막 값으로 연장.
    """
    where = f"assm_profile = ? "
    params = [assm_profile]
    if filter_col and filter_val:
        where += f"AND {filter_col} = ? "
        params.append(filter_val)

    df = conn.execute(f"""
        SELECT duration, {val_col} AS val
        FROM {table}
        WHERE {where}
        ORDER BY duration
    """, params).fetchdf()

    vec = np.full(max_dur, default, dtype=np.float64)
    if not df.empty:
        durations = df["duration"].values.astype(int) - 1  # 0-based
        valid = (durations >= 0) & (durations < max_dur)
        vec[durations[valid]] = df["val"].values[valid]

        if extend_last:
            last_idx = int(durations[valid].max())
            if last_idx < max_dur - 1:
                vec[last_idx + 1:] = vec[last_idx]
    return vec


def _load_beprd(
    conn: duckdb.DuckDBPyConnection,
    assm_profile: str,
    risk_cds: np.ndarray,
    max_dur: int,
) -> Dict[str, np.ndarray]:
    """BEPRD 경과년도별 지급률 로드. 데이터 범위 초과 시 마지막 값 연장."""
    result = {}
    for risk_cd in risk_cds:
        vec = _load_rate_vector(
            conn, "fact_beprd", assm_profile,
            filter_col="risk_cd", filter_val=str(risk_cd),
            max_dur=max_dur, default=1.0,
        )
        # 마지막 유효값으로 나머지 채움 (데이터 범위 초과 시 1.0 대신)
        df = conn.execute("""
            SELECT MAX(duration) FROM fact_beprd
            WHERE assm_profile = ? AND risk_cd = ?
        """, [assm_profile, str(risk_cd)]).fetchone()
        if df[0] is not None:
            last_dur = int(df[0]) - 1  # 0-based
            if last_dur >= 0 and last_dur < max_dur:
                vec[last_dur + 1:] = vec[last_dur]
        result[str(risk_cd)] = vec
    return result


# ============================================================
# C행렬 구축 (순수 numpy, Python 루프 최소화)
# ============================================================

def build_c_matrix(
    risk_meta: RiskMeta,
    exit_type: str = "ctr",
) -> Tuple[np.ndarray, np.ndarray]:
    """중복제거 C행렬 구축.

    C행렬 규칙 (Cij = 0):
    1. i == j (자기자신)
    2. risk_group[i] == risk_group[j] (동일위험그룹)
    3. is_death[j] == True (j가 사망위험)

    Returns:
        C: (n_rates, n_rates) — n_rates = 1(wx) + n_exit_risks
        exit_idx: (n_exit_risks,) — risk_meta.risk_cds 기준 인덱스
    """
    if exit_type == "ctr":
        exit_mask = risk_meta.is_exit_ctr
    else:
        exit_mask = risk_meta.is_exit_pay

    exit_idx = np.where(exit_mask)[0]
    n_exit = len(exit_idx)
    n_rates = 1 + n_exit  # wx(0번) + exit risks

    C = np.ones((n_rates, n_rates), dtype=np.float64)

    # 1. 대각선 = 0 (자기자신)
    np.fill_diagonal(C, 0.0)

    # 2. 동일위험그룹 = 0
    # g[0] = "wx" (고유), g[1:] = risk_group of exit risks
    groups = np.empty(n_rates, dtype=object)
    groups[0] = "__wx__"  # wx는 고유 그룹
    groups[1:] = risk_meta.risk_group[exit_idx]

    same_group = (groups[:, None] == groups[None, :])
    np.fill_diagonal(same_group, False)  # 자기자신 제외
    C[same_group] = 0.0

    # 3. j가 사망위험이면 해당 열 = 0 (column mask)
    death_mask = np.zeros(n_rates, dtype=bool)
    death_mask[0] = False  # wx는 사망 아님
    death_mask[1:] = risk_meta.is_death[exit_idx]
    C[:, death_mask] = 0.0

    return C, exit_idx


# ============================================================
# 벡터 프로젝션 (핵심 연산)
# ============================================================

def project_group(
    conn: duckdb.DuckDBPyConnection,
    assm: GroupAssumptions,
    contract_ids: np.ndarray,
    entry_ages: np.ndarray,
    bterms: np.ndarray,
    pterms: np.ndarray,
    elapsed_months: np.ndarray,
    clos_ym: str = "202309",
    max_proj_months: int = 1200,
) -> ProjectionResultV2:
    """한 그룹의 모든 계약을 벡터 연산으로 프로젝션.

    Args:
        conn: v2 DuckDB (미사용, 향후 확장)
        assm: 그룹 가정 데이터 (이미 로드됨)
        contract_ids: (n,) 계약 ID
        entry_ages: (n,) 가입연령
        bterms: (n,) 보장기간(년)
        pterms: (n,) 납입기간(년)
        elapsed_months: (n,) 기준일 기준 경과월
        clos_ym: 결산년월
        max_proj_months: 최대 프로젝션 개월

    Returns:
        ProjectionResultV2
    """
    n = len(contract_ids)
    bterm_months = bterms * 12
    pterm_months = pterms * 12

    # 프로젝션 길이
    proj_lengths = np.maximum(bterm_months - elapsed_months + 1, 0)
    max_t = min(int(proj_lengths.max()), max_proj_months)

    if max_t <= 0:
        empty = np.zeros((n, 0))
        return ProjectionResultV2(
            contract_ids=contract_ids,
            tpx=empty, pay_tpx=empty,
            qx_monthly=empty, wx_monthly=empty, wx_pay_monthly=empty,
            d_death=empty, d_lapse=empty,
            d_rsvamt=empty, d_bnft=empty, d_pyexsp=empty,
        )

    # ── 1. 시간축 (n, max_t) ──
    t_range = np.arange(max_t, dtype=np.int32)               # (max_t,)
    duration_months = elapsed_months[:, None] + t_range[None, :]  # (n, max_t)
    # 경과연수: month 0~11=year1, 12~23=year2, ... (PROJ_O2 기준)
    duration_years = duration_months // 12 + 1                # (n, max_t)
    ages = entry_ages[:, None] + (duration_months // 12)      # (n, max_t)
    is_in_force = duration_months < bterm_months[:, None]     # (n, max_t)
    is_paying = duration_months < pterm_months[:, None]       # (n, max_t)

    # 경과월 인덱스 (0-based, clipped) — 여러 곳에서 재사용
    dur_idx_0 = np.clip(duration_months - 1, 0, max_proj_months - 1)  # (n, max_t)

    # ── 2. 위험률 추출 (n_risks, n, max_t) ──
    risk_meta = assm.risk_meta
    n_risks = len(risk_meta.risk_cds)
    qx_raw = np.empty((n_risks, n, max_t), dtype=np.float64)

    # age clipping 사전 계산 (모든 A타입이 공유)
    ages_i32 = ages.astype(np.int32)

    for r_idx, risk_cd in enumerate(risk_meta.risk_cds):
        rate_arr = assm.mortality_rates.get(str(risk_cd))
        if rate_arr is None:
            qx_raw[r_idx] = 0.0
            continue

        if risk_meta.chr_cd[r_idx] == "S":
            qx_raw[r_idx] = rate_arr[0]
        else:
            clipped = np.clip(ages_i32, 0, len(rate_arr) - 1)
            qx_raw[r_idx] = rate_arr[clipped]

    # BEPRD 적용 + 월변환을 하나의 루프로 병합
    # BEPRD는 연도 단위 — duration_years 기반 인덱스 사용
    beprd_dur_idx = np.clip(duration_years - 1, 0, max_proj_months - 1)  # (n, max_t)
    for r_idx, risk_cd in enumerate(risk_meta.risk_cds):
        # BEPRD
        beprd_vec = assm.beprd.get(str(risk_cd))
        if beprd_vec is not None:
            beprd_idx = np.clip(beprd_dur_idx, 0, len(beprd_vec) - 1)
            qx_raw[r_idx] *= beprd_vec[beprd_idx]

        # 월변환
        if risk_meta.mm_trf_way_cd[r_idx] == 1:
            np.subtract(1.0, qx_raw[r_idx], out=qx_raw[r_idx])
            np.clip(qx_raw[r_idx], 0, None, out=qx_raw[r_idx])
            np.power(qx_raw[r_idx], 1.0 / 12, out=qx_raw[r_idx])
            np.subtract(1.0, qx_raw[r_idx], out=qx_raw[r_idx])
        else:
            qx_raw[r_idx] /= 12.0

    # ── 3. 해지율 (n, max_t) ──
    # fact_lapse는 연도별 동일 rate × 12개월 확장 — duration_years 기반 인덱스 사용
    # paying: 계약 시작 기준 경과연수
    lapse_pay_idx = np.clip((duration_years - 1) * 12, 0, len(assm.lapse_paying) - 1)
    wx_paying = assm.lapse_paying[lapse_pay_idx]   # (n, max_t)
    # paidup: 납입후 시작 기준 경과연수 (pterm 이후부터 1년차)
    paidup_months = np.maximum(duration_months - pterm_months[:, None], 0)  # (n, max_t)
    paidup_years = paidup_months // 12 + 1                                  # (n, max_t)
    lapse_paidup_idx = np.clip((paidup_years - 1) * 12, 0, len(assm.lapse_paidup) - 1)
    wx_paidup = assm.lapse_paidup[lapse_paidup_idx]   # (n, max_t)
    wx_raw = np.where(is_paying, wx_paying, wx_paidup)  # (n, max_t)

    # 연률 → 월률 변환: 1-(1-q)^(1/12)  (v1 _annual_to_monthly 동일)
    np.clip(wx_raw, 0, 1, out=wx_raw)
    wx_raw = 1.0 - (1.0 - wx_raw) ** (1.0 / 12.0)

    # ── 4. 중복제거 (einsum) ──
    # CTR
    qx_ctr, wx_ctr = _apply_dedup(
        qx_raw, wx_raw, risk_meta,
        assm.c_matrix_ctr, assm.exit_idx_ctr, "ctr"
    )

    # PAY
    qx_pay, wx_pay = _apply_dedup(
        qx_raw, wx_raw, risk_meta,
        assm.c_matrix_pay, assm.exit_idx_pay, "pay"
    )

    # ── 5. tpx 계산 (CTR/PAY) ──
    total_qx_ctr = qx_ctr.sum(axis=0)      # (n, max_t)
    dx_ctr = total_qx_ctr + wx_ctr          # (n, max_t)
    dx_ctr = np.clip(dx_ctr, 0, 1)
    dx_ctr *= is_in_force                    # out-of-force 마스킹
    tpx = np.cumprod(1.0 - dx_ctr, axis=1)  # (n, max_t)

    total_qx_pay = qx_pay.sum(axis=0)
    dx_pay = total_qx_pay + wx_pay
    dx_pay = np.clip(dx_pay, 0, 1)
    dx_pay *= is_in_force
    pay_tpx = np.cumprod(1.0 - dx_pay, axis=1)

    # ── 6. 탈퇴자 분해 ──
    # tpx_bot (기시 유지자 — CTR 기준)
    tpx_bot = np.ones_like(tpx)
    tpx_bot[:, 1:] = tpx[:, :-1]

    # pay_tpx_bot (기시 납입자 — PAY 기준)
    pay_tpx_bot = np.ones_like(pay_tpx)
    pay_tpx_bot[:, 1:] = pay_tpx[:, :-1]

    # 위험률코드별 중복제거 결과 → 탈퇴 사유별 합산
    d_death = tpx_bot * total_qx_ctr
    d_lapse = tpx_bot * wx_ctr

    # 준비금/급부 분해
    rsv_idx = np.where(risk_meta.is_exit_rsv)[0]
    bnft_idx = np.where(risk_meta.is_exit_bnft)[0]

    d_rsvamt = _sum_risk_rates(qx_ctr, rsv_idx, assm.exit_idx_ctr, tpx_bot)
    d_bnft = _sum_risk_rates(qx_ctr, bnft_idx, assm.exit_idx_ctr, tpx_bot)

    # PAY 납면 탈퇴자 — PAY tpx_bot 기준
    pyexsp_only_idx = np.where(
        risk_meta.is_exit_pyexsp & ~risk_meta.is_exit_rsv & ~risk_meta.is_exit_bnft
    )[0]
    d_pyexsp = _sum_risk_rates(qx_pay, pyexsp_only_idx, assm.exit_idx_pay, pay_tpx_bot)

    return ProjectionResultV2(
        contract_ids=contract_ids,
        tpx=tpx,
        pay_tpx=pay_tpx,
        qx_monthly=total_qx_ctr,
        wx_monthly=wx_ctr,
        wx_pay_monthly=wx_pay,
        d_death=d_death,
        d_lapse=d_lapse,
        d_rsvamt=d_rsvamt,
        d_bnft=d_bnft,
        d_pyexsp=d_pyexsp,
    )


def _apply_dedup(
    qx_raw: np.ndarray,       # (n_risks, n, max_t)
    wx_raw: np.ndarray,        # (n, max_t)
    risk_meta: RiskMeta,
    c_matrix: np.ndarray,      # (n_rates, n_rates)
    exit_idx: np.ndarray,      # (n_exit,)
    exit_type: str,
) -> Tuple[np.ndarray, np.ndarray]:
    """중복제거 위험률 적용.

    q'ᵢ = qᵢ × (1 - Σⱼ(qⱼ × Cᵢⱼ) / 2)

    Args:
        qx_raw: (n_risks, n, max_t) 원 위험률
        wx_raw: (n, max_t) 원 해지율
        c_matrix: (n_rates, n_rates) C행렬, n_rates = 1 + len(exit_idx)
        exit_idx: C행렬에 참여하는 risk 인덱스

    Returns:
        qx_dedup: (n_exit, n, max_t) 중복제거 위험률
        wx_dedup: (n, max_t) 중복제거 해지율
    """
    n_exit = len(exit_idx)
    if n_exit == 0:
        return np.zeros_like(qx_raw[:0]), wx_raw.copy()

    n_rates = 1 + n_exit  # wx + exit risks
    n, max_t = wx_raw.shape

    # r: (n_rates, n, max_t) — [0]=wx, [1:]=exit_qx
    r = np.empty((n_rates, n, max_t), dtype=np.float64)
    r[0] = wx_raw
    r[1:] = qx_raw[exit_idx]

    # 중복제거: r @ C.T를 (n*max_t) 배치 행렬곱으로 수행
    # r을 (n*max_t, n_rates)로 reshape → matmul → reshape back
    C_T = c_matrix.T  # (n_rates, n_rates)
    r_flat = r.reshape(n_rates, -1).T        # (n*max_t, n_rates)
    adj_flat = r_flat @ C_T                   # (n*max_t, n_rates)
    adjustment = adj_flat.T.reshape(n_rates, n, max_t) / 2.0

    # q'ᵢ = qᵢ × (1 - adjustment)
    r_dedup = r * (1.0 - adjustment)
    np.maximum(r_dedup, 0.0, out=r_dedup)  # 음수 방지 (in-place)

    wx_dedup = r_dedup[0]       # (n, max_t)
    qx_dedup = r_dedup[1:]     # (n_exit, n, max_t)

    return qx_dedup, wx_dedup


def _sum_risk_rates(
    qx_dedup: np.ndarray,      # (n_exit, n, max_t) from C matrix
    target_idx: np.ndarray,     # risk_meta 기준 인덱스
    exit_idx: np.ndarray,       # C행렬 exit 인덱스
    tpx_bot: np.ndarray,        # (n, max_t)
) -> np.ndarray:
    """특정 탈퇴 사유의 위험률 합산 × 기시유지자."""
    if len(target_idx) == 0:
        return np.zeros_like(tpx_bot)

    # target_idx는 risk_meta 기준, exit_idx도 risk_meta 기준
    # qx_dedup의 인덱스는 exit_idx 기준 (0, 1, 2, ...)
    # → target_idx가 exit_idx 내 몇 번째인지 찾기
    dedup_positions = []
    for t_idx in target_idx:
        pos = np.where(exit_idx == t_idx)[0]
        if len(pos) > 0:
            dedup_positions.append(pos[0])

    if not dedup_positions:
        return np.zeros_like(tpx_bot)

    dedup_positions = np.array(dedup_positions)
    summed_rate = qx_dedup[dedup_positions].sum(axis=0)  # (n, max_t)
    return tpx_bot * summed_rate

"""
v2 엔진 검증 테스트

Legacy DB 없이도 실행 가능:
1. DuckDB in-memory에 합성 데이터 생성
2. 760397 계약 기준값으로 C행렬/중복제거 검증
3. 벡터 연산 정확성 검증 (einsum vs 수동 루프)
4. 성능 벤치마크 (N건 확장)

Usage:
    python -m cf_module.v2.test_v2
    python -m cf_module.v2.test_v2 --benchmark
"""

import argparse
import time
from typing import Dict

import numpy as np

from cf_module.v2.schema import create_schema
from cf_module.v2.engine import (
    RiskMeta,
    GroupAssumptions,
    build_c_matrix,
    load_group_assumptions,
    project_group,
    _apply_dedup,
)
from cf_module.v2.orchestrator import OrchestratorConfig, run_all


# ============================================================
# 합성 데이터 생성
# ============================================================

def create_test_db(n_contracts: int = 100):
    """테스트용 DuckDB를 in-memory로 생성.

    760397 계약 구조를 모사:
    - 7개 위험률코드
    - CTR exit 2개 (111018-BNFT, 212015-RSVAMT)
    - PAY exit 7개 전부
    """
    conn = create_schema(":memory:")

    # 위험률 차원 (760397 기준)
    risk_data = [
        ("111018", "사망위험1", "A", True,  "G1", 1),
        ("212015", "비사망1",   "A", False, "G1", 1),
        ("241208", "비사망2",   "A", False, "G2", 2),
        ("121108", "비사망3",   "A", False, "G3", 1),
        ("241171", "비사망4",   "A", False, "G4", 2),
        ("211024", "비사망5",   "A", False, "G5", 1),
        ("221139", "비사망6",   "A", False, "G6", 1),
    ]

    for rcd, name, chr_cd, is_death, grp, mm_trf in risk_data:
        conn.execute("""
            INSERT INTO dim_risk VALUES (?, ?, ?, ?, ?, ?, '202306')
        """, [rcd, name, chr_cd, is_death, grp, mm_trf])

    # 위험률 값 (A타입: 연령별 합성 데이터)
    for rcd, _, _, _, _, _ in risk_data:
        for age in range(0, 120):
            # 연령별 증가하는 위험률 (합성)
            base_rate = 0.001 * (1 + age / 100)
            conn.execute(
                "INSERT INTO fact_mortality VALUES (?, ?, ?)",
                [rcd, age, base_rate]
            )

    # 상품 차원
    conn.execute("""
        INSERT INTO dim_product VALUES ('P001', 'PG01', 'C1', 'COV01', '테스트상품')
    """)

    # 가정 프로파일
    profile = "PG01|M|C1|COV01"
    conn.execute("""
        INSERT INTO meta_assm_profile VALUES (?, 'PG01', '테스트', ?)
    """, [profile, n_contracts])

    # 계약 벌크 생성 (pandas → DuckDB 직접 INSERT)
    import pandas as pd
    contract_ids = np.arange(760397, 760397 + n_contracts)
    entry_ages = 30 + (np.arange(n_contracts) % 40)

    contracts_df = pd.DataFrame({
        "contract_id": contract_ids,
        "prod_cd": "P001",
        "prod_grp": "PG01",
        "sex": "M",
        "entry_age": entry_ages,
        "ctr_ym": "202009",
        "bterm": 20,
        "pterm": 20,
        "premium": 100000.0,
        "sum_assured": 10000000.0,
        "cls_cd": "C1",
        "cov_cd": "COV01",
        "assm_profile": profile,
    })
    # assm_div_val 컬럼 추가 (NULL)
    for i in range(1, 16):
        contracts_df[f"assm_div_val{i}"] = None

    conn.execute("INSERT INTO dim_contract SELECT * FROM contracts_df")

    # 계약-위험률 매핑 벌크 생성
    risk_exit = [
        ("111018", False, True,  False),  # BNFT only
        ("212015", True,  False, False),  # RSVAMT only
        ("241208", False, False, True),   # PYEXSP only
        ("121108", False, False, True),
        ("241171", False, False, True),
        ("211024", False, False, True),
        ("221139", False, False, True),
    ]
    cr_rows = []
    for cid in contract_ids:
        for rcd, rsv, bnft, pyexsp in risk_exit:
            cr_rows.append((int(cid), rcd, rsv, bnft, pyexsp, False))

    cr_df = pd.DataFrame(cr_rows, columns=[
        "contract_id", "risk_cd", "is_exit_rsv", "is_exit_bnft",
        "is_exit_pay", "is_bnft_risk"
    ])
    conn.execute("INSERT INTO map_contract_risk SELECT * FROM cr_df")

    # 해지율 (합성: 경과월별 체감)
    for dur in range(1, 241):
        rate = max(0.02 * (1 - dur / 240), 0.001)
        conn.execute(
            "INSERT INTO fact_lapse VALUES (?, 'paying', ?, ?)",
            [profile, dur, rate]
        )
        conn.execute(
            "INSERT INTO fact_lapse VALUES (?, 'paidup', ?, ?)",
            [profile, dur, rate * 0.5]
        )

    # 스큐 (합성: 24개월까지 1.2, 이후 1.0)
    for dur in range(1, 241):
        factor = 1.2 if dur <= 24 else 1.0
        conn.execute(
            "INSERT INTO fact_skew VALUES (?, ?, ?)",
            [profile, dur, factor]
        )

    return conn


# ============================================================
# 테스트 함수들
# ============================================================

def test_schema_creation():
    """스키마 생성 테스트."""
    conn = create_schema(":memory:")
    tables = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchdf()

    expected_tables = {
        "dim_contract", "dim_product", "dim_risk",
        "fact_beprd", "fact_interest", "fact_lapse",
        "fact_mortality", "fact_reserve", "fact_skew",
        "map_contract_risk", "meta_assm_profile",
    }

    actual = set(tables["table_name"].values)
    assert expected_tables.issubset(actual), f"누락 테이블: {expected_tables - actual}"
    print(f"  [PASS] 스키마 생성: {len(actual)}개 테이블")
    conn.close()


def test_c_matrix_construction():
    """C행렬 구축 테스트 (760397 기준)."""
    # 760397 계약의 위험률 메타
    risk_meta = RiskMeta(
        risk_cds=np.array(["111018", "212015", "241208", "121108", "241171", "211024", "221139"]),
        chr_cd=np.array(["A", "A", "A", "A", "A", "A", "A"]),
        is_death=np.array([True, False, False, False, False, False, False]),
        risk_group=np.array(["G1", "G1", "G2", "G3", "G4", "G5", "G6"]),
        mm_trf_way_cd=np.array([1, 1, 2, 1, 2, 1, 1]),
        is_exit_ctr=np.array([True, True, False, False, False, False, False]),  # BNFT, RSVAMT
        is_exit_pay=np.array([True, True, True, True, True, True, True]),       # 전부
        is_exit_pyexsp=np.array([False, False, True, True, True, True, True]),
        is_exit_rsv=np.array([False, True, False, False, False, False, False]),
        is_exit_bnft=np.array([True, False, False, False, False, False, False]),
    )

    # CTR C행렬: wx + 2 exit = 3×3
    c_ctr, exit_idx_ctr = build_c_matrix(risk_meta, "ctr")
    assert c_ctr.shape == (3, 3), f"CTR C행렬 크기: {c_ctr.shape} != (3,3)"
    assert np.all(np.diag(c_ctr) == 0), "대각선이 0이 아님"

    # 111018과 212015는 동일그룹(G1) → C[1,2]=0, C[2,1]=0
    # wx(0)는 별도 그룹, 111018(1)은 사망 → 열 마스크
    # C[0,1] = 0 (j=111018은 사망)
    # C[0,2] = 1 (j=212015는 비사망, 다른 그룹)
    # C[1,2] = 0 (동일그룹)
    # C[2,1] = 0 (j=111018은 사망)
    print(f"  CTR C행렬:\n{c_ctr}")
    assert c_ctr[0, 1] == 0, "wx→사망위험 열은 0이어야 함"
    assert c_ctr[1, 2] == 0, "동일그룹(G1) 간 C=0이어야 함"

    # PAY C행렬: wx + 7 exit = 8×8
    c_pay, exit_idx_pay = build_c_matrix(risk_meta, "pay")
    assert c_pay.shape == (8, 8), f"PAY C행렬 크기: {c_pay.shape} != (8,8)"
    assert len(exit_idx_pay) == 7, f"PAY exit 수: {len(exit_idx_pay)} != 7"

    # 사망위험(111018) 열은 전부 0
    death_col = 1  # exit_idx_pay에서 111018의 위치
    assert np.all(c_pay[:, death_col] == 0), "사망위험 열이 0이 아님"

    print(f"  PAY C행렬 (8×8):\n{c_pay}")
    print(f"  [PASS] C행렬 구축: CTR {c_ctr.shape}, PAY {c_pay.shape}")


def test_einsum_dedup():
    """einsum 중복제거 vs 수동 루프 비교."""
    np.random.seed(42)

    n, max_t = 100, 120
    n_risks = 7

    # 합성 위험률
    qx = np.random.uniform(0.0001, 0.005, (n_risks, n, max_t))
    wx = np.random.uniform(0.005, 0.02, (n, max_t))

    # 합성 C행렬
    n_rates = 1 + n_risks
    C = np.random.choice([0.0, 1.0], size=(n_rates, n_rates), p=[0.5, 0.5])
    np.fill_diagonal(C, 0.0)

    # einsum 방식
    r = np.zeros((n_rates, n, max_t))
    r[0] = wx
    r[1:] = qx
    adjustment_einsum = np.einsum("jnt,ij->int", r, C) / 2.0
    r_dedup_einsum = r * (1.0 - adjustment_einsum)

    # 수동 루프 (검증용)
    r_dedup_loop = r.copy()
    for t in range(max_t):
        for point in range(n):
            for i in range(n_rates):
                adj = 0.0
                for j in range(n_rates):
                    adj += r[j, point, t] * C[i, j]
                r_dedup_loop[i, point, t] = r[i, point, t] * (1.0 - adj / 2.0)

    max_diff = np.max(np.abs(r_dedup_einsum - r_dedup_loop))
    assert max_diff < 1e-12, f"einsum vs loop 차이: {max_diff}"
    print(f"  [PASS] einsum 검증: max diff = {max_diff:.2e}")


def test_full_pipeline():
    """전체 파이프라인 테스트 (합성 데이터)."""
    conn = create_test_db(n_contracts=10)

    profile = "PG01|M|C1|COV01"

    # 가정 로드
    assm = load_group_assumptions(conn, profile, 760397, max_duration=240)

    assert len(assm.risk_meta.risk_cds) == 7, f"위험률 수: {len(assm.risk_meta.risk_cds)}"
    assert assm.c_matrix_ctr.shape[0] == 3, "CTR C행렬 크기 오류"
    assert assm.c_matrix_pay.shape[0] == 8, "PAY C행렬 크기 오류"

    # 프로젝션
    contracts = conn.execute("""
        SELECT contract_id, entry_age, bterm, pterm, ctr_ym
        FROM dim_contract WHERE assm_profile = ?
    """, [profile]).fetchdf()

    result = project_group(
        conn=conn,
        assm=assm,
        contract_ids=contracts["contract_id"].values,
        entry_ages=contracts["entry_age"].values.astype(int),
        bterms=contracts["bterm"].values.astype(int),
        pterms=contracts["pterm"].values.astype(int),
        elapsed_months=np.full(len(contracts), 37, dtype=np.int32),
        clos_ym="202309",
        max_proj_months=240,
    )

    n = len(contracts)
    assert result.tpx.shape[0] == n, f"tpx 행 수: {result.tpx.shape[0]} != {n}"
    assert result.tpx.shape[1] > 0, "tpx 열이 0"

    # tpx 기본 검증
    assert np.all(result.tpx[:, 0] <= 1.0), "tpx > 1"
    assert np.all(result.tpx[:, 0] > 0.9), "tpx가 비정상적으로 낮음"
    assert np.all(result.tpx[:, -1] >= 0), "tpx < 0"

    # pay_tpx <= tpx (PAY는 탈퇴 사유가 더 많으므로)
    assert np.all(result.pay_tpx <= result.tpx + 1e-10), "pay_tpx > tpx"

    # 탈퇴자 합계 검증
    total_d = result.d_death + result.d_lapse
    assert np.all(total_d >= 0), "탈퇴자 음수"

    print(f"  [PASS] 전체 파이프라인: {n}건 × {result.tpx.shape[1]}개월")
    print(f"    tpx[0, :5] = {result.tpx[0, :5]}")
    print(f"    pay_tpx[0, :5] = {result.pay_tpx[0, :5]}")
    print(f"    qx[0, :5] = {result.qx_monthly[0, :5]}")
    print(f"    wx[0, :5] = {result.wx_monthly[0, :5]}")

    conn.close()


def test_benchmark(n_contracts: int = 50000):
    """성능 벤치마크."""
    print(f"\n  벤치마크: {n_contracts:,}건...")
    conn = create_test_db(n_contracts=n_contracts)

    profile = "PG01|M|C1|COV01"

    # 가정 로드 (1회)
    t0 = time.time()
    assm = load_group_assumptions(conn, profile, 760397, max_duration=240)
    t_assm = time.time() - t0
    print(f"    가정 로드: {t_assm:.3f}초")

    # 프로젝션
    contracts = conn.execute("""
        SELECT contract_id, entry_age, bterm, pterm
        FROM dim_contract WHERE assm_profile = ?
    """, [profile]).fetchdf()

    t0 = time.time()
    result = project_group(
        conn=conn,
        assm=assm,
        contract_ids=contracts["contract_id"].values,
        entry_ages=contracts["entry_age"].values.astype(int),
        bterms=contracts["bterm"].values.astype(int),
        pterms=contracts["pterm"].values.astype(int),
        elapsed_months=np.full(len(contracts), 37, dtype=np.int32),
        clos_ym="202309",
        max_proj_months=240,
    )
    t_proj = time.time() - t0

    total_cells = n_contracts * result.tpx.shape[1]
    rate = total_cells / t_proj if t_proj > 0 else 0

    print(f"    프로젝션: {t_proj:.3f}초")
    print(f"    {n_contracts:,}건 × {result.tpx.shape[1]}개월 = {total_cells:,} 셀")
    print(f"    처리 속도: {rate:,.0f} 셀/초")
    print(f"    계약당: {t_proj / n_contracts * 1000:.3f} ms")
    print(f"    1억 건 추정: {n_contracts / rate * 100_000_000 / 3600:.1f} 시간 (단일코어)")

    conn.close()


# ============================================================
# 메인
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="v2 엔진 테스트")
    parser.add_argument("--benchmark", action="store_true", help="성능 벤치마크 실행")
    parser.add_argument("-n", type=int, default=50000, help="벤치마크 계약 수")
    args = parser.parse_args()

    print("=" * 60)
    print("CF Module v2 엔진 테스트")
    print("=" * 60)

    print("\n[1] 스키마 생성 테스트")
    test_schema_creation()

    print("\n[2] C행렬 구축 테스트")
    test_c_matrix_construction()

    print("\n[3] einsum 중복제거 검증")
    test_einsum_dedup()

    print("\n[4] 전체 파이프라인 테스트")
    test_full_pipeline()

    if args.benchmark:
        print("\n[5] 성능 벤치마크")
        test_benchmark(n_contracts=args.n)

    print("\n" + "=" * 60)
    print("모든 테스트 통과!")
    print("=" * 60)


if __name__ == "__main__":
    main()

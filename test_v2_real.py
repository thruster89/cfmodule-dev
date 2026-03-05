"""
v2 엔진 실제 DB 연동 테스트

순수 v2 ETL: VSOLN2.vdb → DuckDB (드라이버 기반 키매칭 포함)
→ v2 engine 프로젝션 → PROJ_O2.vdb 기대값 비교

v1 의존 없음 — 완전한 v2 파이프라인.

Usage:
    python test_v2_real.py
    python test_v2_real.py --keep-db
"""

import argparse
import os
import time

import numpy as np

from cf_module.v2.etl import migrate_legacy_db
from cf_module.v2.engine import load_group_assumptions, project_group


# ── 설정 ──
LEGACY_DB = r"C:\Users\thrus\Downloads\VSOLN2\VSOLN2.vdb"
V2_DB_PATH = os.path.join(os.path.dirname(__file__), "v2_test_760397.duckdb")

IDNO = 760397
CLOS_YM = "202309"

EXPECTED = {
    "PAY_TRME_t1": 0.9925024849,
    "PAY_TRMNAT_RT_t1": 0.0063239731,
    "PYEXSP_DRPO_RSKRT_t1": 0.0010430155,
}


def main():
    parser = argparse.ArgumentParser(description="v2 실제 DB 연동 테스트")
    parser.add_argument("--keep-db", action="store_true", help="DuckDB 파일 보존")
    args = parser.parse_args()

    print("=" * 70)
    print("v2 실제 DB 연동 테스트 (순수 v2 ETL, v1 의존 없음)")
    print("=" * 70)

    # ── 1. ETL ──
    print("\n[1] ETL: VSOLN2.vdb → DuckDB")
    t0 = time.time()

    if os.path.exists(V2_DB_PATH):
        os.remove(V2_DB_PATH)

    v2 = migrate_legacy_db(
        legacy_path=LEGACY_DB,
        v2_path=V2_DB_PATH,
        infrc_seq=1,
        assm_ym="202306",
        idno_start=IDNO,
        idno_end=IDNO,
    )
    print(f"    ETL 완료 ({time.time()-t0:.1f}s)")

    # ── 2. ETL 결과 확인 ──
    print("\n[2] DuckDB 테이블 요약")
    tables = ["dim_contract", "dim_risk", "map_contract_risk",
              "fact_mortality", "fact_lapse", "fact_skew", "fact_beprd", "fact_reserve"]
    for tbl in tables:
        cnt = v2.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl}: {cnt}건")

    # 계약 정보
    contracts = v2.execute("SELECT * FROM dim_contract").fetchdf()
    c = contracts.iloc[0]
    print(f"\n  계약: id={c['contract_id']}, prod={c['prod_cd']}, age={c['entry_age']}")
    print(f"        bterm={c['bterm']}, pterm={c['pterm']}, ctr_ym={c['ctr_ym']}")
    print(f"        assm_profile={c['assm_profile']}")

    # 위험률 메타
    risks = v2.execute("SELECT * FROM dim_risk ORDER BY risk_cd").fetchdf()
    print(f"\n  위험률 ({len(risks)}개):")
    for _, r in risks.iterrows():
        print(f"    {r['risk_cd']} | death={'Y' if r['is_death'] else 'N'} | grp={r['risk_group']}")

    # exit 플래그
    cr = v2.execute("SELECT * FROM map_contract_risk WHERE contract_id=? ORDER BY risk_cd", [IDNO]).fetchdf()
    ctr_cnt = ((cr['is_exit_rsv']) | (cr['is_exit_bnft'])).sum()
    pay_cnt = ((cr['is_exit_rsv']) | (cr['is_exit_bnft']) | (cr['is_exit_pay'])).sum()
    print(f"\n  CTR exit: {ctr_cnt}개, PAY exit: {pay_cnt}개")
    for _, r in cr.iterrows():
        flags = []
        if r['is_exit_rsv']: flags.append("RSV")
        if r['is_exit_bnft']: flags.append("BNFT")
        if r['is_exit_pay']: flags.append("PYEXSP")
        print(f"    {r['risk_cd']} → [{', '.join(flags) or '-'}]")

    # 해지율 확인
    lapse_cnt = v2.execute("SELECT COUNT(*) FROM fact_lapse").fetchone()[0]
    if lapse_cnt > 0:
        sample = v2.execute("SELECT pay_phase, duration, rate FROM fact_lapse ORDER BY pay_phase, duration LIMIT 3").fetchdf()
        print(f"\n  해지율 ({lapse_cnt}건): {sample.values.tolist()[:3]}")

    # ── 3. GroupAssumptions 로드 ──
    print("\n[3] GroupAssumptions 로드")
    t1 = time.time()
    assm_profile = c["assm_profile"]
    assm = load_group_assumptions(v2, assm_profile, IDNO, max_duration=1200)
    print(f"    로드 완료 ({time.time()-t1:.1f}s)")

    rm = assm.risk_meta
    print(f"  risks: {rm.risk_cds}")
    print(f"  CTR exit: {rm.is_exit_ctr.sum()}개, PAY exit: {rm.is_exit_pay.sum()}개")
    print(f"  C행렬 CTR: {assm.c_matrix_ctr.shape}, PAY: {assm.c_matrix_pay.shape}")
    print(f"  lapse_paying[:3]: {assm.lapse_paying[:3]}")

    # ── 4. 프로젝션 ──
    print("\n[4] 프로젝션")
    t2 = time.time()

    ctr_ym = str(c["ctr_ym"])
    elapsed = (int(CLOS_YM[:4]) - int(ctr_ym[:4])) * 12 + (int(CLOS_YM[4:6]) - int(ctr_ym[4:6])) + 1  # 계약월 포함

    result = project_group(
        conn=v2,
        assm=assm,
        contract_ids=np.array([int(c["contract_id"])]),
        entry_ages=np.array([int(c["entry_age"])]),
        bterms=np.array([int(c["bterm"])]),
        pterms=np.array([int(c["pterm"])]),
        elapsed_months=np.array([elapsed], dtype=np.int32),
        clos_ym=CLOS_YM,
        max_proj_months=1200,
    )
    print(f"    완료 ({time.time()-t2:.1f}s), shape: {result.tpx.shape}")
    print(f"  tpx[:5]:     {result.tpx[0, :5]}")
    print(f"  pay_tpx[:5]: {result.pay_tpx[0, :5]}")

    # ── 5. 검증 ──
    print("\n" + "=" * 70)
    print("PROJ_O2.vdb 기대값 검증")
    print("=" * 70)

    pay_tpx_t1 = result.pay_tpx[0, 0]
    pay_trmnat_rt = result.wx_pay_monthly[0, 0]  # PAY 해약률 (중복제거 후)
    pyexsp_rate = result.d_pyexsp[0, 0]

    tol = 1e-6
    checks = [
        ("PAY_TRME(t=1)", pay_tpx_t1, EXPECTED["PAY_TRME_t1"]),
        ("PAY_TRMNAT_RT(t=1)", pay_trmnat_rt, EXPECTED["PAY_TRMNAT_RT_t1"]),
        ("PYEXSP_DRPO_RSKRT(t=1)", pyexsp_rate, EXPECTED["PYEXSP_DRPO_RSKRT_t1"]),
        ("pay_tpx <= tpx", float(np.all(result.pay_tpx <= result.tpx + 1e-10)), 1.0),
    ]

    all_pass = True
    for name, actual, expected in checks:
        diff = abs(actual - expected)
        status = "PASS" if diff < tol else "FAIL"
        if status == "FAIL":
            all_pass = False
        print(f"  [{status}] {name}: {actual:.10f} (기대: {expected:.10f}, diff={diff:.2e})")

    print(f"\n  CTR tpx[t=1] = {result.tpx[0, 0]:.10f}")
    print(f"\n{'모든 검증 통과!' if all_pass else '일부 검증 실패'}")
    print("=" * 70)

    # ── 정리 ──
    v2.close()
    if not args.keep_db and os.path.exists(V2_DB_PATH):
        os.remove(V2_DB_PATH)
    elif args.keep_db:
        print(f"\n  DuckDB 보존: {V2_DB_PATH}")


if __name__ == "__main__":
    main()

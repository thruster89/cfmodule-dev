"""OP_BEL PREM_BASE 전건 검증.

Usage:
    python test_bel_prem_base.py              # 100건 샘플
    python test_bel_prem_base.py --n 1000     # 1000건
    python test_bel_prem_base.py --all        # 42,001건 전건
    python test_bel_prem_base.py --idno 760397  # 특정 계약
"""
import argparse
import time

import duckdb
import numpy as np

from cf_module.calc.bel import compute_bel
from cf_module.calc.cf import compute_cf
from cf_module.calc.dc_rt import compute_dc_rt
from cf_module.calc.exp import compute_exp
from cf_module.calc.pvcf import compute_pvcf
from cf_module.data.bn_loader import BNDataCache
from cf_module.data.exp_loader import ExpDataCache
from cf_module.data.rsk_lapse_loader import RawAssumptionLoader
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached
from cf_module.run import (
    SingleResult, compute_n_steps, _compute_mn_chain,
    _compute_trad_pv_single, _compute_bn_single, _compute_exp_single,
)


def compute_bel_prem_base(
    con, idno, loader, trad_cache, bn_cache, exp_cache, dc_curve,
):
    """단일 계약의 BEL.PREM_BASE 산출. 최소 경로만 실행."""
    ctr = loader.load_contract(idno)
    n_steps = compute_n_steps(ctr)

    # MN
    rsk_rt, lapse_rt, tbl_mn = _compute_mn_chain(loader, ctr, n_steps)

    # TRAD_PV
    trad_pv = _compute_trad_pv_single(con, loader, trad_cache, idno, tbl_mn, n_steps)
    if not trad_pv:
        return None, "NO_TRAD_PV"
    pv_d = trad_pv.to_dict()

    # BN
    acum_bnft = pv_d.get("APLY_PREM_ACUMAMT_BNFT")
    tbl_bn = _compute_bn_single(con, bn_cache, ctr, rsk_rt, lapse_rt, n_steps, acum_bnft)

    # BN INSUAMT
    bn_insuamt = np.zeros(n_steps, dtype=np.float64)
    if tbl_bn:
        for br in tbl_bn.bnft_results.values():
            bn_insuamt += br.bnft_insuamt

    # EXP
    infrc_raw = trad_cache.infrc.get(idno, {})
    gprem = infrc_raw.get("gprem") or infrc_raw.get("effective_gprem", 0)
    # val5: bulk query로 미리 로드 안 했으므로 fallback
    row = con.execute(
        "SELECT ASSM_DIV_VAL5 FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
        [idno]).fetchone()
    val5 = str(row[0]) if row and row[0] else None

    exp_results, exp_items = _compute_exp_single(
        exp_cache, ctr, n_steps, pv_d, gprem=gprem, val5=val5)

    # LSVY rate
    lsvy_rate = 0.0
    for tp, kd, it in (exp_items or []):
        if tp == "LSVY":
            lsvy_rate = it.get("rate", 0)

    # CF
    cf = compute_cf(n_steps, tbl_mn, pv_d, bn_insuamt,
                    exp_results or [], exp_items or [], lsvy_rate)

    # DC_RT
    dc = compute_dc_rt(n_steps, dc_curve)

    # PVCF
    pvcf = compute_pvcf(cf, dc)

    # BEL
    bel = compute_bel(pvcf)

    return bel.prem_base, None


def main():
    parser = argparse.ArgumentParser(description="OP_BEL PREM_BASE 전건 검증")
    parser.add_argument("--n", type=int, default=100, help="테스트 건수 (기본: 100)")
    parser.add_argument("--all", action="store_true", help="전건 (42,001건)")
    parser.add_argument("--idno", type=int, default=None, help="특정 IDNO")
    parser.add_argument("--db", type=str, default="duckdb_transform.duckdb")
    parser.add_argument("--tol", type=float, default=1.0, help="허용 오차 (기본: 1.0)")
    args = parser.parse_args()

    con = duckdb.connect(args.db, read_only=True)

    # 기대값 로드
    expected = {}
    rows = con.execute("SELECT INFRC_IDNO, PREM_BASE FROM OP_BEL").fetchall()
    for r in rows:
        expected[r[0]] = r[1]
    print(f"OP_BEL 기대값: {len(expected)}건")

    # 대상 IDNO
    if args.idno:
        target_ids = [args.idno]
    elif args.all:
        target_ids = [r[0] for r in con.execute(
            "SELECT DISTINCT INFRC_IDNO FROM II_INFRC WHERE INFRC_SEQ=1 ORDER BY INFRC_IDNO"
        ).fetchall()]
    else:
        target_ids = [r[0] for r in con.execute(
            "SELECT DISTINCT INFRC_IDNO FROM II_INFRC WHERE INFRC_SEQ=1 ORDER BY INFRC_IDNO"
        ).fetchall()][:args.n]

    print(f"대상: {len(target_ids)}건")

    # 캐시 로드
    print("캐시 로드 중...")
    t0 = time.time()
    loader = RawAssumptionLoader(con)
    loader.preload_contracts()
    trad_cache = TradPVDataCache(con)
    bn_cache = BNDataCache(con)
    exp_cache = ExpDataCache(con)
    dc_curve = trad_cache.dc_rt_curve
    cache_time = time.time() - t0
    print(f"캐시 로드: {cache_time:.1f}s ({len(loader._contract_cache)}건 사전로드)")

    # 실행
    print("산출 시작...")
    t0 = time.time()
    ok = 0
    fail = 0
    err = 0
    max_diff = 0
    max_diff_idno = 0
    fail_examples = []

    for i, idno in enumerate(target_ids):
        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(target_ids) - i - 1) / rate
            print(f"  [{i+1}/{len(target_ids)}] OK={ok} FAIL={fail} ERR={err}"
                  f"  {rate:.1f}건/s  ETA={eta:.0f}s")

        try:
            calc_prem, error = compute_bel_prem_base(
                con, idno, loader, trad_cache, bn_cache, exp_cache, dc_curve)
        except Exception as e:
            err += 1
            if err <= 5:
                print(f"  ERR IDNO={idno}: {e}")
            continue

        if error:
            err += 1
            continue

        # 기대값 비교
        if idno in expected:
            exp_val = expected[idno]
            diff = abs(calc_prem - exp_val)
            if diff > max_diff:
                max_diff = diff
                max_diff_idno = idno
            if diff <= args.tol:
                ok += 1
            else:
                fail += 1
                if len(fail_examples) < 10:
                    fail_examples.append((idno, exp_val, calc_prem, diff))
        else:
            ok += 1  # 기대값 없으면 산출 성공만 확인

    total_time = time.time() - t0

    # 결과
    print(f"\n{'='*60}")
    print(f"  OP_BEL PREM_BASE 검증 결과")
    print(f"{'='*60}")
    print(f"  대상: {len(target_ids)}건")
    print(f"  PASS: {ok}")
    print(f"  FAIL: {fail}")
    print(f"  ERROR: {err}")
    print(f"  max_diff: {max_diff:.6f} (IDNO={max_diff_idno})")
    print(f"  소요: {total_time:.1f}s ({len(target_ids)/total_time:.1f}건/s)")

    if fail_examples:
        print(f"\n  FAIL 예시:")
        for idno, exp, calc, diff in fail_examples:
            print(f"    IDNO={idno:8d} exp={exp:>14.2f} calc={calc:>14.2f} diff={diff:.4f}")

    print(f"{'='*60}")

    con.close()
    return 1 if fail > 0 else 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

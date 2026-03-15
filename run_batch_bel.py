"""OP_BEL 전건 배치 산출 → DuckDB 저장.

Usage:
    python run_batch_bel.py                    # 전건 산출 → output_bel.duckdb
    python run_batch_bel.py --n 1000           # 1000건만
    python run_batch_bel.py -o result.duckdb   # 출력 DB 지정
"""
import argparse
import time

import duckdb
import numpy as np
import pandas as pd

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
    compute_n_steps, _compute_mn_chain,
    _compute_trad_pv_single, _compute_bn_single, _compute_exp_single,
)


def main():
    parser = argparse.ArgumentParser(description="OP_BEL 전건 배치 산출")
    parser.add_argument("--n", type=int, default=None, help="건수 제한")
    parser.add_argument("--db", type=str, default="duckdb_transform.duckdb")
    parser.add_argument("-o", "--output", type=str, default="output_bel.duckdb",
                        help="출력 DB (기본: output_bel.duckdb)")
    parser.add_argument("--chunk", type=int, default=1000, help="DB 쓰기 단위")
    args = parser.parse_args()

    con = duckdb.connect(args.db, read_only=True)

    # 대상 IDNO
    target_ids = [r[0] for r in con.execute(
        "SELECT DISTINCT INFRC_IDNO FROM II_INFRC WHERE INFRC_SEQ=1 ORDER BY INFRC_IDNO"
    ).fetchall()]
    if args.n:
        target_ids = target_ids[:args.n]

    print(f"대상: {len(target_ids):,}건")

    # 캐시 로드
    print("캐시 로드 중...")
    t0 = time.time()
    loader = RawAssumptionLoader(con)
    loader.preload_contracts()
    trad_cache = TradPVDataCache(con)
    bn_cache = BNDataCache(con)
    exp_cache = ExpDataCache(con)
    dc_curve = trad_cache.dc_rt_curve
    print(f"캐시 로드: {time.time() - t0:.1f}s")

    # 기대값 (있으면)
    expected = {}
    for r in con.execute("SELECT INFRC_IDNO, PREM_BASE FROM OP_BEL").fetchall():
        expected[r[0]] = r[1]

    # 출력 DB
    out_con = duckdb.connect(args.output)
    out_con.execute("""
        CREATE TABLE IF NOT EXISTS OP_BEL (
            INFRC_IDNO BIGINT PRIMARY KEY,
            PREM_BASE DOUBLE, PREM_PYEX DOUBLE, PREM_ADD DOUBLE,
            TMRFND DOUBLE, DRPO_PYRV DOUBLE,
            INSUAMT_GEN DOUBLE, INSUAMT_HAFWAY DOUBLE, INSUAMT_MATU DOUBLE, INSUAMT_PENS DOUBLE,
            ACQSEXP_DR DOUBLE, ACQSEXP_INDR DOUBLE, ACQSEXP_REDEM DOUBLE,
            MNTEXP_DR DOUBLE, MNTEXP_INDR DOUBLE,
            IV_MGMEXP_MNTEXP_CCRFND DOUBLE, IV_MGMEXP_MNTEXP_CL_REMAMT DOUBLE,
            LOSS_SVYEXP DOUBLE, HAFWDR DOUBLE,
            LOAN_NEW DOUBLE, LOAN_INT DOUBLE, LOAN_RPAY_HAFWAY DOUBLE, LOAN_RPAY_MATU DOUBLE,
            PREM_ACUM_RSVAMT_ALTER DOUBLE, PREM_ADD_ACUMAMT_DEPL DOUBLE,
            BEL DOUBLE, LOAN_ASET DOUBLE
        )
    """)

    # 산출
    print("산출 시작...")
    t0 = time.time()
    batch_rows = []
    ok = err = 0
    match_ok = match_fail = 0

    for i, idno in enumerate(target_ids):
        if (i + 1) % 2000 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(target_ids) - i - 1) / rate
            print(f"  [{i+1:,}/{len(target_ids):,}] OK={ok:,} ERR={err}"
                  f"  {rate:.0f}건/s  ETA={eta:.0f}s")

        try:
            ctr = loader.load_contract(idno)
            n_steps = compute_n_steps(ctr)

            rsk_rt, lapse_rt, tbl_mn = _compute_mn_chain(loader, ctr, n_steps)

            trad_pv = _compute_trad_pv_single(con, loader, trad_cache, idno, tbl_mn, n_steps)
            if not trad_pv:
                err += 1
                continue
            pv_d = trad_pv.to_dict()

            acum_bnft = pv_d.get("APLY_PREM_ACUMAMT_BNFT")
            tbl_bn = _compute_bn_single(con, bn_cache, ctr, rsk_rt, lapse_rt, n_steps, acum_bnft)

            bn_insuamt = np.zeros(n_steps, dtype=np.float64)
            if tbl_bn:
                for br in tbl_bn.bnft_results.values():
                    bn_insuamt += br.bnft_insuamt

            infrc_raw = trad_cache.infrc.get(idno, {})
            gprem = infrc_raw.get("gprem") or infrc_raw.get("effective_gprem", 0)
            val5 = ctr.assm_divs[4] if len(ctr.assm_divs) > 4 else None
            exp_results, exp_items = _compute_exp_single(
                exp_cache, ctr, n_steps, pv_d, gprem=gprem, val5=val5)

            lsvy_rate = 0.0
            for tp, kd, it in (exp_items or []):
                if tp == "LSVY":
                    lsvy_rate = it.get("rate", 0)

            cf = compute_cf(n_steps, tbl_mn, pv_d, bn_insuamt,
                            exp_results or [], exp_items or [], lsvy_rate)
            dc = compute_dc_rt(n_steps, dc_curve)
            pvcf = compute_pvcf(cf, dc)
            bel = compute_bel(pvcf)

            row = {"INFRC_IDNO": idno}
            row.update(bel.to_dict())
            batch_rows.append(row)
            ok += 1

            # 기대값 비교
            if idno in expected:
                diff = abs(bel.prem_base - expected[idno])
                if diff < 1.0:
                    match_ok += 1
                else:
                    match_fail += 1

        except Exception as e:
            err += 1
            if err <= 5:
                print(f"  ERR IDNO={idno}: {e}")

        # chunk 단위 DB 쓰기
        if len(batch_rows) >= args.chunk:
            df = pd.DataFrame(batch_rows)
            out_con.execute("INSERT INTO OP_BEL SELECT * FROM df")
            batch_rows = []

    # 잔여 쓰기
    if batch_rows:
        df = pd.DataFrame(batch_rows)
        out_con.execute("INSERT INTO OP_BEL SELECT * FROM df")

    total_time = time.time() - t0

    # 결과
    total_rows = out_con.execute("SELECT COUNT(*) FROM OP_BEL").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"  OP_BEL 배치 산출 완료")
    print(f"{'='*60}")
    print(f"  산출: {ok:,} / {len(target_ids):,} (ERR={err})")
    print(f"  기대값 비교: PASS={match_ok} FAIL={match_fail}")
    print(f"  출력: {args.output} ({total_rows:,}행)")
    print(f"  소요: {total_time:.1f}s ({ok/total_time:.0f}건/s)")
    print(f"{'='*60}")

    out_con.close()
    con.close()


if __name__ == "__main__":
    main()

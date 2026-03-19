"""OP_BEL 배치 산출 → DuckDB 저장.

청크 기반 배치: PROD_CD/COV_CD 단위 캐시 + IDNO 청크 분할 처리.
실행번호(RUN_ID) 관리 + 삭제 후 재실행(--reset) 지원.

Usage:
    python run_batch_bel.py                          # 전건 산출 → output_bel.duckdb
    python run_batch_bel.py --n 1000                 # 1000건만
    python run_batch_bel.py -o result.duckdb         # 출력 DB 지정
    python run_batch_bel.py --reset                  # 기존 결과 삭제 후 재실행
    python run_batch_bel.py --run-id 3               # 특정 실행번호 지정
    python run_batch_bel.py --chunk 3000             # 청크 크기 조정 (기본 5000)
    python run_batch_bel.py --preload                # 전건 캐시 프리로드 (메모리 충분 시)
"""
import argparse
import time
from collections import defaultdict
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd

from cf_module.calc.bel import compute_bel
from cf_module.calc.cf import compute_cf
from cf_module.calc.dc_rt import compute_dc_rt
from cf_module.calc.pvcf import compute_pvcf
from cf_module.data.bn_loader import BNDataCache
from cf_module.data.exp_loader import ExpDataCache
from cf_module.data.rsk_lapse_loader import RawAssumptionLoader
from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached
from cf_module.run import (
    compute_n_steps, _compute_mn_chain,
    _compute_trad_pv_single, _compute_bn_single, _compute_exp_single,
)


# ---------------------------------------------------------------------------
# 출력 DB 스키마
# ---------------------------------------------------------------------------

_BEL_COLUMNS = """
    INFRC_IDNO BIGINT,
    RUN_ID INTEGER,
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
"""

_RUN_LOG_COLUMNS = """
    RUN_ID INTEGER PRIMARY KEY,
    STARTED_AT TIMESTAMP,
    FINISHED_AT TIMESTAMP,
    TOTAL_TARGET INTEGER,
    TOTAL_OK INTEGER,
    TOTAL_ERR INTEGER,
    ELAPSED_SEC DOUBLE,
    STATUS VARCHAR
"""


def _init_output_db(out_con):
    """출력 DB 테이블 초기화."""
    out_con.execute(f"CREATE TABLE IF NOT EXISTS OP_BEL ({_BEL_COLUMNS})")
    out_con.execute(f"CREATE TABLE IF NOT EXISTS RUN_LOG ({_RUN_LOG_COLUMNS})")


def _next_run_id(out_con):
    """다음 실행번호."""
    row = out_con.execute("SELECT COALESCE(MAX(RUN_ID), 0) FROM RUN_LOG").fetchone()
    return row[0] + 1


def _reset_run(out_con, run_id=None):
    """결과 삭제. run_id 지정 시 해당 실행만, 미지정 시 전체."""
    if run_id:
        out_con.execute("DELETE FROM OP_BEL WHERE RUN_ID = ?", [run_id])
        out_con.execute("DELETE FROM RUN_LOG WHERE RUN_ID = ?", [run_id])
        print(f"  RUN_ID={run_id} 결과 삭제 완료")
    else:
        out_con.execute("DELETE FROM OP_BEL")
        out_con.execute("DELETE FROM RUN_LOG")
        print("  전체 결과 삭제 완료")


# ---------------------------------------------------------------------------
# 단건 산출 (배치용, 캐시 주입)
# ---------------------------------------------------------------------------

def _compute_one(con, loader, trad_cache, bn_cache, exp_cache, dc_curve, idno):
    """BEL 1건 산출. 성공 시 dict, 실패 시 None."""
    ctr = loader.load_contract(idno)
    n_steps = compute_n_steps(ctr)

    rsk_rt, lapse_rt, tbl_mn = _compute_mn_chain(loader, ctr, n_steps)

    trad_pv = _compute_trad_pv_single(con, loader, trad_cache, idno, tbl_mn, n_steps)
    if not trad_pv:
        return None
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
    return row


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OP_BEL 배치 산출 (청크 기반)")
    parser.add_argument("--n", type=int, default=None, help="건수 제한")
    parser.add_argument("--db", type=str, default="duckdb_transform.duckdb")
    parser.add_argument("-o", "--output", type=str, default="output_bel.duckdb",
                        help="출력 DB (기본: output_bel.duckdb)")
    parser.add_argument("--chunk", type=int, default=5000,
                        help="IDNO 청크 크기 (기본: 5000)")
    parser.add_argument("--run-id", type=int, default=None,
                        help="실행번호 지정 (미지정 시 자동 채번)")
    parser.add_argument("--reset", action="store_true",
                        help="기존 결과 삭제 후 재실행")
    parser.add_argument("--reset-run", type=int, default=None,
                        help="특정 실행번호 결과만 삭제 후 재실행")
    parser.add_argument("--preload", action="store_true",
                        help="전건 캐시 프리로드 (메모리 충분 시 사용, 더 빠름)")
    args = parser.parse_args()

    con = duckdb.connect(args.db, read_only=True)

    # 출력 DB
    out_con = duckdb.connect(args.output)
    _init_output_db(out_con)

    # 삭제 처리
    if args.reset:
        _reset_run(out_con)
    elif args.reset_run:
        _reset_run(out_con, args.reset_run)

    # 실행번호
    run_id = args.run_id if args.run_id else _next_run_id(out_con)
    started_at = datetime.now()

    # 대상 IDNO + (PROD_CD, COV_CD) 그룹핑
    rows = con.execute("""
        SELECT INFRC_IDNO, PROD_CD, COV_CD
        FROM II_INFRC WHERE INFRC_SEQ = 1
        ORDER BY PROD_CD, COV_CD, INFRC_IDNO
    """).fetchall()

    pcv_groups = defaultdict(list)
    for idno, prod, cov in rows:
        pcv_groups[(prod, cov)].append(idno)

    # 건수 제한
    all_ids = [idno for _, prod, cov in rows for idno in [_] if True]  # placeholder
    all_ids = [r[0] for r in rows]
    if args.n:
        all_ids = all_ids[:args.n]
        # 제한된 ID만 남기도록 그룹 재구성
        id_set = set(all_ids)
        pcv_groups = {
            k: [i for i in v if i in id_set]
            for k, v in pcv_groups.items()
        }
        pcv_groups = {k: v for k, v in pcv_groups.items() if v}

    total_target = len(all_ids)
    total_groups = len(pcv_groups)

    print(f"{'='*60}")
    print(f"  OP_BEL 배치 산출  RUN_ID={run_id}")
    print(f"  대상: {total_target:,}건  상품그룹: {total_groups}개  청크: {args.chunk}")
    print(f"{'='*60}")

    # RUN_LOG 시작 기록
    out_con.execute(
        "INSERT INTO RUN_LOG (RUN_ID, STARTED_AT, TOTAL_TARGET, STATUS) VALUES (?, ?, ?, ?)",
        [run_id, started_at, total_target, "RUNNING"])

    # 공통 캐시 (상품 독립적인 것들)
    print("공통 캐시 로드 중...")
    t0 = time.time()
    loader = RawAssumptionLoader(con)
    loader.preload_contracts()
    exp_cache = ExpDataCache(con)

    if args.preload:
        # 전건 프리로드 모드
        print("  전건 프리로드: TradPV + BN 캐시...")
        trad_cache_all = TradPVDataCache(con)
        bn_cache_all = BNDataCache(con)
        dc_curve = trad_cache_all.dc_rt_curve
    else:
        # dc_curve만 미리 로드 (상품 독립)
        dc_curve_rows = con.execute(
            "SELECT DC_RT FROM IE_DC_RT ORDER BY CTR_AFT_PASS_MMCNT"
        ).fetchall()
        dc_curve = np.array([r[0] for r in dc_curve_rows], dtype=np.float64)
        trad_cache_all = None
        bn_cache_all = None

    print(f"공통 캐시 로드: {time.time() - t0:.1f}s")

    # 산출
    print("산출 시작...")
    t_start = time.time()
    batch_rows = []
    ok = err = 0
    processed = 0

    for gi, ((prod, cov), group_ids) in enumerate(pcv_groups.items()):
        if not group_ids:
            continue

        # 상품 그룹별 캐시 로드
        if args.preload:
            trad_cache = trad_cache_all
            bn_cache = bn_cache_all
        else:
            # 그룹의 전체 IDNO로 TradPV 캐시 로드
            trad_cache = TradPVDataCache(con, idno_filter=set(group_ids))
            bn_cache = BNDataCache(con, pcv_filter=[(prod, cov)])

        # 그룹 내 IDNO를 청크로 분할
        for ci in range(0, len(group_ids), args.chunk):
            chunk_ids = group_ids[ci:ci + args.chunk]

            for idno in chunk_ids:
                try:
                    row = _compute_one(
                        con, loader, trad_cache, bn_cache, exp_cache, dc_curve, idno)
                    if row:
                        row["RUN_ID"] = run_id
                        batch_rows.append(row)
                        ok += 1
                    else:
                        err += 1
                except Exception as e:
                    err += 1
                    if err <= 5:
                        print(f"  ERR IDNO={idno}: {e}")

                processed += 1

            # 청크 단위 DB 쓰기
            if batch_rows:
                df = pd.DataFrame(batch_rows)
                out_con.execute("INSERT INTO OP_BEL SELECT * FROM df")
                batch_rows = []

        # 진행 상황 (그룹 단위)
        elapsed = time.time() - t_start
        rate = processed / elapsed if elapsed > 0 else 0
        eta = (total_target - processed) / rate if rate > 0 else 0
        print(f"  [{processed:,}/{total_target:,}] "
              f"그룹 {gi+1}/{total_groups} ({prod}/{cov}, {len(group_ids)}건) "
              f"OK={ok:,} ERR={err} "
              f"{rate:.0f}건/s ETA={eta:.0f}s")

    # 잔여 쓰기
    if batch_rows:
        df = pd.DataFrame(batch_rows)
        out_con.execute("INSERT INTO OP_BEL SELECT * FROM df")

    total_time = time.time() - t_start
    finished_at = datetime.now()

    # RUN_LOG 업데이트
    out_con.execute("""
        UPDATE RUN_LOG
        SET FINISHED_AT = ?, TOTAL_OK = ?, TOTAL_ERR = ?,
            ELAPSED_SEC = ?, STATUS = ?
        WHERE RUN_ID = ?
    """, [finished_at, ok, err, total_time, "DONE", run_id])

    # 결과 출력
    total_rows = out_con.execute(
        "SELECT COUNT(*) FROM OP_BEL WHERE RUN_ID = ?", [run_id]).fetchone()[0]
    print(f"\n{'='*60}")
    print(f"  OP_BEL 배치 산출 완료  RUN_ID={run_id}")
    print(f"{'='*60}")
    print(f"  산출: {ok:,} / {total_target:,} (ERR={err})")
    print(f"  출력: {args.output} ({total_rows:,}행)")
    print(f"  소요: {total_time:.1f}s ({ok/total_time:.0f}건/s)")
    print(f"{'='*60}")

    out_con.close()
    con.close()


if __name__ == "__main__":
    main()

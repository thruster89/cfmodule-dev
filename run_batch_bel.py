"""OP_BEL 배치 산출 → DuckDB 저장.

IDNO 기준 청크 분할: 5000건씩 flat하게 잘라서 캐시 로드 + 처리.
멀티프로세스 병렬 처리 + 실행번호(RUN_ID) 관리.

Usage:
    python run_batch_bel.py                          # 전건 산출 → output_bel.duckdb
    python run_batch_bel.py --n 1000                 # 1000건만
    python run_batch_bel.py -o result.duckdb         # 출력 DB 지정
    python run_batch_bel.py --reset                  # 기존 결과 삭제 후 재실행
    python run_batch_bel.py --reset-run 3            # 특정 RUN_ID만 삭제 후 재실행
    python run_batch_bel.py --preload                # 전건 캐시 프리로드 (메모리 충분 시)
    python run_batch_bel.py --chunk 3000             # 청크 크기 조정 (기본 5000)
    python run_batch_bel.py --workers 4              # 4 프로세스 병렬 (기본: CPU수-1)
    python run_batch_bel.py --workers 1              # 단일 프로세스 (디버그용)
"""
import argparse
import multiprocessing as mp
import os
import time
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
    PipelineContext,
    compute_n_steps, _compute_mn_chain,
    _compute_trad_pv_single, _compute_bn_single, _compute_exp_single,
)
from cf_module.utils.logger import enable_file_logging, get_logger

batch_logger = get_logger("batch")


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
    WORKERS INTEGER,
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

def _compute_one(ctx: PipelineContext, idno: int, timings=None):
    """BEL 1건 산출. 성공 시 dict, 실패 시 None.

    Args:
        ctx: 파이프라인 공유 자원 (con, loader, caches, polno_map, mn_cache 등).
        idno: 대상 계약 INFRC_IDNO.
        timings: dict가 전달되면 단계별 누적 시간(초) 기록.
    """
    t0 = time.time()

    ctr = ctx.loader.load_contract(idno)
    n_steps = compute_n_steps(ctr)
    t1 = time.time()

    # mn_cache에 이미 있으면 재사용
    if ctx.mn_cache is not None and idno in ctx.mn_cache:
        rsk_rt, lapse_rt, tbl_mn = ctx.mn_cache[idno]
    else:
        rsk_rt, lapse_rt, tbl_mn = _compute_mn_chain(ctx.loader, ctr, n_steps, mn_timings=timings)
        if ctx.mn_cache is not None:
            ctx.mn_cache[idno] = (rsk_rt, lapse_rt, tbl_mn)
    t2 = time.time()

    trad_pv = _compute_trad_pv_single(
        ctx.con, ctx.loader, ctx.trad_cache, idno, tbl_mn, n_steps,
        polno_map=ctx.polno_map, mn_cache=ctx.mn_cache)
    if not trad_pv:
        return None
    pv_d = trad_pv.to_dict()
    t3 = time.time()

    acum_bnft = pv_d.get("APLY_PREM_ACUMAMT_BNFT")
    tbl_bn = _compute_bn_single(
        ctx.con, ctx.bn_cache, ctr, rsk_rt, lapse_rt, n_steps, acum_bnft)

    bn_insuamt = np.zeros(n_steps, dtype=np.float64)
    if tbl_bn:
        for br in tbl_bn.bnft_results.values():
            bn_insuamt += br.bnft_insuamt
    t4 = time.time()

    infrc_raw = ctx.trad_cache.infrc.get(idno, {})
    gprem = infrc_raw.get("gprem") or infrc_raw.get("effective_gprem", 0)
    val5 = ctr.assm_divs[4] if len(ctr.assm_divs) > 4 else None
    exp_results, exp_items = _compute_exp_single(
        ctx.exp_cache, ctr, n_steps, pv_d, gprem=gprem, val5=val5)
    t5 = time.time()

    lsvy_rate = 0.0
    for tp, kd, it in (exp_items or []):
        if tp == "LSVY":
            lsvy_rate = it.get("rate", 0)

    cf = compute_cf(n_steps, tbl_mn, pv_d, bn_insuamt,
                    exp_results or [], exp_items or [], lsvy_rate)
    dc = compute_dc_rt(n_steps, ctx.dc_curve)
    pvcf = compute_pvcf(cf, dc)
    bel = compute_bel(pvcf)
    t6 = time.time()

    if timings is not None:
        timings["load_ctr"] = timings.get("load_ctr", 0) + (t1 - t0)
        timings["mn_chain"] = timings.get("mn_chain", 0) + (t2 - t1)
        timings["trad_pv"] = timings.get("trad_pv", 0) + (t3 - t2)
        timings["tbl_bn"] = timings.get("tbl_bn", 0) + (t4 - t3)
        timings["exp"] = timings.get("exp", 0) + (t5 - t4)
        timings["cf_bel"] = timings.get("cf_bel", 0) + (t6 - t5)
        timings["_count"] = timings.get("_count", 0) + 1

    row = {"INFRC_IDNO": idno, "RUN_ID": 0}
    row.update(bel.to_dict())
    return row


def _print_timings(timings, label=""):
    """단계별 누적 타이밍 출력 (콘솔 + 로그 파일)."""
    n = timings.get("_count", 1)
    total = sum(v for k, v in timings.items() if k != "_count")
    prefix = f"  [{label}] " if label else "  "
    header = f"{prefix}단계별 소요시간 ({n:,}건, 합계 {total:.2f}s):"
    print(header)
    batch_logger.info(header.strip())
    for key in ["load_ctr", "mn_chain", "trad_pv", "tbl_bn", "exp", "cf_bel"]:
        v = timings.get(key, 0)
        pct = v / total * 100 if total > 0 else 0
        avg_ms = v / n * 1000 if n > 0 else 0
        line = f"{prefix}  {key:>10s}: {v:7.2f}s ({pct:5.1f}%) avg {avg_ms:.2f}ms"
        print(line)
        batch_logger.info(line.strip())
    # mn_chain 세부 분석
    mn_keys = [k for k in timings if k.startswith("mn_")]
    if mn_keys:
        mn_total = timings.get("mn_chain", 1)
        print(f"{prefix}  mn_chain 세부:")
        for key in ["mn_load_risk", "mn_load_mort", "mn_calc_rsk",
                     "mn_load_lapse", "mn_calc_lapse", "mn_calc_tblmn"]:
            v = timings.get(key, 0)
            pct = v / mn_total * 100 if mn_total > 0 else 0
            avg_ms = v / n * 1000 if n > 0 else 0
            line = f"{prefix}    {key:>15s}: {v:7.2f}s ({pct:5.1f}%) avg {avg_ms:.3f}ms"
            print(line)
            batch_logger.info(line.strip())


# ---------------------------------------------------------------------------
# POLNO 그룹 사전 매핑
# ---------------------------------------------------------------------------

def _build_polno_map(con):
    """CTR_POLNO→IDNO 그룹 사전 매핑 구축.

    Returns:
        {idno: (polno, [(idno, cov_cd), ...])} — 각 IDNO에서 자신의 그룹 조회 가능.
    """
    rows = con.execute("""
        SELECT INFRC_IDNO, CTR_POLNO, COV_CD
        FROM II_INFRC WHERE INFRC_SEQ = 1
        ORDER BY CTR_POLNO, INFRC_IDNO
    """).fetchall()

    # polno → [(idno, cov_cd), ...]
    polno_groups = {}
    for idno, polno, cov in rows:
        polno_groups.setdefault(polno, []).append((idno, cov))

    # idno → (polno, group_list)
    polno_map = {}
    for polno, grp in polno_groups.items():
        for idno, _ in grp:
            polno_map[idno] = (polno, grp)

    return polno_map


# ---------------------------------------------------------------------------
# 청크 유틸: flat 리스트에서 unique (prod, cov) 추출
# ---------------------------------------------------------------------------

def _extract_unique_pcv(chunk):
    """청크 [(idno, prod, cov), ...] 에서 unique (prod, cov) set 추출."""
    return list({(prod, cov) for _, prod, cov in chunk})


def _chunk_idnos(chunk):
    """청크에서 idno만 추출."""
    return [idno for idno, _, _ in chunk]


# ---------------------------------------------------------------------------
# 단일 프로세스 모드
# ---------------------------------------------------------------------------

def _run_single_process(con, flat_list, loader, exp_cache, dc_curve,
                        preload, chunk_size, run_id, out_con, polno_map=None):
    """단일 프로세스 실행. flat_list = [(idno, prod, cov), ...]"""
    trad_cache_all = None
    bn_cache_all = None
    if preload:
        trad_cache_all = TradPVDataCache(con)
        bn_cache_all = BNDataCache(con)

    total_target = len(flat_list)
    batch_rows = []
    ok = err = 0
    t_start = time.time()
    timings_all = {}
    mn_cache = {}  # {idno: (rsk_rt, lapse_rt, tbl_mn)} — 그룹 내 중복계산 방지
    err_summary = {}  # {error_type: count} — 에러 카테고리화

    for ci in range(0, total_target, chunk_size):
        chunk = flat_list[ci:ci + chunk_size]
        chunk_ids = _chunk_idnos(chunk)

        t_cache = time.time()
        if preload:
            trad_cache = trad_cache_all
            bn_cache = bn_cache_all
        else:
            unique_pcv = _extract_unique_pcv(chunk)
            trad_cache = TradPVDataCache(con, idno_filter=set(chunk_ids))
            bn_cache = BNDataCache(con, pcv_filter=unique_pcv)
        cache_sec = time.time() - t_cache

        # 청크별 PipelineContext 구성
        ctx = PipelineContext(
            con=con, loader=loader, trad_cache=trad_cache,
            bn_cache=bn_cache, exp_cache=exp_cache, dc_curve=dc_curve,
            polno_map=polno_map, mn_cache=mn_cache,
        )

        timings_chunk = {}
        for idno in chunk_ids:
            try:
                row = _compute_one(ctx, idno, timings=timings_chunk)
                if row:
                    row["RUN_ID"] = run_id
                    batch_rows.append(row)
                    ok += 1
                else:
                    err += 1
            except Exception as e:
                err += 1
                etype = type(e).__name__
                err_summary[etype] = err_summary.get(etype, 0) + 1
                if err_summary[etype] <= 3:
                    print(f"  ERR [{etype}] IDNO={idno}: {e}")

        # 청크 완료 → DB flush
        t_flush = time.time()
        if batch_rows:
            df = pd.DataFrame(batch_rows)
            out_con.execute(
                "INSERT INTO OP_BEL BY NAME SELECT * FROM df")
            batch_rows = []
        flush_sec = time.time() - t_flush

        # 청크 완료 → mn_cache 정리 (메모리 관리)
        mn_cache.clear()

        # 청크 타이밍 누적
        for k, v in timings_chunk.items():
            timings_all[k] = timings_all.get(k, 0) + v
        timings_all["cache_load"] = timings_all.get("cache_load", 0) + cache_sec
        timings_all["db_flush"] = timings_all.get("db_flush", 0) + flush_sec

        processed = ci + len(chunk)
        elapsed = time.time() - t_start
        rate = processed / elapsed if elapsed > 0 else 0
        eta = (total_target - processed) / rate if rate > 0 else 0
        print(f"  [{processed:,}/{total_target:,}] "
              f"OK={ok:,} ERR={err} "
              f"cache={cache_sec:.1f}s flush={flush_sec:.2f}s "
              f"{rate:.0f}건/s ETA={eta:.0f}s")

    if batch_rows:
        df = pd.DataFrame(batch_rows)
        out_con.execute("INSERT INTO OP_BEL SELECT * FROM df")

    # 전체 타이밍 요약
    _print_timings(timings_all, "단일프로세스")
    cache_total = timings_all.get("cache_load", 0)
    flush_total = timings_all.get("db_flush", 0)
    print(f"  [단일프로세스]  cache_load: {cache_total:.2f}s  db_flush: {flush_total:.2f}s")
    if err_summary:
        print(f"  [단일프로세스] 에러 요약: {err_summary}")
        batch_logger.warning("에러 요약: %s", err_summary)

    return ok, err


# ---------------------------------------------------------------------------
# 멀티프로세스 워커 + 실행
# ---------------------------------------------------------------------------

def _worker_process(task):
    """멀티프로세스 워커. flat IDNO 리스트를 청크 단위로 처리.

    Args:
        task: (db_path, worker_id, worker_items, dc_curve, chunk_size)
              worker_items = [(idno, prod, cov), ...]
    Returns:
        (worker_id, rows, ok_count, err_count, err_msgs)
    """
    db_path, worker_id, worker_items, dc_curve, chunk_size, preload_file = task

    con = duckdb.connect(db_path, read_only=True)
    loader = RawAssumptionLoader(con)
    all_ids = _chunk_idnos(worker_items)
    loader.preload_contracts(idnos=all_ids)
    # 프리로드 파일에서 로드 (파일 경로 전달 → 워커가 1회 읽기)
    if preload_file is not None:
        import pickle
        with open(preload_file, 'rb') as f:
            preload_data = pickle.load(f)
        # risk 관련 (mn_load_risk 78% 병목 해결)
        loader._risk_preload = preload_data.get("risk")
        loader._cov_rskrt_preload = preload_data.get("cov_rskrt")
        loader._bnft_rskrt_preload = preload_data.get("bnft_rskrt")
        loader._chr_preload = preload_data.get("chr")
        # lapse 관련 (작음)
        loader._lapse_preload = preload_data.get("lapse")
        loader._beprd_preload = preload_data.get("beprd")
        loader._skew_preload = preload_data.get("skew")
        # MORT는 미포함 → 워커별 SQL+_data_cache로 처리
        del preload_data
    exp_cache = ExpDataCache(con)
    polno_map = _build_polno_map(con)

    rows = []
    ok = err = 0
    err_msgs = []
    err_summary = {}
    timings = {}
    mn_cache = {}

    # 워커 내에서도 청크 단위로 캐시 로드
    for ci in range(0, len(worker_items), chunk_size):
        chunk = worker_items[ci:ci + chunk_size]
        chunk_ids = _chunk_idnos(chunk)
        unique_pcv = _extract_unique_pcv(chunk)

        t_cache = time.time()
        trad_cache = TradPVDataCache(con, idno_filter=set(chunk_ids))
        bn_cache = BNDataCache(con, pcv_filter=unique_pcv)
        timings["cache_load"] = timings.get("cache_load", 0) + (time.time() - t_cache)

        # 청크별 PipelineContext 구성
        ctx = PipelineContext(
            con=con, loader=loader, trad_cache=trad_cache,
            bn_cache=bn_cache, exp_cache=exp_cache, dc_curve=dc_curve,
            polno_map=polno_map, mn_cache=mn_cache,
        )

        mn_cache.clear()
        for idno in chunk_ids:
            try:
                row = _compute_one(ctx, idno, timings=timings)
                if row:
                    rows.append(row)
                    ok += 1
                else:
                    err += 1
            except Exception as e:
                err += 1
                etype = type(e).__name__
                err_summary[etype] = err_summary.get(etype, 0) + 1
                if err_summary[etype] <= 3:
                    err_msgs.append(f"[{etype}] IDNO={idno}: {e}")

    con.close()
    return (worker_id, rows, ok, err, err_msgs, timings)


def _run_multi_process(db_path, flat_list, dc_curve, n_workers,
                       chunk_size, run_id, out_con, preload_data=None):
    """멀티프로세스 병렬 실행. IDNO 균등분배 → 워커 내 청크 처리."""
    total_target = len(flat_list)

    # IDNO 균등분배: 라운드로빈
    worker_items = [[] for _ in range(n_workers)]
    for i, item in enumerate(flat_list):
        worker_items[i % n_workers].append(item)

    print(f"  워커 {n_workers}개에 분배:")
    for wid in range(n_workers):
        print(f"    워커 {wid}: {len(worker_items[wid]):,}건")

    # 워커 태스크 생성 (프리로드 데이터 포함)
    tasks = []
    for wid in range(n_workers):
        if worker_items[wid]:
            tasks.append((db_path, wid, worker_items[wid], dc_curve, chunk_size, preload_data))

    # 병렬 실행
    t_start = time.time()
    print(f"\n  병렬 산출 시작 ({n_workers} workers)...")

    with mp.Pool(processes=n_workers) as pool:
        results = pool.map(_worker_process, tasks)

    # 결과 집계 + DB 쓰기
    ok = err = 0
    timings_all = {}
    for worker_id, w_rows, w_ok, w_err, w_msgs, w_timings in results:
        ok += w_ok
        err += w_err
        for msg in w_msgs:
            print(f"  ERR (워커{worker_id}) {msg}")

        if w_rows:
            for row in w_rows:
                row["RUN_ID"] = run_id
            df = pd.DataFrame(w_rows)
            out_con.execute(
                "INSERT INTO OP_BEL BY NAME SELECT * FROM df")

        # 워커별 타이밍 출력
        if w_timings:
            _print_timings(w_timings, f"워커{worker_id}")
            cache_t = w_timings.get("cache_load", 0)
            print(f"  [워커{worker_id}]  cache_load: {cache_t:.2f}s")
            for k, v in w_timings.items():
                timings_all[k] = timings_all.get(k, 0) + v

    elapsed = time.time() - t_start
    rate = ok / elapsed if elapsed > 0 else 0
    print(f"\n  병렬 산출 완료: {ok:,}건 {elapsed:.1f}s ({rate:.0f}건/s)")
    if timings_all:
        _print_timings(timings_all, "전체합산")

    return ok, err


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OP_BEL 배치 산출 (청크 기반 + 병렬)")
    parser.add_argument("--n", type=int, default=None, help="건수 제한")
    parser.add_argument("--db", type=str, default="duckdb_transform.duckdb")
    parser.add_argument("-o", "--output", type=str, default="output_bel.duckdb",
                        help="출력 DB (기본: output_bel.duckdb)")
    parser.add_argument("--chunk", type=int, default=5000,
                        help="IDNO 청크 크기 (기본: 5000)")
    parser.add_argument("--workers", "-w", type=int, default=None,
                        help="병렬 워커 수 (기본: CPU수-1, 1이면 단일프로세스)")
    parser.add_argument("--run-id", type=int, default=None,
                        help="실행번호 지정 (미지정 시 자동 채번)")
    parser.add_argument("--reset", action="store_true",
                        help="기존 결과 삭제 후 재실행")
    parser.add_argument("--reset-run", type=int, default=None,
                        help="특정 실행번호 결과만 삭제 후 재실행")
    parser.add_argument("--preload", action="store_true",
                        help="전건 캐시 프리로드 (단일프로세스 전용, --workers 1)")
    args = parser.parse_args()

    # 워커 수 결정
    MAX_WORKERS = 8
    n_workers = args.workers
    if n_workers is None:
        n_workers = min(MAX_WORKERS, max(1, mp.cpu_count() - 1))
    n_workers = max(1, min(n_workers, MAX_WORKERS))

    # --preload는 단일프로세스 전용
    if args.preload and n_workers > 1:
        print("ERROR: --preload는 단일프로세스(--workers 1) 전용입니다.")
        print("  멀티프로세스는 청크별 캐시를 자동 사용합니다.")
        return 1

    # 로그 파일 활성화 (실행 단위)
    log_path = enable_file_logging(log_dir="logs", prefix="cf_batch")
    print(f"  로그 파일: {log_path}")

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

    # 대상: flat (idno, prod, cov) 리스트
    # CTR_POLNO 순 정렬: 같은 증권번호 그룹이 같은 청크에 모이도록
    # → mn_cache 히트율 향상 (그룹 내 타 IDNO의 MN 중복계산 방지)
    rows = con.execute("""
        SELECT INFRC_IDNO, PROD_CD, COV_CD
        FROM II_INFRC WHERE INFRC_SEQ = 1
        ORDER BY CTR_POLNO, INFRC_IDNO
    """).fetchall()

    flat_list = [(idno, prod, cov) for idno, prod, cov in rows]

    # 건수 제한
    if args.n:
        flat_list = flat_list[:args.n]

    total_target = len(flat_list)
    mode_str = f"병렬 {n_workers}워커" if n_workers > 1 else "단일프로세스"

    header = (f"OP_BEL 배치 산출  RUN_ID={run_id}  [{mode_str}]  "
              f"대상={total_target:,}건  청크={args.chunk}건")
    print(f"{'='*60}")
    print(f"  {header}")
    print(f"{'='*60}")
    batch_logger.info("=== 배치 시작 === %s", header)

    # RUN_LOG 시작 기록
    out_con.execute(
        "INSERT INTO RUN_LOG (RUN_ID, STARTED_AT, TOTAL_TARGET, WORKERS, STATUS) "
        "VALUES (?, ?, ?, ?, ?)",
        [run_id, started_at, total_target, n_workers, "RUNNING"])

    t_total = time.time()

    if n_workers == 1:
        # 단일 프로세스 모드
        print("공통 캐시 로드 중...")
        t0 = time.time()
        loader = RawAssumptionLoader(con)
        loader.preload_contracts()
        loader.preload_data_tables()
        exp_cache = ExpDataCache(con)

        dc_curve_rows = con.execute(
            "SELECT DC_RT FROM IE_DC_RT ORDER BY PASS_PRD_NO"
        ).fetchall()
        dc_curve = np.array([r[0] for r in dc_curve_rows], dtype=np.float64)

        polno_map = _build_polno_map(con)
        print(f"공통 캐시 로드: {time.time() - t0:.1f}s (polno_map: {len(polno_map):,}건)")

        ok, err = _run_single_process(
            con, flat_list, loader, exp_cache, dc_curve,
            args.preload, args.chunk, run_id, out_con, polno_map=polno_map)
    else:
        # 멀티프로세스 모드 — 메인에서 1회 프리로드 → 파일로 공유
        # MORT(89만행)은 너무 커서 제외 → 워커별 SQL+_data_cache로 처리
        import pickle, tempfile
        print("공통 데이터 프리로드 중 (메인 프로세스)...")
        t0 = time.time()
        tmp_loader = RawAssumptionLoader(con)
        tmp_loader.preload_data_tables(include_mort=False)
        preload_data = {
            # risk 관련 (작음: 수천행) — mn_load_risk 78% 병목 해결
            "risk": tmp_loader._risk_preload,
            "cov_rskrt": tmp_loader._cov_rskrt_preload,
            "bnft_rskrt": tmp_loader._bnft_rskrt_preload,
            "chr": tmp_loader._chr_preload,
            # lapse 관련 (작음: ~6천행)
            "lapse": tmp_loader._lapse_preload,
            "beprd": tmp_loader._beprd_preload,
            "skew": tmp_loader._skew_preload,
            # MORT는 제외 (89만행 → 워커 11개 복사 시 메모리 폭발)
        }
        del tmp_loader

        # 파일에 1회 저장 → 워커가 각자 읽기
        preload_file = tempfile.mktemp(suffix='.pkl', prefix='cf_preload_')
        with open(preload_file, 'wb') as f:
            pickle.dump(preload_data, f, protocol=pickle.HIGHEST_PROTOCOL)
        preload_mb = os.path.getsize(preload_file) / 1024 / 1024
        del preload_data

        dc_curve_rows = con.execute(
            "SELECT DC_RT FROM IE_DC_RT ORDER BY PASS_PRD_NO"
        ).fetchall()
        dc_curve = np.array([r[0] for r in dc_curve_rows], dtype=np.float64)
        print(f"공통 프리로드 + 파일 저장: {time.time() - t0:.1f}s ({preload_mb:.1f}MB)")

        try:
            ok, err = _run_multi_process(
                args.db, flat_list, dc_curve, n_workers,
                args.chunk, run_id, out_con, preload_data=preload_file)
        finally:
            if os.path.exists(preload_file):
                os.remove(preload_file)

    total_time = time.time() - t_total
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
    rate = ok / total_time if total_time > 0 else 0
    summary = (f"RUN_ID={run_id} {mode_str} "
               f"OK={ok:,}/{total_target:,} ERR={err} "
               f"{total_time:.1f}s ({rate:.0f}건/s)")
    print(f"\n{'='*60}")
    print(f"  OP_BEL 배치 산출 완료  RUN_ID={run_id}")
    print(f"{'='*60}")
    print(f"  모드: {mode_str}")
    print(f"  산출: {ok:,} / {total_target:,} (ERR={err})")
    print(f"  출력: {args.output} ({total_rows:,}행)")
    print(f"  소요: {total_time:.1f}s ({rate:.0f}건/s)")
    print(f"{'='*60}")
    batch_logger.info("=== 배치 완료 === %s", summary)
    batch_logger.info("로그 저장: %s", log_path)

    out_con.close()
    con.close()


if __name__ == "__main__":
    main()

"""
v1 엔진 vs proj_o.duckdb OD_TBL_BN 급부 비교 테스트

BN(급부 테이블)은 급부번호(BNFT_NO)별로 위험률과 보험금을 분해한 테이블.
MN의 decrement 결과를 기반으로 급부별 세부 계산을 검증한다.

Usage:
    python test_v1_bn_vs_proj_o.py                    # 기본 10건 샘플
    python test_v1_bn_vs_proj_o.py --n 100            # 100건 샘플
    python test_v1_bn_vs_proj_o.py --idno 50          # 특정 IDNO
    python test_v1_bn_vs_proj_o.py --csv              # 불일치 상세 CSV 저장
"""

import argparse
import logging
import os
import sqlite3
import time
import sys

import duckdb
import numpy as np
import pandas as pd

from cf_module.config import (
    CFConfig, DBConfig, RunsetParams, ProjectionConfig,
    BatchConfig, ScenarioConfig, OutputConfig,
)
from cf_module.io.reader import DataReader
from cf_module.data.model_point import load_model_points
from cf_module.data.assumptions import AssumptionLoader
from cf_module.calc.timing import build_timing
from cf_module.calc.decrement import build_decrement

# 로깅 끄기
logging.disable(logging.CRITICAL)

LEGACY_DB = "VSOLN.vdb"
PROJ_O_DB = "proj_o.duckdb"
CLOS_YM = "202309"
ASSM_YM = "202306"
TOL = 1e-8


def load_benefit_meta(conn: sqlite3.Connection, prod_cd: str, cls_cd: str, cov_cd: str) -> dict:
    """급부 메타데이터 로드.

    Returns:
        {
            'bnft_nos': [1, 2, 3, ...],
            'bnft_risk_map': {bnft_no: {'rskrt_rsk': rsk_cd, 'drpo_rsks': [rsk_cd, ...]}},
            'defry_rates': {bnft_no: [(strt, end, rate), ...]},
        }
    """
    # IP_R_BNFT_RSKRT_C: 급부별 위험률 매핑
    rows = conn.execute("""
        SELECT BNFT_NO, RSK_RT_CD, BNFT_RSKRT_YN, BNFT_DRPO_RSKRT_YN
        FROM IP_R_BNFT_RSKRT_C
        WHERE PROD_CD = ? AND CLS_CD = ? AND COV_CD = ?
        ORDER BY BNFT_NO, RSK_RT_CD
    """, (prod_cd, cls_cd, cov_cd)).fetchall()

    bnft_risk_map = {}  # {bnft_no: {'rskrt_rsk': rsk_cd, 'drpo_rsks': set()}}
    for bnft_no, rsk_cd, rskrt_yn, drpo_yn in rows:
        if bnft_no == 0:
            continue  # BNFT_NO=0 is contract-level (used in MN)
        if bnft_no not in bnft_risk_map:
            bnft_risk_map[bnft_no] = {'rskrt_rsk': None, 'drpo_rsks': set()}
        if rskrt_yn == 1 or rskrt_yn == '1':
            bnft_risk_map[bnft_no]['rskrt_rsk'] = rsk_cd
        if drpo_yn == 1 or drpo_yn == '1':
            bnft_risk_map[bnft_no]['drpo_rsks'].add(rsk_cd)

    # IP_B_BNFT_DEFRY_RT: 급부별 지급률
    defry_rows = conn.execute("""
        SELECT BNFT_NO, SETN_STRT_VAL, SETN_END_VAL, DEFRY_RT
        FROM IP_B_BNFT_DEFRY_RT
        WHERE PROD_CD = ? AND CLS_CD = ? AND COV_CD = ?
        ORDER BY BNFT_NO, SETN_STRT_VAL
    """, (prod_cd, cls_cd, cov_cd)).fetchall()

    defry_rates = {}
    for bnft_no, strt, end, rate in defry_rows:
        if bnft_no not in defry_rates:
            defry_rates[bnft_no] = []
        defry_rates[bnft_no].append((int(strt), int(end), float(rate)))

    return {
        'bnft_nos': sorted(bnft_risk_map.keys()),
        'bnft_risk_map': bnft_risk_map,
        'defry_rates': defry_rates,
    }


def get_defry_rate(defry_list: list, duration_year: int) -> float:
    """지급률 조회 (경과연수 기준)."""
    if not defry_list:
        return 1.0
    for strt, end, rate in defry_list:
        if strt <= duration_year <= end:
            return rate
    # STRT=0, END=0 → 전 기간 적용
    if defry_list[0][0] == 0 and defry_list[0][1] == 0:
        return defry_list[0][2]
    # 999,999 → 전 기간
    if defry_list[0][0] == 999 and defry_list[0][1] == 999:
        return defry_list[0][2]
    return defry_list[-1][2]


def run_bn_single(idno: int, reader: DataReader, legacy_conn: sqlite3.Connection) -> dict:
    """v1 파이프라인으로 단건 BN 프로젝션 실행.

    Returns:
        {
            'n_steps': int,
            'bnft_nos': [int, ...],
            'bnft_data': {
                bnft_no: {
                    'bnft_rskrt': array,       # 급부 위험률 (월)
                    'bnft_drpo_rskrt': array,   # 급부 탈퇴율
                    'trme': array,              # 급부별 기말유지자수
                    'trmo': array,              # 급부별 기시유지자수
                    'bnft_ocurpe': array,       # 급부 발생건수
                    'crit_amt': float,          # 기준금액
                    'defry_rt': array,          # 지급률 (시점별)
                    'pyamt': array,             # 지급금액
                    'bnft_insuamt': array,      # 급부보험금
                }
            },
            'trmnat_rt': array,  # CTR 전체 해약률 (MN과 동일)
            'rsvamt_drpo': array,  # RSVAMT 탈퇴율 (MN과 동일)
        }
    """
    config = CFConfig(
        db=DBConfig(db_type="sqlite", path=LEGACY_DB),
        runset=RunsetParams(infrc_seq=1, infrc_idno=idno, clos_ym=CLOS_YM, assm_ym=ASSM_YM),
        projection=ProjectionConfig(time_step="monthly", base_date=CLOS_YM),
        batch=BatchConfig(chunk_size=100_000),
        scenario=ScenarioConfig(),
        output=OutputConfig(output_dir="./output"),
        run_targets=["ifrs17"],
        run_mode="valuation",
        debug=False,
    )

    mp = load_model_points(
        reader, config,
        query_name="II_INFRC_SINGLE",
        params=config.runset.query_params_single,
    )
    loader = AssumptionLoader(reader, config)
    assumptions = loader.load_all(
        params=config.runset.query_params_with_assm,
        mp_ids=mp.mp_ids, mp=mp,
    )
    timing = build_timing(mp, config.projection)
    dec = build_decrement(mp, timing, assumptions, config.scenario, reader=reader, config=config)

    s = timing.n_steps
    prod_cd = mp.product_cd[0]
    cls_cd = mp.cls_cd[0]
    cov_cd = mp.cov_cd[0]

    # 급부 메타데이터
    meta = load_benefit_meta(legacy_conn, prod_cd, cls_cd, cov_cd)

    # 기준금액 (GRNTPT_JOIN_AMT)
    crit_amt = float(legacy_conn.execute(
        "SELECT GRNTPT_JOIN_AMT FROM II_INFRC WHERE INFRC_SEQ=1 AND INFRC_IDNO=?",
        (idno,)
    ).fetchone()[0])

    # v1의 위험률코드별 dedup 월율 (n_steps, n_risks)
    rsk_codes = dec.rsk_rt_cd  # list of risk code strings
    qx_be_monthly = dec.qx_be_by_risk  # (n_steps, n_risks) - dedup monthly rates

    # MN에서 사용하는 CTR 탈퇴 구성요소
    tpx_bot = np.ones(s)
    if s > 1:
        tpx_bot[1:] = dec.tpx[0, :-1]

    # d_rsvamt / tpx_bot = rsvamt 탈퇴율
    rsvamt_drpo = np.where(tpx_bot > 0, dec.d_rsvamt[0] / tpx_bot, 0) if dec.d_rsvamt is not None else np.zeros(s)

    # wx (전체 해약률)
    wx_ctr = dec.wx_monthly[0]

    # 위험률 코드 → 인덱스 매핑
    rsk_idx = {cd: i for i, cd in enumerate(rsk_codes)} if rsk_codes is not None and len(rsk_codes) > 0 else {}

    bnft_data = {}
    for bnft_no in meta['bnft_nos']:
        bm = meta['bnft_risk_map'][bnft_no]

        # 급부 위험률 (BNFT_RSKRT_YN=1인 위험률의 dedup 월율)
        rskrt_rsk = bm['rskrt_rsk']
        if rskrt_rsk and rskrt_rsk in rsk_idx and qx_be_monthly is not None:
            # is_in_force mask 적용
            mask = timing.is_in_force[0]
            # qx_be_by_risk는 masked 형태일 수 있음 → full array 추출
            bnft_rskrt = np.zeros(s)
            bnft_rskrt[mask] = qx_be_monthly[:, rsk_idx[rskrt_rsk]]
        else:
            bnft_rskrt = np.zeros(s)

        # 급부별 BNFT_DRPO 탈퇴율 (해당 급부의 BNFT_DRPO_YN=1인 위험률 합)
        bnft_drpo_rskrt = np.zeros(s)
        for drpo_rsk in bm['drpo_rsks']:
            if drpo_rsk in rsk_idx and qx_be_monthly is not None:
                drpo_rates = np.zeros(s)
                drpo_rates[mask] = qx_be_monthly[:, rsk_idx[drpo_rsk]]
                bnft_drpo_rskrt += drpo_rates

        # 급부별 tpx: exit = wx + rsvamt_drpo + bnft_drpo (급부별)
        # BN TRME = cumprod(1 - exit), SETL=0 기준 시작
        bn_exit = wx_ctr + rsvamt_drpo + bnft_drpo_rskrt
        bn_survive = np.maximum(1.0 - bn_exit, 0.0)

        # TRME[0]=1.0, TRME[t]=product(bn_survive[1:t+1]) for t>=1
        bn_trme = np.ones(s)
        if s > 1:
            bn_trme[1:] = np.cumprod(bn_survive[1:])

        bn_trmo = np.ones(s)
        if s > 1:
            bn_trmo[1:] = bn_trme[:-1]

        # 급부 발생건수
        bnft_ocurpe = bn_trmo * bnft_rskrt

        # 지급률
        defry_rt = np.ones(s)
        defry_list = meta['defry_rates'].get(bnft_no, [])
        if defry_list:
            for t in range(s):
                dur_year = int(timing.duration_years[0, t]) if t < timing.duration_years.shape[1] else 1
                defry_rt[t] = get_defry_rate(defry_list, dur_year)

        # 지급금액 & 급부보험금
        pyamt = crit_amt * defry_rt
        bnft_insuamt = bnft_ocurpe * pyamt

        bnft_data[bnft_no] = {
            'bnft_rskrt': bnft_rskrt,
            'bnft_drpo_rskrt': bnft_drpo_rskrt,
            'trme': bn_trme,
            'trmo': bn_trmo,
            'bnft_ocurpe': bnft_ocurpe,
            'crit_amt': crit_amt,
            'defry_rt': defry_rt,
            'pyamt': pyamt,
            'bnft_insuamt': bnft_insuamt,
        }

    return {
        'n_steps': s,
        'bnft_nos': meta['bnft_nos'],
        'bnft_data': bnft_data,
        'trmnat_rt': wx_ctr,
        'rsvamt_drpo': rsvamt_drpo,
    }


def compare_bn_single(idno: int, v1: dict, expected: pd.DataFrame) -> dict:
    """단건 BN 비교."""
    if expected.empty:
        return {"idno": idno, "pass": True, "items": {}, "compare_n": 0, "bnft_results": {}}

    exp_bnft_nos = sorted(expected['BNFT_NO'].unique().astype(int).tolist())
    v1_bnft_nos = v1['bnft_nos']

    # BNFT_NO 집합 비교
    if set(exp_bnft_nos) != set(v1_bnft_nos):
        return {
            "idno": idno, "pass": False,
            "items": {"BNFT_NO_MISMATCH": {
                "max_diff": 999, "pass": False,
                "v1_val": str(v1_bnft_nos), "exp_val": str(exp_bnft_nos),
            }},
            "compare_n": 0, "bnft_results": {},
        }

    all_pass = True
    bnft_results = {}

    for bnft_no in exp_bnft_nos:
        if bnft_no not in v1['bnft_data']:
            all_pass = False
            bnft_results[bnft_no] = {"pass": False, "error": "missing in v1"}
            continue

        exp_bn = expected[expected['BNFT_NO'] == bnft_no].sort_values('SETL_AFT_PASS_MMCNT').reset_index(drop=True)
        v1_bn = v1['bnft_data'][bnft_no]

        n_exp = len(exp_bn)
        n_v1 = v1['n_steps']
        compare_n = min(n_exp - 1, n_v1 - 1)

        if compare_n <= 0:
            bnft_results[bnft_no] = {"pass": True, "compare_n": 0, "items": {}}
            continue

        exp = exp_bn.iloc[1:compare_n + 1].reset_index(drop=True)

        # Phase 1: decrement 기반 항목 (PYAMT/INSUAMT는 Phase 2)
        compare_items = [
            ("BNFT_RSKRT",    v1_bn['bnft_rskrt'],     "BNFT_RSKRT"),
            ("TRME",          v1_bn['trme'],            "TRME_MTNPSN_CNT"),
            ("BNFT_OCURPE",   v1_bn['bnft_ocurpe'],    "BNFT_OCURPE_CNT"),
        ]

        items = {}
        bn_pass = True
        for name, v1_arr, exp_col in compare_items:
            v1_slice = v1_arr[1:compare_n + 1]
            exp_arr = exp[exp_col].values.astype(np.float64)
            diff = np.abs(v1_slice - exp_arr)
            max_diff = float(diff.max())
            max_idx = int(diff.argmax())
            passed = max_diff < TOL

            items[name] = {
                "max_diff": max_diff,
                "setl": max_idx + 1,
                "pass": passed,
                "v1_val": float(v1_slice[max_idx]),
                "exp_val": float(exp_arr[max_idx]),
            }
            if not passed:
                bn_pass = False
                all_pass = False

        bnft_results[bnft_no] = {"pass": bn_pass, "compare_n": compare_n, "items": items}

    return {
        "idno": idno,
        "pass": all_pass,
        "bnft_results": bnft_results,
        "bnft_nos": exp_bnft_nos,
    }


def main():
    parser = argparse.ArgumentParser(description="v1 vs proj_o.duckdb OD_TBL_BN 비교")
    parser.add_argument("--n", type=int, default=10, help="랜덤 샘플 수 (default: 10)")
    parser.add_argument("--idno", type=str, default=None, help="특정 IDNO (콤마 구분)")
    parser.add_argument("--csv", action="store_true", help="불일치 상세 CSV 저장")
    parser.add_argument("--seed", type=int, default=42, help="랜덤 시드")
    args = parser.parse_args()

    proj = duckdb.connect(PROJ_O_DB, read_only=True)
    legacy_conn = sqlite3.connect(LEGACY_DB)

    # BN이 있는 IDNO 목록
    all_bn_idnos = proj.execute(
        "SELECT DISTINCT INFRC_IDNO FROM OD_TBL_BN ORDER BY INFRC_IDNO"
    ).fetchdf()["INFRC_IDNO"].values

    if args.idno:
        idnos = [int(x.strip()) for x in args.idno.split(",")]
    else:
        rng = np.random.default_rng(args.seed)
        idnos = sorted(rng.choice(all_bn_idnos, size=min(args.n, len(all_bn_idnos)), replace=False))

    print("=" * 80)
    print(f"v1 vs proj_o.duckdb OD_TBL_BN 비교")
    print(f"  Legacy: {LEGACY_DB}")
    print(f"  Expected: {PROJ_O_DB} ({len(all_bn_idnos):,} BN IDNOs)")
    print(f"  Test IDNOs: {len(idnos)}건")
    print("=" * 80)

    # Reader
    config_base = CFConfig(
        db=DBConfig(db_type="sqlite", path=LEGACY_DB),
        runset=RunsetParams(infrc_seq=1, infrc_idno=idnos[0], clos_ym=CLOS_YM, assm_ym=ASSM_YM),
        projection=ProjectionConfig(time_step="monthly", base_date=CLOS_YM),
        batch=BatchConfig(chunk_size=100_000),
        scenario=ScenarioConfig(),
        output=OutputConfig(output_dir="./output"),
        run_targets=["ifrs17"],
        run_mode="valuation",
        debug=False,
    )
    reader = DataReader(config_base.db)
    reader.load_queries(config_base.queries_path)

    results = []
    pass_count = 0
    fail_count = 0

    for i, idno in enumerate(idnos):
        t0 = time.time()
        try:
            v1 = run_bn_single(int(idno), reader, legacy_conn)

            expected = proj.execute(
                "SELECT * FROM OD_TBL_BN WHERE INFRC_IDNO = ? ORDER BY BNFT_NO, SETL_AFT_PASS_MMCNT",
                [int(idno)]
            ).fetchdf()

            result = compare_bn_single(int(idno), v1, expected)
            elapsed = time.time() - t0

            if result["pass"]:
                pass_count += 1
                status = "PASS"
            else:
                fail_count += 1
                status = "FAIL"

            # 진행 표시
            bn_str = f"{len(result.get('bnft_nos', []))}bnft"
            print(f"  [{i+1:4d}/{len(idnos)}] IDNO={idno:>8d}  {bn_str:>6s}  {elapsed:.1f}s  [{status}]", end="")

            if status == "FAIL":
                for bno, br in result.get("bnft_results", {}).items():
                    if isinstance(br, dict) and not br.get("pass", True):
                        fail_items = [k for k, v in br.get("items", {}).items() if not v.get("pass", True)]
                        if fail_items:
                            fi = br["items"][fail_items[0]]
                            print(f"  BN{bno}:{fail_items[0]}({fi['max_diff']:.2e})", end="")
            print()

            results.append(result)

        except Exception as e:
            fail_count += 1
            elapsed = time.time() - t0
            print(f"  [{i+1:4d}/{len(idnos)}] IDNO={idno:>8d}  ERROR  {elapsed:.1f}s  {str(e)[:60]}")
            results.append({"idno": int(idno), "pass": False, "bnft_results": {}, "error": str(e)})

    proj.close()
    legacy_conn.close()

    print()
    print("=" * 80)
    print(f"결과: PASS={pass_count}, FAIL={fail_count}, 총={len(idnos)}")
    print("=" * 80)

    # 항목별 통계
    if results:
        item_stats = {}
        for r in results:
            for bno, br in r.get("bnft_results", {}).items():
                for name, info in br.get("items", {}).items():
                    if name not in item_stats:
                        item_stats[name] = {"pass": 0, "fail": 0, "max_diff": 0}
                    if info["pass"]:
                        item_stats[name]["pass"] += 1
                    else:
                        item_stats[name]["fail"] += 1
                    item_stats[name]["max_diff"] = max(item_stats[name]["max_diff"], info["max_diff"])

        print(f"\n{'항목':<20} {'PASS':>6} {'FAIL':>6} {'Max Diff':>12}")
        print("-" * 50)
        for name in ["BNFT_RSKRT", "TRME", "BNFT_OCURPE"]:
            if name in item_stats:
                s = item_stats[name]
                print(f"  {name:<18} {s['pass']:>6} {s['fail']:>6} {s['max_diff']:>12.2e}")

    if args.csv:
        rows = []
        for r in results:
            for bno, br in r.get("bnft_results", {}).items():
                for name, info in br.get("items", {}).items():
                    if not info.get("pass", True):
                        rows.append({
                            "idno": r["idno"], "bnft_no": bno, "item": name,
                            "max_diff": info["max_diff"], "setl": info["setl"],
                            "v1_val": info["v1_val"], "exp_val": info["exp_val"],
                        })
        if rows:
            csv_path = "v1_bn_vs_proj_o_fails.csv"
            pd.DataFrame(rows).to_csv(csv_path, index=False)
            print(f"\n불일치 상세: {csv_path}")


if __name__ == "__main__":
    main()

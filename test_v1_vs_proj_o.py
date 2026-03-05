"""
v1 엔진 vs proj_o.duckdb OD_TBL_MN 대량 비교 테스트

Legacy: VSOLN.vdb, Expected: proj_o.duckdb (42,000 IDNOs)

Usage:
    python test_v1_vs_proj_o.py                    # 기본 10건 샘플
    python test_v1_vs_proj_o.py --n 100            # 100건 샘플
    python test_v1_vs_proj_o.py --idno 17          # 특정 IDNO
    python test_v1_vs_proj_o.py --idno 17,50,124   # 복수 IDNO
    python test_v1_vs_proj_o.py --csv              # 불일치 상세 CSV 저장
"""

import argparse
import logging
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

# 비교 항목 정의
COMPARE_ITEMS = [
    # (이름, v1 추출 함수, 기대값 컬럼)
    ("CTR_TRME",           "tpx",           "CTR_TRME_MTNPSN_CNT"),
    ("CTR_TRMNAT_RT",      "wx_ctr",        "CTR_TRMNAT_RT"),
    ("CTR_RSVAMT_DRPO",    "ctr_rsvamt_rt", "CTR_RSVAMT_DEFRY_DRPO_RSKRT"),
    ("CTR_BNFT_DRPO",      "ctr_bnft_rt",   "CTR_BNFT_DRPO_RSKRT"),
    ("CTR_TRMPSN",         "ctr_trmpsn",    "CTR_TRMPSN_CNT"),
    ("CTR_RSVAMT_DRPSN",   "d_rsvamt",      "CTR_RSVAMT_DEFRY_DRPSN_CNT"),
    ("CTR_BNFT_DRPSN",     "d_bnft",        "CTR_BNFT_DEFRY_DRPSN_CNT"),
    ("PAY_TRME",           "pay_tpx",       "PAY_TRME_MTNPSN_CNT"),
    ("PAY_TRMNAT_RT",      "wx_pay",        "PAY_TRMNAT_RT"),
    ("PYEXSP_DRPO",        "pyexsp_rt",     "PYEXSP_DRPO_RSKRT"),
    ("PAY_TRMPSN",         "pay_trmpsn",    "PAY_TRMPSN_CNT"),
    ("PYEXSP_DRPSN",       "d_pyexsp",      "PYEXSP_DRPSN_CNT"),
]


def run_v1_single(idno: int, reader: DataReader) -> dict:
    """v1 파이프라인으로 단건 프로젝션 실행, 비교용 배열 반환."""
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

    # CTR tpx_bot
    tpx_bot = np.ones(s)
    if s > 1:
        tpx_bot[1:] = dec.tpx[0, :-1]

    # PAY tpx_bot
    pay_tpx_bot = np.ones(s)
    if dec.pay_tpx is not None and s > 1:
        pay_tpx_bot[1:] = dec.pay_tpx[0, :-1]

    # CTR 탈퇴율 (d_rsvamt / tpx_bot)
    ctr_rsvamt_rt = np.where(tpx_bot > 0, dec.d_rsvamt[0] / tpx_bot, 0) if dec.d_rsvamt is not None else np.zeros(s)
    ctr_bnft_rt = np.where(tpx_bot > 0, dec.d_bnft[0] / tpx_bot, 0) if dec.d_bnft is not None else np.zeros(s)

    # PAY 탈퇴율
    pyexsp_rt = np.where(pay_tpx_bot > 0, dec.pay_d_pyexsp[0] / pay_tpx_bot, 0) if dec.pay_d_pyexsp is not None else np.zeros(s)

    # PAY wx (pay_qx_monthly는 전체 exit rate, wx는 별도)
    # v1에서 PAY wx = pay_dx - pay_qx (총탈퇴 - 위험률)
    if dec.pay_dx_monthly is not None and dec.pay_qx_monthly is not None:
        wx_pay = dec.pay_dx_monthly[0] - dec.pay_qx_monthly[0]
    else:
        wx_pay = dec.wx_monthly[0]

    return {
        "n_steps": s,
        "tpx": dec.tpx[0],
        "wx_ctr": dec.wx_monthly[0],
        "ctr_rsvamt_rt": ctr_rsvamt_rt,
        "ctr_bnft_rt": ctr_bnft_rt,
        "ctr_trmpsn": tpx_bot * dec.wx_monthly[0],
        "d_rsvamt": dec.d_rsvamt[0] if dec.d_rsvamt is not None else np.zeros(s),
        "d_bnft": dec.d_bnft[0] if dec.d_bnft is not None else np.zeros(s),
        "pay_tpx": dec.pay_tpx[0] if dec.pay_tpx is not None else np.zeros(s),
        "wx_pay": wx_pay,
        "pyexsp_rt": pyexsp_rt,
        "pay_trmpsn": pay_tpx_bot * wx_pay,
        "d_pyexsp": dec.pay_d_pyexsp[0] if dec.pay_d_pyexsp is not None else np.zeros(s),
    }


def compare_single(idno: int, v1: dict, expected: pd.DataFrame) -> dict:
    """단건 비교 결과 반환.

    v1 인덱싱: tpx[0]=1.0(SETL=0), tpx[1]=EOT(SETL=1), ...
    기대값: SETL=0은 초기행(CTR_TRMO=0), SETL=1부터 실제 프로젝션.
    → SETL=1부터 비교, v1도 인덱스 1부터 사용.
    """
    n_exp = len(expected)
    n_v1 = v1["n_steps"]
    compare_n = min(n_exp - 1, n_v1 - 1)

    if compare_n <= 0:
        return {"idno": idno, "pass": True, "items": {}, "compare_n": 0}

    exp = expected.iloc[1:compare_n + 1].reset_index(drop=True)
    items = {}

    for name, v1_key, exp_col in COMPARE_ITEMS:
        v1_arr = v1[v1_key][1:compare_n + 1]  # v1 인덱스 1부터 (SETL=1 대응)
        exp_arr = exp[exp_col].values.astype(np.float64)
        diff = np.abs(v1_arr - exp_arr)
        max_diff = float(diff.max())
        max_idx = int(diff.argmax())

        items[name] = {
            "max_diff": max_diff,
            "setl": max_idx + 1,  # SETL 기준 (1-based)
            "pass": max_diff < TOL,
            "v1_val": float(v1_arr[max_idx]),
            "exp_val": float(exp_arr[max_idx]),
        }

    all_pass = all(item["pass"] for item in items.values())
    return {"idno": idno, "pass": all_pass, "items": items, "compare_n": compare_n}


def main():
    parser = argparse.ArgumentParser(description="v1 vs proj_o.duckdb OD_TBL_MN 비교")
    parser.add_argument("--n", type=int, default=10, help="랜덤 샘플 수 (default: 10)")
    parser.add_argument("--idno", type=str, default=None, help="특정 IDNO (콤마 구분)")
    parser.add_argument("--csv", action="store_true", help="불일치 상세 CSV 저장")
    parser.add_argument("--seed", type=int, default=42, help="랜덤 시드")
    args = parser.parse_args()

    # 기대값 DB
    proj = duckdb.connect(PROJ_O_DB, read_only=True)
    all_idnos = proj.execute(
        "SELECT DISTINCT INFRC_IDNO FROM OD_TBL_MN ORDER BY INFRC_IDNO"
    ).fetchdf()["INFRC_IDNO"].values

    # IDNO 선택
    if args.idno:
        idnos = [int(x.strip()) for x in args.idno.split(",")]
    else:
        rng = np.random.default_rng(args.seed)
        idnos = sorted(rng.choice(all_idnos, size=min(args.n, len(all_idnos)), replace=False))

    print("=" * 80)
    print(f"v1 vs proj_o.duckdb OD_TBL_MN 비교")
    print(f"  Legacy: {LEGACY_DB}")
    print(f"  Expected: {PROJ_O_DB} ({len(all_idnos):,} IDNOs)")
    print(f"  Test IDNOs: {len(idnos)}건")
    print("=" * 80)

    # v1 Reader (재사용)
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

    # 비교 실행
    results = []
    pass_count = 0
    fail_count = 0
    fail_details = []

    for i, idno in enumerate(idnos):
        t0 = time.time()
        try:
            # v1 실행
            v1 = run_v1_single(int(idno), reader)

            # 기대값 로드
            expected = proj.execute(
                "SELECT * FROM OD_TBL_MN WHERE INFRC_IDNO = ? ORDER BY SETL_AFT_PASS_MMCNT",
                [int(idno)]
            ).fetchdf()

            # 비교
            result = compare_single(int(idno), v1, expected)
            elapsed = time.time() - t0

            if result["pass"]:
                pass_count += 1
                status = "PASS"
            else:
                fail_count += 1
                status = "FAIL"
                fail_items = [k for k, v in result["items"].items() if not v["pass"]]
                fail_details.append(result)

            # 진행 표시
            print(f"  [{i+1:4d}/{len(idnos)}] IDNO={idno:>8d}  {result['compare_n']:>4d}mo  {elapsed:.1f}s  [{status}]", end="")
            if status == "FAIL":
                for fname in fail_items[:3]:
                    fi = result["items"][fname]
                    print(f"  {fname}({fi['max_diff']:.2e}@SETL{fi['setl']})", end="")
            print()

            results.append(result)

        except Exception as e:
            fail_count += 1
            elapsed = time.time() - t0
            print(f"  [{i+1:4d}/{len(idnos)}] IDNO={idno:>8d}  ERROR  {elapsed:.1f}s  {str(e)[:60]}")
            results.append({"idno": int(idno), "pass": False, "items": {}, "error": str(e)})

    proj.close()

    # 요약
    print()
    print("=" * 80)
    print(f"결과: PASS={pass_count}, FAIL={fail_count}, 총={len(idnos)}")
    print("=" * 80)

    # 항목별 통계
    if results:
        print(f"\n{'항목':<25} {'PASS':>6} {'FAIL':>6} {'Max Diff':>12}")
        print("-" * 55)
        for name, _, _ in COMPARE_ITEMS:
            item_pass = sum(1 for r in results if name in r.get("items", {}) and r["items"][name]["pass"])
            item_fail = sum(1 for r in results if name in r.get("items", {}) and not r["items"][name]["pass"])
            max_d = max((r["items"][name]["max_diff"] for r in results if name in r.get("items", {})), default=0)
            print(f"  {name:<23} {item_pass:>6} {item_fail:>6} {max_d:>12.2e}")

    # FAIL 상세
    if fail_details and args.csv:
        rows = []
        for r in fail_details:
            for name, info in r["items"].items():
                if not info["pass"]:
                    rows.append({
                        "idno": r["idno"],
                        "item": name,
                        "max_diff": info["max_diff"],
                        "setl": info["setl"],
                        "v1_val": info["v1_val"],
                        "exp_val": info["exp_val"],
                    })
        if rows:
            csv_path = "v1_vs_proj_o_fails.csv"
            pd.DataFrame(rows).to_csv(csv_path, index=False)
            print(f"\n불일치 상세: {csv_path}")


if __name__ == "__main__":
    main()

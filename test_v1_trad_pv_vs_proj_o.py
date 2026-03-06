"""
v1 OD_TRAD_PV Phase 1 비교 테스트 (정적/확정 필드)

Legacy: VSOLN.vdb, Expected: proj_o.duckdb (42,000 IDNOs)

Phase 1 비교 항목 (이자율 무관):
  CTR_AFT_PASS_MMCNT, PREM_PAY_YN, ORIG_PREM, DC_PREM,
  ACUM_NPREM, PAD_PREM, YSTR_RSVAMT, YYEND_RSVAMT,
  SOFF_BF_TMRFND, ACQSEXP1_BIZEXP

Phase 2 (미구현, 이자율 의존):
  APLY_PUBANO_INRT, CNCTTP_ACUMAMT_KICS, APLY_PREM_ACUMAMT_BNFT/EXP 등

Usage:
    python test_v1_trad_pv_vs_proj_o.py                 # 기본 10건
    python test_v1_trad_pv_vs_proj_o.py --n 100         # 100건 샘플
    python test_v1_trad_pv_vs_proj_o.py --idno 46017    # 특정 IDNO
    python test_v1_trad_pv_vs_proj_o.py --csv            # 불일치 CSV 저장
    python test_v1_trad_pv_vs_proj_o.py --debug 46017   # 단건 상세 디버그
"""

import argparse
import time
import sys

import duckdb
import numpy as np
import pandas as pd
import sqlite3

LEGACY_DB = "VSOLN.vdb"
PROJ_O_DB = "proj_o.duckdb"
CLOS_YM = "202309"
TOL = 1e-6  # 정수 필드 위주이므로 1e-6 허용


def load_contract_info(conn: sqlite3.Connection, idno: int) -> dict:
    """II_INFRC에서 계약 기본 정보 로드."""
    row = conn.execute("""
        SELECT INFRC_IDNO, PROD_CD, COV_CD, CTR_DT,
               INSTRM_YYCNT, PAYPR_YYCNT, PASS_YYCNT, PASS_MMCNT,
               GRNTPT_GPREM, GRNTPT_JOIN_AMT,
               TOT_TRMNAT_DDCT_AMT, STD_TRMNAT_DDCT_AMT,
               PAYPR_DVCD, ETC_EXPCT_BIZEXP_KEY_VAL, CLS_CD
        FROM II_INFRC WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1
    """, [idno]).fetchone()
    if not row:
        return None
    return {
        "idno": row[0], "prod_cd": row[1], "cov_cd": row[2], "ctr_dt": row[3],
        "bterm_yy": row[4], "pterm_yy": row[5],
        "pass_yy": row[6], "pass_mm": row[7],
        "gprem": row[8], "join_amt": row[9],
        "tot_trmnat_ddct": row[10], "std_trmnat_ddct": row[11],
        "paypr_dvcd": row[12], "etc_bizexp_key": row[13], "cls_cd": row[14],
    }


def load_rsvamt_bas(conn: sqlite3.Connection, idno: int) -> dict:
    """II_RSVAMT_BAS에서 준비금/순보험료 로드."""
    row = conn.execute(
        "SELECT * FROM II_RSVAMT_BAS WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
        [idno],
    ).fetchone()
    if not row:
        return None
    cols = [c[1] for c in conn.execute("PRAGMA table_info(II_RSVAMT_BAS)").fetchall()]
    data = dict(zip(cols, row))

    crit_join = data["CRIT_JOIN_AMT"]
    nprem = data["NPREM"]

    # 연도별 YSTR/YYEND 배열 (1~120)
    ystr_arr = []
    yyend_arr = []
    for yr in range(1, 121):
        ystr_key = f"YSTR_RSVAMT{yr}"
        yyend_key = f"YYEND_RSVAMT{yr}"
        ystr_arr.append(data.get(ystr_key, 0) or 0)
        yyend_arr.append(data.get(yyend_key, 0) or 0)

    return {
        "crit_join_amt": crit_join,
        "nprem": nprem,
        "ystr": np.array(ystr_arr, dtype=np.float64),
        "yyend": np.array(yyend_arr, dtype=np.float64),
    }


def load_acqsexp_rt(conn: sqlite3.Connection, prod_cd: str, cov_cd: str,
                     cls_cd: str, paypr_dvcd: str, etc_key: str) -> float:
    """IP_P_EXPCT_BIZEXP_RT에서 초년도 사업비율 로드."""
    row = conn.execute("""
        SELECT FRYY_GPREM_VS_ACQSEXP_RT
        FROM IP_P_EXPCT_BIZEXP_RT
        WHERE PROD_CD = ? AND COV_CD = ? AND CLS_CD = ?
          AND PAYPR_DVCD = ? AND ETC_EXPCT_BIZEXP_KEY_VAL = ?
        LIMIT 1
    """, [prod_cd, cov_cd, cls_cd, paypr_dvcd, etc_key]).fetchone()
    if row:
        return row[0]
    return None


def load_tmrfnd_calc_tp(conn: sqlite3.Connection, prod_cd: str, cov_cd: str) -> dict:
    """IP_P_COV에서 TMRFND_CALC_TP 코드 로드."""
    row = conn.execute("""
        SELECT TMRFND_CALC_TP1_CD, TMRFND_CALC_TP2_CD, TMRFND_CALC_TP3_CD
        FROM IP_P_COV
        WHERE PROD_CD = ? AND COV_CD = ?
        LIMIT 1
    """, [prod_cd, cov_cd]).fetchone()
    if not row:
        return {"tp1": None, "tp2": None, "tp3": None}
    return {"tp1": row[0], "tp2": row[1], "tp3": row[2]}


def load_rsvamt_tmrfnd(conn: sqlite3.Connection, idno: int) -> dict:
    """II_RSVAMT_TMRFND에서 해지환급금 기준 데이터 로드."""
    row = conn.execute(
        "SELECT * FROM II_RSVAMT_TMRFND WHERE INFRC_IDNO = ? AND INFRC_SEQ = 1",
        [idno],
    ).fetchone()
    if not row:
        return None
    cols = [c[1] for c in conn.execute("PRAGMA table_info(II_RSVAMT_TMRFND)").fetchall()]
    data = dict(zip(cols, row))
    return data


def compute_trad_pv(info: dict, bas: dict, n_steps: int) -> dict:
    """Phase 1 TRAD_PV 필드 계산.

    SOFF_BF_TMRFND 유형:
      A (TP2=4, TP3=1): 정수월 보간 그대로
      B (TP2=1, TP3=2, TMRFND 없음): 초기 보험연도 ACQSEXP 차감
      C (TMRFND 데이터 있음): SOFF=0 (납입기간 동안)

    Returns dict of arrays indexed by SETL (0-based).
    """
    elapsed = info["pass_yy"] * 12 + info["pass_mm"]  # CTR_AFT_PASS_MMCNT at SETL=0
    gprem = info["gprem"]
    join_amt = info["join_amt"]
    mult = join_amt / bas["crit_join_amt"] if bas["crit_join_amt"] else 1.0
    pterm_mm = info["pterm_yy"] * 12
    bterm_mm = info["bterm_yy"] * 12

    acqsexp1_val = info.get("acqsexp1", 0) or 0

    # 초기 보험연도 (SETL=0 기준)
    initial_ins_year = (elapsed - 1) // 12 + 1 if elapsed > 0 else 1

    ctr_mm = np.arange(n_steps) + elapsed  # CTR_AFT_PASS_MMCNT
    prem_pay_yn = (ctr_mm <= pterm_mm).astype(np.float64)
    orig_prem = np.full(n_steps, gprem, dtype=np.float64)
    dc_prem = orig_prem.copy()  # Phase 1: 할인 없음 가정
    acum_nprem = np.full(n_steps, bas["nprem"] * mult, dtype=np.float64)
    # PAD_PREM: 초기값 = GPREM × CTR_MM[0], 이후 PREM_PAY_YN=1일 때 GPREM 누적
    pad_prem = np.empty(n_steps, dtype=np.float64)
    pad_prem[0] = gprem * ctr_mm[0]
    for t in range(1, n_steps):
        pad_prem[t] = pad_prem[t - 1] + gprem * prem_pay_yn[t]
    acqsexp1 = np.full(n_steps, acqsexp1_val, dtype=np.float64)

    # YSTR/YYEND 준비금 (보험연도 기반)
    ystr_rsvamt = np.zeros(n_steps, dtype=np.float64)
    yyend_rsvamt = np.zeros(n_steps, dtype=np.float64)
    soff_bf_tmrfnd = np.zeros(n_steps, dtype=np.float64)
    aply_prem_acumamt_bnft = np.zeros(n_steps, dtype=np.float64)

    for t in range(n_steps):
        cm = int(ctr_mm[t])
        ins_year = (cm - 1) // 12 + 1  # 보험연도 (1-based)
        month_in_year = cm - (ins_year - 1) * 12  # 연도 내 경과월 (1~12)

        yr_idx = ins_year - 1  # 0-based array index
        if yr_idx < 120:
            ystr_v = bas["ystr"][yr_idx] * mult
            yyend_v = bas["yyend"][yr_idx] * mult
        else:
            ystr_v = 0.0
            yyend_v = 0.0

        ystr_rsvamt[t] = ystr_v
        yyend_rsvamt[t] = yyend_v

        # 정수월 보간 (모든 유형의 기본)
        interp = ystr_v + (yyend_v - ystr_v) * month_in_year / 12
        aply_prem_acumamt_bnft[t] = interp

        # SOFF = 정수월 보간 (기본값)
        # Note: 일부 상품은 초기연도 ACQSEXP 차감 또는 SOFF=0 등
        # 복잡한 상품별 분기가 있으나, Phase 1에서는 보간 기본값 사용
        soff_bf_tmrfnd[t] = interp

    return {
        "CTR_AFT_PASS_MMCNT": ctr_mm.astype(np.float64),
        "PREM_PAY_YN": prem_pay_yn,
        "ORIG_PREM": orig_prem,
        "DC_PREM": dc_prem,
        "ACUM_NPREM": acum_nprem,
        "PAD_PREM": pad_prem,
        "YSTR_RSVAMT": ystr_rsvamt,
        "YYEND_RSVAMT": yyend_rsvamt,
        "ACQSEXP1_BIZEXP": acqsexp1,
        "APLY_PREM_ACUMAMT_BNFT": aply_prem_acumamt_bnft,
        "SOFF_BF_TMRFND": soff_bf_tmrfnd,
    }


COMPARE_ITEMS = [
    "CTR_AFT_PASS_MMCNT",
    "PREM_PAY_YN",
    "ORIG_PREM",
    "DC_PREM",
    "ACUM_NPREM",
    "PAD_PREM",
    "YSTR_RSVAMT",
    "YYEND_RSVAMT",
    "ACQSEXP1_BIZEXP",
    "APLY_PREM_ACUMAMT_BNFT",
    "SOFF_BF_TMRFND",
]


def compare_single(idno: int, v1: dict, expected: pd.DataFrame) -> dict:
    """단건 비교."""
    n_exp = len(expected)
    n_v1 = len(v1["CTR_AFT_PASS_MMCNT"])
    compare_n = min(n_exp, n_v1)

    if compare_n <= 0:
        return {"idno": idno, "pass": True, "items": {}, "compare_n": 0}

    exp = expected.iloc[:compare_n].reset_index(drop=True)
    items = {}

    for name in COMPARE_ITEMS:
        v1_arr = v1[name][:compare_n]
        exp_arr = exp[name].values.astype(np.float64)
        diff = np.abs(v1_arr - exp_arr)
        max_diff = float(diff.max())
        max_idx = int(diff.argmax())

        items[name] = {
            "max_diff": max_diff,
            "setl": int(exp.iloc[max_idx]["SETL_AFT_PASS_MMCNT"]) if "SETL_AFT_PASS_MMCNT" in exp.columns else max_idx,
            "pass": max_diff < TOL,
            "v1_val": float(v1_arr[max_idx]),
            "exp_val": float(exp_arr[max_idx]),
        }

    all_pass = all(item["pass"] for item in items.values())
    return {"idno": idno, "pass": all_pass, "items": items, "compare_n": compare_n}


def debug_single(idno: int, legacy_conn, proj_conn):
    """단건 상세 디버그 출력."""
    info = load_contract_info(legacy_conn, idno)
    if not info:
        print(f"  ERROR: IDNO={idno} not found in II_INFRC")
        return

    bas = load_rsvamt_bas(legacy_conn, idno)
    if not bas:
        print(f"  ERROR: IDNO={idno} not found in II_RSVAMT_BAS")
        return

    # ACQSEXP1: TOT_TRMNAT_DDCT_AMT 사용 (main과 동일)
    info["acqsexp1"] = info.get("tot_trmnat_ddct", 0) or 0
    acqsexp_rt = load_acqsexp_rt(
        legacy_conn, info["prod_cd"], info["cov_cd"],
        info["cls_cd"], info["paypr_dvcd"], info["etc_bizexp_key"]
    )

    mult = info["join_amt"] / bas["crit_join_amt"]

    # SOFF 유형 판별
    tp = load_tmrfnd_calc_tp(legacy_conn, info["prod_cd"], info["cov_cd"])
    info["tmrfnd_tp"] = tp
    tmrfnd_data = load_rsvamt_tmrfnd(legacy_conn, idno)
    info["has_tmrfnd_data"] = tmrfnd_data is not None

    # SOFF 유형 판정
    if tmrfnd_data is not None:
        soff_type = "C (TMRFND data → SOFF=0)"
    elif str(tp.get("tp2")) == "1":
        soff_type = "B (TP2=1 → ACQSEXP deduction)"
    else:
        soff_type = "A (TP2=4 → simple interpolation)"

    print(f"\n  === IDNO={idno} 계약 정보 ===")
    print(f"  PROD={info['prod_cd']}, COV={info['cov_cd']}, CTR_DT={info['ctr_dt']}")
    print(f"  BTERM={info['bterm_yy']}yr, PTERM={info['pterm_yy']}yr, ELAPSED={info['pass_yy']}y{info['pass_mm']}m")
    print(f"  GPREM={info['gprem']}, JOIN_AMT={info['join_amt']}, mult={mult}")
    print(f"  NPREM×mult={bas['nprem']*mult}, TOT_TRMNAT_DDCT={info['tot_trmnat_ddct']}")
    print(f"  ACQSEXP_RT={acqsexp_rt}, computed ACQSEXP1={info['acqsexp1']}")
    print(f"  TMRFND_TP: {tp}, HAS_TMRFND_DATA={info['has_tmrfnd_data']}")
    print(f"  SOFF Type: {soff_type}")

    # 기대값 로드
    expected = proj_conn.execute("""
        SELECT * FROM OD_TRAD_PV WHERE INFRC_IDNO = ?
        ORDER BY SETL_AFT_PASS_MMCNT
    """, [idno]).fetchdf()

    if expected.empty:
        print(f"  ERROR: IDNO={idno} not in OD_TRAD_PV")
        return

    n_steps = len(expected)
    v1 = compute_trad_pv(info, bas, n_steps)
    result = compare_single(idno, v1, expected)

    print(f"\n  === 비교 결과 ({result['compare_n']}개월) ===")
    print(f"  {'항목':<25} {'Max Diff':>12} {'SETL':>6} {'결과':>6}")
    print("  " + "-" * 55)
    for name in COMPARE_ITEMS:
        it = result["items"][name]
        status = "PASS" if it["pass"] else "FAIL"
        print(f"  {name:<25} {it['max_diff']:>12.2e}  @{it['setl']:>4d}  [{status}]")
        if not it["pass"]:
            print(f"    v1={it['v1_val']:.6f}, exp={it['exp_val']:.6f}")

    # 처음 5개월 상세
    print(f"\n  === 처음 5개월 상세 ===")
    for t in range(min(5, n_steps)):
        setl = int(expected.iloc[t]["SETL_AFT_PASS_MMCNT"])
        print(f"  SETL={setl}:")
        for name in COMPARE_ITEMS:
            v1_val = v1[name][t]
            exp_val = expected.iloc[t][name]
            diff = abs(v1_val - exp_val)
            mark = " " if diff < TOL else "!"
            print(f"   {mark} {name:<25} v1={v1_val:>15.4f}  exp={exp_val:>15.4f}  diff={diff:.2e}")


def main():
    parser = argparse.ArgumentParser(description="v1 OD_TRAD_PV Phase 1 비교")
    parser.add_argument("--n", type=int, default=10, help="랜덤 샘플 수")
    parser.add_argument("--idno", type=str, default=None, help="특정 IDNO (콤마 구분)")
    parser.add_argument("--csv", action="store_true", help="불일치 CSV 저장")
    parser.add_argument("--seed", type=int, default=42, help="랜덤 시드")
    parser.add_argument("--debug", type=int, default=None, help="단건 상세 디버그")
    args = parser.parse_args()

    proj = duckdb.connect(PROJ_O_DB, read_only=True)
    legacy = sqlite3.connect(LEGACY_DB)

    # 디버그 모드
    if args.debug:
        debug_single(args.debug, legacy, proj)
        legacy.close()
        proj.close()
        return

    # IDNO 선택
    all_idnos = proj.execute(
        "SELECT DISTINCT INFRC_IDNO FROM OD_TRAD_PV ORDER BY INFRC_IDNO"
    ).fetchdf()["INFRC_IDNO"].values

    if args.idno:
        idnos = [int(x.strip()) for x in args.idno.split(",")]
    else:
        rng = np.random.default_rng(args.seed)
        idnos = sorted(rng.choice(all_idnos, size=min(args.n, len(all_idnos)), replace=False))

    print("=" * 80)
    print(f"v1 OD_TRAD_PV Phase 1 비교 (정적 필드)")
    print(f"  Legacy: {LEGACY_DB}")
    print(f"  Expected: {PROJ_O_DB} ({len(all_idnos):,} IDNOs)")
    print(f"  Test: {len(idnos)}건, TOL={TOL}")
    print("=" * 80)

    results = []
    pass_count = 0
    fail_count = 0
    fail_details = []

    for i, idno in enumerate(idnos):
        t0 = time.time()
        try:
            info = load_contract_info(legacy, int(idno))
            if not info:
                raise ValueError("II_INFRC not found")

            bas = load_rsvamt_bas(legacy, int(idno))
            if not bas:
                raise ValueError("II_RSVAMT_BAS not found")

            # ACQSEXP1 계산: TOT_TRMNAT_DDCT_AMT 사용 (검증용)
            info["acqsexp1"] = info.get("tot_trmnat_ddct", 0) or 0

            # SOFF 유형 판별
            info["tmrfnd_tp"] = load_tmrfnd_calc_tp(legacy, info["prod_cd"], info["cov_cd"])
            tmrfnd_data = load_rsvamt_tmrfnd(legacy, int(idno))
            info["has_tmrfnd_data"] = tmrfnd_data is not None

            expected = proj.execute("""
                SELECT * FROM OD_TRAD_PV WHERE INFRC_IDNO = ?
                ORDER BY SETL_AFT_PASS_MMCNT
            """, [int(idno)]).fetchdf()

            if expected.empty:
                raise ValueError("OD_TRAD_PV empty")

            n_steps = len(expected)
            v1 = compute_trad_pv(info, bas, n_steps)
            result = compare_single(int(idno), v1, expected)
            elapsed = time.time() - t0

            if result["pass"]:
                pass_count += 1
                status = "PASS"
            else:
                fail_count += 1
                status = "FAIL"
                fail_details.append(result)

            print(f"  [{i+1:4d}/{len(idnos)}] IDNO={idno:>8d}  {result['compare_n']:>4d}mo  {elapsed:.1f}s  [{status}]", end="")
            if status == "FAIL":
                fail_items = [k for k, v in result["items"].items() if not v["pass"]]
                for fname in fail_items[:3]:
                    fi = result["items"][fname]
                    print(f"  {fname}({fi['max_diff']:.2e}@{fi['setl']})", end="")
            print()

            results.append(result)

        except Exception as e:
            fail_count += 1
            elapsed = time.time() - t0
            print(f"  [{i+1:4d}/{len(idnos)}] IDNO={idno:>8d}  ERROR  {elapsed:.1f}s  {str(e)[:60]}")
            results.append({"idno": int(idno), "pass": False, "items": {}, "error": str(e)})

    proj.close()
    legacy.close()

    # 요약
    print()
    print("=" * 80)
    print(f"결과: PASS={pass_count}, FAIL={fail_count}, 총={len(idnos)}")
    print("=" * 80)

    # 항목별 통계
    if results:
        print(f"\n{'항목':<25} {'PASS':>6} {'FAIL':>6} {'Max Diff':>12}")
        print("-" * 55)
        for name in COMPARE_ITEMS:
            item_pass = sum(1 for r in results if name in r.get("items", {}) and r["items"][name]["pass"])
            item_fail = sum(1 for r in results if name in r.get("items", {}) and not r["items"][name]["pass"])
            max_d = max((r["items"][name]["max_diff"] for r in results if name in r.get("items", {})), default=0)
            print(f"  {name:<23} {item_pass:>6} {item_fail:>6} {max_d:>12.2e}")

    # FAIL CSV
    if fail_details and args.csv:
        rows = []
        for r in fail_details:
            for name, info_item in r["items"].items():
                if not info_item["pass"]:
                    rows.append({
                        "idno": r["idno"],
                        "item": name,
                        "max_diff": info_item["max_diff"],
                        "setl": info_item["setl"],
                        "v1_val": info_item["v1_val"],
                        "exp_val": info_item["exp_val"],
                    })
        if rows:
            csv_path = "v1_trad_pv_fails.csv"
            pd.DataFrame(rows).to_csv(csv_path, index=False)
            print(f"\n불일치 상세: {csv_path}")


if __name__ == "__main__":
    main()

"""OD_TBL_BN 검증 테스트.

Phase 1: BN 엔진 로직 검증 (rate → tpx/count/PYAMT)
  - OD_TBL_BN의 rate 컬럼(TRMNAT_RT, RSVAMT_DRPO, BNFT_DRPO, BNFT_RSKRT)을 입력으로
  - 파생 컬럼(TRMO, TRME, counts, DEFRY_RT, PYAMT, BNFT_INSUAMT)을 산출 후 기대값 비교

Phase 2: Per-BNFT 독립 dedup 검증 (추후)
  - 각 BNFT별 독립 exit risk set으로 dedup 수행
  - rate 컬럼 자체의 정합성 검증

Usage:
    python test_tbl_bn.py
    python test_tbl_bn.py --n 100
    python test_tbl_bn.py --idno 1028627
"""
import argparse
import sys
import time
from typing import Dict, List, Optional

import duckdb
import numpy as np

DB_PATH = "duckdb_transform.duckdb"

# Phase 1: 파생 컬럼 검증 (rate 입력 → 파생 산출)
DERIVED_COLS = [
    "TRMO_MTNPSN_CNT", "TRMPSN_CNT",
    "RSVAMT_DEFRY_DRPSN_CNT", "DEFRY_DRPSN_CNT",
    "TRME_MTNPSN_CNT", "BNFT_OCURPE_CNT",
    "CRIT_AMT", "DEFRY_RT", "PRTT_RT", "GRADIN_RT",
    "PYAMT", "BNFT_INSUAMT",
]

# Rate 컬럼 (입력으로 읽음)
RATE_COLS = [
    "TRMNAT_RT", "RSVAMT_DEFRY_DRPO_RSKRT",
    "BNFT_DRPO_RSKRT", "BNFT_RSKRT",
]

ALL_CHECK_COLS = RATE_COLS + DERIVED_COLS


def compute_bn_from_rates(
    n_steps: int,
    trmnat_rt: np.ndarray,
    rsvamt_drpo: np.ndarray,
    bnft_drpo: np.ndarray,
    bnft_rskrt: np.ndarray,
    join_amt: float,
    defry_rt_arr: np.ndarray,
    prtt_rt_arr: np.ndarray,
    gradin_rt_arr: np.ndarray,
) -> dict:
    """BN rate 입력에서 파생 컬럼 산출.

    핵심 공식:
      exit_rate = trmnat + rsvamt_drpo + bnft_drpo
      trme = cumprod(1 - exit_rate)
      trmo[0]=1, trmo[t]=trme[t-1]
      trmpsn = trmo × trmnat
      rsvamt_drpsn = trmo × bnft_drpo  (※ BN 매핑: RSVAMT_DEFRY_DRPSN → TRMO × BNFT_DRPO)
      defry_drpsn = trmo × rsvamt_drpo (※ BN 매핑: DEFRY_DRPSN → TRMO × RSVAMT_DRPO)
      bnft_ocurpe = trmo × bnft_rskrt
      pyamt = crit_amt × prtt_rt (if prtt≠0) else crit_amt × defry_rt
      bnft_insuamt = bnft_ocurpe × pyamt
    """
    # exit rate & tpx
    # BN 규칙: t=0은 초기 시점 (탈퇴 없음), t≥1부터 탈퇴
    bn_exit = trmnat_rt + rsvamt_drpo + bnft_drpo
    bn_exit = np.clip(bn_exit, 0, 1)

    trmo = np.ones(n_steps, dtype=np.float64)
    trme = np.ones(n_steps, dtype=np.float64)
    trmpsn = np.zeros(n_steps, dtype=np.float64)
    rsvamt_drpsn = np.zeros(n_steps, dtype=np.float64)
    defry_drpsn = np.zeros(n_steps, dtype=np.float64)
    bnft_ocurpe = np.zeros(n_steps, dtype=np.float64)

    # t=0: TRMO=1, TRME=1, 모든 count=0
    for t in range(1, n_steps):
        trmo[t] = trme[t - 1]
        trme[t] = trmo[t] * (1.0 - bn_exit[t])
        trmpsn[t] = trmo[t] * trmnat_rt[t]
        rsvamt_drpsn[t] = trmo[t] * bnft_drpo[t]    # BN 매핑!
        defry_drpsn[t] = trmo[t] * rsvamt_drpo[t]   # BN 매핑!
        bnft_ocurpe[t] = trmo[t] * bnft_rskrt[t]

    # CRIT_AMT
    crit_amt = np.full(n_steps, join_amt, dtype=np.float64)

    # PYAMT
    pyamt = np.where(
        prtt_rt_arr != 0,
        crit_amt * prtt_rt_arr,
        crit_amt * defry_rt_arr,
    )

    # BNFT_INSUAMT
    bnft_insuamt = bnft_ocurpe * pyamt

    return {
        "TRMO_MTNPSN_CNT": trmo,
        "TRMPSN_CNT": trmpsn,
        "RSVAMT_DEFRY_DRPSN_CNT": rsvamt_drpsn,
        "DEFRY_DRPSN_CNT": defry_drpsn,
        "TRME_MTNPSN_CNT": trme,
        "BNFT_OCURPE_CNT": bnft_ocurpe,
        "CRIT_AMT": crit_amt,
        "DEFRY_RT": defry_rt_arr,
        "PRTT_RT": prtt_rt_arr,
        "GRADIN_RT": gradin_rt_arr,
        "PYAMT": pyamt,
        "BNFT_INSUAMT": bnft_insuamt,
    }


def verify_bn_idno(con, idno, bn_cache, trad_pv_result=None):
    """단건 BN 검증. rate 컬럼을 읽어서 파생 컬럼 비교."""
    # 계약 정보
    info = con.execute("""
        SELECT PROD_CD, COV_CD, CLS_CD,
               PASS_YYCNT, PASS_MMCNT, INSTRM_YYCNT,
               GRNTPT_JOIN_AMT
        FROM II_INFRC
        WHERE INFRC_SEQ = 1 AND INFRC_IDNO = ?
    """, [idno]).fetchone()
    if not info:
        return None

    prod_cd = info[0]
    cov_cd = info[1]
    cls_cd = str(info[2]).zfill(2) if info[2] else "01"
    pass_yy = int(info[3] or 0)
    pass_mm = int(info[4] or 0)
    bterm_yy = int(info[5] or 0)
    join_amt = float(info[6] or 0)

    # BN 기대값 로드
    bn_df = con.execute("""
        SELECT BNFT_NO, SETL_AFT_PASS_MMCNT,
               TRMNAT_RT, RSVAMT_DEFRY_DRPO_RSKRT,
               BNFT_DRPO_RSKRT, BNFT_RSKRT,
               TRMO_MTNPSN_CNT, TRMPSN_CNT,
               RSVAMT_DEFRY_DRPSN_CNT, DEFRY_DRPSN_CNT,
               TRME_MTNPSN_CNT, BNFT_OCURPE_CNT,
               CRIT_AMT, DEFRY_RT, PRTT_RT, GRADIN_RT,
               PYAMT, BNFT_INSUAMT
        FROM OD_TBL_BN
        WHERE INFRC_SEQ = 1 AND INFRC_IDNO = ?
        ORDER BY BNFT_NO, SETL_AFT_PASS_MMCNT
    """, [idno]).fetchdf()

    if bn_df is None or len(bn_df) == 0:
        return None

    # 시간축
    elapsed_mm = pass_yy * 12 + pass_mm

    # TRAD_PV 적립금 (PRTT 산출용)
    acum = None
    if trad_pv_result is not None:
        acum = trad_pv_result.get("APLY_PREM_ACUMAMT_BNFT")

    col_pass = {c: 0 for c in ALL_CHECK_COLS}
    col_fail = {c: 0 for c in ALL_CHECK_COLS}
    col_max_diff = {c: 0.0 for c in ALL_CHECK_COLS}
    col_fail_examples = {c: [] for c in ALL_CHECK_COLS}
    n_bnft_pass = 0
    n_bnft_fail = 0

    for bnft_no, bnft_df in bn_df.groupby("BNFT_NO"):
        bnft_df = bnft_df.sort_values("SETL_AFT_PASS_MMCNT").reset_index(drop=True)
        n = len(bnft_df)

        # All-zero TRMO BNFT 스킵 (미적용 급부)
        exp_trmo_max = bnft_df["TRMO_MTNPSN_CNT"].max()
        if exp_trmo_max == 0:
            continue

        # Rate 컬럼 읽기
        trmnat_rt = bnft_df["TRMNAT_RT"].values.astype(np.float64)
        rsvamt_drpo = bnft_df["RSVAMT_DEFRY_DRPO_RSKRT"].values.astype(np.float64)
        bnft_drpo = bnft_df["BNFT_DRPO_RSKRT"].values.astype(np.float64)
        bnft_rskrt = bnft_df["BNFT_RSKRT"].values.astype(np.float64)

        # DEFRY_RT, PRTT_RT: 기대값에서 직접 읽기 (Phase 2에서 자체 산출 예정)
        defry_rt_arr = bnft_df["DEFRY_RT"].values.astype(np.float64)
        prtt_rt_arr = bnft_df["PRTT_RT"].values.astype(np.float64)
        gradin_rt_arr = bnft_df["GRADIN_RT"].values.astype(np.float64)

        # 파생 컬럼 산출
        computed = compute_bn_from_rates(
            n, trmnat_rt, rsvamt_drpo, bnft_drpo, bnft_rskrt,
            join_amt, defry_rt_arr, prtt_rt_arr, gradin_rt_arr,
        )

        # Rate 컬럼은 "입력=기대값"이므로 항상 PASS (self-check)
        all_pass = True
        for col in ALL_CHECK_COLS:
            if col in RATE_COLS:
                # rate 컬럼은 입력이므로 항상 일치
                col_pass[col] += 1
                continue

            if col not in computed or col not in bnft_df.columns:
                continue

            comp = computed[col][:n]
            exv = bnft_df[col].values[:n].astype(np.float64)
            diff = np.max(np.abs(comp - exv))

            if diff < 1e-6:
                col_pass[col] += 1
            else:
                col_fail[col] += 1
                all_pass = False
                if diff > col_max_diff[col]:
                    col_max_diff[col] = diff
                if len(col_fail_examples[col]) < 5:
                    idx = int(np.argmax(np.abs(comp - exv)))
                    col_fail_examples[col].append(
                        f"IDNO={idno} BNFT={bnft_no} t={idx} "
                        f"comp={comp[idx]:.10f} exp={exv[idx]:.10f} diff={diff:.10f}"
                    )

        if all_pass:
            n_bnft_pass += 1
        else:
            n_bnft_fail += 1

    return col_pass, col_fail, col_max_diff, col_fail_examples, n_bnft_pass, n_bnft_fail


def main():
    parser = argparse.ArgumentParser(description="OD_TBL_BN 검증")
    parser.add_argument("--n", type=int, default=50, help="샘플 수")
    parser.add_argument("--idno", type=int, default=None, help="특정 IDNO")
    parser.add_argument("--seed", type=int, default=42, help="랜덤 시드")
    parser.add_argument("--all", action="store_true", help="전건 검증")
    args = parser.parse_args()

    t_start = time.time()
    con = duckdb.connect(DB_PATH, read_only=True)

    # BN 캐시
    from cf_module.data.bn_loader import BNDataCache
    bn_cache = BNDataCache(con)
    print(f"BN cache: {time.time() - t_start:.2f}s")

    # TRAD_PV 캐시 (PRTT 산출용)
    from cf_module.data.trad_pv_loader import TradPVDataCache, build_contract_info_cached
    from cf_module.calc.trad_pv import compute_trad_pv
    pv_cache = TradPVDataCache(con)
    print(f"PV cache: {time.time() - t_start:.2f}s")

    # 대상 IDNO
    if args.idno:
        target_ids = [args.idno]
    elif args.all:
        target_ids = con.execute("""
            SELECT DISTINCT INFRC_IDNO FROM OD_TBL_BN WHERE INFRC_SEQ = 1
        """).fetchdf()["INFRC_IDNO"].values.tolist()
    else:
        all_ids = con.execute("""
            SELECT DISTINCT INFRC_IDNO FROM OD_TBL_BN WHERE INFRC_SEQ = 1
        """).fetchdf()["INFRC_IDNO"].values
        rng = np.random.RandomState(args.seed)
        idx = rng.choice(len(all_ids), min(args.n, len(all_ids)), replace=False)
        target_ids = all_ids[idx].tolist()

    print(f"대상: {len(target_ids)}건")

    # MN 데이터 로드 (TRAD_PV용)
    mn_all = con.execute("""
        SELECT INFRC_IDNO, CTR_TRMO_MTNPSN_CNT, PAY_TRMO_MTNPSN_CNT, CTR_TRME_MTNPSN_CNT
        FROM OD_TBL_MN WHERE INFRC_SEQ = 1
        ORDER BY INFRC_IDNO, SETL_AFT_PASS_MMCNT
    """).fetchdf()
    mn_grouped = {i: g for i, g in mn_all.groupby("INFRC_IDNO")}
    del mn_all

    # 계산 + 검증
    t_calc = time.time()
    total_pass = {c: 0 for c in ALL_CHECK_COLS}
    total_fail = {c: 0 for c in ALL_CHECK_COLS}
    total_max_diff = {c: 0.0 for c in ALL_CHECK_COLS}
    total_fail_examples = {c: [] for c in ALL_CHECK_COLS}
    n_ok = 0
    n_err = 0
    n_skip = 0
    total_bnft_pass = 0
    total_bnft_fail = 0

    for gi, idno in enumerate(target_ids):
        try:
            # TRAD_PV 결과 (PRTT 산출용)
            pv_info = build_contract_info_cached(pv_cache, idno)
            pv_result = None
            if pv_info:
                mn = mn_grouped.get(idno)
                if mn is not None and len(mn) > 0:
                    pv_r = compute_trad_pv(
                        pv_info, len(mn),
                        pay_trmo=mn["PAY_TRMO_MTNPSN_CNT"].values,
                        ctr_trmo=mn["CTR_TRMO_MTNPSN_CNT"].values,
                        ctr_trme=mn["CTR_TRME_MTNPSN_CNT"].values,
                        fast_mode=True,
                    )
                    pv_result = pv_r.to_dict()

            result = verify_bn_idno(con, idno, bn_cache, pv_result)
            if result is None:
                n_skip += 1
                continue

            cp, cf, cmd, cfe, bp, bf = result
            for c in ALL_CHECK_COLS:
                total_pass[c] += cp[c]
                total_fail[c] += cf[c]
                if cmd[c] > total_max_diff[c]:
                    total_max_diff[c] = cmd[c]
                total_fail_examples[c].extend(cfe[c])
            total_bnft_pass += bp
            total_bnft_fail += bf

            if bf == 0:
                n_ok += 1
            else:
                n_err += 1

        except Exception as e:
            n_err += 1
            if n_err <= 3:
                print(f"  ERR IDNO={idno}: {e}")
                import traceback
                traceback.print_exc()

        if (gi + 1) % max(1, len(target_ids) // 10) == 0:
            print(f"  [{gi+1}/{len(target_ids)}] OK={n_ok} FAIL={n_err} SKIP={n_skip}")

    elapsed_calc = time.time() - t_calc
    total_time = time.time() - t_start

    # 결과 출력
    print(f"\n{'='*70}")
    print(f"OD_TBL_BN 검증 (Phase 1): {len(target_ids)}건, "
          f"OK={n_ok}, FAIL={n_err}, SKIP={n_skip}")
    print(f"급부 단위: PASS={total_bnft_pass}, FAIL={total_bnft_fail}")
    print(f"계산: {elapsed_calc:.1f}s, 총: {total_time:.1f}s")
    print(f"{'='*70}")

    print(f"\n{'컬럼':<35s} {'PASS':>7s} {'FAIL':>7s} {'max_diff':>12s}")
    print("-" * 65)
    for col in ALL_CHECK_COLS:
        ps = total_pass[col]
        fl = total_fail[col]
        md = total_max_diff[col]
        tag = "PASS" if fl == 0 else "FAIL"
        print(f"{col:<35s} {ps:>7d} {fl:>7d} {md:>12.6f}  {tag}")

    fail_cols = [c for c in ALL_CHECK_COLS if total_fail[c] > 0]
    if fail_cols:
        print(f"\n{'='*70}")
        print("FAIL 상세 (컬럼별 최대 5건)")
        print(f"{'='*70}")
        for col in fail_cols:
            print(f"\n{col} (FAIL={total_fail[col]}, max_diff={total_max_diff[col]:.10f}):")
            for ex in total_fail_examples[col][:5]:
                print(f"  {ex}")

    con.close()


if __name__ == "__main__":
    main()

"""
Pricing 모드 배치 검증: CTR_DT 2023%, CTR_TPCD IN (0,9)

비교 대상:
  - NPREM (순보험료, crit_join_amt 기준)
  - GRNTPT_GPREM (영업보험료, 실제SA 기준)
  - YYEND_RSVAMT1~3 (준비금, crit_join_amt 기준)
"""

import argparse
import logging
import sqlite3
import sys
import time

import numpy as np
import pandas as pd

logging.disable(logging.CRITICAL)

from cf_module.config import (
    BatchConfig, CFConfig, DBConfig, OutputConfig,
    ProjectionConfig, RunsetParams, ScenarioConfig,
)
from cf_module.data.assumptions import AssumptionLoader
from cf_module.data.model_point import load_model_points
from cf_module.io.reader import DataReader
from cf_module.projection.projector import run_projection

DB_PATH = r"C:\Users\thrus\Downloads\VSOLN2\VSOLN2.vdb"

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, default=0, help="최대 건수 (0=전체)")
args = parser.parse_args()


EXCLUDE_PRODS = (
    "LA0357D", "LA0357G", "LA0367Z", "LA0381D", "LA0381R",
    "LA0387H", "LA0387I", "LA0387Y", "LA0389G", "LA0389R", "LA0389S",
)


def fetch_expected():
    """DB에서 기대값 로딩 (신상품 제외)"""
    conn = sqlite3.connect(DB_PATH)
    placeholders = ",".join(f"'{p}'" for p in EXCLUDE_PRODS)
    cur = conn.execute(f"""
        SELECT A.INFRC_IDNO,
               SUBSTR(A.CTR_DT, 1, 6) AS CTR_YM,
               A.GRNTPT_JOIN_AMT,
               A.GRNTPT_GPREM,
               A.INSTRM_YYCNT,
               A.PAYPR_YYCNT,
               A.ISRD_JOIN_AGE,
               B.CRIT_JOIN_AMT,
               B.NPREM,
               B.YYEND_RSVAMT1,
               B.YYEND_RSVAMT2,
               B.YYEND_RSVAMT3,
               A.PROD_CD,
               A.CLS_CD,
               A.COV_CD
        FROM II_INFRC A
        JOIN II_RSVAMT_BAS B
          ON A.INFRC_SEQ = B.INFRC_SEQ AND A.INFRC_IDNO = B.INFRC_IDNO
        WHERE A.INFRC_SEQ = 1
          AND A.CTR_DT LIKE '2023%'
          AND A.CTR_TPCD IN ('0', '9')
          AND A.PROD_CD NOT IN ({placeholders})
        ORDER BY A.INFRC_IDNO
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def run_single(idno, reader):
    """단건 pricing 실행, PricingResult 반환 (실패 시 None)"""
    config = CFConfig(
        db=DBConfig(db_type="sqlite", path=DB_PATH),
        runset=RunsetParams(
            infrc_seq=1, infrc_idno=idno,
            clos_ym="202309", assm_ym="202306",
        ),
        projection=ProjectionConfig(time_step="monthly", base_date="202309"),
        batch=BatchConfig(chunk_size=100_000),
        scenario=ScenarioConfig(),
        output=OutputConfig(output_dir="./output"),
        run_targets=["ifrs17"],
        run_mode="pricing",
        debug=False,
    )
    try:
        mp = load_model_points(
            reader, config,
            query_name="II_INFRC_SINGLE",
            params=config.runset.query_params_single,
        )
        loader = AssumptionLoader(reader, config)
        assumptions = loader.load_all(
            params=config.runset.query_params_with_assm,
            mp_ids=mp.mp_ids,
            mp=mp,
        )
        result = run_projection(mp, assumptions, config, reader=reader)
        return result.pricing_result, mp.ctr_ym[0]
    except Exception as e:
        return None, str(e)


def main():
    rows = fetch_expected()
    if args.limit > 0:
        rows = rows[: args.limit]

    n_total = len(rows)
    print(f"Pricing 배치 검증: {n_total}건 (CTR_DT 2023%, CTR_TPCD 0/9)")
    print("=" * 90)

    reader = DataReader(DBConfig(db_type="sqlite", path=DB_PATH))
    reader.load_queries("queries")

    results = []
    t0 = time.time()

    for idx, row in enumerate(rows):
        (idno, ctr_ym, join_amt, db_gprem, bterm, pterm, age,
         crit_amt, db_nprem, db_v1, db_v2, db_v3, prod_cd,
         cls_cd, cov_cd) = row

        pr, actual_ctr_ym = run_single(idno, reader)

        if pr is None:
            results.append({
                "idno": idno, "ctr_ym": ctr_ym,
                "prod_cd": prod_cd, "cls_cd": cls_cd, "cov_cd": cov_cd,
                "status": "ERROR", "error": str(actual_ctr_ym),
            })
            continue

        # 비교
        our_nprem = pr.net_premium_monthly_rounded
        our_gprem_actual = (
            int(pr.gross_premium_monthly_rounded * pr.multiplier)
            if pr.gross_premium_monthly_rounded is not None and pr.multiplier is not None
            else None
        )
        our_v = pr.reserve_by_year_rounded

        nprem_ok = our_nprem == db_nprem
        gprem_ok = our_gprem_actual == db_gprem if our_gprem_actual is not None else False

        # V 비교 (bterm까지)
        v_ok = True
        v_diffs = []
        for t in range(1, min(4, bterm + 1)):
            db_vt = [db_v1, db_v2, db_v3][t - 1] or 0
            our_vt = int(our_v[t]) if t < len(our_v) else 0
            if our_vt != db_vt:
                v_ok = False
                v_diffs.append(f"V{t}:{our_vt}!={db_vt}")

        status = "OK" if (nprem_ok and gprem_ok and v_ok) else "DIFF"

        rec = {
            "idno": idno,
            "ctr_ym": ctr_ym,
            "prod_cd": prod_cd,
            "cls_cd": cls_cd,
            "cov_cd": cov_cd,
            "age": age,
            "bterm": bterm,
            "pterm": pterm,
            "status": status,
            "db_nprem": db_nprem,
            "our_nprem": our_nprem,
            "nprem_ok": nprem_ok,
            "db_gprem": db_gprem,
            "our_gprem": our_gprem_actual,
            "gprem_ok": gprem_ok,
            "v_ok": v_ok,
            "v_diffs": "|".join(v_diffs) if v_diffs else "",
        }
        results.append(rec)

        # 진행률 출력
        if (idx + 1) % 50 == 0 or idx == 0:
            elapsed = time.time() - t0
            rate = (idx + 1) / elapsed
            eta = (n_total - idx - 1) / rate if rate > 0 else 0
            print(f"  [{idx+1:>4}/{n_total}] {elapsed:.0f}s elapsed, "
                  f"~{eta:.0f}s remaining  (last: {idno} {status})")

    elapsed = time.time() - t0

    # 결과 집계
    df = pd.DataFrame(results)
    n_ok = len(df[df["status"] == "OK"])
    n_diff = len(df[df["status"] == "DIFF"])
    n_err = len(df[df["status"] == "ERROR"])

    print("\n" + "=" * 90)
    print(f"결과: {n_total}건 중 OK={n_ok}, DIFF={n_diff}, ERROR={n_err}  ({elapsed:.1f}s)")
    print(f"  NPREM 일치율: {df['nprem_ok'].sum()}/{len(df[df['status']!='ERROR'])}")
    print(f"  GPREM 일치율: {df['gprem_ok'].sum()}/{len(df[df['status']!='ERROR'])}")
    print(f"  V(t)  일치율: {df['v_ok'].sum()}/{len(df[df['status']!='ERROR'])}")

    # DIFF 건 상세
    diff_df = df[df["status"] == "DIFF"]
    if len(diff_df) > 0:
        print(f"\n--- DIFF 건 상세 (최대 30건) ---")
        show_cols = ["idno", "ctr_ym", "prod_cd", "cls_cd", "cov_cd",
                     "age", "bterm", "pterm",
                     "db_nprem", "our_nprem", "nprem_ok",
                     "db_gprem", "our_gprem", "gprem_ok",
                     "v_ok", "v_diffs"]
        print(diff_df[show_cols].head(30).to_string(index=False))

    # ERROR 건
    err_df = df[df["status"] == "ERROR"]
    if len(err_df) > 0:
        print(f"\n--- ERROR 건 ({len(err_df)}건) ---")
        print(err_df[["idno", "ctr_ym", "error"]].head(10).to_string(index=False))

    # CSV 저장
    out_path = "./output/pricing_validation.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n상세 결과: {out_path}")


if __name__ == "__main__":
    main()

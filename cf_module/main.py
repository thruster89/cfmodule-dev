"""
CF Module 실행 진입점

전체 파이프라인을 실행한다:
1. 설정 로딩
2. 데이터 로딩 (MP + 가정)
3. 프로젝션 실행 (배치)
4. 산출물 생성 (IFRS17 / K-ICS / Pricing - 선택 가능)
5. 결과 저장

사용법:
    # 샘플 데이터로 전체 실행
    python -m cf_module.main

    # IFRS17만 월별 실행
    python -m cf_module.main --target ifrs17

    # K-ICS + Pricing, 연별 실행
    python -m cf_module.main --target kics pricing --time-step yearly

    # 실제 DB 연결
    python -m cf_module.main --db-path "C:/path/to/db.vdb" --target ifrs17
"""

import argparse
import time

import numpy as np
import pandas as pd

from cf_module.config import (
    VALID_RUN_MODES,
    VALID_TARGETS,
    BatchConfig,
    CFConfig,
    RunsetParams,
    DBConfig,
    OutputConfig,
    ProjectionConfig,
    ScenarioConfig,
)
from cf_module.data.assumptions import AssumptionLoader, AssumptionSet
from cf_module.data.model_point import ModelPointSet, load_model_points
from cf_module.io.reader import DataReader
from cf_module.io.writer import DataWriter
from cf_module.output.ifrs17 import calc_ifrs17, ifrs17_to_df
from cf_module.output.kics import calc_kics, kics_to_df
from cf_module.output.pricing import calc_pricing, pricing_to_df
from cf_module.projection.batch import run_batch_projection
from cf_module.projection.projector import (
    result_to_summary_df,
    run_projection,
)
from cf_module.utils.logger import get_logger

logger = get_logger("main")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """커맨드라인 인자를 파싱한다."""
    parser = argparse.ArgumentParser(
        prog="cf_module",
        description="CF Module - 보험 Cash Flow 프로젝션 엔진",
    )

    parser.add_argument(
        "--target",
        nargs="+",
        choices=sorted(VALID_TARGETS),
        default=None,
        help="실행할 산출물 (기본: 전부). 예: --target ifrs17 kics",
    )
    parser.add_argument(
        "--time-step",
        choices=["monthly", "yearly"],
        default="monthly",
        help="시간축 단위 (기본: monthly)",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="DB 파일 경로 (미지정 시 샘플 데이터 사용)",
    )
    parser.add_argument(
        "--db-type",
        choices=["sqlite", "duckdb"],
        default="sqlite",
        help="DB 유형 (기본: sqlite)",
    )
    parser.add_argument(
        "--base-date",
        default="202309",
        help="결산기준일 yyyymm (기본: 202309)",
    )
    parser.add_argument(
        "--scenario",
        default="BASE",
        help="시나리오 ID (기본: BASE)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        help="배치 청크 크기 (기본: 10000)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="병렬 워커 수 (기본: 4)",
    )
    parser.add_argument(
        "--output-dir",
        default="./output",
        help="결과 출력 디렉토리 (기본: ./output)",
    )
    parser.add_argument(
        "--output-format",
        choices=["csv", "excel"],
        default="csv",
        help="출력 형식 (기본: csv)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        const=100,
        nargs="?",
        help="샘플 모드 실행. 건수 지정 가능 (기본: 100건). 예: --sample 500",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="디버그 모드: 단건 MP에 대해 7단계 중간테이블을 CSV로 출력",
    )
    parser.add_argument(
        "--run-mode",
        choices=sorted(VALID_RUN_MODES),
        default="valuation",
        help="실행 모드 (기본: valuation). pricing 모드는 예정기초(II_*/IP_*/IR_*)만 사용",
    )

    return parser.parse_args()


def config_from_args(args: argparse.Namespace) -> CFConfig:
    """CLI 인자로부터 CFConfig를 생성한다."""
    run_mode = getattr(args, "run_mode", "valuation")
    targets = args.target if args.target else ["ifrs17", "kics", "pricing"]

    # Pricing 모드: run_targets에 "pricing" 자동 포함
    if run_mode == "pricing" and "pricing" not in targets:
        targets.append("pricing")

    # Pricing 모드이고 --time-step 미지정 시 기본값 "yearly"
    time_step = args.time_step
    if run_mode == "pricing" and time_step == "monthly":
        # argparse default가 "monthly"이므로 사용자가 명시하지 않은 경우만 변경
        # sys.argv를 확인하여 사용자가 명시적으로 --time-step을 지정했는지 판별
        import sys
        if "--time-step" not in sys.argv:
            time_step = "yearly"

    return CFConfig(
        db=DBConfig(
            db_type=args.db_type,
            path=args.db_path or "",
        ),
        runset=RunsetParams(
            clos_ym=args.base_date,
        ),
        projection=ProjectionConfig(
            time_step=time_step,
            base_date=args.base_date,
        ),
        batch=BatchConfig(
            chunk_size=args.chunk_size,
            n_workers=args.workers,
        ),
        scenario=ScenarioConfig(
            scenario_id=args.scenario,
        ),
        output=OutputConfig(
            output_dir=args.output_dir,
            output_format=args.output_format,
        ),
        run_targets=targets,
        run_mode=run_mode,
        debug=getattr(args, "debug", False),
    )


# ---------------------------------------------------------------------------
# 샘플 데이터 생성
# ---------------------------------------------------------------------------

def create_sample_model_points(n: int = 100) -> ModelPointSet:
    """테스트용 샘플 Model Point를 생성한다."""
    rng = np.random.default_rng(42)

    df = pd.DataFrame({
        "mp_id": np.arange(1, n + 1),
        "product_cd": rng.choice(["PROD001", "PROD002", "PROD003"], n),
        "sex_cd": rng.choice(["M", "F"], n),
        "age_at_entry": rng.integers(20, 60, n),
        "ctr_ym": rng.choice([202001, 202101, 202201, 202301], n),
        "bterm": rng.choice([10, 15, 20, 30], n),
        "pterm": rng.choice([5, 10, 15, 20], n),
        "premium": rng.uniform(50_000, 500_000, n).round(0),
        "sum_assured": rng.uniform(10_000_000, 100_000_000, n).round(0),
        "deductible": rng.uniform(100_000, 2_000_000, n).round(0),
    })

    # pterm <= bterm 보정
    df["pterm"] = np.minimum(df["pterm"], df["bterm"])

    return ModelPointSet(
        df=df,
        n_points=n,
        mp_ids=df["mp_id"].to_numpy(),
        age_at_entry=df["age_at_entry"].to_numpy(dtype=np.int32),
        bterm=df["bterm"].to_numpy(dtype=np.int32),
        pterm=df["pterm"].to_numpy(dtype=np.int32),
        premium=df["premium"].to_numpy(dtype=np.float64),
        sum_assured=df["sum_assured"].to_numpy(dtype=np.float64),
        sex_cd=df["sex_cd"].to_numpy(),
        product_cd=df["product_cd"].to_numpy(),
        ctr_ym=df["ctr_ym"].to_numpy(),
        deductible=df["deductible"].to_numpy(dtype=np.float64),
    )


def create_sample_assumptions(n_points: int = 100) -> AssumptionSet:
    """테스트용 샘플 가정을 생성한다."""
    from cf_module.data.assumptions import (
        ExpenseTable,
        InterestRate,
        LapseTable,
        MortalityTable,
        ReserveTable,
        SkewTable,
    )

    # 샘플 준비금(V): 연도별 선형 증가 (1~120년)
    max_years = 120
    year_idx = np.arange(1, max_years + 1, dtype=np.float64)  # (120,)
    # 각 MP별 V = 기본금액 × 연도/보장기간 (선형 증가 패턴)
    base_v = np.linspace(1_000_000, 50_000_000, n_points)  # (n_points,)
    v_end = base_v[:, np.newaxis] * (year_idx[np.newaxis, :] / max_years)  # (n_points, 120)

    return AssumptionSet(
        mortality=MortalityTable(
            rsk_rt_cd=np.array([]),
            rsk_rt_nm=np.array([]),
            chr_cd=np.array([]),
            mm_trf_way_cd=np.array([]),
            div_keys=np.array([]),
        ),
        lapse=LapseTable(),
        expense=ExpenseTable(),
        interest=InterestRate(
            term_months=np.arange(1, 1201, dtype=np.float64),
            spot_rates=np.full(1200, 0.035),  # 3.5% 고정
            forward_rates=np.full(1200, 0.035),
        ),
        skew=SkewTable(),
        reserve=ReserveTable(v_end=v_end, max_years=max_years),
    )


# ---------------------------------------------------------------------------
# 파이프라인
# ---------------------------------------------------------------------------

def run_full_pipeline(
    config: CFConfig | None = None,
    mp: ModelPointSet | None = None,
    assumptions: AssumptionSet | None = None,
    use_sample: bool = False,
    sample_n: int = 100,
) -> dict:
    """전체 파이프라인을 실행한다.

    Args:
        config: 설정 (None이면 기본 설정 사용)
        mp: Model Point (None이면 DB 로딩 또는 샘플)
        assumptions: 가정 (None이면 DB 로딩 또는 샘플)
        use_sample: True이면 샘플 데이터 사용
        sample_n: 샘플 건수

    Returns:
        결과 dict (실행된 타겟에 해당하는 키만 포함)
    """
    start_time = time.time()

    # 1. 설정
    if config is None:
        config = CFConfig()
    config.validate()

    if config.debug:
        from cf_module.utils.logger import enable_debug
        enable_debug()

    targets = config.run_targets

    logger.info("=== CF Module 실행 시작 ===")
    logger.info(f"  실행모드  : {config.run_mode}")
    logger.info(f"  시나리오  : {config.scenario.scenario_id}")
    logger.info(f"  시간단위  : {config.projection.time_step}")
    logger.info(f"  실행대상  : {', '.join(targets)}")

    # 2. 데이터 로딩
    reader_instance = None
    if use_sample or (mp is None and not config.db.path):
        if mp is None:
            mp = create_sample_model_points(sample_n)
            logger.info(f"  샘플 MP  : {mp.n_points}건 생성")
        if assumptions is None:
            assumptions = create_sample_assumptions(n_points=mp.n_points)
            logger.info("  샘플 가정 : 생성 완료")
    else:
        reader_instance = DataReader(config.db)
        reader_instance.load_queries(config.queries_path)
        if mp is None:
            mp = load_model_points(
                reader_instance, config,
                params=config.runset.query_params,
            )
        if assumptions is None:
            loader = AssumptionLoader(reader_instance, config)
            assumptions = loader.load_all(
                params=config.runset.query_params_with_assm,
                mp_ids=mp.mp_ids,
                mp=mp,
            )

    # 3. 프로젝션 실행
    logger.info(f"프로젝션 실행: {mp.n_points}건")

    if mp.n_points <= config.batch.chunk_size:
        result = run_projection(mp, assumptions, config, reader=reader_instance)
        summary_df = result_to_summary_df(result)
    else:
        summary_df = run_batch_projection(mp, assumptions, config)
        result = run_projection(mp, assumptions, config, reader=reader_instance)

    # 4. 산출물 생성 (선택된 타겟만)
    writer = DataWriter(config.output)
    writer.write(summary_df, "cf_summary")

    output = {"summary": summary_df, "projection_result": result}

    if "ifrs17" in targets:
        logger.info("IFRS17 산출 중...")
        ifrs17_result = calc_ifrs17(result)
        ifrs17_df = ifrs17_to_df(ifrs17_result, mp.mp_ids)
        writer.write(ifrs17_df, "ifrs17_result")
        output["ifrs17"] = ifrs17_df
        output["ifrs17_result"] = ifrs17_result

    if "kics" in targets:
        logger.info("K-ICS 산출 중...")
        kics_result = calc_kics(result)
        kics_df = kics_to_df(kics_result, mp.mp_ids)
        writer.write(kics_df, "kics_result")
        output["kics"] = kics_df
        output["kics_result"] = kics_result

    if "pricing" in targets:
        logger.info("Pricing 산출 중...")
        pricing_result = calc_pricing(result)
        pricing_df = pricing_to_df(pricing_result, mp.mp_ids)
        writer.write(pricing_df, "pricing_result")
        output["pricing"] = pricing_df
        output["pricing_result"] = pricing_result

    # 5. 완료
    elapsed = time.time() - start_time
    logger.info(f"=== CF Module 실행 완료: {elapsed:.2f}초 ===")

    return output


# ---------------------------------------------------------------------------
# 엔트리포인트
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    config = config_from_args(args)

    use_sample = args.sample is not None or not args.db_path
    sample_n = args.sample if args.sample else 100

    # debug 모드: sample_n을 1로 강제 + DEBUG 로깅 활성화
    if config.debug:
        from cf_module.utils.logger import enable_debug
        enable_debug()
        sample_n = 1
        use_sample = True
        logger.info("debug 모드: sample_n=1, DEBUG 로깅 활성화")

    results = run_full_pipeline(
        config=config,
        use_sample=use_sample,
        sample_n=sample_n,
    )

    # 결과 출력
    print("\n=== CF 요약 ===")
    print(results["summary"].describe())

    for target in config.run_targets:
        if target in results:
            print(f"\n=== {target.upper()} ===")
            print(results[target].describe())

    if config.debug:
        import os
        debug_path = os.path.join(config.output.output_dir, "debug")
        logger.info(f"debug 중간테이블 CSV 출력 경로: {os.path.abspath(debug_path)}")

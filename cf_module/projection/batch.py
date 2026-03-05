"""
배치 처리 모듈

수십만 건 Model Point를 청크 단위로 분할하고,
병렬 처리(multiprocessing)로 프로젝션을 실행한다.
"""

import math
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable, List

import numpy as np
import pandas as pd

from cf_module.config import BatchConfig, CFConfig
from cf_module.data.assumptions import AssumptionSet
from cf_module.data.model_point import ModelPointSet
from cf_module.projection.projector import (
    ProjectionResult,
    result_to_summary_df,
    run_projection,
)
from cf_module.utils.logger import get_logger

logger = get_logger("batch")


def split_model_points(mp: ModelPointSet, chunk_size: int) -> List[ModelPointSet]:
    """Model Point를 청크 단위로 분할한다.

    Args:
        mp: 전체 Model Point
        chunk_size: 청크 크기

    Returns:
        ModelPointSet 리스트
    """
    n = mp.n_points
    if n <= chunk_size:
        return [mp]

    n_chunks = math.ceil(n / chunk_size)
    chunks = []

    for i in range(n_chunks):
        start = i * chunk_size
        end = min(start + chunk_size, n)

        chunk_df = mp.df.iloc[start:end].reset_index(drop=True)
        chunk = ModelPointSet(
            df=chunk_df,
            n_points=end - start,
            mp_ids=mp.mp_ids[start:end],
            age_at_entry=mp.age_at_entry[start:end],
            bterm=mp.bterm[start:end],
            pterm=mp.pterm[start:end],
            premium=mp.premium[start:end],
            sum_assured=mp.sum_assured[start:end],
            sex_cd=mp.sex_cd[start:end],
            product_cd=mp.product_cd[start:end],
            ctr_ym=mp.ctr_ym[start:end],
            deductible=mp.deductible[start:end],
            cls_cd=mp.cls_cd[start:end] if mp.cls_cd.size > 0 else np.array([]),
            cov_cd=mp.cov_cd[start:end] if mp.cov_cd.size > 0 else np.array([]),
            clos_ym=mp.clos_ym[start:end] if mp.clos_ym.size > 0 else np.array([]),
            assm_div_vals=mp.assm_div_vals[start:end] if mp.assm_div_vals is not None else None,
            rsk_rt_div_vals=mp.rsk_rt_div_vals[start:end] if mp.rsk_rt_div_vals is not None else None,
        )
        chunks.append(chunk)

    logger.info(f"MP 분할 완료: {n}건 → {n_chunks}청크 (각 {chunk_size}건)")
    return chunks


def _run_single_chunk(args: tuple) -> pd.DataFrame:
    """단일 청크 프로젝션 실행 (ProcessPoolExecutor용 직렬화 가능 함수)

    Args:
        args: (chunk_mp, assumptions, config) 튜플

    Returns:
        요약 DataFrame
    """
    chunk_mp, assumptions, config = args
    result = run_projection(chunk_mp, assumptions, config)
    return result_to_summary_df(result)


def run_batch_projection(
    mp: ModelPointSet,
    assumptions: AssumptionSet,
    config: CFConfig,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    """배치 프로젝션을 실행한다.

    Args:
        mp: 전체 Model Point
        assumptions: 가정 데이터
        config: 전역 설정
        progress_callback: 진행률 콜백 (completed, total)

    Returns:
        전체 요약 DataFrame (MP별 현가 등)
    """
    batch_cfg = config.batch
    chunks = split_model_points(mp, batch_cfg.chunk_size)
    n_chunks = len(chunks)

    logger.info(
        f"배치 프로젝션 시작: {mp.n_points}건, "
        f"{n_chunks}청크, workers={batch_cfg.n_workers}"
    )

    results: List[pd.DataFrame] = []

    if batch_cfg.use_multiprocessing and n_chunks > 1 and batch_cfg.n_workers > 1:
        # 병렬 처리
        with ProcessPoolExecutor(max_workers=batch_cfg.n_workers) as executor:
            futures = {
                executor.submit(_run_single_chunk, (chunk, assumptions, config)): idx
                for idx, chunk in enumerate(chunks)
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    df = future.result()
                    results.append(df)
                except Exception as e:
                    logger.error(f"청크 {idx} 프로젝션 실패: {e}")
                    raise

                if progress_callback:
                    progress_callback(len(results), n_chunks)

                logger.info(f"청크 완료: {len(results)}/{n_chunks}")
    else:
        # 순차 처리
        for idx, chunk in enumerate(chunks):
            result = run_projection(chunk, assumptions, config)
            df = result_to_summary_df(result)
            results.append(df)

            if progress_callback:
                progress_callback(idx + 1, n_chunks)

            logger.info(f"청크 완료: {idx + 1}/{n_chunks}")

    # 결과 병합
    merged = pd.concat(results, ignore_index=True)
    logger.info(f"배치 프로젝션 완료: {len(merged)}건")

    return merged


def run_batch_projection_detail(
    mp: ModelPointSet,
    assumptions: AssumptionSet,
    config: CFConfig,
) -> List[ProjectionResult]:
    """배치 프로젝션을 실행하고, 청크별 상세 결과를 반환한다.

    병렬 처리 없이 순차 실행 (상세 결과는 직렬화가 어려움).

    Returns:
        ProjectionResult 리스트 (청크별)
    """
    chunks = split_model_points(mp, config.batch.chunk_size)
    results = []

    for idx, chunk in enumerate(chunks):
        result = run_projection(chunk, assumptions, config)
        results.append(result)
        logger.info(f"상세 프로젝션 청크 완료: {idx + 1}/{len(chunks)}")

    return results

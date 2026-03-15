"""OD_DC_RT 할인율 산출.

IE_DC_RT 커브 기반 월별 할인계수 + 누적할인계수 산출.
"""
from dataclasses import dataclass
from typing import Dict

import numpy as np


@dataclass
class DCRTResult:
    """할인율 결과."""
    n_steps: int
    dc_rt: np.ndarray          # 월별 할인율 (IE_DC_RT 커브값)
    trmo_mm_dc_rt: np.ndarray  # 기시 할인계수 (lag 1 of trme)
    trme_mm_dc_rt: np.ndarray  # 기말 할인계수 cumprod(v)

    def to_dict(self) -> Dict[str, np.ndarray]:
        return {
            "DC_RT": self.dc_rt,
            "TRMO_MM_DC_RT": self.trmo_mm_dc_rt,
            "TRME_MM_DC_RT": self.trme_mm_dc_rt,
        }


def compute_dc_rt(
    n_steps: int,
    dc_rt_curve: np.ndarray,
) -> DCRTResult:
    """할인율 산출.

    Args:
        n_steps: 프로젝션 스텝 수
        dc_rt_curve: IE_DC_RT 커브 (0-indexed, PRD 1 = index 0)

    Returns:
        DCRTResult
    """
    dc_rt = np.zeros(n_steps, dtype=np.float64)
    v = np.ones(n_steps, dtype=np.float64)

    for s in range(1, n_steps):
        # step s → IE_DC_RT PRD_NO = s (1-based)
        idx = s - 1
        if idx < len(dc_rt_curve):
            rate = dc_rt_curve[idx]
        else:
            rate = dc_rt_curve[-1] if len(dc_rt_curve) > 0 else 0.0
        dc_rt[s] = rate
        v[s] = 1 / (1 + rate) ** (1 / 12) if rate > 0 else 1.0

    trme_mm = np.cumprod(v)

    # 기시 = lag(기말): TRMO[0]=1, TRMO[s]=TRME[s-1]
    trmo_mm = np.ones(n_steps, dtype=np.float64)
    trmo_mm[1:] = trme_mm[:-1]

    return DCRTResult(
        n_steps=n_steps,
        dc_rt=dc_rt,
        trmo_mm_dc_rt=trmo_mm,
        trme_mm_dc_rt=trme_mm,
    )

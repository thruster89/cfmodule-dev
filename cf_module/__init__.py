"""
CF Module - 보험 Cash Flow 프로젝션 엔진

생명보험/손해보험 범용 Cash Flow Module
IFRS17 (BEL/RA/CSM), K-ICS (지급여력), Pricing 등 다목적 지원
"""

from pathlib import Path

__version__ = (Path(__file__).resolve().parent.parent / "VERSION").read_text().strip()

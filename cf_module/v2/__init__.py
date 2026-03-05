"""
CF Module v2 — 그룹 기반 벡터 연산 엔진

1억 건 대응을 위한 아키텍처:
- DuckDB + Parquet 열 지향 저장
- assm_profile 기반 그룹 처리 (같은 가정 = 같은 그룹)
- numpy einsum 벡터 연산 (Python 루프 0개)
- 청크 단위 스트리밍 입출력
"""

"""
전역 설정 모듈

DB 경로, 결산년월, 시나리오, 시간축 단위 등 프로젝션에 필요한 모든 설정을 관리한다.
"""

from dataclasses import dataclass, field
from typing import List, Optional

# 지원하는 런셋(산출 목적) 목록
VALID_TARGETS = {"ifrs17", "kics", "pricing"}

# 지원하는 실행 모드
VALID_RUN_MODES = {"valuation", "pricing"}


@dataclass
class DBConfig:
    """데이터베이스 접속 설정"""
    db_type: str = "sqlite"  # "sqlite", "duckdb", "csv"
    path: str = ""
    # PostgreSQL/Oracle 접속 정보 (해당 시)
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    schema: str = "public"


@dataclass
class RunsetParams:
    """실행셋(Runset) 파라미터 — 계약 범위 + 가정 세트 설정"""
    exe_setup_id: tuple = ("sample1",)
    clos_ym: str = "202309"          # 결산년월 (yyyymm)
    assm_ym: str = "202306"          # 가정년월
    assm_grp_id: tuple = ("AGR06328",)
    infrc_seq: int = 1               # conn.py의 II_INFRC (계약 구분자)
    infrc_idno: int = 8833           # conn.py의 INFRC_IDNO (계약 ID 번호)
    idno_start: Optional[int] = None  # 범위 시작 (None이면 infrc_idno)
    idno_end: Optional[int] = None    # 범위 끝 (None이면 infrc_idno)

    @property
    def query_params(self) -> dict:
        """범위 쿼리용 (II_INFRC BETWEEN)"""
        return {
            "infrc_seq": self.infrc_seq,
            "idno_start": self.idno_start if self.idno_start is not None else self.infrc_idno,
            "idno_end": self.idno_end if self.idno_end is not None else self.infrc_idno,
        }

    @property
    def query_params_single(self) -> dict:
        """단건 조회용 (II_INFRC_SINGLE)"""
        return {"infrc_seq": self.infrc_seq, "infrc_idno": self.infrc_idno}

    @property
    def query_params_with_assm(self) -> dict:
        """가정년월 포함 (IR_RSKRT_CHR 등)"""
        return {**self.query_params, "assm_ym": self.assm_ym}


@dataclass
class ProjectionConfig:
    """프로젝션 설정"""
    time_step: str = "monthly"       # "monthly" | "yearly"
    max_proj_months: int = 1200      # 최대 프로젝션 개월수 (100년)
    base_date: str = "202309"        # 기준일 (yyyymm)

    @property
    def max_proj_years(self) -> int:
        return self.max_proj_months // 12


@dataclass
class BatchConfig:
    """배치 처리 설정"""
    chunk_size: int = 10_000         # MP 청크 크기
    n_workers: int = 4               # 병렬 워커 수
    use_multiprocessing: bool = True


@dataclass
class ScenarioConfig:
    """시나리오 설정 (금리 충격 등)"""
    scenario_id: str = "BASE"
    interest_rate_shock: float = 0.0  # 금리 충격 (bp)
    mortality_multiplier: float = 1.0  # 위험률 배수
    lapse_multiplier: float = 1.0      # 해약률 배수


@dataclass
class OutputConfig:
    """출력 설정"""
    output_dir: str = "./output"
    output_format: str = "csv"       # "csv", "excel", "db"
    # DB 출력 시 설정
    output_db: Optional[DBConfig] = None


@dataclass
class CFConfig:
    """CF Module 통합 설정"""
    db: DBConfig = field(default_factory=DBConfig)
    runset: RunsetParams = field(default_factory=RunsetParams)
    projection: ProjectionConfig = field(default_factory=ProjectionConfig)
    batch: BatchConfig = field(default_factory=BatchConfig)
    scenario: ScenarioConfig = field(default_factory=ScenarioConfig)
    output: OutputConfig = field(default_factory=OutputConfig)

    # 쿼리 정의 경로 (디렉토리 또는 .json 파일)
    queries_path: str = "queries"

    # 실행 대상 런셋 (기본: 전부)
    # "ifrs17", "kics", "pricing" 중 선택
    run_targets: List[str] = field(default_factory=lambda: ["ifrs17", "kics", "pricing"])

    # 실행 모드: "valuation" (기본) | "pricing" (예정기초만 사용)
    run_mode: str = "valuation"

    # 디버그 모드: 단건 MP 중간테이블 CSV 출력
    debug: bool = False

    @property
    def is_monthly(self) -> bool:
        return self.projection.time_step == "monthly"

    @property
    def is_pricing(self) -> bool:
        """Pricing 모드 여부"""
        return self.run_mode == "pricing"

    def validate(self) -> None:
        """설정값 유효성 검증"""
        if self.run_mode not in VALID_RUN_MODES:
            raise ValueError(
                f"잘못된 run_mode: {self.run_mode!r}. "
                f"허용값: {VALID_RUN_MODES}"
            )
        invalid = set(self.run_targets) - VALID_TARGETS
        if invalid:
            raise ValueError(
                f"잘못된 run_targets: {invalid}. "
                f"허용값: {VALID_TARGETS}"
            )

"""
가정(Assumption) 로딩 모듈

위험률(qx), 해약률(wx), 사업비율, 금리 등 보험 계리 가정 데이터를 로딩한다.
기존 코드의 복합키(^, |) 기반 조회 로직을 범용화한다.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from cf_module.config import CFConfig
from cf_module.io.reader import DataReader
from cf_module.utils.logger import get_logger

logger = get_logger("assumptions")


@dataclass
class MortalityTable:
    """위험률 테이블

    Attributes:
        rsk_rt_cd: 위험률 코드 배열
        rsk_rt_nm: 위험률 명칭 배열
        chr_cd: 특성코드 배열 ('A'=연령별, 'S'=고정)
        mm_trf_way_cd: 월변환방식 (1=연리→월리, 2=월리/12)
        div_keys: 분류키 배열 (n_risks, 10)
        rates: 위험률 값 dict {composite_key: rate_array}
        revi_ym: 적용 리비전 년월 배열
        raw_chr: IR_RSKRT_CHR 원시 DataFrame
        raw_val: IR_RSKRT_VAL 원시 numpy 배열

        -- 중복제거 관련 (n_risks,) --
        dead_rt_dvcd: DEAD_RT_DVCD (0=사망, 1=비사망) from IR_RSKRT_CHR
        rsk_grp_no: RSK_GRP_NO (동일위험그룹) from IP_R_RSKRT_C
        rsvamt_defry_yn: RSVAMT_DEFRY_DRPO_RSKRT_YN from IP_R_COV_RSKRT_C
        bnft_drpo_yn: BNFT_DRPO_RSKRT_YN from IP_R_BNFT_RSKRT_C
        pyexsp_drpo_yn: PYEXSP_DRPO_RSKRT_YN from IP_R_COV_RSKRT_C
    """
    rsk_rt_cd: np.ndarray
    rsk_rt_nm: np.ndarray
    chr_cd: np.ndarray
    mm_trf_way_cd: np.ndarray
    div_keys: np.ndarray
    rates: Dict[str, np.ndarray] = field(default_factory=dict)
    revi_ym: np.ndarray = field(default_factory=lambda: np.array([]))
    raw_chr: Optional[pd.DataFrame] = None
    raw_val: Optional[np.ndarray] = None
    dead_rt_dvcd: Optional[np.ndarray] = None
    rsk_grp_no: Optional[np.ndarray] = None
    rsvamt_defry_yn: Optional[np.ndarray] = None
    bnft_drpo_yn: Optional[np.ndarray] = None
    bnft_rskrt_yn: Optional[np.ndarray] = None       # (n_risks,) 급부위험률 여부
    pyexsp_drpo_yn: Optional[np.ndarray] = None


@dataclass
class LapseTable:
    """해약률 테이블

    Attributes:
        rates_pay: 납입기간 해약률 dict {composite_key: rate_array}
        rates_npay: 납입후 해약률 dict {composite_key: rate_array}
        driver_config: 가정 드라이버 설정
        raw_data: IA_T_TRMNAT_RT 원시 numpy 배열
        raw_keys: 복합키 배열
    """
    rates_pay: Dict[str, np.ndarray] = field(default_factory=dict)
    rates_npay: Dict[str, np.ndarray] = field(default_factory=dict)
    driver_config: np.ndarray = field(default_factory=lambda: np.array([]))
    raw_data: Optional[np.ndarray] = None
    raw_keys: Optional[np.ndarray] = None


@dataclass
class ExpenseTable:
    """사업비율 테이블

    Attributes:
        acq_rate: 신계약비율 dict {key: rate}
        maint_rate: 유지비율 dict {key: rate}
        collect_rate: 수금비율 dict {key: rate}
    """
    acq_rate: Dict[str, float] = field(default_factory=dict)
    maint_rate: Dict[str, float] = field(default_factory=dict)
    collect_rate: Dict[str, float] = field(default_factory=dict)


@dataclass
class InterestRate:
    """금리 커브

    Attributes:
        term_months: 기간(월) 배열
        spot_rates: 현물금리 배열
        forward_rates: 선도금리 배열
    """
    term_months: np.ndarray = field(default_factory=lambda: np.array([]))
    spot_rates: np.ndarray = field(default_factory=lambda: np.array([]))
    forward_rates: np.ndarray = field(default_factory=lambda: np.array([]))


@dataclass
class SkewTable:
    """왜도(Skew) 테이블

    Attributes:
        rates: 왜도 값 dict {composite_key: skew_array}
        raw_data: IA_T_SKEW 원시 numpy 배열
        raw_keys: 복합키 배열
    """
    rates: Dict[str, np.ndarray] = field(default_factory=dict)
    raw_data: Optional[np.ndarray] = None
    raw_keys: Optional[np.ndarray] = None


@dataclass
class ReserveTable:
    """DB에서 로딩한 준비금(V) 테이블

    Attributes:
        v_end: 연도별 기말 준비금 (n_points, max_years)
        max_years: 최대 연도수
        crit_join_amt: 기준가입금액 (n_points,) — II_RSVAMT_BAS.CRIT_JOIN_AMT
    """
    v_end: np.ndarray  # (n_points, max_years)
    max_years: int = 120
    crit_join_amt: Optional[np.ndarray] = None  # (n_points,)


@dataclass
class ExpectedInterestRate:
    """예정이율 (IP_P_EXPCT_INRT)

    3단계 예정이율 구조:
    - rates[0] = EXPCT_INRT1 (기본 이율)
    - rates[1] = EXPCT_INRT2 (2단계 이율, 0이면 미사용)
    - rates[2] = EXPCT_INRT3 (3단계 이율, 0이면 미사용)
    - change_years[0] = EXPCT_INRT_CHNG_YYCNT1 (1→2 전환 경과년수)
    - change_years[1] = EXPCT_INRT_CHNG_YYCNT2 (2→3 전환 경과년수)
    """
    rates: np.ndarray        # (3,) EXPCT_INRT1/2/3
    change_years: np.ndarray  # (2,) EXPCT_INRT_CHNG_YYCNT1/2

    def get_flat_rate(self) -> Optional[float]:
        """단일 이율이면 float 반환, 3단계이면 None"""
        if self.change_years[0] == 0 and self.change_years[1] == 0:
            return float(self.rates[0])
        return None

    def get_rate_at_year(self, year: int) -> float:
        """경과연수별 이율 반환"""
        cy1, cy2 = int(self.change_years[0]), int(self.change_years[1])
        if cy1 > 0 and year > cy1:
            if cy2 > 0 and year > cy2:
                return float(self.rates[2])
            return float(self.rates[1])
        return float(self.rates[0])


@dataclass
class ExpectedExpenseRate:
    """예정사업비율 (IP_P_EXPCT_BIZEXP_RT)

    초년도 신계약비:
    - fryy_gprem_acqs_rt: 영업보험료 대비 신계약비율
    - fryy_join_amt_acqs_rt: 가입금액 대비 신계약비율

    납입중 유지비:
    - inpay_gprem_mnt_rt: 영업보험료 대비 유지비율 (5개 구간)
    - inpay_join_amt_mnt_rt: 가입금액 대비 유지비율
    - inpay_fxamt_mntexp: 고정금액 유지비

    납입후 유지비:
    - afpay_gprem_mnt_rt: 영업보험료 대비 납입후 유지비율
    - afpay_join_amt_mnt_rt: 가입금액 대비 납입후 유지비율

    수금비:
    - inpay_gprem_colm_rt: 영업보험료 대비 수금비율

    손해조사비:
    - inpay_gprem_loss_svyexp_rt: 영업보험료 대비 손해조사비율
    """
    fryy_gprem_acqs_rt: float = 0.0
    fryy_join_amt_acqs_rt: float = 0.0
    inpay_gprem_mnt_rt: float = 0.0      # RT1 (기본 구간)
    inpay_join_amt_mnt_rt: float = 0.0
    inpay_fxamt_mntexp: float = 0.0
    afpay_gprem_mnt_rt: float = 0.0
    afpay_join_amt_mnt_rt: float = 0.0
    inpay_gprem_acqs_rt: float = 0.0    # 납입중 영보 대비 신계약비율
    inpay_gprem_colm_rt: float = 0.0
    inpay_gprem_loss_svyexp_rt: float = 0.0


@dataclass
class AssumptionSet:
    """전체 가정 데이터셋"""
    mortality: MortalityTable
    lapse: LapseTable
    expense: ExpenseTable
    interest: InterestRate
    skew: SkewTable
    reserve: ReserveTable = field(default_factory=lambda: ReserveTable(v_end=np.zeros((0, 120))))
    # 경과년도별 지급률: {composite_key: rate_array}
    beprd_defry_rt: Dict[str, np.ndarray] = field(default_factory=dict)
    # BEPRD 원시 데이터 (키 매칭용)
    beprd_raw_data: Optional[np.ndarray] = None
    beprd_raw_keys: Optional[np.ndarray] = None
    # Pricing 모드 전용
    expected_interest: Optional[ExpectedInterestRate] = None
    expected_expense: Optional[ExpectedExpenseRate] = None


class AssumptionLoader:
    """가정 데이터 로더

    기존 코드의 복합키 기반 가정 조회 로직을 통합 관리한다.
    """

    def __init__(self, reader: DataReader, config: CFConfig):
        self.reader = reader
        self.config = config

    def load_all(
        self,
        params: Optional[dict] = None,
        mp_ids: Optional[np.ndarray] = None,
        mp: Optional[Any] = None,
    ) -> AssumptionSet:
        """전체 가정 데이터를 로딩한다.

        Args:
            params: 쿼리 파라미터
            mp_ids: MP ID 배열 (준비금 정렬용)
            mp: ModelPointSet (DB 모드에서 가정 키 구축에 사용)
        """
        logger.info("가정 데이터 로딩 시작")
        logger.debug("[ASSM] params=%s, run_mode=%s", params, self.config.run_mode)

        # IR_* (예정기초 위험률) → 항상 로딩
        # Pricing 모드: CTR_YM 기준 REVI_YM 필터
        if self.config.is_pricing and mp is not None:
            pricing_params = {**params, "ctr_ym": str(mp.ctr_ym[0])}
            mortality = self._load_mortality(pricing_params, pricing=True)
        else:
            mortality = self._load_mortality(params)

        if self.config.is_pricing:
            # Pricing 모드: IA_*/IE_* 경험가정 미적용 — 빈 구조체 반환
            lapse = LapseTable()
            expense = ExpenseTable()
            interest = InterestRate()
            skew = SkewTable()
            beprd_rates, beprd_raw_data, beprd_raw_keys = {}, None, None
            logger.info("Pricing 모드: IA_*/IE_* 경험가정 미적용")
            # 예정기초율 로딩 (IP_P_EXPCT_*)
            expected_interest = self._load_expected_interest(params)
            expected_expense = self._load_expected_expense(params, mp)
            logger.info("Pricing 모드: 예정이율=%s, 예정사업비=%s",
                        expected_interest, "로딩완료" if expected_expense else "없음")
        else:
            lapse = self._load_lapse(params)
            expense = self._load_expense(params)
            interest = self._load_interest(params)
            skew = self._load_skew(params)
            beprd_rates, beprd_raw_data, beprd_raw_keys = self._load_beprd_defry_rt(params)

        # II_* (준비금) → 항상 로딩
        reserve = self._load_reserve(params, mp_ids)

        logger.info("가정 데이터 로딩 완료")

        result = AssumptionSet(
            mortality=mortality,
            lapse=lapse,
            expense=expense,
            interest=interest,
            skew=skew,
            reserve=reserve,
            beprd_defry_rt=beprd_rates,
            beprd_raw_data=beprd_raw_data,
            beprd_raw_keys=beprd_raw_keys,
        )

        if self.config.is_pricing:
            result.expected_interest = expected_interest
            result.expected_expense = expected_expense

        return result

    def _load_mortality(self, params: Optional[dict], pricing: bool = False) -> MortalityTable:
        """위험률 데이터를 로딩한다.

        IP_R_RSKRT_C (상품별 위험률 코드) → IR_RSKRT_CHR (위험률 특성)
        → IR_RSKRT_VAL (위험률 값)

        named params로 IR_RSKRT_CHR, IR_RSKRT_VAL 모두 동일한 dict 사용 가능.

        Args:
            params: 쿼리 파라미터
            pricing: True면 IR_RSKRT_CHR_PRICING (CTR_YM 기준 REVI_YM) 사용
        """
        logger.info("위험률 로딩")
        logger.debug("[IR] -- 위험률(mortality) 로딩 시작 --")

        # IR_RSKRT_CHR: named params (UNION ALL에서 :infrc_seq 등 재사용)
        chr_query = "IR_RSKRT_CHR_PRICING" if pricing else "IR_RSKRT_CHR"
        try:
            df_chr = self.reader.fetch_data(chr_query, params or {})
        except (KeyError, Exception):
            logger.warning("IR_RSKRT_CHR 쿼리 실패, 빈 위험률 테이블 반환")
            return MortalityTable(
                rsk_rt_cd=np.array([]),
                rsk_rt_nm=np.array([]),
                chr_cd=np.array([]),
                mm_trf_way_cd=np.array([]),
                div_keys=np.array([]),
            )

        # IR_RSKRT_VAL: 동일 named params (SQL이 필요한 키만 참조)
        try:
            df_val = self.reader.fetch_data("IR_RSKRT_VAL", params or {})
        except (KeyError, Exception):
            df_val = pd.DataFrame()

        logger.debug("[IR] IR_RSKRT_CHR: %d행 × %d컬럼", len(df_chr), len(df_chr.columns))
        logger.debug("[IR] IR_RSKRT_VAL: %d행 × %d컬럼", len(df_val), len(df_val.columns))

        # 위험률 특성 정보 추출
        rsk_rt_cd = df_chr["RSK_RT_CD"].to_numpy() if "RSK_RT_CD" in df_chr.columns else np.array([])
        rsk_rt_nm = df_chr["RSK_RT_NM"].to_numpy() if "RSK_RT_NM" in df_chr.columns else np.array([])
        chr_cd = df_chr["RSK_RT_CHR_CD"].to_numpy() if "RSK_RT_CHR_CD" in df_chr.columns else np.array([])
        mm_trf = df_chr["MM_TRF_WAY_CD"].to_numpy() if "MM_TRF_WAY_CD" in df_chr.columns else np.array([])
        revi_ym = df_chr["REVI_YM"].to_numpy() if "REVI_YM" in df_chr.columns else np.array([])

        # 분류키 (DIV_VAL 컬럼들)
        div_cols = [c for c in df_chr.columns if "DIV" in c.upper() and "VAL" in c.upper()]
        div_keys = df_chr[div_cols].to_numpy() if div_cols else np.array([])

        # 위험률 값을 복합키 dict로 변환
        rates = self._build_mortality_rates(df_chr, df_val)

        # 원시 데이터 보존
        raw_val = df_val.to_numpy() if not df_val.empty else None

        # DEAD_RT_DVCD from IR_RSKRT_CHR
        dead_rt_dvcd = df_chr["DEAD_RT_DVCD"].to_numpy().astype(int) if "DEAD_RT_DVCD" in df_chr.columns else None

        # 중복제거 메타: IP_R_RSKRT_C, IP_R_COV_RSKRT_C, IP_R_BNFT_RSKRT_C
        rsk_grp_no, rsvamt_defry_yn, bnft_drpo_yn, bnft_rskrt_yn, pyexsp_drpo_yn = self._load_dedup_meta(
            params, rsk_rt_cd
        )

        logger.debug("[IR] rates dict: %d keys", len(rates))
        # 위험률코드별 속성 요약 테이블
        logger.debug("[IR]")
        logger.debug("[IR] --- risk code summary (%d codes) ---", len(rsk_rt_cd))
        logger.debug("[IR]   %-8s  %-6s  %-5s  %-5s  %-7s  %-5s  %-7s  %-9s  %-9s",
                     "RSK_CD", "mm_trf", "DEAD", "GRP", "RSVAMT", "BNFT", "PYEXSP", "CTR_exit", "PAY_exit")
        for j in range(len(rsk_rt_cd)):
            cd = rsk_rt_cd[j]
            trf = int(mm_trf[j]) if mm_trf is not None and j < len(mm_trf) else 0
            dead = int(dead_rt_dvcd[j]) if dead_rt_dvcd is not None and j < len(dead_rt_dvcd) else -1
            grp = int(rsk_grp_no[j]) if rsk_grp_no is not None and j < len(rsk_grp_no) else 0
            rsv = int(rsvamt_defry_yn[j]) if rsvamt_defry_yn is not None and j < len(rsvamt_defry_yn) else 0
            bnft = int(bnft_drpo_yn[j]) if bnft_drpo_yn is not None and j < len(bnft_drpo_yn) else 0
            pye = int(pyexsp_drpo_yn[j]) if pyexsp_drpo_yn is not None and j < len(pyexsp_drpo_yn) else 0
            ctr_exit = "Y" if (rsv == 1 or bnft == 1) else "-"
            pay_exit = "Y" if (rsv == 1 or bnft == 1 or pye == 1) else "-"
            dead_label = "death" if dead == 0 else "non-d" if dead == 1 else "?"
            trf_label = "1-(1-q)^1/12" if trf == 1 else "q/12" if trf == 2 else "raw"
            logger.debug("[IR]   %-8s  %-12s  %-5s  %-5d  %-7d  %-5d  %-7d  %-9s  %-9s",
                         cd, trf_label, dead_label, grp, rsv, bnft, pye, ctr_exit, pay_exit)
        logger.debug("[IR] -- 위험률 로딩 완료 --")

        return MortalityTable(
            rsk_rt_cd=rsk_rt_cd,
            rsk_rt_nm=rsk_rt_nm,
            chr_cd=chr_cd,
            mm_trf_way_cd=mm_trf,
            div_keys=div_keys,
            rates=rates,
            revi_ym=revi_ym,
            raw_chr=df_chr,
            raw_val=raw_val,
            dead_rt_dvcd=dead_rt_dvcd,
            rsk_grp_no=rsk_grp_no,
            rsvamt_defry_yn=rsvamt_defry_yn,
            bnft_drpo_yn=bnft_drpo_yn,
            bnft_rskrt_yn=bnft_rskrt_yn,
            pyexsp_drpo_yn=pyexsp_drpo_yn,
        )

    def _load_dedup_meta(
        self, params: Optional[dict], rsk_rt_cd: np.ndarray
    ) -> tuple:
        """중복제거에 필요한 메타데이터를 로딩한다.

        IP_R_RSKRT_C → RSK_GRP_NO
        IP_R_COV_RSKRT_C → RSVAMT_DEFRY_DRPO_RSKRT_YN, PYEXSP_DRPO_RSKRT_YN
        IP_R_BNFT_RSKRT_C → BNFT_DRPO_RSKRT_YN, BNFT_RSKRT_YN

        Returns:
            (rsk_grp_no, rsvamt_defry_yn, bnft_drpo_yn, bnft_rskrt_yn, pyexsp_drpo_yn) — 각 (n_risks,)
        """
        n = len(rsk_rt_cd)
        rsk_grp_no = np.zeros(n, dtype=int)
        rsvamt_defry_yn = np.zeros(n, dtype=int)
        bnft_drpo_yn = np.zeros(n, dtype=int)
        bnft_rskrt_yn = np.zeros(n, dtype=int)
        pyexsp_drpo_yn = np.zeros(n, dtype=int)

        if params is None:
            return rsk_grp_no, rsvamt_defry_yn, bnft_drpo_yn, bnft_rskrt_yn, pyexsp_drpo_yn

        # IP_R_RSKRT_C → RSK_GRP_NO
        try:
            df_rskrt = self.reader.fetch_data("IP_R_RSKRT_C", params)
            if not df_rskrt.empty and "RSK_GRP_NO" in df_rskrt.columns:
                grp_map = dict(zip(
                    df_rskrt["RSK_RT_CD"].astype(str),
                    df_rskrt["RSK_GRP_NO"].astype(int),
                ))
                rsk_grp_no = np.array([grp_map.get(str(cd), 0) for cd in rsk_rt_cd])
            logger.debug("[IR] IP_R_RSKRT_C: %d행 → RSK_GRP_NO 매핑", len(df_rskrt))
        except Exception:
            logger.debug("[IR] IP_R_RSKRT_C 로딩 실패, RSK_GRP_NO=0 기본값")

        # IP_R_COV_RSKRT_C → RSVAMT_DEFRY_DRPO_RSKRT_YN + PYEXSP_DRPO_RSKRT_YN
        try:
            df_cov = self.reader.fetch_data("IP_R_COV_RSKRT_C", params)
            if not df_cov.empty:
                if "RSVAMT_DEFRY_DRPO_RSKRT_YN" in df_cov.columns:
                    cov_map = dict(zip(
                        df_cov["RSK_RT_CD"].astype(str),
                        df_cov["RSVAMT_DEFRY_DRPO_RSKRT_YN"].astype(int),
                    ))
                    rsvamt_defry_yn = np.array([cov_map.get(str(cd), 0) for cd in rsk_rt_cd])
                if "PYEXSP_DRPO_RSKRT_YN" in df_cov.columns:
                    pyexsp_map = dict(zip(
                        df_cov["RSK_RT_CD"].astype(str),
                        df_cov["PYEXSP_DRPO_RSKRT_YN"].astype(int),
                    ))
                    pyexsp_drpo_yn = np.array([pyexsp_map.get(str(cd), 0) for cd in rsk_rt_cd])
            logger.debug("[IR] IP_R_COV_RSKRT_C: %d행 → RSVAMT_DEFRY_YN, PYEXSP_DRPO_YN 매핑", len(df_cov))
        except Exception:
            logger.debug("[IR] IP_R_COV_RSKRT_C 로딩 실패, RSVAMT_DEFRY_YN/PYEXSP_DRPO_YN=0 기본값")

        # IP_R_BNFT_RSKRT_C → BNFT_DRPO_RSKRT_YN, BNFT_RSKRT_YN
        try:
            df_bnft = self.reader.fetch_data("IP_R_BNFT_RSKRT_C", params)
            if not df_bnft.empty:
                # 급부별 여러 행 가능 → RSK_RT_CD별 max 취함
                if "BNFT_DRPO_RSKRT_YN" in df_bnft.columns:
                    drpo_map = df_bnft.groupby(
                        df_bnft["RSK_RT_CD"].astype(str)
                    )["BNFT_DRPO_RSKRT_YN"].max().astype(int).to_dict()
                    bnft_drpo_yn = np.array([drpo_map.get(str(cd), 0) for cd in rsk_rt_cd])
                if "BNFT_RSKRT_YN" in df_bnft.columns:
                    rskrt_map = df_bnft.groupby(
                        df_bnft["RSK_RT_CD"].astype(str)
                    )["BNFT_RSKRT_YN"].max().astype(int).to_dict()
                    bnft_rskrt_yn = np.array([rskrt_map.get(str(cd), 0) for cd in rsk_rt_cd])
            logger.debug("[IR] IP_R_BNFT_RSKRT_C: %d행 → BNFT_DRPO_YN, BNFT_RSKRT_YN 매핑", len(df_bnft))
        except Exception:
            logger.debug("[IR] IP_R_BNFT_RSKRT_C 로딩 실패, BNFT_DRPO_YN/BNFT_RSKRT_YN=0 기본값")

        return rsk_grp_no, rsvamt_defry_yn, bnft_drpo_yn, bnft_rskrt_yn, pyexsp_drpo_yn

    def _build_mortality_rates(
        self, df_chr: pd.DataFrame, df_val: pd.DataFrame
    ) -> Dict[str, np.ndarray]:
        """위험률 값을 복합키 기반 dict로 구축한다.

        복합키 형식: RSK_RT_CD^REVI_YM^DIV_VAL1^...^DIV_VAL10[^AGE]
        """
        if df_val.empty:
            return {}

        rates: Dict[str, np.ndarray] = {}

        # 키 컬럼 식별
        key_cols = ["RSK_RT_CD", "REVI_YM"]
        div_val_cols = [c for c in df_val.columns if c.startswith("RSK_RT_DIV_VAL")]
        div_val_cols.sort()
        key_cols.extend(div_val_cols)

        has_age = "AGE" in df_val.columns
        has_pass_y = "PASS_YYCNT" in df_val.columns
        rate_col = "RSK_RT" if "RSK_RT" in df_val.columns else None

        if rate_col is None:
            return rates

        # 복합키 생성 및 rates dict 구축
        val_arr = df_val[key_cols].fillna("").astype(str).to_numpy()
        composite_keys = np.array(["^".join(row) for row in val_arr])

        if has_age:
            ages = df_val["AGE"].fillna("").astype(str).to_numpy()
            composite_keys = np.char.add(composite_keys, "^")
            composite_keys = np.char.add(composite_keys, ages)

        rate_values = df_val[rate_col].to_numpy(dtype=np.float64)

        for key, rate in zip(composite_keys, rate_values):
            rates[key] = rate

        return rates

    def _load_lapse(self, params: Optional[dict]) -> LapseTable:
        """해약률 데이터를 로딩한다."""
        logger.info("해약률 로딩")
        logger.debug("[ASSM-WX] -- 해약률(lapse) 로딩 시작 --")

        try:
            df_trmnat = self.reader.fetch_data("IA_T_TRMNAT_RT", {})
        except (KeyError, Exception):
            logger.warning("IA_T_TRMNAT_RT 쿼리 실패, 빈 해약률 테이블 반환")
            return LapseTable()

        try:
            df_driver = self.reader.fetch_data("IA_M_ASSM_DRIV", {})
        except (KeyError, Exception):
            df_driver = pd.DataFrame()

        rates_pay: Dict[str, np.ndarray] = {}
        rates_npay: Dict[str, np.ndarray] = {}
        raw_data_np = None
        raw_keys_np = None

        if not df_trmnat.empty:
            raw_data_np = df_trmnat.to_numpy()

            # 복합키 생성 (| 구분자)
            n_key_cols = min(18, len(df_trmnat.columns))
            key_arr = df_trmnat.iloc[:, :n_key_cols].fillna("").astype(str).to_numpy()
            raw_keys_np = np.array(["|".join(row) for row in key_arr])

            # 값 컬럼: 20번째부터 마지막-2까지
            if df_trmnat.shape[1] > 22:
                val_start = 20
                val_end = df_trmnat.shape[1] - 2
                rate_arr = df_trmnat.iloc[:, val_start:val_end].to_numpy(dtype=np.float64)

                for key, rate_row in zip(raw_keys_np, rate_arr):
                    rates_pay[key] = rate_row

        driver = df_driver.to_numpy() if not df_driver.empty else np.array([])

        logger.debug("[ASSM-WX] IA_T_TRMNAT_RT: %d행, raw_keys: %d개, rates_pay: %d개",
                     len(df_trmnat), len(raw_keys_np) if raw_keys_np is not None else 0, len(rates_pay))
        logger.debug("[ASSM-WX] -- 해약률 로딩 완료 --")

        return LapseTable(
            rates_pay=rates_pay,
            rates_npay=rates_npay,
            driver_config=driver,
            raw_data=raw_data_np,
            raw_keys=raw_keys_np,
        )

    def _load_expense(self, params: Optional[dict]) -> ExpenseTable:
        """사업비율 데이터를 로딩한다."""
        logger.info("사업비율 로딩")
        # 사업비율은 별도 가정 테이블에서 로딩
        # 현재는 빈 테이블 반환 (향후 확장)
        return ExpenseTable()

    def _load_interest(self, params: Optional[dict]) -> InterestRate:
        """금리 커브를 로딩한다."""
        logger.info("금리 로딩")
        # 금리 커브는 별도 테이블/파일에서 로딩
        # 현재는 빈 금리 반환 (향후 확장)
        return InterestRate()

    def _load_skew(self, params: Optional[dict]) -> SkewTable:
        """왜도 데이터를 로딩한다."""
        logger.info("왜도 로딩")
        logger.debug("[ASSM-SKEW] -- 스큐(skew) 로딩 시작 --")

        try:
            df_skew = self.reader.fetch_data("IA_T_SKEW", {})
        except (KeyError, Exception):
            logger.warning("IA_T_SKEW 쿼리 실패, 빈 왜도 테이블 반환")
            return SkewTable()

        rates: Dict[str, np.ndarray] = {}
        raw_data_np = None
        raw_keys_np = None

        if not df_skew.empty:
            raw_data_np = df_skew.to_numpy()

            n_key_cols = min(17, len(df_skew.columns))
            key_arr = df_skew.iloc[:, :n_key_cols].fillna("").astype(str).to_numpy()
            raw_keys_np = np.array(["|".join(row) for row in key_arr])

            # 왜도 값: 17~40번째 컬럼 (24개월분)
            if df_skew.shape[1] > 40:
                val_arr = df_skew.iloc[:, 17:41].to_numpy(dtype=np.float64)
                for key, val_row in zip(raw_keys_np, val_arr):
                    rates[key] = val_row

        logger.debug("[ASSM-SKEW] IA_T_SKEW: %d행, raw_keys: %d개, rates: %d개",
                     len(df_skew), len(raw_keys_np) if raw_keys_np is not None else 0, len(rates))
        logger.debug("[ASSM-SKEW] -- 스큐 로딩 완료 --")

        return SkewTable(rates=rates, raw_data=raw_data_np, raw_keys=raw_keys_np)

    def _load_reserve(self, params: Optional[dict], mp_ids: Optional[np.ndarray] = None) -> ReserveTable:
        """준비금(V) 테이블을 로딩한다.

        DB에서 vend_rsvamt1 ~ vend_rsvamt120 pivot 컬럼을 읽어
        (n_points, 120) 2D numpy 배열로 변환한다.
        """
        logger.info("준비금(V) 테이블 로딩")
        logger.debug("[II] -- 준비금(V) 로딩 시작 --")
        max_years = 120

        try:
            df = self.reader.fetch_data("II_RSVAMT_BAS", params or {})
        except (KeyError, Exception):
            logger.warning("II_RSVAMT_BAS 쿼리 실패, 빈 준비금 테이블 반환")
            n = len(mp_ids) if mp_ids is not None else 0
            return ReserveTable(v_end=np.zeros((n, max_years), dtype=np.float64), max_years=max_years)

        if df.empty:
            n = len(mp_ids) if mp_ids is not None else 0
            return ReserveTable(v_end=np.zeros((n, max_years), dtype=np.float64), max_years=max_years)

        # vend_rsvamt 컬럼 추출 (vend_rsvamt1 ~ vend_rsvamt120)
        v_cols = []
        for y in range(1, max_years + 1):
            col_name = f"vend_rsvamt{y}"
            # 대소문자 무관 매칭
            matched = [c for c in df.columns if c.lower() == col_name.lower()]
            if matched:
                v_cols.append(matched[0])
            else:
                v_cols.append(None)

        # 2D 배열 구축
        n_rows = len(df)
        v_end = np.zeros((n_rows, max_years), dtype=np.float64)
        for idx, col in enumerate(v_cols):
            if col is not None:
                v_end[:, idx] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).to_numpy()

        # mp_id 기준 정렬/매칭
        id_col = None
        for candidate in ["INFRC_IDNO", "mp_id", "MP_ID"]:
            if candidate in df.columns:
                id_col = candidate
                break

        # CRIT_JOIN_AMT 추출
        crit_col = None
        for candidate in ["CRIT_JOIN_AMT", "crit_join_amt"]:
            if candidate in df.columns:
                crit_col = candidate
                break
        crit_join_amt = None
        if crit_col is not None:
            crit_join_amt = pd.to_numeric(df[crit_col], errors="coerce").fillna(0.0).to_numpy()

        if mp_ids is not None and id_col is not None:
            df_ids = df[id_col].to_numpy()
            # mp_ids 순서에 맞게 재정렬
            id_to_idx = {mid: i for i, mid in enumerate(df_ids)}
            reordered = np.zeros((len(mp_ids), max_years), dtype=np.float64)
            reordered_crit = np.zeros(len(mp_ids), dtype=np.float64) if crit_join_amt is not None else None
            for i, mid in enumerate(mp_ids):
                if mid in id_to_idx:
                    reordered[i] = v_end[id_to_idx[mid]]
                    if reordered_crit is not None:
                        reordered_crit[i] = crit_join_amt[id_to_idx[mid]]
            v_end = reordered
            crit_join_amt = reordered_crit

        logger.debug("[II] v_end shape=%s, 첫행 sum=%.0f",
                     v_end.shape, float(np.sum(v_end[0])) if v_end.shape[0] > 0 else 0)
        if crit_join_amt is not None:
            logger.debug("[II] crit_join_amt: %s", crit_join_amt[:min(5, len(crit_join_amt))].tolist())
        logger.debug("[II] -- 준비금 로딩 완료 --")

        return ReserveTable(v_end=v_end, max_years=max_years, crit_join_amt=crit_join_amt)

    def _load_beprd_defry_rt(self, params: Optional[dict]):
        """경과년도별 지급률을 로딩한다.

        Returns:
            (rates_dict, raw_data, raw_keys)
        """
        logger.info("경과년도별 지급률 로딩")
        logger.debug("[ASSM-BEPRD] -- 경과년도별 지급률 로딩 시작 --")

        try:
            df = self.reader.fetch_data("IA_R_BEPRD_DEFRY_RT", {})
        except (KeyError, Exception):
            logger.warning("IA_R_BEPRD_DEFRY_RT 쿼리 실패, 빈 dict 반환")
            return {}, None, None

        rates: Dict[str, np.ndarray] = {}
        raw_data_np = None
        raw_keys_np = None

        if not df.empty:
            raw_data_np = df.to_numpy()

            n_key_cols = min(18, len(df.columns))
            key_arr = df.iloc[:, :n_key_cols].fillna("").astype(str).to_numpy()
            raw_keys_np = np.array(["|".join(row) for row in key_arr])

            if df.shape[1] > 20:
                val_start = 18
                val_end = df.shape[1] - 2
                val_arr = df.iloc[:, val_start:val_end].to_numpy(dtype=np.float64)
                for key, val_row in zip(raw_keys_np, val_arr):
                    rates[key] = val_row

        logger.debug("[ASSM-BEPRD] raw_keys: %d개, rates: %d개",
                     len(raw_keys_np) if raw_keys_np is not None else 0, len(rates))
        logger.debug("[ASSM-BEPRD] -- 경과년도별 지급률 로딩 완료 --")

        return rates, raw_data_np, raw_keys_np

    def _load_expected_interest(self, params: Optional[dict]) -> Optional[ExpectedInterestRate]:
        """예정이율(IP_P_EXPCT_INRT)을 로딩한다.

        단건(i=0) 기준 첫 행에서 rates/change_years 추출.

        Returns:
            ExpectedInterestRate 또는 None (쿼리 실패 시)
        """
        logger.debug("[ASSM-EXPCT] -- 예정이율 로딩 시작 --")
        try:
            df = self.reader.fetch_data("IP_P_EXPCT_INRT", params or {})
        except (KeyError, Exception):
            logger.warning("IP_P_EXPCT_INRT 쿼리 실패, 예정이율 None 반환")
            return None

        if df.empty:
            logger.warning("IP_P_EXPCT_INRT 결과 없음")
            return None

        row = df.iloc[0]
        rates = np.array([
            float(row.get("EXPCT_INRT1", 0.0)),
            float(row.get("EXPCT_INRT2", 0.0)),
            float(row.get("EXPCT_INRT3", 0.0)),
        ], dtype=np.float64)
        change_years = np.array([
            int(row.get("EXPCT_INRT_CHNG_YYCNT1", 0)),
            int(row.get("EXPCT_INRT_CHNG_YYCNT2", 0)),
        ], dtype=np.int32)

        result = ExpectedInterestRate(rates=rates, change_years=change_years)
        logger.debug("[ASSM-EXPCT] 예정이율: rates=%s, change_years=%s, flat=%s",
                     rates, change_years, result.get_flat_rate())
        logger.debug("[ASSM-EXPCT] -- 예정이율 로딩 완료 --")
        return result

    def _load_expected_expense(
        self, params: Optional[dict], mp: Optional[Any] = None
    ) -> Optional[ExpectedExpenseRate]:
        """예정사업비율(IP_P_EXPCT_BIZEXP_RT)을 로딩한다.

        1. IP_P_EXPCT_BIZEXP_CMPT_CRIT에서 DIV_YN 플래그 조회
        2. IP_P_EXPCT_BIZEXP_RT 로딩 후 DIV_YN=1인 키만 mp.df의 값으로 필터
        3. 첫 행에서 ExpectedExpenseRate 생성

        Returns:
            ExpectedExpenseRate 또는 None
        """
        logger.debug("[ASSM-EXPCT] -- 예정사업비율 로딩 시작 --")

        # 1. CMPT_CRIT 조회
        try:
            df_crit = self.reader.fetch_data("IP_P_EXPCT_BIZEXP_CMPT_CRIT", params or {})
        except (KeyError, Exception):
            logger.warning("IP_P_EXPCT_BIZEXP_CMPT_CRIT 쿼리 실패")
            return None

        if df_crit.empty:
            logger.warning("IP_P_EXPCT_BIZEXP_CMPT_CRIT 결과 없음")
            return None

        crit = df_crit.iloc[0]
        paypr_div_yn = int(crit.get("PAYPR_DIV_YN", 0))
        renw_st_div_yn = int(crit.get("RENW_ST_DIV_YN", 0))
        etc_key_yn = int(crit.get("ETC_EXPCT_BIZEXP_KEY_YN", 0))
        instrm_div_yn = int(crit.get("INSTRM_DIV_YN", 0))

        logger.debug("[ASSM-EXPCT] CMPT_CRIT: PAYPR_DIV=%d, RENW_ST_DIV=%d, "
                     "ETC_KEY=%d, INSTRM_DIV=%d",
                     paypr_div_yn, renw_st_div_yn, etc_key_yn, instrm_div_yn)

        # 2. BIZEXP_RT 로딩
        try:
            df_rt = self.reader.fetch_data("IP_P_EXPCT_BIZEXP_RT", params or {})
        except (KeyError, Exception):
            logger.warning("IP_P_EXPCT_BIZEXP_RT 쿼리 실패")
            return None

        if df_rt.empty:
            logger.warning("IP_P_EXPCT_BIZEXP_RT 결과 없음")
            return None

        logger.debug("[ASSM-EXPCT] IP_P_EXPCT_BIZEXP_RT: %d행 로딩", len(df_rt))

        # DIV_YN=1인 키만 mp.df 값으로 필터
        mask = pd.Series([True] * len(df_rt), index=df_rt.index)

        if mp is not None and mp.df is not None and len(mp.df) > 0:
            mp_row = mp.df.iloc[0]

            if paypr_div_yn == 1 and "PAYPR_DVCD" in df_rt.columns and "PAYPR_DVCD" in mp.df.columns:
                mp_val = str(mp_row["PAYPR_DVCD"])
                mask = mask & (df_rt["PAYPR_DVCD"].astype(str) == mp_val)
                logger.debug("[ASSM-EXPCT] PAYPR_DVCD 필터: %s", mp_val)

            if renw_st_div_yn == 1 and "RENW_STCD" in df_rt.columns and "RENW_STCD" in mp.df.columns:
                mp_val = str(mp_row["RENW_STCD"])
                mask = mask & (df_rt["RENW_STCD"].astype(str) == mp_val)
                logger.debug("[ASSM-EXPCT] RENW_STCD 필터: %s", mp_val)

            if etc_key_yn == 1 and "ETC_EXPCT_BIZEXP_KEY_VAL" in df_rt.columns and "ETC_EXPCT_BIZEXP_KEY_VAL" in mp.df.columns:
                mp_val = str(mp_row["ETC_EXPCT_BIZEXP_KEY_VAL"])
                mask = mask & (df_rt["ETC_EXPCT_BIZEXP_KEY_VAL"].astype(str) == mp_val)
                logger.debug("[ASSM-EXPCT] ETC_EXPCT_BIZEXP_KEY_VAL 필터: %s", mp_val)

            if instrm_div_yn == 1 and "INSTRM_DVCD" in df_rt.columns and "INSTRM_DVCD" in mp.df.columns:
                mp_val = str(mp_row["INSTRM_DVCD"])
                mask = mask & (df_rt["INSTRM_DVCD"].astype(str) == mp_val)
                logger.debug("[ASSM-EXPCT] INSTRM_DVCD 필터: %s", mp_val)

        df_filtered = df_rt[mask]
        if df_filtered.empty:
            logger.warning("IP_P_EXPCT_BIZEXP_RT 필터 후 결과 없음")
            return None

        logger.debug("[ASSM-EXPCT] 필터 후 %d행, 첫 행 사용", len(df_filtered))
        row = df_filtered.iloc[0]

        result = ExpectedExpenseRate(
            fryy_gprem_acqs_rt=float(row.get("FRYY_GPREM_VS_ACQSEXP_RT", 0.0)),
            fryy_join_amt_acqs_rt=float(row.get("FRYY_JOIN_AMT_VS_ACQSEXP_RT", 0.0)),
            inpay_gprem_mnt_rt=float(row.get("INPAY_GPREM_VS_MNTEXP_RT1", 0.0)),
            inpay_gprem_acqs_rt=float(row.get("INPAY_GPREM_VS_ACQSEXP_RT", 0.0)),
            inpay_join_amt_mnt_rt=float(row.get("INPAY_JOIN_AMT_VS_MNTEXP_RT", 0.0)),
            inpay_fxamt_mntexp=float(row.get("INPAY_FXAMT_MNTEXP", 0.0)),
            afpay_gprem_mnt_rt=float(row.get("AFPAY_GPREM_VS_MNTEXP_RT", 0.0)),
            afpay_join_amt_mnt_rt=float(row.get("AFPAY_JOIN_AMT_VS_MNTEXP_RT", 0.0)),
            inpay_gprem_colm_rt=float(row.get("INPAY_GPREM_VS_COLMEXP_RT1", 0.0)),
            inpay_gprem_loss_svyexp_rt=float(row.get("INPAY_GPREM_VS_LOSS_SVYEXP_RT", 0.0)),
        )

        logger.debug("[ASSM-EXPCT] 예정사업비: 신계약비(영보)=%.4f, 신계약비(가입금액)=%.4f, "
                     "유지비(영보)=%.4f, 유지비(고정)=%.0f, "
                     "납입후유지비(영보)=%.4f, 수금비=%.4f, 손해조사비=%.4f",
                     result.fryy_gprem_acqs_rt, result.fryy_join_amt_acqs_rt,
                     result.inpay_gprem_mnt_rt, result.inpay_fxamt_mntexp,
                     result.afpay_gprem_mnt_rt, result.inpay_gprem_colm_rt,
                     result.inpay_gprem_loss_svyexp_rt)
        logger.debug("[ASSM-EXPCT] -- 예정사업비율 로딩 완료 --")
        return result

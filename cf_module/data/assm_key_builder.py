"""
가정 복합키 구축 엔진

기존 lapse.py, skew.py, IA_R_BEPRD_DEFRY_RT.py의 공통 9단계 드라이버 패턴을
범용 클래스로 통합한다. 또한 qx_read.py의 위험률 전용 키 빌더도 포함한다.

드라이버 패턴 (lapse/skew/beprd 공유):
1. MD_ASSM_KDCD에서 ASSM_KDCD 결정
2. EXE_ASSM_GRP_I에서 ASSM_FILE_ID 결정
3. IA_M_ASSM_DRIV에서 드라이버 설정 추출
4. IA_M_ETC_ASSM_KEY에서 ETC 키 매핑 로딩
5. IA_M_PROD_GRP에서 상품그룹 매핑
6. 드라이버 로직(0='^', 1=매핑, 2=유지) 적용
7. '|' 구분자로 최종키 조합
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from cf_module.io.reader import DataReader
from cf_module.utils.logger import get_logger

logger = get_logger("assm_key_builder")


# 가정 종류별 설정
ASSM_TYPE_CONFIG = {
    "해지율": {
        "file_id_col": "ASSM_FILE_ID_TRMNAT_RT",
        "n_key_cols": 18,
        "has_pay_type": True,
        "has_rsk_key": False,
    },
    "스큐": {
        "file_id_col": "ASSM_FILE_ID_SKEW",
        "n_key_cols": 17,
        "has_pay_type": False,
        "has_rsk_key": False,
    },
    "경과년도별지급률": {
        "file_id_col": "ASSM_FILE_ID_BEPRD_DEFRY_RT",
        "n_key_cols": 18,
        "has_pay_type": False,
        "has_rsk_key": True,
    },
}

# RSK_RT_DIV_VAL_DEF_CD → RSK_RT_DIV_VAL 인덱스 매핑 (qx_read.py 로직)
DIV_VAL_DEF_CD_MAP = {
    "49": 0,   # DIV_VAL1
    "21": 1,   # DIV_VAL2
    "22": 2,   # DIV_VAL3
    "03": 3,   # DIV_VAL4
    "70": 4,   # DIV_VAL5
    "71": 5,   # DIV_VAL6
}


@dataclass
class DriverResolved:
    """드라이버 해석 결과"""
    assm_kdcd: str
    assm_file_id: str
    driv_key_prod: np.ndarray    # 드라이버의 PROD 열 값
    driv_key_rsk: np.ndarray     # 드라이버의 RSK 열 값 (BEPRD 전용)
    driv_key_etc: np.ndarray     # 드라이버의 ETC 열 값 (1, 15)
    etc_assm_key_no: np.ndarray
    etc_assm_div_val: np.ndarray
    etc_assm_grp_cd: np.ndarray


class AssumptionKeyBuilder:
    """가정 복합키 구축 엔진 (해지율/스큐/경과년도별지급률 공통)

    기존 lapse.py, skew.py, IA_R_BEPRD_DEFRY_RT.py의 공통 드라이버 패턴을 범용화한다.
    """

    def __init__(self, reader: DataReader, assm_grp_id: tuple):
        self.reader = reader
        self.assm_grp_id = assm_grp_id
        self._common_loaded = False
        self._md_assm_kdcd: Optional[pd.DataFrame] = None
        self._ia_m_assm_driv: Optional[pd.DataFrame] = None
        self._exe_assm_grp_i: Optional[pd.DataFrame] = None

    def _ensure_common_tables(self) -> None:
        """공통 마스터 테이블 1회 로딩"""
        if self._common_loaded:
            return
        logger.debug("[KEY] -- 공통 마스터 테이블 로딩 시작 --")
        self._md_assm_kdcd = self.reader.fetch_data("MD_ASSM_KDCD", {})
        self._ia_m_assm_driv = self.reader.fetch_data("IA_M_ASSM_DRIV", {})
        self._exe_assm_grp_i = self.reader.fetch_data(
            "EXE_ASSM_GRP_I", {"assm_grp_id": self.assm_grp_id[0]}
        )
        logger.debug("[KEY] MD_ASSM_KDCD: %d행, IA_M_ASSM_DRIV: %d행, EXE_ASSM_GRP_I: %d행",
                     len(self._md_assm_kdcd), len(self._ia_m_assm_driv), len(self._exe_assm_grp_i))
        self._common_loaded = True

    def resolve_driver_config(self, assm_type_name: str) -> DriverResolved:
        """드라이버 설정을 해석한다.

        Args:
            assm_type_name: 가정 종류 한글명 ('해지율', '스큐', '경과년도별지급률')

        Returns:
            DriverResolved
        """
        logger.debug("[KEY] -- resolve_driver_config('%s') 시작 --", assm_type_name)
        self._ensure_common_tables()
        cfg = ASSM_TYPE_CONFIG[assm_type_name]

        # ASSM_KDCD 결정 (원본 타입 유지 — int)
        row = self._md_assm_kdcd.loc[
            self._md_assm_kdcd["ASSM_KDCD_KOR_NM"] == assm_type_name
        ]
        assm_kdcd_raw = row["ASSM_KDCD"].values[0]  # 보통 int

        # ASSM_FILE_ID 결정 (원본 타입 유지 — str)
        assm_file_id = self._exe_assm_grp_i[cfg["file_id_col"]].values[0]
        logger.debug("[KEY] ASSM_KDCD=%s, ASSM_FILE_ID=%s (from %s)", assm_kdcd_raw, assm_file_id, cfg["file_id_col"])

        # 드라이버 필터링 (타입 일치 보장)
        driv_df = self._ia_m_assm_driv[
            (self._ia_m_assm_driv["ASSM_KDCD"] == assm_kdcd_raw)
            & (self._ia_m_assm_driv["ASSM_FILE_ID"] == assm_file_id)
        ]
        driv_arr = driv_df.to_numpy()
        logger.debug("[KEY] 드라이버 필터: %d행 매칭", len(driv_df))

        driv_key_prod = driv_arr[:, 5] if driv_arr.shape[0] > 0 else np.array([])
        driv_key_rsk = driv_arr[:, 7] if (cfg["has_rsk_key"] and driv_arr.shape[0] > 0) else np.array([])
        driv_key_etc = driv_arr[:, 8:23] if driv_arr.shape[0] > 0 else np.zeros((0, 15))

        # ETC 키 매핑 로딩
        etc_key_df = self.reader.fetch_data(
            "IA_M_ETC_ASSM_KEY",
            {"assm_kdcd": int(assm_kdcd_raw), "assm_file_id": str(assm_file_id)},
        )
        etc_arr = etc_key_df.to_numpy() if not etc_key_df.empty else np.zeros((0, 6))

        logger.debug("[KEY] ETC 키 매핑: %d건", len(etc_arr))
        logger.debug("[KEY] -- resolve_driver_config('%s') 완료 --", assm_type_name)

        return DriverResolved(
            assm_kdcd=str(assm_kdcd_raw),
            assm_file_id=str(assm_file_id),
            driv_key_prod=driv_key_prod,
            driv_key_rsk=driv_key_rsk,
            driv_key_etc=driv_key_etc,
            etc_assm_key_no=etc_arr[:, 3].astype(int) if etc_arr.shape[0] > 0 else np.array([], dtype=int),
            etc_assm_div_val=etc_arr[:, 4] if etc_arr.shape[0] > 0 else np.array([]),
            etc_assm_grp_cd=etc_arr[:, 5] if etc_arr.shape[0] > 0 else np.array([]),
        )

    def resolve_prod_group(
        self, resolved: DriverResolved, prod_cd: np.ndarray, cls_cd: np.ndarray
    ) -> list:
        """상품그룹 매핑을 수행한다.

        Args:
            resolved: 드라이버 해석 결과
            prod_cd: 상품코드 배열 (n_points,)
            cls_cd: 분류코드 배열 (n_points,)

        Returns:
            상품그룹 키 리스트 (n_points,)
        """
        # 대표 상품코드로 IA_M_PROD_GRP 조회
        unique_prod = np.unique(prod_cd)
        all_prod_grp = []
        for pc in unique_prod:
            prod_params = {
                "assm_kdcd": int(resolved.assm_kdcd),
                "assm_file_id": resolved.assm_file_id,
                "prod_cd": str(pc),
            }
            try:
                df = self.reader.fetch_data("IA_M_PROD_GRP", prod_params)
                all_prod_grp.append(df.to_numpy())
            except Exception:
                pass

        if not all_prod_grp:
            return [None] * len(prod_cd)

        prod_grp_arr = np.vstack(all_prod_grp)

        result = []
        for pc, cc in zip(prod_cd, cls_cd):
            match1 = (
                (prod_grp_arr[:, 3] == 1)
                & (prod_grp_arr[:, 4] == pc)
                & (prod_grp_arr[:, 5] == cc)
            )
            match0 = (prod_grp_arr[:, 3] == 1) & (prod_grp_arr[:, 4] == pc)
            idx = np.where(match1)[0]
            if len(idx) == 0:
                idx = np.where(match0)[0]
            result.append(prod_grp_arr[idx[0], 6] if len(idx) > 0 else None)

        return result

    def resolve_rsk_category(
        self, resolved: DriverResolved, rsk_rt_cd_list: np.ndarray
    ) -> list:
        """위험 카테고리 매핑 (BEPRD 전용)

        Args:
            resolved: 드라이버 해석 결과
            rsk_rt_cd_list: 위험률코드 배열 (n_risks,)

        Returns:
            위험 카테고리 키 리스트 (n_risks,)
        """
        rsk_params = {
            "assm_kdcd": int(resolved.assm_kdcd),
            "assm_file_id": resolved.assm_file_id,
        }
        try:
            rsk_cat_arr = self.reader.fetch_data("IA_M_RSK_CAT", rsk_params).to_numpy()
        except Exception:
            return [None] * len(rsk_rt_cd_list)

        result = []
        for rsk_cd in rsk_rt_cd_list:
            match = rsk_cat_arr[:, 2] == rsk_cd
            idx = np.where(match)[0]
            result.append(rsk_cat_arr[idx[0], 3] if len(idx) > 0 else None)
        return result

    def build_etc_keys(
        self, resolved: DriverResolved, assm_div_vals: np.ndarray
    ) -> np.ndarray:
        """드라이버 로직(0='^', 1=매핑, 2=유지)을 적용하여 ETC 키를 구축한다.

        Args:
            resolved: 드라이버 해석 결과
            assm_div_vals: (n_points, 15) ASSM_DIV_VAL1~15

        Returns:
            (n_points, 15) 변환된 ETC 키 배열
        """
        key_etc = assm_div_vals.copy()
        driv_etc = resolved.driv_key_etc  # (1, 15) or (n_driv, 15)

        if driv_etc.size == 0:
            # 드라이버 설정이 없으면 모두 '^'
            key_etc[:] = "^"
            return key_etc

        for col_idx in range(driv_etc.shape[1]):
            driv_value = driv_etc[0, col_idx]
            if driv_value == 0:
                key_etc[:, col_idx] = "^"
            elif driv_value == 2:
                key_etc[:, col_idx] = np.where(
                    key_etc[:, col_idx] == "", "^", key_etc[:, col_idx]
                )
            elif driv_value == 1:
                # 빈값 → '^'
                key_etc[:, col_idx] = np.where(
                    key_etc[:, col_idx] == "", "^", key_etc[:, col_idx]
                )
                # 매핑 적용
                key_filter = resolved.etc_assm_key_no == (col_idx + 1)
                if np.any(key_filter):
                    map_keys = resolved.etc_assm_div_val[key_filter]
                    map_vals = resolved.etc_assm_grp_cd[key_filter]
                    for k, v in zip(map_keys, map_vals):
                        key_etc[:, col_idx] = np.where(
                            key_etc[:, col_idx] == k, v, key_etc[:, col_idx]
                        )

        return key_etc

    def assemble_composite_key(
        self,
        assm_file_id: str,
        prod_keys: list,
        etc_keys: np.ndarray,
        pay_type: Optional[str] = None,
        rsk_keys: Optional[list] = None,
    ) -> np.ndarray:
        """'|' 구분자로 최종 복합키를 조합한다.

        Args:
            assm_file_id: 가정 파일 ID
            prod_keys: 상품그룹 키 리스트 (n_points,)
            etc_keys: ETC 키 배열 (n_points, 15)
            pay_type: '1'(납입기간) 또는 '2'(납입후) — 해지율 전용
            rsk_keys: 위험 카테고리 키 리스트 (n_risks,) — BEPRD 전용

        Returns:
            복합키 배열. 일반: (n_points,), BEPRD: (n_risks,)
        """
        n_points = len(prod_keys)
        etc_str_list = [
            "|".join(str(v).strip("'") for v in row) for row in etc_keys
        ]

        if rsk_keys is not None:
            # BEPRD: 위험별 키 생성 — 키 = FILE_ID|RSK_KEY|PROD_KEY|ETC_KEY
            # rsk_keys is (n_risks,), 나머지는 (n_points=1,) 가정 (단건)
            result = []
            for rsk_key in rsk_keys:
                key = "|".join([
                    str(assm_file_id),
                    str(rsk_key),
                    str(prod_keys[0]),
                    etc_str_list[0],
                ])
                result.append(key)
            return np.array(result)

        # 일반: 키 = FILE_ID|PROD_KEY|ETC_KEY[|PAY_TYPE]
        result = []
        for i in range(n_points):
            parts = [str(assm_file_id), str(prod_keys[i]), etc_str_list[i]]
            if pay_type is not None:
                parts.append(pay_type)
            result.append("|".join(parts))
        return np.array(result)

    def build_keys_for_type(
        self,
        assm_type_name: str,
        prod_cd: np.ndarray,
        cls_cd: np.ndarray,
        assm_div_vals: np.ndarray,
        pay_type: Optional[str] = None,
        rsk_rt_cd_list: Optional[np.ndarray] = None,
    ) -> Tuple[np.ndarray, DriverResolved]:
        """특정 가정 종류에 대한 전체 키 구축 파이프라인을 실행한다.

        Returns:
            (composite_keys, resolved)
        """
        resolved = self.resolve_driver_config(assm_type_name)
        prod_keys = self.resolve_prod_group(resolved, prod_cd, cls_cd)
        etc_keys = self.build_etc_keys(resolved, assm_div_vals)

        rsk_keys = None
        if ASSM_TYPE_CONFIG[assm_type_name]["has_rsk_key"] and rsk_rt_cd_list is not None:
            rsk_keys = self.resolve_rsk_category(resolved, rsk_rt_cd_list)

        keys = self.assemble_composite_key(
            resolved.assm_file_id, prod_keys, etc_keys,
            pay_type=pay_type, rsk_keys=rsk_keys,
        )
        logger.debug("[KEY] build_keys_for_type('%s') → %d개 키 생성. 첫번째: %s",
                     assm_type_name, len(keys), keys[0] if len(keys) > 0 else "(empty)")
        return keys, resolved


class MortalityKeyBuilder:
    """위험률 전용 키 빌더 (qx_read.py 로직)

    RSK_RT_DIV_VAL_DEF_CD 코드 기반으로 복합키를 구축하고,
    IR_RSKRT_VAL에서 S/A 타입별로 위험률을 매칭한다.
    """

    @staticmethod
    def build_risk_keys(
        rsk_rt_chr: pd.DataFrame,
        rsk_rt_div_vals: np.ndarray,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """위험률 복합키를 구축한다.

        Args:
            rsk_rt_chr: IR_RSKRT_CHR DataFrame
            rsk_rt_div_vals: (n_points, 10) RSK_RT_DIV_VAL1~10

        Returns:
            (risk_keys, range_qx_code, range_qx_info)
            - risk_keys: (n_risks,) 복합키 배열
            - range_qx_code: (n_risks,) RSK_RT_CD^REVI_YM
            - range_qx_info: (n_risks, 16) 위험률 특성 정보
        """
        range_qx_code = np.array([
            "^".join(row)
            for row in rsk_rt_chr[["RSK_RT_CD", "REVI_YM"]].astype(str).to_numpy()
        ])

        info_cols = [
            "RSK_RT_NM", "RSK_RT_CHR_CD", "MM_TRF_WAY_CD",
            "DEAD_RT_DVCD", "RISK_DTLS_COV_CD",
        ]
        def_cd_cols = [f"RSK_RT_DIV_VAL_DEF_CD{i}" for i in range(1, 11)]
        info_cols.extend(def_cd_cols)
        info_cols.append("REVI_RESTRI_YYCNT")

        # 고정 인덱스 보장: 누락 컬럼은 None으로 채움
        n_rows = len(rsk_rt_chr)
        range_qx_info = np.full((n_rows, len(info_cols)), None, dtype=object)
        for col_idx, col in enumerate(info_cols):
            if col in rsk_rt_chr.columns:
                range_qx_info[:, col_idx] = rsk_rt_chr[col].to_numpy()

        # risk_key 구축: RSK_RT_DIV_VAL_DEF_CD → RSK_RT_DIV_VAL 매핑
        risk_key_defs = range_qx_info[:, 5:15]  # (n_risks, 10)
        risk_type_range = rsk_rt_div_vals  # (n_points, 10) — 단건이면 (1, 10)

        # np.select 조건부 매핑
        # NaN과 None 모두 null로 처리 (pd.isna로 통합)
        is_null = pd.isna(risk_key_defs)
        conditions = [
            is_null,
            risk_key_defs == "49",
            risk_key_defs == "21",
            risk_key_defs == "22",
            risk_key_defs == "03",
            risk_key_defs == "70",
            risk_key_defs == "71",
            ~is_null,
        ]
        choices = [
            "00",
            risk_type_range[:, 0] if risk_type_range.shape[0] > 0 else "00",
            risk_type_range[:, 1] if risk_type_range.shape[0] > 0 else "00",
            risk_type_range[:, 2] if risk_type_range.shape[0] > 0 else "00",
            risk_type_range[:, 3] if risk_type_range.shape[0] > 0 else "00",
            risk_type_range[:, 4] if risk_type_range.shape[0] > 0 else "00",
            risk_type_range[:, 5] if risk_type_range.shape[0] > 0 else "00",
            risk_type_range[:, :] if risk_type_range.shape[0] > 0 else "00",
        ]

        risk_key = np.select(conditions, choices, default="00")
        risk_key_joined = np.array(["^".join(str(v) for v in row) for row in risk_key])
        risk_keys = np.char.add(range_qx_code, "^")
        risk_keys = np.char.add(risk_keys, risk_key_joined)

        return risk_keys, range_qx_code, range_qx_info

    @staticmethod
    def match_rates(
        risk_keys: np.ndarray,
        range_qx_info: np.ndarray,
        ir_rskrt_val: np.ndarray,
        age: int,
        bterm: int,
        duration_values: np.ndarray,
    ) -> np.ndarray:
        """위험률을 매칭한다.

        Args:
            risk_keys: (n_risks,) 복합키 배열
            range_qx_info: (n_risks, 16) 위험률 특성 정보
            ir_rskrt_val: IR_RSKRT_VAL numpy 배열
            age: 가입연령
            bterm: 보장기간 (년)
            duration_values: 경과년도 배열

        Returns:
            rsk_rt: (n_steps, n_risks) 위험률 배열
        """
        chr_cd_arr = range_qx_info[:, 1]  # 'S' or 'A'
        s_indices = chr_cd_arr == "S"
        a_indices = chr_cd_arr == "A"

        # 찾기 키 생성
        qx_find_key = np.array([
            "^".join(str(v) for v in row[:12]) for row in ir_rskrt_val
        ])
        qx_find_key2 = np.array([
            "^".join(str(v) for v in row[:13]) for row in ir_rskrt_val
        ])

        # S 타입 매칭: 각 키에 대해 첫 번째 매치 인덱스만 사용
        rsk_rt_S = np.array([]).reshape(0, 1)
        s_risk_keys = risk_keys[s_indices]
        if len(s_risk_keys) > 0:
            s_match_indices = []
            for key in s_risk_keys:
                idx = np.where(qx_find_key == key)[0]
                s_match_indices.append(idx[0] if len(idx) > 0 else -1)
            s_match_indices = np.array(s_match_indices)
            valid = s_match_indices >= 0
            if np.any(valid):
                rsk_rt_S = np.zeros((len(s_risk_keys), 1), dtype=np.float64)
                rsk_rt_S[valid, 0] = ir_rskrt_val[s_match_indices[valid], 14].astype(np.float64)

        # A 타입 매칭
        rsk_rt_A = np.array([]).reshape(0, bterm)
        a_risk_keys = risk_keys[a_indices]
        if len(a_risk_keys) > 0:
            age_list = np.arange(age, age + bterm).reshape(-1, 1)
            result_keys = np.array([
                [f"{base_key}^{a[0]}" for a in age_list]
                for base_key in a_risk_keys
            ])

            index_map = {value: idx for idx, value in enumerate(qx_find_key2)}
            matching_a = np.array([
                [index_map.get(value, -1) for value in row]
                for row in result_keys
            ])

            rsk_rt_A = np.where(
                matching_a == -1, 0, ir_rskrt_val[matching_a, 14]
            )

        # S와 A 결합
        if rsk_rt_S.size > 0 and rsk_rt_A.size > 0:
            rsk_rt = np.vstack((np.tile(rsk_rt_S, (1, bterm)), rsk_rt_A))
        elif rsk_rt_S.size > 0:
            rsk_rt = np.tile(rsk_rt_S, (1, bterm))
        elif rsk_rt_A.size > 0:
            rsk_rt = rsk_rt_A
        else:
            rsk_rt = np.zeros((0, max(bterm, 1)))

        # duration_values로 시간축 추출: (n_risks, bterm) → (n_steps, n_risks)
        if rsk_rt.shape[0] > 0 and len(duration_values) > 0:
            dur_idx = np.clip(duration_values - 1, 0, rsk_rt.shape[1] - 1)
            rsk_rt = rsk_rt[:, dur_idx].T  # (n_steps, n_risks)
        else:
            rsk_rt = np.zeros((len(duration_values), max(rsk_rt.shape[0], 1)))

        return rsk_rt.astype(np.float64)

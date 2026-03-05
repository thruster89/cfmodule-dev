WITH AA AS (
    SELECT *
    FROM IR_RSKRT_CHR A
    WHERE A.RSK_RT_CD IN (
        SELECT RSK_RT_CD
        FROM IP_R_RSKRT_C
        WHERE (PROD_CD, CLS_CD, COV_CD) IN (
            SELECT PROD_CD, CLS_CD, COV_CD
            FROM II_INFRC
            WHERE INFRC_SEQ = :infrc_seq
              AND INFRC_IDNO BETWEEN :idno_start AND :idno_end
        )
    )
    AND A.REVI_YM = (
        SELECT MAX(REVI_YM)
        FROM IR_RSKRT_CHR Z
        WHERE Z.RSK_RT_CD = A.RSK_RT_CD
          AND A.REVI_YM <= :assm_ym
    )
)
SELECT * FROM AA
UNION ALL
SELECT *
FROM IR_RSKRT_CHR A
WHERE A.RSK_RT_CD IN (
    SELECT RSK_RT_CD
    FROM IP_R_RSKRT_C
    WHERE (PROD_CD, CLS_CD, COV_CD) IN (
        SELECT PROD_CD, CLS_CD, COV_CD
        FROM II_INFRC
        WHERE INFRC_SEQ = :infrc_seq
          AND INFRC_IDNO BETWEEN :idno_start AND :idno_end
    )
)
AND A.REVI_YM = (
    SELECT MIN(REVI_YM)
    FROM IR_RSKRT_CHR Z
    WHERE Z.RSK_RT_CD = A.RSK_RT_CD
      AND A.REVI_YM > :assm_ym
)
AND A.RSK_RT_CD NOT IN (SELECT RSK_RT_CD FROM AA)

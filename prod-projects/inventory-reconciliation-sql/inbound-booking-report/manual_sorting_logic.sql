/****************************************************************************************************
 1. Execute the script (Stng + A and Strng + Enter).
 2. A dialog box will appear for the variables.
 3. :ref_lhm_filter ->
    - ALL: Leave BLANK.
    - MULTIPLE: Enter a comma-separated list (e.g., '11,303,ZFS24').
    - WILDCARD: Enter a single value with '%' (e.g., 'ay%').
 4. - :start_datetime (Format: 'DD.MM.YYYY HH24:MI:SS') -> The start of the reporting period. (e.g., '13.10.2025 06:00:00')
    - :end_datetime   (Format: 'DD.MM.YYYY HH24:MI:SS') -> The end of the reporting period. (e.g., '13.10.2025 23:59:00')
 ****************************************************************************************************/

WITH
    -- =========================================================================
    -- PART 1: NORMAL GOODS (Standard Logic)
    -- =========================================================================
    normal_goods_t1 AS (
        SELECT /*+ MATERIALIZE */
            hv.LOCAL_TRANSACTION_ID, hv.ARTNR, hv.ZIEL, hv.CREATEDBY, hv.LHMNR, hv.CREATED, hv.MENGE, hv.CUST_DATA,
            JSON_VALUE(hv.CUST_DATA, '$.REFERENCENUMBER_LHM') AS Reference_LHM
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 520
            AND hv.MENGE < 0 AND hv.MENGE IS NOT NULL
            AND hv.ZIEL IN ('IN_SZR_OV', 'SZR_OV')
            -- Performance: Filter Date here
            AND (:start_datetime IS NULL OR hv.CREATED BETWEEN TO_DATE(:start_datetime, 'DD.MM.YYYY HH24:MI:SS') 
                                                        AND TO_DATE(:end_datetime, 'DD.MM.YYYY HH24:MI:SS'))
    ),

    normal_goods_t2 AS (
        SELECT /*+ MATERIALIZE */
            hv.LHMNR, hv.LOCAL_TRANSACTION_ID
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 520
            AND hv.ZIEL IN ('IN_SZR_OV', 'SZR_OV')
            AND hv.MENGE = 1
            -- Performance: Filter Date here
            AND (:start_datetime IS NULL OR hv.CREATED BETWEEN TO_DATE(:start_datetime, 'DD.MM.YYYY HH24:MI:SS') 
                                                        AND TO_DATE(:end_datetime, 'DD.MM.YYYY HH24:MI:SS'))
    ),

    -- =========================================================================
    -- PART 2: DUMMY GOODS (Logic with ROW NUMBER Fix)
    -- =========================================================================
    dummy_goods_t1 AS (
        SELECT /*+ MATERIALIZE */
            hv.LOCAL_TRANSACTION_ID,
            hv.ARTNR, 
            hv.ZIEL, hv.CREATEDBY, hv.LHMNR, hv.CREATED, hv.MENGE, hv.CUST_DATA,
            JSON_VALUE(hv.CUST_DATA, '$.REFERENCENUMBER_LHM') AS Reference_LHM,
            -- FIX: Add Row Number to prevent duplicates in the dummy join
            ROW_NUMBER() OVER (PARTITION BY hv.LOCAL_TRANSACTION_ID ORDER BY hv."SEQUENCE" ASC) as rn
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 614 AND hv.TYP_ID = 120
            AND hv.MENGE < 0 AND hv.MENGE IS NOT NULL
            AND hv.ZIEL IN ('IN_SZR_OV')
            -- Performance: Filter Date here
            AND (:start_datetime IS NULL OR hv.CREATED BETWEEN TO_DATE(:start_datetime, 'DD.MM.YYYY HH24:MI:SS') 
                                                        AND TO_DATE(:end_datetime, 'DD.MM.YYYY HH24:MI:SS'))
    ),

    dummy_goods_t2 AS (
        SELECT /*+ MATERIALIZE */
            hv.LHMNR,
            hv.LOCAL_TRANSACTION_ID,
            hv.CUST_DATA,
            hv.ARTNR,
            -- FIX: Add Row Number to prevent duplicates in the dummy join
            ROW_NUMBER() OVER (PARTITION BY hv.LOCAL_TRANSACTION_ID ORDER BY hv."SEQUENCE" ASC) as rn
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 207
            AND hv.ZIEL IN ('IN_SZR_OV', 'SZR_OV')
            AND hv.MENGE > 0
            -- Performance: Filter Date here
            AND (:start_datetime IS NULL OR hv.CREATED BETWEEN TO_DATE(:start_datetime, 'DD.MM.YYYY HH24:MI:SS') 
                                                        AND TO_DATE(:end_datetime, 'DD.MM.YYYY HH24:MI:SS'))
    )

SELECT
    TO_CHAR(ag.CREATED, 'DD.MM.YYYY HH24:MI:SS')        AS Timestamp,
    ag.ARTNR                                            AS EAN,
    ag.ZIEL                                             AS AP,
    ag.CREATEDBY                                        AS BENUTZER,
    ag.Source_LHM,
    ag.ZIEL_LHM,
    ABS(ag.MENGE)                                       AS Quantity,
    ag.Reference_LHM,
    DECODE(JSON_VALUE(ag.CUST_DATA, '$.SOURCEID_SEKTOR'),
        '1',  'Zalando SE', '10', 'OSR', '11', 'OSR (OV)', 'Zircle'
    ) AS Source_Channel,
    DECODE(JSON_VALUE(ag.CUST_DATA, '$.QUALITYID_SEKTOR'),
        '1', 'A', '2', 'B', '3', 'C', '4', 'D', 'Unknown'
    ) AS Quality,
    DECODE(JSON_VALUE(ag.CUST_DATA, '$.CATEGORYID_ART'),
        '1', 'Schuhe', '2', 'Textil', '3', 'ACC', '4', 'Home', '5', 'Beauty', 'Unknown'
    ) AS Category,
    CASE
        WHEN ag.ZIEL_LHM LIKE '50%'                                     THEN 'Overstock'
        WHEN JSON_VALUE(ag.CUST_DATA, '$.QUALITYID_SEKTOR') IN ('3', '4') THEN 'Overstock'
        WHEN JSON_VALUE(ag.CUST_DATA, '$.DISTRIBUTIONCHANNELID_ART') = '4'  THEN 'Outlet'
        WHEN JSON_VALUE(ag.CUST_DATA, '$.DISTRIBUTIONCHANNELID_ART') = '3'  THEN 'Overstock'
        ELSE 'Unknown'
    END AS Distribution_Channel,
    CASE
        WHEN ag.good_type = 'NORMAL' THEN JSON_VALUE(ag.CUST_DATA, '$.SKU_ART')
        WHEN ag.good_type = 'DUMMY'  THEN JSON_VALUE(ag.t2_cust_data, '$.SKU_ART')
    END AS SKU,
    CASE
        WHEN ag.good_type = 'NORMAL' THEN JSON_VALUE(ag.CUST_DATA, '$.SORTINGCRITERIAID_ART')
        WHEN ag.good_type = 'DUMMY'  THEN JSON_VALUE(ag.t2_cust_data, '$.SORTINGCRITERIAID_ART')
    END AS SORT_ID

FROM (
    -- PART 1: NORMAL GOODS (Standard Join, NO Row Number)
    SELECT
        t1.CREATED, t1.ARTNR, t1.ZIEL, t1.CREATEDBY, t1.LHMNR AS Source_LHM, t1.CUST_DATA,
        t2.LHMNR AS ZIEL_LHM, t1.MENGE, t1.Reference_LHM,
        'NORMAL' AS good_type,
        NULL AS t2_cust_data
    FROM
        normal_goods_t1 t1
        LEFT JOIN normal_goods_t2 t2 ON t1.LOCAL_TRANSACTION_ID = t2.LOCAL_TRANSACTION_ID
    WHERE
        (:ref_lhm_filter IS NULL
        OR (INSTR(:ref_lhm_filter, '%') > 0 AND UPPER(t1.Reference_LHM) LIKE UPPER(:ref_lhm_filter))
        OR (INSTR(:ref_lhm_filter, ',') > 0 AND ',' || UPPER(:ref_lhm_filter) || ',' LIKE '%,' || UPPER(t1.Reference_LHM) || ',%')
        OR (INSTR(:ref_lhm_filter, '%') = 0 AND INSTR(:ref_lhm_filter, ',') = 0 AND UPPER(t1.Reference_LHM) = UPPER(:ref_lhm_filter)))

    UNION ALL

    -- PART 2: DUMMY GOODS (Strict Join using Row Number 'rn')
    SELECT
        t1.CREATED,
        -- New 3-Step EAN Logic
        CASE
            WHEN t1.ARTNR LIKE '2%' THEN t1.ARTNR
            WHEN JSON_VALUE(t1.CUST_DATA, '$.LASTEANGOTFROMMAUS_ZIEL') IS NOT NULL 
                 THEN JSON_VALUE(t1.CUST_DATA, '$.LASTEANGOTFROMMAUS_ZIEL')
            WHEN t2.ARTNR LIKE '2%' THEN t2.ARTNR
            ELSE t1.ARTNR
        END AS ARTNR,
        t1.ZIEL, t1.CREATEDBY, t1.LHMNR AS Source_LHM, t1.CUST_DATA,
        t2.LHMNR AS ZIEL_LHM, t1.MENGE, t1.Reference_LHM,
        'DUMMY' AS good_type,
        t2.CUST_DATA AS t2_cust_data
    FROM
        dummy_goods_t1 t1
        -- STRICT JOIN on ID + Row Number
        LEFT JOIN dummy_goods_t2 t2 ON t1.LOCAL_TRANSACTION_ID = t2.LOCAL_TRANSACTION_ID AND t1.rn = t2.rn
    WHERE
        (:ref_lhm_filter IS NULL
        OR (INSTR(:ref_lhm_filter, '%') > 0 AND UPPER(t1.Reference_LHM) LIKE UPPER(:ref_lhm_filter))
        OR (INSTR(:ref_lhm_filter, ',') > 0 AND ',' || UPPER(:ref_lhm_filter) || ',' LIKE '%,' || UPPER(t1.Reference_LHM) || ',%')
        OR (INSTR(:ref_lhm_filter, '%') = 0 AND INSTR(:ref_lhm_filter, ',') = 0 AND UPPER(t1.Reference_LHM) = UPPER(:ref_lhm_filter)))
) ag

WHERE
    NVL(ag.Source_LHM, 'value1') <> NVL(ag.ZIEL_LHM, 'value2')
    AND REGEXP_LIKE(ag.ZIEL_LHM, '^[0-9]+$');

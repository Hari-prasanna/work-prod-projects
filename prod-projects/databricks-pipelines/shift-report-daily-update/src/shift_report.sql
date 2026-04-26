WITH
    -- CTE 1: Normal Goods Initial
    normal_goods_t1 AS (
        SELECT
            hv.LOCAL_TRANSACTION_ID, hv.ARTNR, hv.ZIEL, hv.CREATEDBY, hv.LHMNR, hv.CREATED, hv.MENGE, hv.CUST_DATA,
            JSON_VALUE(hv.CUST_DATA, '$.REFERENCENUMBER_LHM') AS Reference_LHM
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 520 AND hv.MENGE < 0 AND hv.LAGBEZ IN ('Overstock', 'SZROV') AND hv.ZIEL LIKE 'OV%'
    ),

    -- CTE 2: Normal Goods Completion
    normal_goods_t2 AS (
        SELECT hv.LHMNR, hv.LOCAL_TRANSACTION_ID, hv.CUST_DATA
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 520 AND hv.LAGBEZ IN ('Overstock', 'SZROV') AND hv.MENGE = 1 AND hv.LHMNR NOT LIKE '000%'
    ),

    -- CTE 3: Dummy Goods Initial
    dummy_goods_t1 AS (
        SELECT
            hv.LOCAL_TRANSACTION_ID,
            CASE WHEN hv.ARTNR NOT LIKE '2%' THEN JSON_VALUE(hv.CUST_DATA, '$.LASTEANGOTFROMMAUS_ZIEL') ELSE hv.ARTNR END AS ARTNR,
            hv.ZIEL, hv.CREATEDBY, hv.LHMNR, hv.CREATED, hv.MENGE, hv.CUST_DATA,
            JSON_VALUE(hv.CUST_DATA, '$.REFERENCENUMBER_LHM') AS Reference_LHM
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 614 AND hv.MENGE < 0 AND hv.LAGBEZ IN ('Overstock', 'SZROV') AND hv.ZIEL LIKE 'OV%'
    ),

    -- CTE 4: Dummy Goods Completion
    dummy_goods_t2 AS (
        SELECT hv.LHMNR, hv.LOCAL_TRANSACTION_ID, hv.CUST_DATA
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 207 AND hv.LAGBEZ IN ('Overstock', 'SZROV') AND hv.MENGE = 1 AND hv.ZIEL LIKE 'OV%'
    ),

    -- CTE 5: Union Data with Script 2 Logic
    union_data AS (
        -- Part 1: Normal Goods
        SELECT
            t1.CREATED, t1.Reference_LHM, t1.MENGE,
            t1.CUST_DATA AS t1_cust_data, 
            t2.CUST_DATA AS t2_cust_data,
            t1.LHMNR AS Source_LHM, t2.LHMNR AS ZIEL_LHM,
            JSON_VALUE(t1.CUST_DATA, '$.SORTINGCRITERIAID_ART') AS SORT_ID,
            CASE
                WHEN JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR') = '1' AND LOWER(JSON_VALUE(t1.CUST_DATA, '$.SORTABLE_ART')) = 'false' THEN 'B'
                WHEN JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR') = '1' THEN 'A'
                WHEN JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR') = '2' THEN 'B'
                WHEN JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR') = '3' THEN 'C'
                WHEN JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR') = '4' THEN 'D'
                ELSE 'Unknown'
            END AS Quality,
            DECODE(
                JSON_VALUE(t1.CUST_DATA, '$.CATEGORYID_ART'),
                '1', 'Schuhe', '2', 'Textil', '3', 'ACC', '4', 'Home', '5', 'Beauty', 'Unknown'
            ) AS Category
        FROM normal_goods_t1 t1
        LEFT JOIN normal_goods_t2 t2 ON t1.LOCAL_TRANSACTION_ID = t2.LOCAL_TRANSACTION_ID
        WHERE (:start_datetime IS NULL OR t1.CREATED BETWEEN TO_DATE(:start_datetime, 'DD.MM.YYYY HH24:MI:SS') AND TO_DATE(:end_datetime, 'DD.MM.YYYY HH24:MI:SS'))
          AND (:ref_lhm_filter IS NULL
               OR (INSTR(:ref_lhm_filter, '%') > 0 AND UPPER(t1.Reference_LHM) LIKE UPPER(:ref_lhm_filter))
               OR (INSTR(:ref_lhm_filter, ',') > 0 AND ',' || UPPER(:ref_lhm_filter) || ',' LIKE '%,' || UPPER(t1.Reference_LHM) || ',%')
               OR (INSTR(:ref_lhm_filter, '%') = 0 AND INSTR(:ref_lhm_filter, ',') = 0 AND UPPER(t1.Reference_LHM) = UPPER(:ref_lhm_filter)))

        UNION ALL

        -- Part 2: Dummy Goods
        SELECT
            t1.CREATED, t1.Reference_LHM, t1.MENGE,
            t1.CUST_DATA AS t1_cust_data, 
            t2.CUST_DATA AS t2_cust_data,
            t1.LHMNR AS Source_LHM, t2.LHMNR AS ZIEL_LHM,
            JSON_VALUE(t2.CUST_DATA, '$.SORTINGCRITERIAID_ART') AS SORT_ID,
            CASE
                WHEN COALESCE(JSON_VALUE(t2.CUST_DATA, '$.QUALITYID_ART'), JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR')) = '1' 
                 AND COALESCE(JSON_VALUE(t2.CUST_DATA, '$.SORTABLE_ART'), JSON_VALUE(t1.CUST_DATA, '$.SORTABLE_ART')) = 'false' THEN 'B'
                WHEN COALESCE(JSON_VALUE(t2.CUST_DATA, '$.QUALITYID_ART'), JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR')) = '1' THEN 'A'
                WHEN COALESCE(JSON_VALUE(t2.CUST_DATA, '$.QUALITYID_ART'), JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR')) = '2' THEN 'B'
                WHEN COALESCE(JSON_VALUE(t2.CUST_DATA, '$.QUALITYID_ART'), JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR')) = '3' THEN 'C'
                WHEN COALESCE(JSON_VALUE(t2.CUST_DATA, '$.QUALITYID_ART'), JSON_VALUE(t1.CUST_DATA, '$.QUALITYID_SEKTOR')) = '4' THEN 'D'
                ELSE 'Unknown'
            END AS Quality,
            DECODE(
                COALESCE(JSON_VALUE(t2.CUST_DATA, '$.CATEGORYID_ART'), JSON_VALUE(t1.CUST_DATA, '$.CATEGORYID_ART')),
                '1', 'Schuhe', '2', 'Textil', '3', 'ACC', '4', 'Home', '5', 'Beauty', 'Unknown'
            ) AS Category
        FROM dummy_goods_t1 t1
        LEFT JOIN dummy_goods_t2 t2 ON t1.LOCAL_TRANSACTION_ID = t2.LOCAL_TRANSACTION_ID
        WHERE (:start_datetime IS NULL OR t1.CREATED BETWEEN TO_DATE(:start_datetime, 'DD.MM.YYYY HH24:MI:SS') AND TO_DATE(:end_datetime, 'DD.MM.YYYY HH24:MI:SS'))
          AND t2.LHMNR NOT LIKE '000%'
          AND (:ref_lhm_filter IS NULL
               OR (INSTR(:ref_lhm_filter, '%') > 0 AND UPPER(t1.Reference_LHM) LIKE UPPER(:ref_lhm_filter))
               OR (INSTR(:ref_lhm_filter, ',') > 0 AND ',' || UPPER(:ref_lhm_filter) || ',' LIKE '%,' || UPPER(t1.Reference_LHM) || ',%')
               OR (INSTR(:ref_lhm_filter, '%') = 0 AND INSTR(:ref_lhm_filter, ',') = 0 AND UPPER(t1.Reference_LHM) = UPPER(:ref_lhm_filter)))
    ),

    -- CTE 6: Dataset Formatting
    main_dataset AS (
        SELECT
            TO_CHAR(ag.CREATED, 'DD.MM.YYYY') AS Date_Only,
            CASE
                WHEN TO_CHAR(ag.CREATED, 'HH24:MI:SS') BETWEEN '05:50:00' AND '14:44:59' THEN 1
                WHEN TO_CHAR(ag.CREATED, 'HH24:MI:SS') BETWEEN '14:45:00' AND '23:59:00' THEN 2
                ELSE NULL
            END                                                 AS Shift,
            DECODE(
                COALESCE(JSON_VALUE(ag.t2_cust_data, '$.SOURCEID_SEKTOR'), JSON_VALUE(ag.t1_cust_data, '$.SOURCEID_SEKTOR')),
                '1', 'Zalando SE', '10', 'OSR', '11', 'OSR (OV)', 'Unknown'
            ) AS Source_Channel,
            ag.Quality,
            CASE
                WHEN ag.ZIEL_LHM LIKE '50%' THEN 'Overstock'
                WHEN COALESCE(JSON_VALUE(ag.t2_cust_data, '$.DISTRIBUTIONCHANNELID_ART'), JSON_VALUE(ag.t1_cust_data, '$.DISTRIBUTIONCHANNELID_ART')) = '4'  THEN 'Outlet'
                WHEN COALESCE(JSON_VALUE(ag.t2_cust_data, '$.DISTRIBUTIONCHANNELID_ART'), JSON_VALUE(ag.t1_cust_data, '$.DISTRIBUTIONCHANNELID_ART')) = '3'  THEN 'Overstock'
                WHEN COALESCE(JSON_VALUE(ag.t2_cust_data, '$.SOURCEID_SEKTOR'), JSON_VALUE(ag.t1_cust_data, '$.SOURCEID_SEKTOR')) = '11' THEN 'Overstock'
                ELSE 'Unknown'
            END AS Distribution_Channel,
            ag.Reference_LHM,
            ag.Category, -- Pulled directly from the union_data CTE
            ag.SORT_ID,
            ABS(ag.MENGE) AS Quantity
        FROM union_data ag
        WHERE NVL(ag.Source_LHM, 'x') <> NVL(ag.ZIEL_LHM, 'y') AND REGEXP_LIKE(ag.ZIEL_LHM, '^[0-9]+$')
    )

-- FINAL STRUCTURE:
SELECT
    Date_Only            AS "DATE",
    Shift                AS "SHIFT",
    Source_Channel       AS "SOURCE",
    Quality              AS "QUALITY",
    Distribution_Channel AS "DISTCHANNEL",
    Reference_LHM        AS "REFERENCE",
    Category             AS "CATEGORY",
    SORT_ID              AS "SORTID",
    SUM(Quantity)        AS "QUANTITY"
FROM
    main_dataset
GROUP BY
    Date_Only,
    Shift,
    Source_Channel,
    Quality,
    Distribution_Channel,
    Reference_LHM,
    Category,
    SORT_ID
ORDER BY
    TO_DATE(Date_Only, 'DD.MM.YYYY'),
    Shift,
    Reference_LHM
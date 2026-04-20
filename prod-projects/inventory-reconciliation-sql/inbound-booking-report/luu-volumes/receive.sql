WITH
    -- CTE 1: Normal Goods Initial
    normal_goods_t1 AS (
        SELECT
            hv.LOCAL_TRANSACTION_ID, hv.ARTNR, hv.ZIEL, hv.CREATEDBY, hv.LHMNR, hv.CREATED, hv.MENGE, hv.CUST_DATA,
            JSON_VALUE(hv.CUST_DATA, '$.REFERENCENUMBER_LHM') AS Reference_LHM
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 520 AND hv.MENGE < 0 AND hv.LAGBEZ IN ('Overstock', 'SZROV') --AND hv.ZIEL LIKE 'OV%'
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
        WHERE hv.TPARTNR = 614 AND hv.MENGE < 0 AND hv.LAGBEZ IN ('Overstock', 'SZROV') --AND hv.ZIEL LIKE 'OV%'
    ),

    -- CTE 4: Dummy Goods Completion
    dummy_goods_t2 AS (
        SELECT hv.LHMNR, hv.LOCAL_TRANSACTION_ID, hv.CUST_DATA
        FROM HISTORIE_V hv
        WHERE hv.TPARTNR = 207 AND hv.LAGBEZ IN ('Overstock', 'SZROV') AND hv.MENGE = 1 --AND hv.ZIEL LIKE 'OV%'
    ),

   -- CTE 5: Union Data with Script 2 Logic
    union_data AS (
        -- Part 1: Normal Goods
        SELECT
            t1.CREATED, t1.Reference_LHM, t1.MENGE,
            t1.CUST_DATA AS t1_cust_data, 
            t2.CUST_DATA AS t2_cust_data,
            t1.LHMNR AS Source_LHM, t2.LHMNR AS ZIEL_LHM,
            t1.ZIEL AS Original_Ziel, 
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
            t1.ZIEL AS Original_Ziel,
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
            TO_CHAR(ag.CREATED, 'YYYY_MM') AS year_cw,
            ag.Quality,
            CASE
                WHEN ag.ZIEL_LHM LIKE '50%' THEN 'Overstock'
                WHEN COALESCE(JSON_VALUE(ag.t2_cust_data, '$.DISTRIBUTIONCHANNELID_ART'), JSON_VALUE(ag.t1_cust_data, '$.DISTRIBUTIONCHANNELID_ART')) = '4'  THEN 'Outlet'
                WHEN COALESCE(JSON_VALUE(ag.t2_cust_data, '$.DISTRIBUTIONCHANNELID_ART'), JSON_VALUE(ag.t1_cust_data, '$.DISTRIBUTIONCHANNELID_ART')) = '3'  THEN 'Overstock'
                WHEN COALESCE(JSON_VALUE(ag.t2_cust_data, '$.SOURCEID_SEKTOR'), JSON_VALUE(ag.t1_cust_data, '$.SOURCEID_SEKTOR')) = '11' THEN 'Overstock'
                ELSE 'Unknown'
            END AS Distribution_Channel,
            ag.Reference_LHM,
            ag.Category, 
            ag.SORT_ID,
            ag.ZIEL_LHM AS Ziel, 
            ag.Original_Ziel, 
            ABS(ag.MENGE) AS Quantity
        FROM union_data ag
        WHERE NVL(ag.Source_LHM, 'x') <> NVL(ag.ZIEL_LHM, 'y') AND REGEXP_LIKE(ag.ZIEL_LHM, '^[0-9]+$')
    )

SELECT
    Date_Only,
    year_cw, 
    
    SUM(CASE WHEN Ziel LIKE '6%' AND Distribution_Channel = 'Outlet' THEN Quantity ELSE 0 END) AS OU_BSF_SORT,
    SUM(CASE WHEN Ziel LIKE '3%' AND Distribution_Channel = 'Outlet' THEN Quantity ELSE 0 END) AS OU_bigitems,
    SUM(CASE WHEN Distribution_Channel = 'Outlet' THEN Quantity ELSE 0 END) AS Total_Outlet,
    
    SUM(CASE WHEN Original_Ziel LIKE 'IN_SZR_OV' THEN Quantity ELSE 0 END) AS in_srz,
    
    SUM(CASE WHEN Ziel LIKE '6%' AND Distribution_Channel = 'Overstock' 
             AND (Original_Ziel IS NULL OR Original_Ziel NOT LIKE 'IN_SZR_OV') THEN Quantity ELSE 0 END) AS OV_Overstock_sort,
             
    SUM(CASE WHEN Ziel LIKE '5%' AND Distribution_Channel = 'Overstock' 
             AND (Original_Ziel IS NULL OR Original_Ziel NOT LIKE 'IN_SZR_OV') THEN Quantity ELSE 0 END) AS OV_Fin_AP,
             
    SUM(CASE WHEN Distribution_Channel = 'Overstock' 
             AND (Original_Ziel IS NULL OR Original_Ziel NOT LIKE 'IN_SZR_OV') THEN Quantity ELSE 0 END) AS Total_Overstock

FROM
    main_dataset
GROUP BY
    Date_Only,
    year_cw
ORDER BY 
    Date_Only DESC;

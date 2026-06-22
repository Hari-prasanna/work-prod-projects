WITH params AS (
    SELECT
        CASE WHEN :start_datetime IS NOT NULL THEN TO_DATE(:start_datetime, 'DD.MM.YYYY HH24:MI:SS') END AS start_dt,
        CASE WHEN :end_datetime IS NOT NULL THEN TO_DATE(:end_datetime, 'DD.MM.YYYY HH24:MI:SS') END AS end_dt
    FROM dual
),

-- 1. Get total clarifications per Article/Pallet to subtract from normal bookings
clarified_totals AS (
    SELECT 
        hv.ARTNR, 
        hv.LHMNR, 
        SUM(ABS(hv.MENGE)) AS total_clarified
    FROM HISTORIE_V hv
    CROSS JOIN params p
    WHERE hv.TYP_ID = 102 
      AND hv.TPARTNR = 402
      AND (hv.LHMNR LIKE '4%' OR hv.LHMNR LIKE '5%')
      AND (p.start_dt IS NULL OR hv.CREATED >= p.start_dt)
      AND (p.end_dt IS NULL OR hv.CREATED <= p.end_dt)
    GROUP BY 
        hv.ARTNR, 
        hv.LHMNR
),

-- 2. Group normal bookings together to prevent row splitting
booked_grouped AS (
    SELECT
        hv.LHMNR AS LC,
        hv.ARTNR AS "EANs",
        SUM(hv.MENGE) AS Total_Booked,
        MAX(hv.CREATED) AS raw_created, 
        MAX(hv.CUST_DATA) AS raw_cust_data 
    FROM HISTORIE_V hv
    CROSS JOIN params p
    WHERE hv.TYP_ID = 103 
      AND hv.TPARTNR = 202
      AND (hv.LHMNR LIKE '4%' OR hv.LHMNR LIKE '5%')
      AND (p.start_dt IS NULL OR hv.CREATED >= p.start_dt)
      AND (p.end_dt IS NULL OR hv.CREATED <= p.end_dt)
    GROUP BY 
        hv.LHMNR, 
        hv.ARTNR
),

-- 3. Build the main dataset (Normal Net Items + Clarification Items)
cte1 AS (
    
    -- DATASET 1: Normal Items (Net Quantity)

    SELECT 
        TO_CHAR(TRUNC(b.raw_created), 'YYYY-MM-DD') AS date_only,
        CASE
            WHEN TO_CHAR(b.raw_created, 'HH24:MI:SS') BETWEEN '05:50:00' AND '14:44:59' THEN 1
            WHEN TO_CHAR(b.raw_created, 'HH24:MI:SS') BETWEEN '14:45:00' AND '23:59:00' THEN 2
            ELSE NULL
        END AS Shift,
        b.LC,
        DECODE(JSON_VALUE(b.raw_cust_data, '$.CATEGORYID_ART'), '1', 'Schuhe', '2', 'Textil', '3', 'ACC', '4', 'Home', '5', 'Beauty') AS "Category",
        JSON_VALUE(b.raw_cust_data, '$.SORTINGCRITERIAID_ART') AS "Sort_ID",
        b."EANs",
        (b.Total_Booked - NVL(c.total_clarified, 0)) AS "Quantity",
        
        -- Fallback: Looks up history record 120 for quality
        (SELECT DECODE(JSON_VALUE(q.CUST_DATA, '$.QUALITYID_SEKTOR'), '1', 'A', '2', 'B', '3', 'C', '4', 'D')
         FROM HISTORIE_V q
         WHERE q.ARTNR = b."EANs" 
           AND q.LHMNR = b.LC 
           AND q.TYP_ID = 120 
           AND q.TPARTNR = 614 
           AND q.MENGE < 0 
           AND ROWNUM = 1) AS "Quality_Fallback",
        
        1 AS origin 
    FROM booked_grouped b
    LEFT JOIN clarified_totals c 
      ON b."EANs" = c.ARTNR 
     AND b.LC = c.LHMNR
    WHERE (b.Total_Booked - NVL(c.total_clarified, 0)) > 0
    
    UNION ALL

    -- DATASET 2: Clarification Items
    SELECT
        TO_CHAR(TRUNC(hv.CREATED), 'YYYY-MM-DD') AS date_only,
        CASE
            WHEN TO_CHAR(hv.CREATED, 'HH24:MI:SS') BETWEEN '05:50:00' AND '14:44:59' THEN 1
            WHEN TO_CHAR(hv.CREATED, 'HH24:MI:SS') BETWEEN '14:45:00' AND '23:59:00' THEN 2
            ELSE NULL
        END AS Shift,
        hv.LHMNR AS LC,
        DECODE(JSON_VALUE(hv.CUST_DATA, '$.CATEGORYID_ART'), '1', 'Schuhe', '2', 'Textil', '3', 'ACC', '4', 'Home', '5', 'Beauty') AS "Category",
        JSON_VALUE(hv.CUST_DATA, '$.SORTINGCRITERIAID_ART') AS "Sort_ID",
        hv.ARTNR AS "EANs",
        ABS(hv.MENGE) AS "Quantity",
        
        -- 4-TIER QUALITY FALLBACK LOGIC
        COALESCE(
            -- TIER 1: Check the item's own JSON data
            DECODE(JSON_VALUE(hv.CUST_DATA, '$.QUALITYID_SEKTOR'), '1', 'A', '2', 'B', '3', 'C', '4', 'D'),
            
            -- TIER 2: Check the target workstation (KF_ZIEL)
            (SELECT DECODE(JSON_VALUE(q.CUST_DATA, '$.QUALITYID_SEKTOR'), '1', 'A', '2', 'B', '3', 'C', '4', 'D')
             FROM HISTORIE_V q
             WHERE q.ARTNR = hv.ARTNR 
               AND TRIM(q.LHMNR) = 'KF_' || TRIM(hv.ZIEL)
               AND q.TYP_ID = 101 
               AND q.TPARTNR = 520 
               AND ROWNUM = 1),
               
            -- TIER 3 (NEW): Check ZAL_BESTAND (stock) for ANY item remaining on this LHM
            (SELECT MAX(zb."Qualität")
             FROM ZAL_BESTAND zb
             WHERE (zb."MainLhm" = hv.LHMNR OR zb."SubLhm" = hv.LHMNR)
               AND zb."Qualität" IS NOT NULL),
               
            -- TIER 4: Check the parent LHM in History for any recorded quality code
            (SELECT DECODE(JSON_VALUE(q.CUST_DATA, '$.QUALITYID_SEKTOR'), '1', 'A', '2', 'B', '3', 'C', '4', 'D')
             FROM HISTORIE_V q
             WHERE q.LHMNR = hv.LHMNR 
               AND JSON_VALUE(q.CUST_DATA, '$.QUALITYID_SEKTOR') IN ('1', '2', '3', '4')
               AND ROWNUM = 1)
        ) AS "Quality_Fallback",
        
        2 AS origin 
    FROM HISTORIE_V hv 
    CROSS JOIN params p
    WHERE hv.TYP_ID = 102 
      AND hv.TPARTNR = 402 
      AND (hv.LHMNR LIKE '4%' OR hv.LHMNR LIKE '5%')
      AND (p.start_dt IS NULL OR hv.CREATED >= p.start_dt)
      AND (p.end_dt IS NULL OR hv.CREATED <= p.end_dt)
),

-- 4. Deduplicate stock table to safely grab CTE2 Quality for Normal Items
cte2 AS (
    SELECT 
        LC, 
        "EANs", 
        "Quality"
    FROM (
        SELECT 
            u.LC, 
            u.ARTNR AS "EANs", 
            u."Qualität" AS "Quality",
            ROW_NUMBER() OVER(PARTITION BY u.LC, u.ARTNR ORDER BY u."Qualität" DESC) as rn
        FROM ZAL_BESTAND
        UNPIVOT (LC FOR lhm_type IN ("MainLhm", "SubLhm")) u
        WHERE (u.LC LIKE '4%' OR u.LC LIKE '5%')
    )
    WHERE rn = 1
)

-- FINAL SELECT

SELECT 
    t1.date_only AS "Date", 
    t1.Shift, 
    t1.LC AS "HUs", 
    t1."Category", 
    t1."Sort_ID",
    
    -- Checks cte2 first (for normal items). If missing (like clarifications), uses the robust Quality_Fallback
    COALESCE(t2."Quality", t1."Quality_Fallback") AS "Quality",
    
    -- Flags Clarification based on dataset origin
    CASE WHEN t1.origin = 2 THEN 'Yes' ELSE 'No' END AS "Clarification",
    
    SUM(t1."Quantity") AS "Items"
FROM cte1 t1
LEFT JOIN cte2 t2 
  ON t1."EANs" = t2."EANs" 
 AND t1.LC = t2.LC
GROUP BY 
    t1.date_only, 
    t1.Shift, 
    t1.LC, 
    t1."Category", 
    t1."Sort_ID",
    COALESCE(t2."Quality", t1."Quality_Fallback"),
    CASE WHEN t1.origin = 2 THEN 'Yes' ELSE 'No' END
HAVING SUM(t1."Quantity") > 0
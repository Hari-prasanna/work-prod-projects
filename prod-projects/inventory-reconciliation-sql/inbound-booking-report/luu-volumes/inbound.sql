WITH raw_data AS (
    SELECT 
        hv.TRANSPORTLHMNR, 
        hv.ZIEL, 
        hv.MENGE, 
        hv."CREATED",
        hv."SEQUENCE",
        JSON_VALUE(hv.CUST_DATA, '$.QUALITYID_ART') AS q_id,
        JSON_VALUE(hv.CUST_DATA, '$.REFERENCENUMBER_LHM') AS REFERENCE_LHM
    FROM HISTORIE_V hv 
    WHERE hv.TYP_ID = 103 
      AND hv."CREATED" BETWEEN TO_DATE(:start_datetime, 'DD.MM.YYYY HH24:MI:SS') 
                           AND TO_DATE(:end_datetime, 'DD.MM.YYYY HH24:MI:SS')
      AND hv.ZIEL = 'WE' 
      AND hv.HOSTAUFTRNR LIKE 'UN%'
)
SELECT  
    TO_CHAR("CREATED", 'DD.MM.YYYY') AS date_only, 
    TO_CHAR("CREATED", 'YYYY_MM') AS year_cw, 
    TRANSPORTLHMNR, 
    ZIEL AS SOURCE, 
    REFERENCE_LHM, 
    CASE q_id
        WHEN '1' THEN 'A'
        WHEN '2' THEN 'B'
        WHEN '3' THEN 'C'
        WHEN '4' THEN 'D'
        ELSE 'Unknown'
    END AS Quality, 
    MENGE 
FROM raw_data

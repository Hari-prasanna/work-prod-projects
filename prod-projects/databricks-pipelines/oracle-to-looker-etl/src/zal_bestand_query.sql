SELECT *
FROM ZAL_BESTAND zb
WHERE
    zb."Category" = :category
    AND zb.BEZ NOT LIKE 'T%' AND zb.BEZ NOT LIKE 'BSF_T%'
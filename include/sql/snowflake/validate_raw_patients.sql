-- Validate Patient RAW load.

SELECT
    COUNT(*) AS actual_row_count,
    COUNT(ID) AS non_null_id_count,
    COUNT(DISTINCT ID) AS unique_id_count
FROM PATIENT360_PROD.EHR_RAW.RAW_PATIENTS;
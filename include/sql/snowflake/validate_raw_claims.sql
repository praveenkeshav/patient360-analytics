-- Validate Claims RAW load.
SELECT
    COUNT(*) AS actual_row_count,
    COUNT(CLAIM_ID) AS non_null_claim_id_count,
    COUNT(DISTINCT CLAIM_ID) AS unique_claim_id_count
FROM PATIENT360_PROD.CLAIMS_RAW.RAW_CLAIMS;
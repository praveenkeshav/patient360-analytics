-- Validate Claim Transactions RAW load.
SELECT
    COUNT(*) AS actual_row_count,
    COUNT(TRANSACTION_ID) AS non_null_transaction_id_count,
    COUNT(DISTINCT TRANSACTION_ID) AS unique_transaction_id_count
FROM PATIENT360_PROD.CLAIMS_RAW.RAW_CLAIM_TRANSACTIONS;
-- Validate Conditions RAW load.

-- Validate Conditions RAW load.

SELECT
    COUNT(*) >= 30000
    AND COUNT(PATIENT) = COUNT(*)
    AND COUNT(CODE) = COUNT(*)
    AND COUNT("START") = COUNT(*)
    AND COUNT_IF("STOP" < "START") = 0
FROM PATIENT360_PROD.EHR_RAW.RAW_CONDITIONS;
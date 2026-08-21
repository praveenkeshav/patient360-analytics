-- Validate FHIR Labs RAW load.

SELECT
    COUNT(*) >= 1
    AND COUNT(observation_id) = COUNT(*)
    AND COUNT(patient_id) = COUNT(*)
    AND COUNT(observation_date) = COUNT(*)
    AND COUNT(loinc_code) = COUNT(*)
    AND COUNT_IF(value IS NOT NULL AND is_abnormal IS NULL) = 0
FROM PATIENT360_PROD.EHR_RAW.RAW_LABS;
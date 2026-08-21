SELECT
    COUNT(*) >= 60000
    AND COUNT("id") = COUNT(*)
    AND COUNT(DISTINCT "id") = COUNT(*)
    AND COUNT("patient") = COUNT(*)
    AND COUNT("start") = COUNT(*)
    AND COUNT("stop") = COUNT(*)
    AND COUNT_IF("stop" < "start") = 0
FROM PATIENT360_PROD.EHR_RAW.RAW_ENCOUNTERS;
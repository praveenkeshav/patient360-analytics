-- Create FHIR Labs RAW table if it does not exist.
-- Create RAW table for processed FHIR laboratory observations.
CREATE OR REPLACE TABLE PATIENT360_PROD.EHR_RAW.RAW_LABS (
    observation_id VARCHAR,
    patient_id VARCHAR,
    encounter_id VARCHAR,
    observation_date TIMESTAMP_NTZ,
    loinc_code VARCHAR,
    observation_name VARCHAR,
    value NUMBER(12, 8),
    value_text VARCHAR,
    unit VARCHAR,
    lab_name VARCHAR,
    unit_reference VARCHAR,
    normal_low NUMBER(4, 1),
    normal_high NUMBER(4, 1),
    is_abnormal BOOLEAN
);
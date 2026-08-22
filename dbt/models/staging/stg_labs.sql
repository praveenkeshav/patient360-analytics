with source_data as (

    select *
    from {{ source('ehr_raw', 'raw_labs') }}

),

cleaned_data as (

    select
        "OBSERVATION_ID" as lab_id,
        "PATIENT_ID" as patient_id,
        "ENCOUNTER_ID" as encounter_id,

        "OBSERVATION_DATE" as lab_datetime,

        nullif(trim("OBSERVATION_NAME"), '') as labtest_name,
        nullif(trim("LOINC_CODE"), '') as loinc_code,

        "VALUE" as lab_value_numeric,
        nullif(trim("VALUE_TEXT"), '') as lab_value_text,
        nullif(trim("UNIT"), '') as lab_unit,


        "NORMAL_LOW" as normal_low,
        "NORMAL_HIGH" as normal_high,
        "IS_ABNORMAL" as is_abnormal

    from source_data

),

final as (

    select
        lab_id,
        patient_id,
        encounter_id,

        lab_datetime,
        labtest_name,
        loinc_code,

        lab_value_numeric,
        lab_value_text,
        lab_unit,

        normal_low,
        normal_high,
        is_abnormal,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'EHR_RAW.RAW_LABS' as _record_source

    from cleaned_data

)

select *
from final
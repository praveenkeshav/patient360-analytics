with source_data as (

    select
        lab_id,
        patient_id,
        encounter_id,
        lab_datetime,
        loinc_code,
        labtest_name,
        lab_value_numeric,
        lab_value_text,
        lab_unit,
        normal_low,
        normal_high,
        is_abnormal
    from {{ ref('stg_labs') }}

),

business_logic as (

    select
        lab_id,
        patient_id,
        encounter_id,
        lab_datetime,
        loinc_code,
        labtest_name,
        lab_value_numeric,
        lab_value_text,
        lab_unit,
        normal_low,
        normal_high,
        is_abnormal

    from source_data

),

final as (

    select
        lab_id,
        patient_id,
        encounter_id,
        lab_datetime,
        loinc_code,
        labtest_name,
        lab_value_numeric,
        lab_value_text,
        lab_unit,
        normal_low,
        normal_high,
        is_abnormal,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'STAGING.STG_LABS' as _record_source

    from business_logic

)

select *
from final
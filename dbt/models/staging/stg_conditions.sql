with source_data as (

    select *
    from {{ source('ehr_raw', 'raw_conditions') }}

),

cleaned_data as (

    select
        cast("CODE" as varchar) as condition_code,

        nullif(trim("DESCRIPTION"), '') as condition_description,

        "PATIENT" as patient_id,
        "ENCOUNTER" as encounter_id,

        "START" as condition_start_date,
        "STOP" as condition_end_date,

        "CONDITION_DURATION_DAYS" as condition_duration_days

    from source_data

),

final as (

    select
        condition_code,
        condition_description,

        patient_id,
        encounter_id,

        condition_start_date,
        condition_end_date,

        condition_duration_days,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'EHR_RAW.RAW_CONDITIONS' as _record_source

    from cleaned_data

)

select *
from final
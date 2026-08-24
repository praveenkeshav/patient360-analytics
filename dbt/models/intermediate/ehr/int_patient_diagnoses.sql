with source_data as (

    select
        condition_code,
        condition_description,
        condition_duration_days,
        condition_start_date,
        condition_end_date,
        encounter_id,
        patient_id
    from {{ ref('stg_conditions') }}

),

business_logic as (

    select
        patient_id,
        encounter_id,
        condition_code,
        condition_description,
        condition_start_date,
        condition_end_date,
        condition_duration_days

    from source_data

),

final as (

    select
        patient_id,
        encounter_id,
        condition_code,
        condition_description,
        condition_start_date,
        condition_end_date,
        condition_duration_days,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'STAGING.STG_CONDITIONS' as _record_source

    from business_logic

)

select *
from final
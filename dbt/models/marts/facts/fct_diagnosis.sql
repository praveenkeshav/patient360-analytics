with diagnoses as (

    select
        patient_id,
        encounter_id,
        condition_code,
        condition_description,
        condition_start_date,
        condition_end_date,
        condition_duration_days

    from {{ ref('int_patient_diagnoses') }}

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

        case
            when condition_duration_days >= 90 then true
            else false
        end as is_chronic_condition,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'INTERMEDIATE.INT_PATIENT_DIAGNOSES' as _record_source

    from diagnoses

)

select *
from final
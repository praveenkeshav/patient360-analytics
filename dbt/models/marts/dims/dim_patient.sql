with patients as (

    select
        patient_id,
        birth_date,
        gender,
        race,
        ethnicity,
        is_deceased

    from {{ ref('stg_patients') }}

)

select
    patient_id,
    birth_date,
    gender,
    race,
    ethnicity,
    is_deceased,

    case
        when is_deceased then 'DECEASED'
        else 'ACTIVE'
    end as patient_status,

    current_timestamp() as _loaded_at,
    '{{ invocation_id }}' as _dbt_invocation_id,
    'STAGING.STG_PATIENTS' as _record_source

from patients
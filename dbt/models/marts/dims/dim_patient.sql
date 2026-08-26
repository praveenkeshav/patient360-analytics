with patients as (

    select
        patient_id,
        gender,
        age,
        age_group,
        race,
        ethnicity,
        is_deceased

    from {{ ref('stg_patients') }}

)

select
    patient_id,
    age,
    age_group,
    gender,
    race,
    ethnicity,
    is_deceased,

    current_timestamp() as _loaded_at,
    '{{ invocation_id }}' as _dbt_invocation_id,
    'STAGING.STG_PATIENTS' as _record_source

from patients
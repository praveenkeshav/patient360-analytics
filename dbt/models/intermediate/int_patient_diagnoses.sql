with diagnoses as (

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

patients as (

    select
        patient_id,
        birth_date,
        death_date,
        gender,
        race,
        ethnicity
    from {{ ref('stg_patients') }}

),

encounters as (

    select
        encounter_id,
        encounter_start,
        encounter_end,
        encounter_class,
        organization_id,
        provider_id
    from {{ ref('stg_encounters') }}

),

final as (

    select
        d.patient_id,
        d.encounter_id,

        -- Diagnosis attributes
        d.condition_code,
        d.condition_description,
        d.condition_start_date,
        d.condition_end_date,
        d.condition_duration_days,

        -- Patient attributes
        p.birth_date,
        p.death_date,
        p.gender,
        p.race,
        p.ethnicity,

        -- Encounter attributes
        e.encounter_start,
        e.encounter_end,
        e.encounter_class,
        e.organization_id,
        e.provider_id,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'STAGING.STG_CONDITIONS + STAGING.STG_PATIENTS + STAGING.STG_ENCOUNTERS' as _record_source

    from diagnoses d

    left join patients p
        on d.patient_id = p.patient_id

    left join encounters e
        on d.encounter_id = e.encounter_id

)

select * from final
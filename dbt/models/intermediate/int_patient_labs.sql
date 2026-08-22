with labs as (

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
        l.lab_id,
        l.patient_id,
        l.encounter_id,

        -- Lab attributes
        l.lab_datetime,
        l.loinc_code,
        l.labtest_name,
        l.lab_value_numeric,
        l.lab_value_text,
        l.lab_unit,
        l.normal_low,
        l.normal_high,
        l.is_abnormal,

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
        'STAGING.STG_LABS + STAGING.STG_PATIENTS + STAGING.STG_ENCOUNTERS' as _record_source

    from labs l

    left join patients p
        on l.patient_id = p.patient_id

    left join encounters e
        on l.encounter_id = e.encounter_id

)

select * from final
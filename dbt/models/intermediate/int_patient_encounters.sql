with patients as (

    select
        patient_id,
        birth_date,
        death_date,
        marital_status,
        race,
        ethnicity,
        gender,
        birth_place,
        city,
        state,
        county,
        zip,
        lat,
        lon,
        healthcare_expenses,
        healthcare_coverage,
        is_deceased
    from {{ ref('stg_patients') }}

),

encounters as (

    select
        encounter_id,
        patient_id,
        encounter_start,
        encounter_end,
        organization_id,
        provider_id,
        payer_id,
        encounter_class,
        encounter_code,
        encounter_description,
        base_encounter_cost,
        total_claim_cost,
        payer_coverage,
        reason_code,
        reason_description,
        length_of_stay_days
    from {{ ref('stg_encounters') }}

),

final as (

    select
        e.encounter_id,
        e.patient_id,

        -- Patient attributes
        p.birth_date,
        p.death_date,
        p.marital_status,
        p.race,
        p.ethnicity,
        p.gender,
        p.birth_place,
        p.city,
        p.state,
        p.county,
        p.zip,
        p.lat,
        p.lon,
        p.healthcare_expenses,
        p.healthcare_coverage,
        p.is_deceased,

        -- Encounter attributes
        e.encounter_start,
        e.encounter_end,
        e.organization_id,
        e.provider_id,
        e.payer_id,
        e.encounter_class,
        e.encounter_code,
        e.encounter_description,

        -- Financial attributes
        e.base_encounter_cost,
        e.total_claim_cost,
        e.payer_coverage,

        -- Reason attributes
        e.reason_code,
        e.reason_description,

        -- Encounter metric
        e.length_of_stay_days,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'STAGING.STG_PATIENTS + STAGING.STG_ENCOUNTERS' as _record_source

    from encounters e

    left join patients p
        on e.patient_id = p.patient_id

)

select * from final
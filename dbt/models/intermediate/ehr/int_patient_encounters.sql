with source_data as (

    select
        encounter_id,
        patient_id,
        encounter_start,
        encounter_end,
        encounter_class,
        base_encounter_cost,
        total_claim_cost,
        payer_coverage,
        length_of_stay_days
    from {{ ref('stg_encounters') }}

),

business_logic as (

    select
        encounter_id,
        patient_id,
        encounter_start,
        encounter_end,
        encounter_class,
        length_of_stay_days,
        base_encounter_cost,
        total_claim_cost,
        payer_coverage

    from source_data

),

final as (

    select
        encounter_id,
        patient_id,
        encounter_start,
        encounter_end,
        encounter_class,
        length_of_stay_days,
        base_encounter_cost,
        total_claim_cost,
        payer_coverage,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'STAGING.STG_ENCOUNTERS' as _record_source

    from business_logic

)

select *
from final
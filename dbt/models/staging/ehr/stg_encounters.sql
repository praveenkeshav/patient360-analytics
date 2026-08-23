with source_data as(
    select
     *
    from {{ source('ehr_raw', 'raw_encounters') }}
),

cleaned_data as (
    select
        "id" as encounter_id,
        "patient" as patient_id,
        to_timestamp_ntz("start", 9) as encounter_start,
        to_timestamp_ntz("stop", 9) as encounter_end,

        nullif(trim("organization"), '') as organization_id,
        nullif(trim("provider"), '') as provider_id,

        case
            when lower(trim("encounterclass")) = 'ambulatory' then 'Ambulatory'
            when lower(trim("encounterclass")) = 'emergency' then 'Emergency'
            when lower(trim("encounterclass")) = 'inpatient' then 'Inpatient'
            when lower(trim("encounterclass")) = 'urgentcare' then 'Urgent Care'
            when lower(trim("encounterclass")) = 'wellness' then 'Wellness'
            when lower(trim("encounterclass")) = 'home' then 'Home'
            when nullif(trim("encounterclass"), '') is null then 'Unknown'
            else initcap(trim("encounterclass"))
        end as encounter_class,

        "code" as encounter_code,
        nullif(trim("description"), '') as encounter_description,

        "base_encounter_cost" as base_encounter_cost,
        "total_claim_cost" as total_claim_cost,
        "payer_coverage" as payer_coverage,
        nullif(trim("payer"), '') as payer_id,

        "reasoncode" as reason_code,
        nullif(trim("reasondescription"), '') as reason_description,
        "length_of_stay_days" as length_of_stay_days

    from source_data
),

final as (

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

        length_of_stay_days,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'EHR_RAW.RAW_ENCOUNTERS' as _record_source

    from cleaned_data
)

select * from final
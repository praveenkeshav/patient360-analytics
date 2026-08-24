with source_data as (

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

    from {{ ref('int_patient_encounters') }}

),

inpatient_encounters as (

    select
        encounter_id,
        patient_id,
        encounter_start,
        encounter_end,

        lag(encounter_end) over (
            partition by patient_id
            order by encounter_start
        ) as previous_inpatient_end

    from source_data

    where encounter_class = 'Inpatient'

),

business_logic as (

    select
        encounter_id,

        previous_inpatient_end,

        datediff(
            day,
            previous_inpatient_end,
            encounter_start
        ) as days_since_previous_inpatient,

        case
            when previous_inpatient_end is not null
             and datediff(
                    day,
                    previous_inpatient_end,
                    encounter_start
                 ) between 1 and 30
            then true
            else false
        end as is_30_day_readmission

    from inpatient_encounters

),

final as (

    select
        s.encounter_id,
        s.patient_id,

        s.encounter_start,
        s.encounter_end,
        s.encounter_class,
        s.length_of_stay_days,

        s.base_encounter_cost,
        s.total_claim_cost,
        s.payer_coverage,

        b.previous_inpatient_end,
        b.days_since_previous_inpatient,
        coalesce(b.is_30_day_readmission, false) as is_30_day_readmission,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'INTERMEDIATE.INT_PATIENT_ENCOUNTERS' as _record_source

    from source_data s

    left join business_logic b
        on s.encounter_id = b.encounter_id

)

select *
from final
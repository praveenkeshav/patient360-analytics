with chronic_conditions as (

    select distinct
        patient_id,
        age,
        age_group,
        disease_cohort,
        is_chronic_cohort

    from {{ ref('int_chronic_condition_cohorts') }}

    where age >= 18
      and is_chronic_cohort = true

),

final as (

    select
        patient_id,
        age,
        age_group,
        disease_cohort,
        is_chronic_cohort,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'INTERMEDIATE.INT_CHRONIC_CONDITION_COHORTS' as _record_source

    from chronic_conditions

)

select *
from final
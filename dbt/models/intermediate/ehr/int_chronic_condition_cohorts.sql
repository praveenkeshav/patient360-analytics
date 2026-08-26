with diagnoses as (

    select
        patient_id,
        condition_description,
        condition_start_date
    from {{ ref('fct_diagnosis') }}

),

patients as (

    select
        patient_id,
        age,
        age_group
    from {{ ref('stg_patients') }}

),

mapped_conditions as (

    select
        d.patient_id,
        p.age,
        p.age_group,
        d.condition_description,
        d.condition_start_date,

        case

            when lower(d.condition_description) = 'hypertension'
                then 'Hypertension'

            when lower(d.condition_description) in (
                'hyperlipidemia',
                'hypertriglyceridemia (disorder)'
            )
                then 'Lipid Disorders'

            when lower(d.condition_description) in (
                'diabetes',
                'prediabetes',
                'diabetic renal disease (disorder)',
                'hyperglycemia (disorder)'
            )
                then 'Diabetes / Glycemic Disorders'

            when lower(d.condition_description) in (
                'chronic kidney disease stage 1 (disorder)',
                'chronic kidney disease stage 2 (disorder)'
            )
                then 'Chronic Kidney Disease'

            when lower(d.condition_description) in (
                'myocardial infarction',
                'cardiac arrest',
                'coronary heart disease',
                'chronic congestive heart failure (disorder)',
                'injury of heart (disorder)',
                'atrial fibrillation'
            )
                then 'Cardiovascular Disease'

            when lower(d.condition_description) in (
                'osteoarthritis of hip',
                'osteoarthritis of knee',
                'rheumatoid arthritis',
                'osteoporosis (disorder)'
            )
                then 'Arthritis / Bone Disease'

            when lower(d.condition_description) in (
                'asthma',
                'chronic obstructive bronchitis (disorder)',
                'acute bronchitis (disorder)',
                'pneumonia',
                'pneumonia (disorder)'
            )
                then 'Respiratory Disease'

            when lower(d.condition_description) in (
                'body mass index 30+ - obesity (finding)',
                'body mass index 40+ - severely obese (finding)'
            )
                then 'Obesity'

            when lower(d.condition_description) in (
                'chronic sinusitis (disorder)',
                'sinusitis (disorder)'
            )
                then 'Sinusitis'

            when lower(d.condition_description) in (
                'familial alzheimer''s disease of early onset (disorder)',
                'alzheimer''s disease (disorder)'
            )
                then 'Alzheimer''s Disease'

            else null

        end as disease_cohort

    from diagnoses d

    inner join patients p
        on d.patient_id = p.patient_id

),

classified_conditions as (

    select
        patient_id,
        age,
        age_group,
        disease_cohort,
        condition_description,
        condition_start_date,

        case
            when lower(condition_description) in (
                'acute bronchitis (disorder)',
                'pneumonia',
                'pneumonia (disorder)'
            )
                then false

            else true

        end as is_chronic_cohort

    from mapped_conditions

),

final as (

    select
        patient_id,
        age,
        age_group,
        disease_cohort,
        condition_description,
        condition_start_date,
        is_chronic_cohort,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'FCT_DIAGNOSIS + STG_PATIENTS' as _record_source

    from classified_conditions

    where disease_cohort is not null

)

select *
from final
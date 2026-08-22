select
    patient_id,
    encounter_id,
    condition_code,
    condition_start_date,
    count(*) as record_count

from {{ ref('stg_conditions') }}

group by
    patient_id,
    encounter_id,
    condition_code,
    condition_start_date

having count(*) > 1
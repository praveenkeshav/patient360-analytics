with source_data as(

    select *
    from {{ source('ehr_raw', 'raw_patients') }}

),

cleaned_data as (

    select
        patient_id_broken as patient_id,
        birthdate as birth_date,
        deathdate as death_date,
        
        case
            when upper(trim(marital)) = 'M' then 'Married'
            when upper(trim(marital)) = 'S' then 'Single'
            when nullif(trim(marital), '') is null then 'Unknown'
            else initcap(trim(marital))
        end as marital_status,

        case
            when upper(trim(gender)) = 'M' then 'Male'
            when upper(trim(gender)) = 'F' then 'Female'
            when nullif(trim(gender), '') is null then 'Unknown'
            else initcap(trim(gender))
        end as gender,

        case
            when upper(trim(race)) = 'WHITE' then 'White'
            when upper(trim(race)) = 'BLACK' then 'Black'
            when upper(trim(race)) = 'ASIAN' then 'Asian'
            when upper(trim(race)) = 'NATIVE' then 'Native'
            when nullif(trim(race), '') is null then 'Unknown'
            else initcap(trim(race))
        end as race,

        case
            when upper(trim(ethnicity)) = 'HISPANIC' then 'Hispanic'
            when upper(trim(ethnicity)) = 'NONHISPANIC' then 'Non-Hispanic'
            when nullif(trim(ethnicity), '') is null then 'Unknown'
            else initcap(trim(ethnicity))
        end as ethnicity,

        nullif(trim(birthplace), '') as birth_place,
        nullif(trim(city), '') as city,
        nullif(trim(state), '') as state,
        nullif(trim(county), '') as county,

        cast(zip as varchar) as zip,

        lat,
        lon,

        healthcare_expenses,
        healthcare_coverage,

        coalesce(is_deceased, false) as is_deceased

    from source_data
),

final as (
    select
        patient_id,
        birth_date,
        death_date,
        marital_status,
        gender,
        race,
        ethnicity,
        birth_place,
        city,
        state,
        county,
        zip,
        lat,
        lon,
        healthcare_expenses,
        healthcare_coverage,
        is_deceased,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'EHR_RAW.RAW_PATIENTS' as _record_source

    from cleaned_data
)

select *
from final
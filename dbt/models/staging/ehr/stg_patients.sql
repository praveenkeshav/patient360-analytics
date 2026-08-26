with source_data as (

    select *
    from {{ source('ehr_raw', 'raw_patients') }}

),

cleaned_data as (

    select
        id as patient_id,
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

        healthcare_expenses,
        healthcare_coverage,

        coalesce(is_deceased, false) as is_deceased

    from source_data

),

patient_with_age as (

    select
        *,
        datediff(
            year,
            birth_date,
            '2021-11-15'::date
        )
        -
        case
            when dateadd(
                year,
                datediff(year, birth_date, '2021-11-15'::date),
                birth_date
            ) > '2021-11-15'::date
            then 1
            else 0
        end as age

    from cleaned_data

),

final as (

    select
        patient_id,
        birth_date,
        death_date,
        age,

        case
            when age between 18 and 34 then '18-34'
            when age between 35 and 64 then '35-64'
            when age >= 65 then '65+'
            else 'Under 18 / Unknown'
        end as age_group,

        marital_status,
        gender,
        race,
        ethnicity,
        birth_place,
        city,
        state,
        county,
        zip,
        healthcare_expenses,
        healthcare_coverage,
        is_deceased,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'EHR_RAW.RAW_PATIENTS' as _record_source

    from patient_with_age

)

select *
from final
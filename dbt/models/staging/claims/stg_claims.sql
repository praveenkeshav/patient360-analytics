{{ config(
    materialized='incremental',
    unique_key='claim_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with source_data as (

    select *
    from {{ source('claims_raw', 'raw_claims') }}

    {% if is_incremental() %}
        where service_date >= (
            select coalesce(dateadd(day, -1, max(service_date)), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

),

cleaned_data as (

    select

        claim_id,
        patient_id,
        provider_id,

        primary_insurance_id,
        secondary_insurance_id,

        department_id,
        patient_department_id,

        diagnosis_code_1,
        diagnosis_code_2,
        diagnosis_code_3,
        diagnosis_code_4,
        diagnosis_code_5,
        diagnosis_code_6,
        diagnosis_code_7,
        diagnosis_code_8,

        referring_provider_id,
        appointment_id,

        current_illness_date,
        service_date,
        supervising_provider_id,

        nullif(trim(status_1), '') as status_1,
        nullif(trim(status_2), '') as status_2,
        nullif(trim(status_p), '') as status_p,

        outstanding_1,
        outstanding_2,
        outstanding_p,

        last_billed_date_1,
        last_billed_date_2,
        last_billed_date_p,

        healthcare_claim_type_id_1,
        healthcare_claim_type_id_2

    from source_data

),

final as (

    select
        claim_id,
        patient_id,
        provider_id,
        primary_insurance_id,
        secondary_insurance_id,
        department_id,
        patient_department_id,

        diagnosis_code_1,
        diagnosis_code_2,
        diagnosis_code_3,
        diagnosis_code_4,
        diagnosis_code_5,
        diagnosis_code_6,
        diagnosis_code_7,
        diagnosis_code_8,

        referring_provider_id,
        appointment_id,

        current_illness_date,
        service_date,
        supervising_provider_id,

        status_1,
        status_2,
        status_p,

        outstanding_1,
        outstanding_2,
        outstanding_p,

        last_billed_date_1,
        last_billed_date_2,
        last_billed_date_p,

        healthcare_claim_type_id_1,
        healthcare_claim_type_id_2,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'CLAIMS_RAW.RAW_CLAIMS' as _record_source

    from cleaned_data

)

select *
from final
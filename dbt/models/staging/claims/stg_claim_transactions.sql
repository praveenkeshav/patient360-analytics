with source_data as (

    select *
    from {{ source('claims_raw', 'raw_claim_transactions') }}

),

cleaned_data as (

    select

        transaction_id,
        claim_id,
        charge_id,
        patient_id,

        nullif(trim(transaction_type), '') as transaction_type,

        amount,

        nullif(trim(payment_method), '') as payment_method,

        transaction_start,
        transaction_end,

        nullif(trim(place_of_service), '') as place_of_service,

        procedure_code,

        nullif(trim(modifier_1), '') as modifier_1,
        nullif(trim(modifier_2), '') as modifier_2,

        diagnosis_ref_1,
        diagnosis_ref_2,
        diagnosis_ref_3,
        diagnosis_ref_4,

        units,
        department_id,

        nullif(trim(notes), '') as notes,

        unit_amount,

        transfer_out_id,

        nullif(trim(transfer_type), '') as transfer_type,

        payment_amount,
        adjustment_amount,
        transfer_amount,
        outstanding_amount,

        appointment_id,

        nullif(trim(line_note), '') as line_note,

        patient_insurance_id,
        fee_schedule_id,
        provider_id,
        supervising_provider_id,

    from source_data

),

final as (

    select
        transaction_id,
        claim_id,
        charge_id,
        patient_id,
        transaction_type,
        amount,
        payment_method,
        transaction_start,
        transaction_end,
        place_of_service,
        procedure_code,
        modifier_1,
        modifier_2,
        diagnosis_ref_1,
        diagnosis_ref_2,
        diagnosis_ref_3,
        diagnosis_ref_4,
        units,
        department_id,
        notes,
        unit_amount,
        transfer_out_id,
        transfer_type,
        payment_amount,
        adjustment_amount,
        transfer_amount,
        outstanding_amount,
        appointment_id,
        line_note,
        patient_insurance_id,
        fee_schedule_id,
        provider_id,
        supervising_provider_id

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'CLAIMS_RAW.RAW_CLAIM_TRANSACTIONS' as _record_source

    from cleaned_data

)

select *
from final
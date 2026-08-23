with source_data as (

    select *
    from {{ ref('stg_claim_transactions') }}

),

business_logic as (

    select
        transaction_id,
        claim_id,
        patient_id,
        amount,
        transaction_start,
        transaction_end,
        appointment_id

    from source_data

),

final as (

    select
        transaction_id,
        claim_id,
        patient_id,
        amount,
        transaction_start,
        transaction_end,
        appointment_id,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'STAGING.STG_CLAIM_TRANSACTIONS' as _record_source

    from business_logic

)

select *
from final
with source_data as (

    select *
    from {{ ref('stg_claim_transactions') }}

),

final as (

    select
        transaction_id,
        claim_id,
        patient_id,
        amount,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'STAGING.STG_CLAIM_TRANSACTIONS' as _record_source

    from source_data

)

select *
from final
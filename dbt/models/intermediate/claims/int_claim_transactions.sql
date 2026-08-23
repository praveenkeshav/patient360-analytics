with source_data as (

    select *
    from {{ ref('stg_claim_transactions') }}

),

deduplicated as (

    select
        transaction_id,
        claim_id,
        patient_id,
        amount

    from source_data

    qualify row_number() over (
        partition by transaction_id
        order by transaction_id
    ) = 1

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

    from deduplicated

)

select *
from final
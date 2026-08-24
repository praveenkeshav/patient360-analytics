with claims as (

    select
        claim_id,
        patient_id,
        service_date
    from {{ ref('int_claims') }}

),

transactions as (

    select
        claim_id,
        count(transaction_id) as transaction_count,
        sum(amount) as total_claim_cost
    from {{ ref('int_claim_transactions') }}
    group by claim_id

),

final as (

    select
        c.claim_id,
        c.patient_id,
        c.service_date,

        coalesce(t.total_claim_cost, 0) as total_claim_cost,
        coalesce(t.transaction_count, 0) as transaction_count,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'INTERMEDIATE.INT_CLAIMS + INTERMEDIATE.INT_CLAIM_TRANSACTIONS' as _record_source

    from claims c

    left join transactions t
        on c.claim_id = t.claim_id

)

select *
from final
with source_data as (

    select *
    from {{ ref('stg_claims') }}

),

deduplicated as (

    select
        claim_id,
        patient_id,
        service_date,
        outstanding_1,
        outstanding_2,
        outstanding_p

    from source_data

    qualify row_number() over (
        partition by claim_id
        order by service_date
    ) = 1

),

final as (

    select
        claim_id,
        patient_id,
        service_date,

        outstanding_1,
        outstanding_2,
        outstanding_p,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'STAGING.STG_CLAIMS' as _record_source

    from deduplicated

)

select *
from final
with source_data as (

    select *
    from {{ ref('stg_claims') }}

),

business_logic as (

    select
        claim_id,
        patient_id,
        service_date

    from source_data

),

final as (

    select
        claim_id,
        patient_id,
        service_date,

        current_timestamp() as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id,
        'STAGING.STG_CLAIMS' as _record_source

    from business_logic

)

select *
from final
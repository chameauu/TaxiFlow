with quarantine as (
    select count(*) as quarantined_rows
    from {{ ref('stg_tlc_quarantine') }}
),

reasons as (
    select cast(sum(quarantined_rows) as bigint) as quarantined_rows
    from {{ ref('fct_quality_reasons') }}
)

select
    quarantine.quarantined_rows as staging_quarantined_rows,
    reasons.quarantined_rows as reason_quarantined_rows
from quarantine
cross join reasons
where quarantine.quarantined_rows != reasons.quarantined_rows

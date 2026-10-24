select
    quarantine_reason,
    count(*) as quarantined_rows,
    max(ingested_at) as last_seen_at
from {{ ref('stg_tlc_quarantine') }}
group by quarantine_reason

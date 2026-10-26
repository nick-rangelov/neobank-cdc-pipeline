-- Deduplication must remove repeated deliveries and nothing else.
-- Returns a row (= fails) if distinct seqs in raw differ from rows in staging.
select raw_distinct_seqs, staged_rows
from (
    select count(distinct seq) as raw_distinct_seqs
    from {{ source('raw_cdc', 'customer_events') }}
) r
cross join (
    select count(*) as staged_rows
    from {{ ref('stg_customer_events') }}
) s
where raw_distinct_seqs != staged_rows

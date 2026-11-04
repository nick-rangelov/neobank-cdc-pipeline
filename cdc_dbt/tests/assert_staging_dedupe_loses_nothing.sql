-- Deduplication must remove repeated deliveries and nothing else.
-- Returns a row (= fails) if distinct seqs in raw differ from rows in staging.
with raw_count as (

    select count(distinct seq) as raw_distinct_seqs
    from {{ source('raw_cdc', 'customer_events') }}

),

staged_count as (

    select count(*) as staged_rows
    from {{ ref('stg_customer_events') }}

)

select
    raw_count.raw_distinct_seqs,
    staged_count.staged_rows
from raw_count
cross join staged_count
where raw_count.raw_distinct_seqs != staged_count.staged_rows

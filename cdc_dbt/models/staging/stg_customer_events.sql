-- Raw events with repeated deliveries removed: exactly one row per seq.
-- The raw table is append-only and keeps every delivery; deduplication happens here.
with source as (

    select * from {{ source('raw_cdc', 'customer_events') }}

),

numbered as (

    select
        *,
        row_number() over (
            partition by seq
            order by _loaded_at, _source_file
        ) as delivery_rank
    from source

)

select
    seq,
    op,
    commit_ts,
    customer_id,
    first_name,
    last_name,
    email,
    country,
    segment,
    marketing_opt_in,
    _source_file,
    _loaded_at
from numbered
where delivery_rank = 1

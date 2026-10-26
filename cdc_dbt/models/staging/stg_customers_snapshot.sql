-- One row per customer as of the snapshot (the state at snapshot_seq).
select
    customer_id,
    first_name,
    last_name,
    email,
    country,
    segment
from {{ source('raw_cdc', 'customers_snapshot') }}

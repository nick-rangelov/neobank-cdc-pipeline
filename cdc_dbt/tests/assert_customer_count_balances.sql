-- The customer count must balance: snapshot customers + inserts - deletes = customers now.
-- This reaches the same number by a different route than the models do.
-- It is only checked when the snapshot is loaded and no seq is missing: while a late file
-- is outstanding the arithmetic does not have to balance.
with counts as (

    select
        (select count(*) from {{ ref('stg_customers_snapshot') }}) as snapshot_customers,
        (select count(*) from {{ ref('stg_customer_events') }}
          where seq > {{ var('snapshot_seq') }} and op = 'I') as inserts,
        (select count(*) from {{ ref('stg_customer_events') }}
          where seq > {{ var('snapshot_seq') }} and op = 'D') as deletes,
        (select count(*) from {{ ref('customers_current') }}) as customers_now,
        (select count(*) from {{ ref('recon_seq_gaps') }}) as gap_ranges

)

select
    *,
    snapshot_customers + inserts - deletes as expected_customers
from counts
where gap_ranges = 0
  and snapshot_customers > 0
  and snapshot_customers + inserts - deletes != customers_now

-- The key counts at each step of the pipeline, in one place and in order.
with raw_events as (

    select
        count(*) as total_rows,
        count(distinct seq) as distinct_events
    from {{ source('raw_cdc', 'customer_events') }}

),

after_snapshot as (

    select
        count(*) as events,
        sum(case when op = 'I' then 1 else 0 end) as inserts,
        sum(case when op = 'U' then 1 else 0 end) as updates,
        sum(case when op = 'D' then 1 else 0 end) as deletes
    from {{ ref('stg_customer_events') }}
    where seq > {{ var('snapshot_seq') }}

),

state as (

    select
        sum(case when is_deleted then 0 else 1 end) as customers,
        sum(case when is_deleted then 1 else 0 end) as tombstones
    from {{ ref('customers_current_incremental') }}

)

select
    1 as step,
    'raw event rows (all deliveries)' as metric,
    cast(total_rows as bigint) as metric_value
from raw_events
union all
select
    2 as step,
    'distinct events (by seq)' as metric,
    cast(distinct_events as bigint) as metric_value
from raw_events
union all
select
    3 as step,
    'repeated deliveries removed' as metric,
    cast(total_rows - distinct_events as bigint) as metric_value
from raw_events
union all
select
    4 as step,
    'events after the snapshot' as metric,
    cast(events as bigint) as metric_value
from after_snapshot
union all
select
    5 as step,
    '  of which inserts' as metric,
    cast(inserts as bigint) as metric_value
from after_snapshot
union all
select
    6 as step,
    '  of which updates' as metric,
    cast(updates as bigint) as metric_value
from after_snapshot
union all
select
    7 as step,
    '  of which deletes' as metric,
    cast(deletes as bigint) as metric_value
from after_snapshot
union all
select
    8 as step,
    'customers in the snapshot' as metric,
    cast(count(*) as bigint) as metric_value
from {{ ref('stg_customers_snapshot') }}
union all
select
    9 as step,
    'customers now (full rebuild)' as metric,
    cast(count(*) as bigint) as metric_value
from {{ ref('customers_current') }}
union all
select
    10 as step,
    'customers now (incremental table)' as metric,
    cast(customers as bigint) as metric_value
from state
union all
select
    11 as step,
    'tombstones kept (deleted customers)' as metric,
    cast(tombstones as bigint) as metric_value
from state

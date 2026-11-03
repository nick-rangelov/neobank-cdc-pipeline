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

select 1 as step, 'raw event rows (all deliveries)' as metric, cast(total_rows as bigint) as value from raw_events
union all select 2, 'distinct events (by seq)', cast(distinct_events as bigint) from raw_events
union all select 3, 'repeated deliveries removed', cast(total_rows - distinct_events as bigint) from raw_events
union all select 4, 'events after the snapshot', cast(events as bigint) from after_snapshot
union all select 5, '  of which inserts', cast(inserts as bigint) from after_snapshot
union all select 6, '  of which updates', cast(updates as bigint) from after_snapshot
union all select 7, '  of which deletes', cast(deletes as bigint) from after_snapshot
union all select 8, 'customers in the snapshot', cast(count(*) as bigint) from {{ ref('stg_customers_snapshot') }}
union all select 9, 'customers now (full rebuild)', cast(count(*) as bigint) from {{ ref('customers_current') }}
union all select 10, 'customers now (incremental table)', cast(customers as bigint) from state
union all select 11, 'tombstones kept (deleted customers)', cast(tombstones as bigint) from state

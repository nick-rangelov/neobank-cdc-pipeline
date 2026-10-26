-- Every known version of every customer, in log order.
--   * the snapshot rows are the starting version of each customer (op = 'S', seq = snapshot_seq)
--   * plus every event AFTER the snapshot (seq > snapshot_seq); older ones are already in it
-- A delete is kept as a version with op = 'D' so later models can end the previous version.
with baseline as (

    select
        {{ var('snapshot_seq') }} as seq,
        'S' as op,
        customer_id,
        first_name,
        last_name,
        email,
        country,
        segment,
        cast(null as varchar(10)) as marketing_opt_in
    from {{ ref('stg_customers_snapshot') }}

),

changes as (

    select
        seq,
        op,
        customer_id,
        first_name,
        last_name,
        email,
        country,
        segment,
        marketing_opt_in
    from {{ ref('stg_customer_events') }}
    where seq > {{ var('snapshot_seq') }}

)

select * from baseline
union all
select * from changes

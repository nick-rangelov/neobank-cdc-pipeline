{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='delete+insert'
    )
}}

-- Incremental version of customers_current: each run processes only events that were
-- LOADED since the last run (watermark = last_loaded_at), not events that merely look new.
--   * Late events look old by seq and commit date, but they are new by load time, so the
--     watermark is based on _loaded_at.
--   * Which version wins is always decided by seq, never by arrival.
--   * Deleted customers stay as rows with is_deleted = true (soft delete / tombstone), so a
--     late, older update can never bring a deleted customer back to life.
--   * Snapshot rows are added on the first run, and any snapshot customer that is still
--     missing is added on later runs, so it does not matter when the snapshot is loaded.
-- Proof of correctness: assert_incremental_matches_full_rebuild compares this table with
-- customers_current, which recomputes everything from scratch on every run.
with new_events as (

    select
        seq,
        op,
        customer_id,
        first_name,
        last_name,
        email,
        country,
        segment,
        marketing_opt_in,
        _loaded_at as loaded_at
    from {{ ref('stg_customer_events') }}
    where seq > {{ var('snapshot_seq') }}
    {% if is_incremental() %}
      and _loaded_at > coalesce(
            (select max(last_loaded_at) from {{ this }}),
            cast('1900-01-01' as timestamp)
          )
    {% endif %}

),

{% if is_incremental() %}

-- the current stored row of every customer that appears in the new events
previous_state as (

    select
        last_seq as seq,
        case when is_deleted then 'D' else 'E' end as op,
        customer_id,
        first_name,
        last_name,
        email,
        country,
        segment,
        marketing_opt_in,
        last_loaded_at as loaded_at
    from {{ this }}
    where customer_id in (select customer_id from new_events)

),

-- Snapshot customers that have no stored row yet (for example because the snapshot was
-- loaded after the first run). This makes the model repair itself without --full-refresh.
missing_baseline as (

    select
        {{ var('snapshot_seq') }} as seq,
        'S' as op,
        customer_id,
        first_name,
        last_name,
        email,
        country,
        segment,
        cast(null as varchar(10)) as marketing_opt_in,
        cast(null as timestamp) as loaded_at
    from {{ ref('stg_customers_snapshot') }}
    where customer_id not in (select customer_id from {{ this }})

),

candidates as (

    select * from previous_state
    union all
    select * from missing_baseline
    union all
    select * from new_events

),

{% else %}

-- first run (or --full-refresh): start from the snapshot rows
baseline as (

    select
        {{ var('snapshot_seq') }} as seq,
        'S' as op,
        customer_id,
        first_name,
        last_name,
        email,
        country,
        segment,
        cast(null as varchar(10)) as marketing_opt_in,
        cast(null as timestamp) as loaded_at
    from {{ ref('stg_customers_snapshot') }}

),

candidates as (

    select * from baseline
    union all
    select * from new_events

),

{% endif %}

ranked as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by seq desc
        ) as version_rank,
        max(loaded_at) over (
            partition by customer_id
        ) as last_loaded_at
    from candidates

)

select
    customer_id,
    first_name,
    last_name,
    email,
    country,
    segment,
    marketing_opt_in,
    seq as last_seq,
    case when op = 'D' then true else false end as is_deleted,
    last_loaded_at
from ranked
where version_rank = 1

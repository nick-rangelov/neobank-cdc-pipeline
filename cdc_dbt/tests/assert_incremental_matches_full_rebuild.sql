-- The incremental table must equal a full rebuild (customers_current) at all times.
-- Deleted customers are excluded on the incremental side (they are kept as tombstones).
-- Returns the differing rows (= fails).
with incremental as (

    select
        customer_id, first_name, last_name, email, country, segment,
        coalesce(marketing_opt_in, '') as marketing_opt_in
    from {{ ref('customers_current_incremental') }}
    where not is_deleted

),

full_rebuild as (

    select
        customer_id, first_name, last_name, email, country, segment,
        coalesce(marketing_opt_in, '') as marketing_opt_in
    from {{ ref('customers_current') }}

),

missing_in_incremental as (
    select * from full_rebuild
    except
    select * from incremental
),

unexpected_in_incremental as (
    select * from incremental
    except
    select * from full_rebuild
)

select 'missing in incremental' as problem, * from missing_in_incremental
union all
select 'unexpected in incremental' as problem, * from unexpected_in_incremental

-- One row per file: the rows found in the raw tables versus the rows the loader logged.
-- status is 'ok' when they agree. Anything else means rows were lost, doubled, or loaded
-- without a log entry.
with raw_counts as (

    select _source_file as source_file, count(*) as raw_rows
    from {{ source('raw_cdc', 'customer_events') }}
    group by 1

    union all

    select _source_file as source_file, count(*) as raw_rows
    from {{ source('raw_cdc', 'customers_snapshot') }}
    group by 1

),

logged as (

    select source_file, row_count as logged_rows
    from {{ source('raw_cdc', 'load_log') }}

)

select
    coalesce(l.source_file, r.source_file) as source_file,
    l.logged_rows,
    coalesce(r.raw_rows, 0) as raw_rows,
    case
        when l.source_file is null then 'in raw tables but not in load_log'
        when coalesce(r.raw_rows, 0) != l.logged_rows then 'row count differs'
        else 'ok'
    end as status
from logged l
full outer join raw_counts r
    on l.source_file = r.source_file

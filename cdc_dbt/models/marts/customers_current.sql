-- The current state of every customer: the latest version by seq, deleted customers removed.
with ranked as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by seq desc
        ) as version_rank
    from {{ ref('int_customer_versions') }}

)

select
    customer_id,
    first_name,
    last_name,
    email,
    country,
    segment,
    marketing_opt_in
from ranked
where version_rank = 1
  and op != 'D'

-- Customer history: one row per version, with the log positions between which it was valid.
-- valid_to_seq is empty for the current version. Delete markers are not rows, but they
-- end the version before them.
with with_next as (

    select
        *,
        lead(seq) over (
            partition by customer_id
            order by seq
        ) as next_seq
    from {{ ref('int_customer_versions') }}

)

select
    cast(customer_id as varchar) || '-' || cast(seq as varchar) as customer_version_id,
    customer_id,
    first_name,
    last_name,
    email,
    country,
    segment,
    marketing_opt_in,
    seq as valid_from_seq,
    next_seq as valid_to_seq,
    case when next_seq is null then true else false end as is_current
from with_next
where op != 'D'

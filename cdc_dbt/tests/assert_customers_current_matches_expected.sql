-- The key correctness test: the pipeline's current state must equal the known-correct answer
-- (seeds/expected_customers.csv, the ground truth made by generate_events.py).
-- Empty values are compared as empty strings so that NULL equals NULL.
-- Returns the differing rows (= fails); an empty result means the two match exactly.
with pipeline as (

    select
        customer_id, first_name, last_name, email, country, segment,
        coalesce(marketing_opt_in, '') as marketing_opt_in
    from {{ ref('customers_current') }}

),

expected as (

    select
        customer_id, first_name, last_name, email, country, segment,
        coalesce(marketing_opt_in, '') as marketing_opt_in
    from {{ ref('expected_customers') }}

),

missing_in_pipeline as (
    select * from expected
    except
    select * from pipeline
),

unexpected_in_pipeline as (
    select * from pipeline
    except
    select * from expected
)

select 'missing in pipeline' as problem, * from missing_in_pipeline
union all
select 'unexpected in pipeline' as problem, * from unexpected_in_pipeline

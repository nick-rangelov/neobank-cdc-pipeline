-- Exactly the customers in customers_current must have a current row in the history.
select customer_id, 'current in history only' as problem
from (
    select customer_id from {{ ref('dim_customer_history') }} where is_current
    except
    select customer_id from {{ ref('customers_current') }}
) a
union all
select customer_id, 'current in customers_current only' as problem
from (
    select customer_id from {{ ref('customers_current') }}
    except
    select customer_id from {{ ref('dim_customer_history') }} where is_current
) b

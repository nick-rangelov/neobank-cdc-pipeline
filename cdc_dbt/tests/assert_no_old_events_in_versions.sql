-- The snapshot overlap rule: apart from the snapshot rows themselves (op = 'S'),
-- no version may have seq <= snapshot_seq. Returns rows (= fails) if one does.
select *
from {{ ref('int_customer_versions') }}
where op != 'S'
  and seq <= {{ var('snapshot_seq') }}

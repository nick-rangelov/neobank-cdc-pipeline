{{ config(severity='warn') }}
-- A gap in seq means a change is missing or late. This is a warning, not an error, because
-- while a late file is still on its way a gap is expected. Returns the missing ranges.
select *
from {{ ref('recon_seq_gaps') }}

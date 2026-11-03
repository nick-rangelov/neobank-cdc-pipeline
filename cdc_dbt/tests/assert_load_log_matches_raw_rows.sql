-- Every file's rows in the raw tables must equal the row count in the load log.
-- Returns the files that disagree (= fails).
select *
from {{ ref('recon_files') }}
where status != 'ok'

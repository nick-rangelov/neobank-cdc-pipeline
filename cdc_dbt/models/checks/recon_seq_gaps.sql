-- Ranges of missing seq values between the snapshot and the newest event.
-- seq is a gapless counter, so a gap means a change has not arrived yet (a late file)
-- or was lost. No rows = no gaps.
with delivered as (

    select seq
    from {{ ref('stg_customer_events') }}
    where seq > {{ var('snapshot_seq') }}

),

with_next as (

    select
        seq,
        lead(seq) over (order by seq) as next_seq
    from delivered

),

gaps_between as (

    select
        seq + 1 as gap_from,
        next_seq - 1 as gap_to,
        next_seq - seq - 1 as missing_seqs
    from with_next
    where next_seq - seq > 1

),

gap_at_start as (

    select
        {{ var('snapshot_seq') }} + 1 as gap_from,
        lo - 1 as gap_to,
        lo - {{ var('snapshot_seq') }} - 1 as missing_seqs
    from (select min(seq) as lo from delivered) b
    where lo > {{ var('snapshot_seq') }} + 1

)

select * from gaps_between
union all
select * from gap_at_start

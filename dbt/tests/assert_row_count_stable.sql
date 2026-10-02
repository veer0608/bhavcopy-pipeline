-- A truncated or doubled file shows up as a row count far from the exchange's
-- own recent median. Needs five prior days before it judges a day.
with counts as (
    select source as exchange, trade_date, rows
    from {{ source('raw', 'loaded_files') }}
),

banded as (
    select
        *,
        median(rows) over w as trailing_median,
        count(*) over w     as history
    from counts
    window w as (partition by exchange order by trade_date rows between 20 preceding and 1 preceding)
)

select *
from banded
where history >= 5
  and abs(rows - trailing_median) > trailing_median * {{ var('row_count_tolerance') }}

-- Both exchanges keep the same trading calendar. A day one has and the other
-- lacks is a missed download, not a holiday.
{{ config(severity='warn') }}

with days as (
    select
        trade_date,
        count(*) filter (where source = 'NSE') as nse,
        count(*) filter (where source = 'BSE') as bse
    from {{ source('raw', 'loaded_files') }}
    group by trade_date
)

select * from days where nse = 0 or bse = 0

-- The same security trades on both exchanges, so the two closes are an
-- independent check on each other. One row per ISIN per day where both
-- exchanges list it on the mainboard and it is liquid on both.

with liquid as (
    select *
    from {{ ref('fct_daily_prices') }}
    where segment = 'mainboard'
      and turnover >= {{ var('liquid_turnover') }}
      and close_price > 0
)

select
    n.trade_date,
    n.isin,
    n.symbol                as nse_symbol,
    b.symbol                as bse_symbol,
    n.close_price           as nse_close,
    b.close_price           as bse_close,
    round(abs(n.close_price - b.close_price) / n.close_price * 10000, 2) as close_diff_bps
from liquid n
join liquid b
    on b.isin = n.isin and b.trade_date = n.trade_date
where n.exchange = 'NSE' and b.exchange = 'BSE'

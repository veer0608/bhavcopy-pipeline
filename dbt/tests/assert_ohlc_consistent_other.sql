-- Same rule for everything that is not mainboard or SME equity, as a warning.
-- Thinly traded series (T+0 settlement, infrastructure funds) get a published
-- close that is not taken from the day's own trades, so it can sit outside the
-- high-low range. That is the exchange's number, not a load error.
{{ config(severity='warn') }}

select exchange, trade_date, symbol, series, open_price, high_price, low_price, close_price
from {{ ref('fct_daily_prices') }}
where segment = 'other'
  and (low_price > high_price
    or open_price not between low_price and high_price
    or close_price not between low_price and high_price)

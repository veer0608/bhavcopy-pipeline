-- The day's low cannot exceed its high, and open and close must sit inside the range.
-- Equities only. See assert_ohlc_consistent_other for the rest.
select exchange, trade_date, symbol, open_price, high_price, low_price, close_price
from {{ ref('fct_daily_prices') }}
where segment <> 'other'
  and (low_price > high_price
    or open_price not between low_price and high_price
    or close_price not between low_price and high_price)

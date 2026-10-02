select exchange, trade_date, symbol, close_price, volume, turnover
from {{ ref('fct_daily_prices') }}
where close_price <= 0 or volume < 0 or turnover < 0

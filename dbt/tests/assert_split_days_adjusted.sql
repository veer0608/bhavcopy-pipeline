-- On a split or bonus ex-date, the adjusted change should look like an
-- ordinary day. Raw changes on these days run from -24% to -90%; adjusted,
-- the widest seen is about +20% (a stock rallying on the day). 35% leaves room
-- for a real limit move and still catches a wrong or doubled factor.

select exchange, trade_date, symbol, split_factor, pct_change, pct_change_adj
from {{ ref('fct_daily_returns') }}
where split_factor <> 1
  and abs(pct_change_adj) >= 35

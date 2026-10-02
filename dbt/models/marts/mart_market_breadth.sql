-- One row per exchange per trade date: how broad the day's move was.
-- Mainboard equities only, so bonds, ETFs and SME listings do not skew it.

select
    exchange,
    trade_date,
    count(*)                                   as securities_traded,
    count(*) filter (where pct_change > 0)     as advancers,
    count(*) filter (where pct_change < 0)     as decliners,
    count(*) filter (where pct_change = 0)     as unchanged,
    round(sum(turnover) / 1e7, 2)              as turnover_crore,
    sum(trades)                                as trades,
    round(median(pct_change), 4)               as median_pct_change
from {{ ref('fct_daily_prices') }}
where segment = 'mainboard'
group by exchange, trade_date

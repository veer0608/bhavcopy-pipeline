-- Biggest liquid mainboard movers on each exchange's latest trade date.

with latest as (
    select exchange, max(trade_date) as trade_date
    from {{ ref('fct_daily_prices') }}
    group by exchange
),

ranked as (
    select
        f.exchange, f.trade_date, f.symbol, f.security_name, f.close_price,
        f.pct_change, round(f.turnover / 1e7, 2) as turnover_crore,
        row_number() over (partition by f.exchange order by f.pct_change desc, f.symbol) as gain_rank,
        row_number() over (partition by f.exchange order by f.pct_change asc, f.symbol)  as loss_rank
    from {{ ref('fct_daily_prices') }} f
    join latest using (exchange, trade_date)
    where f.segment = 'mainboard'
      and f.turnover >= {{ var('liquid_turnover') }}
      and f.pct_change is not null
)

select
    exchange, trade_date, symbol, security_name, close_price, pct_change, turnover_crore,
    case when gain_rank <= 10 then 'gainer' else 'loser' end as direction,
    case when gain_rank <= 10 then gain_rank else loss_rank end as rank
from ranked
where gain_rank <= 10 or loss_rank <= 10

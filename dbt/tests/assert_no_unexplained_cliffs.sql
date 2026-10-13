-- A mainboard fall of 45% or more with no split, bonus or demerger behind it.
-- Either a real crash or an action this pipeline did not see: a missed fetch,
-- an ETF feed gap, or a rename (NSE files HEG's 2026-09-07 demerger under its
-- new symbol HEGAM). A warning, because real crashes happen.
{{ config(severity='warn') }}

select exchange, trade_date, symbol, security_name, pct_change_adj
from {{ ref('fct_daily_returns') }}
where segment = 'mainboard'
  and pct_change_adj <= -45
  and unadjusted_action is null

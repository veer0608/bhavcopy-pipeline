-- One row per version of a security: a period over which its NSE symbol and
-- ISIN stayed the same. A slowly changing dimension, type 2: a change adds a
-- row instead of overwriting one, so a past date still finds the symbol the
-- stock had then.
--
-- Built from the daily prices, not a dbt snapshot. A snapshot records changes
-- from the day it starts running, and this history (HEG became HEGAM on
-- 2026-09-22) is already in the price files.
--
-- Gaps and islands: mark each day whose symbol or ISIN differs from the
-- previous day's, then a running sum of those marks numbers the versions.
--
-- valid_to is exclusive: the first day of the next version. NULL means current.

with daily as (
    select distinct p.trade_date, m.security_id, p.symbol, p.isin
    from {{ ref('fct_daily_prices') }} p
    join {{ ref('int_security_map') }} m
        on m.node_type = 'isin' and m.node_value = p.isin
    where p.exchange = 'NSE' and p.segment in ('mainboard', 'sme')
),

marked as (
    select
        *,
        case
            when symbol is distinct from lag(symbol) over w
              or isin   is distinct from lag(isin)   over w
            then 1 else 0
        end as starts_version
    from daily
    window w as (partition by security_id order by trade_date)
),

numbered as (
    select
        *,
        sum(starts_version) over (partition by security_id order by trade_date) as version
    from marked
),

versions as (
    select
        security_id,
        version,
        symbol,
        isin,
        min(trade_date) as valid_from,
        max(trade_date) as last_traded,
        count(*)        as days_traded
    from numbered
    group by security_id, version, symbol, isin
)

select
    security_id,
    version,
    symbol,
    isin,
    valid_from,
    lead(valid_from) over (partition by security_id order by valid_from) as valid_to,
    lead(valid_from) over (partition by security_id order by valid_from) is null as is_current,
    last_traded,
    days_traded
from versions

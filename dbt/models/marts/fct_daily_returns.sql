-- Grain: one row per (exchange, trade_date, instrument_id), same as fct_daily_prices.
--
-- The day's change, corrected for splits and bonuses. On an ex-date the
-- exchange quotes in new shares, but its previous close is still in old ones,
-- so the raw change shows a split as a crash (GOODLUCK, Bonus 2:1, -65.90%).
-- Dividing the previous close by the day's factor puts both in new shares.
--
-- NSE publishes the actions, keyed by NSE symbol. A BSE row is matched to its
-- NSE symbol through the ISIN both exchanges list on that day, which survives
-- even though a split itself issues a new ISIN.
--
-- Demergers and other actions with no ratio cannot be adjusted. Their rows
-- keep the raw change and carry the action in unadjusted_action, so marts can
-- leave them out of rankings.

with nse_symbol as (
    -- DISTINCT because a stock's block-deal row (series BL) repeats its ISIN
    -- and symbol on the same day. Two different symbols on one ISIN would
    -- still fan out, and the unique test on price_key would fail.
    select distinct trade_date, isin, symbol
    from {{ ref('fct_daily_prices') }}
    where exchange = 'NSE'
),

day_actions as (
    select
        symbol,
        ex_date,
        -- Two ratio actions on one day multiply. SQL has no product
        -- aggregate, so exp(sum(ln())) stands in; factors are always > 0.
        -- SUM skips a demerger's NULL factor, and a demerger-only day comes
        -- out NULL, which the COALESCE below turns into 1.
        exp(sum(ln(factor)))                                       as factor,
        string_agg(kind, ', ') filter (where kind = 'demerger')    as unadjusted_action
    from {{ ref('stg_corporate_actions') }}
    where kind in ('split', 'bonus', 'demerger')
    group by symbol, ex_date
)

select
    f.price_key,
    f.exchange,
    f.trade_date,
    f.instrument_id,
    f.symbol,
    f.segment,
    f.close_price,
    f.prev_close_price,
    f.pct_change,
    coalesce(a.factor, 1)                                    as split_factor,
    f.prev_close_price / coalesce(a.factor, 1)               as adj_prev_close_price,
    case
        when f.prev_close_price > 0
        then round((f.close_price * coalesce(a.factor, 1) - f.prev_close_price)
                   / f.prev_close_price * 100, 4)
    end                                                       as pct_change_adj,
    a.unadjusted_action,
    f.turnover,
    f.trades,
    f.security_name
from {{ ref('fct_daily_prices') }} f
left join nse_symbol n
    on f.exchange = 'BSE' and n.trade_date = f.trade_date and n.isin = f.isin
left join day_actions a
    on  a.symbol  = case when f.exchange = 'NSE' then f.symbol else n.symbol end
    and a.ex_date = f.trade_date

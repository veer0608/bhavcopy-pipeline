-- Grain: one row per (exchange, trade_date, instrument_id), same as fct_daily_prices.
--
-- The day's change, corrected for splits and bonuses. On an ex-date the
-- exchange quotes in new shares, but its previous close is still in old ones,
-- so the raw change shows a split as a crash (GOODLUCK, Bonus 2:1, -65.90%).
-- Dividing the previous close by the day's factor puts both in new shares.
--
-- NSE publishes the actions keyed by NSE symbol. Both sides are mapped to a
-- security_id (int_security_map) instead of being joined on symbol, so an
-- action still finds its stock across a rename: NSE files HEG's 2026-09-07
-- demerger under HEGAM, the symbol it took two weeks later. A BSE row reaches
-- the same id through its ISIN.
--
-- Demergers and other actions with no ratio cannot be adjusted. Their rows
-- keep the raw change and carry the action in unadjusted_action, so marts can
-- leave them out of rankings.

with symbol_security as (
    select node_value as symbol, security_id
    from {{ ref('int_security_map') }}
    where node_type = 'symbol'
),

isin_security as (
    select node_value as isin, security_id
    from {{ ref('int_security_map') }}
    where node_type = 'isin'
),

day_actions as (
    select
        s.security_id,
        c.ex_date,
        -- Two ratio actions on one day multiply. SQL has no product
        -- aggregate, so exp(sum(ln())) stands in; factors are always > 0.
        -- SUM skips a demerger's NULL factor, and a demerger-only day comes
        -- out NULL, which the COALESCE below turns into 1.
        exp(sum(ln(c.factor)))                                       as factor,
        string_agg(c.kind, ', ') filter (where c.kind = 'demerger')  as unadjusted_action
    from {{ ref('stg_corporate_actions') }} c
    join symbol_security s on s.symbol = c.symbol
    where c.kind in ('split', 'bonus', 'demerger')
    group by s.security_id, c.ex_date
)

select
    f.price_key,
    f.exchange,
    f.trade_date,
    f.instrument_id,
    f.symbol,
    i.security_id,
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
-- NULL for an ISIN NSE has never listed (BSE-only stocks), which then
-- matches no action and keeps its raw change.
left join isin_security i on i.isin = f.isin
left join day_actions a
    on  a.security_id = i.security_id
    and a.ex_date     = f.trade_date

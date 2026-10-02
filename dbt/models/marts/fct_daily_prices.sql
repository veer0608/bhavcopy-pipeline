{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['exchange', 'trade_date']
    )
}}

-- Grain: one row per (exchange, trade_date, instrument_id).
--
-- Incremental by load batch, replaced by exchange-day. A batch newer than the
-- last one this table saw is picked up, and every exchange-day in it is deleted
-- and reinserted whole. So a corrected file restates its day, including rows
-- the correction removed, and a re-run of the same batch changes nothing.

select
    md5(s.exchange || '|' || cast(s.trade_date as varchar) || '|' || s.instrument_id) as price_key,
    s.exchange,
    s.trade_date,
    s.instrument_id,
    s.symbol,
    s.series,
    coalesce(seg.segment, 'other') as segment,
    s.isin,
    s.security_name,
    s.open_price,
    s.high_price,
    s.low_price,
    s.close_price,
    s.prev_close_price,
    case
        when s.prev_close_price > 0
        then round((s.close_price - s.prev_close_price) / s.prev_close_price * 100, 4)
    end as pct_change,
    s.volume,
    s.turnover,
    s.trades,
    s.loaded_at
from {{ ref('stg_bhavcopy') }} s
left join {{ ref('series_segments') }} seg
    on seg.exchange = s.exchange and seg.series = s.series

{% if is_incremental() %}
where s.loaded_at > (select coalesce(max(loaded_at), timestamptz '1900-01-01') from {{ this }})
{% endif %}

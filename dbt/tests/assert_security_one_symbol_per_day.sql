-- One security trades under one symbol on any day. Two symbols on the same
-- day under one security_id means int_security_map merged two companies,
-- the way NSE's actions feed would have merged DTIL and DVL.

select p.trade_date, m.security_id, list(distinct p.symbol) as symbols
from {{ ref('fct_daily_prices') }} p
join {{ ref('int_security_map') }} m
    on m.node_type = 'isin' and m.node_value = p.isin
where p.exchange = 'NSE' and p.segment in ('mainboard', 'sme')
group by p.trade_date, m.security_id
having count(distinct p.symbol) > 1

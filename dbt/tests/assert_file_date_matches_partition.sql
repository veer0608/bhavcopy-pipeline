-- The trade date inside the file must be the date the file was landed under.
select exchange, trade_date, file_trade_date, count(*) as n
from {{ ref('stg_bhavcopy') }}
where file_trade_date <> trade_date
group by all

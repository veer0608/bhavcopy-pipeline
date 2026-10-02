-- Row counts per exchange-day must match the file's own count. This is the
-- check that the incremental model has neither dropped nor duplicated a day.
with fact as (
    select exchange, trade_date, count(*) as n
    from {{ ref('fct_daily_prices') }}
    group by all
)

select f.source, f.trade_date, f.rows as file_rows, fact.n as fact_rows
from {{ source('raw', 'loaded_files') }} f
full join fact
    on fact.exchange = f.source and fact.trade_date = f.trade_date
where f.rows is distinct from fact.n

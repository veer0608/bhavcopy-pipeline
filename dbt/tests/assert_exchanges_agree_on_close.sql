-- A liquid security should close within 2% on the two exchanges. A wider gap
-- usually means one file is wrong or an ISIN was reused. Sub-10-rupee stocks
-- are left out: one tick on a 1-rupee stock is already more than 2%.
{{ config(severity='warn') }}

select *
from {{ ref('mart_exchange_reconciliation') }}
where close_diff_bps > 200
  and nse_close >= 10

-- Every split and bonus must yield a positive factor. A NULL here means the
-- subject line changed shape and the parser in pipeline/actions.py missed it,
-- which would silently leave that stock unadjusted.

select symbol, ex_date, kind, subject
from {{ ref('stg_corporate_actions') }}
where kind in ('split', 'bonus')
  and (factor is null or factor <= 0)

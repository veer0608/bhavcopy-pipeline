-- Typed corporate actions, with the share-count factor for splits and bonuses.
--
-- factor = shares held after the action per share held before.
--   split, face value a -> b:  a / b       (Rs 10 -> Rs 2 is 5)
--   bonus x:y, x new per y:    (x + y) / y (Bonus 2:1 is 3)
-- Every other kind has no ratio NSE publishes, so factor is NULL.

select
    feed,
    symbol,
    isin,
    cast(ex_date as date) as ex_date,
    kind,
    case kind
        when 'split' then cast(num_a as double) / cast(num_b as double)
        when 'bonus' then (cast(num_a as double) + cast(num_b as double)) / cast(num_b as double)
    end as factor,
    subject
from {{ source('raw', 'corporate_actions') }}

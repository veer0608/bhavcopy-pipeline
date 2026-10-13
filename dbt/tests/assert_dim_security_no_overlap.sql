-- The classic SCD2 bug: two versions of one security valid on the same day,
-- so a lookup by date returns two rows. Versions must tile time: each one
-- ends exactly where the next begins, and only the last is open.

select a.security_id, a.version, a.valid_from, a.valid_to, b.version as overlaps_version
from {{ ref('dim_security') }} a
join {{ ref('dim_security') }} b
    on  a.security_id = b.security_id
    and a.version < b.version
    and b.valid_from < coalesce(a.valid_to, date '9999-12-31')

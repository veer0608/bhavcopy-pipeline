-- Maps every NSE symbol and ISIN to one security_id, so a stock keeps its
-- identity through changes that break either key alone:
--   a split issues a new ISIN but keeps the symbol (TDPOWERSYS, 2026-08-24)
--   a rename keeps the ISIN but changes the symbol (HEG to HEGAM, 2026-09-22)
--
-- Symbols and ISINs are the nodes of a graph, linked whenever the price file
-- shows them on the same row. Everything reachable from a node is one
-- security, and its id is the lowest ISIN in the group, which is readable and
-- stays put unless a lower ISIN joins later.
--
-- Built from the price files only. NSE's corporate-actions feed also carries
-- ISINs, but not reliably: it files DTIL's dividend under DVL's ISIN, which
-- would merge two companies that trade side by side.
-- assert_security_one_symbol_per_day fails if a merge like that gets in.

with recursive

pairs as (
    select distinct symbol, isin
    from {{ ref('fct_daily_prices') }}
    where exchange = 'NSE' and segment in ('mainboard', 'sme')
),

-- Both directions, with a prefix so a symbol and an ISIN can never collide.
edges as (
    select 'S:' || symbol as a, 'I:' || isin as b from pairs
    union
    select 'I:' || isin, 'S:' || symbol from pairs
),

-- reach(node, other): "other" can be reached from "node". Starts with every
-- node reaching itself and follows one edge per step. UNION (not UNION ALL)
-- drops pairs already found, so the recursion stops when nothing new turns
-- up. Groups here hold at most 3 nodes, so it is a few steps.
reach(node, other) as (
    select distinct a, a from edges
    union
    select r.node, e.b
    from reach r
    join edges e on e.a = r.other
)

select
    case when node like 'S:%' then 'symbol' else 'isin' end   as node_type,
    substr(node, 3)                                            as node_value,
    substr(min(other) filter (where other like 'I:%'), 3)      as security_id
from reach
group by node

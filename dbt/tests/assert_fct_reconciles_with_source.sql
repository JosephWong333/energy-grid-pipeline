-- Reconciliation: the fact table must contain exactly the BA-hours present in
-- the pivoted intermediate for seeded balancing authorities — no rows lost in
-- the join, none invented by it. A symmetric anti-join in both directions.

with expected as (

    select m.ba_code, m.period_utc
    from {{ ref('int_grid_metrics_pivoted') }} m
    inner join {{ ref('balancing_authorities') }} b
        on m.ba_code = b.ba_code

),

actual as (

    select ba_code, period_utc
    from {{ ref('fct_grid_hourly') }}

)

select e.ba_code, e.period_utc, 'missing_from_fct' as issue
from expected e
left join actual a
    on  e.ba_code = a.ba_code
    and e.period_utc = a.period_utc
where a.ba_code is null

union all

select a.ba_code, a.period_utc, 'not_in_source' as issue
from actual a
left join expected e
    on  a.ba_code = e.ba_code
    and a.period_utc = e.period_utc
where e.ba_code is null

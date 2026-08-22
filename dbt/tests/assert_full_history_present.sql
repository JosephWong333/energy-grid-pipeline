-- Once the initial backfill is DECLARED complete (var expect_full_history,
-- enabled on the nightly prod run), every seeded BA's history must actually
-- reach the configured start — the guard against a pilot-shaped warehouse
-- masquerading as a backfilled one. Freshness checks look at the newest
-- edge; this one looks at the oldest. Disabled by default so fixtures and
-- pilots build green.

{% if var('expect_full_history', false) %}
select
    b.ba_code,
    min(f.period_utc) as oldest_period
from {{ ref('balancing_authorities') }} b
left join {{ ref('fct_grid_hourly') }} f
    on b.ba_code = f.ba_code
group by 1
having min(f.period_utc) is null
    or min(f.period_utc) > timestamp '2019-02-15'  -- backfill_start + tolerance
{% else %}
select 1 as ba_code where 1 = 0
{% endif %}

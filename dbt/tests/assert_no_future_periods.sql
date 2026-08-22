-- No hour in the fact may be more than 2 hours in the future (small grace
-- for clock skew). Future periods would indicate a parsing or timezone bug.

select ba_code, period_utc
from {{ ref('fct_grid_hourly') }}
where period_utc > timezone('UTC', now()) + interval '2 hours'

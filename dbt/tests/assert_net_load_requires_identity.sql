-- The regression for the combined fail-open: a published net load (or a
-- TRUE validity flag) on an hour whose grid identity was not both
-- evaluable and within band is a contract violation, whatever caused it.
-- Fixtures plant the exact scenario (SWPP: 400 GWh demand with the NG row
-- deleted), so this is exercised on every CI run.

select ba_code, period_utc, demand_mwh, net_load_mwh
from {{ ref('fct_grid_hourly') }}
where (net_load_mwh is not null or is_net_load_valid)
  and not is_grid_identity_valid

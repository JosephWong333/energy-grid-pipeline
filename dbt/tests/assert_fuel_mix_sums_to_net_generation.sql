-- Second EIA-930 identity: per-fuel generation should sum to reported net
-- generation. Checked only where the fuel report is present AND fully
-- populated (no missing fuel values), since a null fuel value legitimately
-- shrinks the sum. Warn severity — visibility, not a gate.

{{ config(severity='warn') }}

select
    ba_code,
    period_utc,
    total_fuel_mwh,
    net_generation_mwh,
    total_fuel_mwh - net_generation_mwh as residual_mwh
from {{ ref('fct_grid_hourly') }}
where is_fuel_reported
  and net_generation_mwh is not null
  and not coalesce(has_fuel_missing_value, false)
  and not coalesce(has_fuel_extreme_outlier, false)
  and not coalesce(has_vre_absent, false)
  and not coalesce(has_fuel_group_absent, false)
  and not coalesce(has_fuel_implausible_value, false)
  and not coalesce(has_missing_value, false)
  and not coalesce(has_negative_anomaly, false)
  and abs(total_fuel_mwh - net_generation_mwh)
      > greatest({{ var("recon_rel_tol") }} * abs(net_generation_mwh),
                 {{ var("recon_abs_floor_mwh") }})

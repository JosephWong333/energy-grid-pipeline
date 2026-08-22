-- EIA-930 identity: demand ≈ net generation − total interchange.
-- Checked only on hours that are complete and unflagged; tolerance is a
-- relative band with an absolute floor (vars: recon_rel_tol,
-- recon_abs_floor_mwh). Warn severity: BAs do report internally
-- inconsistent hours, and this test exists to SEE them, not to halt on them.

{{ config(severity='warn') }}

select
    ba_code,
    period_utc,
    demand_mwh,
    net_generation_mwh,
    total_interchange_mwh,
    demand_mwh - (net_generation_mwh - total_interchange_mwh) as residual_mwh
from {{ ref('fct_grid_hourly') }}
where is_region_complete
  and not coalesce(has_negative_anomaly, false)
  and not coalesce(has_extreme_outlier, false)
  and not coalesce(has_missing_value, false)
  and not coalesce(has_grid_implausible_value, false)
  and abs(demand_mwh - (net_generation_mwh - total_interchange_mwh))
      > greatest({{ var("recon_rel_tol") }} * abs(demand_mwh),
                 {{ var("recon_abs_floor_mwh") }})

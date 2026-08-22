-- Daily reconciliation monitoring per BA: how far off are the two EIA-930
-- identities, really? The identity TESTS are deliberately loose sanity
-- bands (see dbt_project vars); this mart is where the actual residual
-- distributions live, so drift is something you can query and chart rather
-- than a wall of warn rows. After the first real backfill, use this to set
-- per-BA tolerances on evidence instead of guesses.

with hourly as (

    select *
    from {{ ref('fct_grid_hourly') }}
    where is_region_complete
      and not coalesce(has_negative_anomaly, false)
      and not coalesce(has_extreme_outlier, false)
      and not coalesce(has_missing_value, false)

)

select
    ba_code,
    local_date,
    count(*)                                                       as n_hours,

    -- Identity 1: demand = net_generation - total_interchange
    median(abs(demand_mwh - (net_generation_mwh - total_interchange_mwh))
           / nullif(abs(demand_mwh), 0))                           as demand_identity_median_rel,
    quantile_cont(abs(demand_mwh - (net_generation_mwh - total_interchange_mwh))
           / nullif(abs(demand_mwh), 0), 0.95)                     as demand_identity_p95_rel,

    -- Identity 2: sum(fuel) = net_generation (fully-reported hours only)
    median(abs(total_fuel_mwh - net_generation_mwh)
           / nullif(abs(net_generation_mwh), 0)) filter (
        where is_fuel_reported
          and not has_fuel_missing_value
          and not has_fuel_extreme_outlier
          and not has_vre_absent)                                  as fuel_identity_median_rel,
    count(*) filter (
        where is_fuel_reported
          and not has_fuel_missing_value
          and not has_fuel_extreme_outlier
          and not has_vre_absent)                                  as n_fuel_hours

from hourly
group by 1, 2

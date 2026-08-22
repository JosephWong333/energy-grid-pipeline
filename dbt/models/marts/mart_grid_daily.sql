-- One row per (balancing authority, local calendar day).
--
-- COMPLETENESS is exact, not approximate: hours_expected is computed from
-- the UTC span between consecutive local midnights in the BA's timezone —
-- 23 on spring-forward days, 25 on fall-back days, 24 otherwise. A day is
-- complete only when every expected hour has a clean, non-null demand
-- observation. (The previous "23-25 rows" heuristic silently accepted a
-- normal day missing an hour, and counted null-demand rows as reported.)
--
-- GRID-metric anomalous hours are EXCLUDED from aggregates and counted in
-- n_anomalous_hours; FUEL-anomalous hours are counted separately in
-- n_fuel_anomalous_hours and gate publication via is_fuel_mix_complete_day
-- rather than silently thinning sums. Shares are
-- volume-weighted (sum/sum). Net-load aggregates use only hours where
-- net_load is valid (fuel report present).

with hourly as (

    select * from {{ ref('fct_grid_hourly') }}

),

ba as (

    select ba_code, timezone from {{ ref('balancing_authorities') }}

),

clean as (

    select *
    from hourly
    where not coalesce(has_negative_anomaly, false)
      and not coalesce(has_extreme_outlier, false)
      and is_grid_identity_valid  -- unverifiable hours are unpublishable,
                                  -- not silently trusted

),

anomalies as (

    select
        ba_code,
        local_date,
        count(*) as n_anomalous_hours
    from hourly
    where coalesce(has_negative_anomaly, false)
       or coalesce(has_extreme_outlier, false)
       or not is_grid_identity_valid
    group by 1, 2

),

presence as (

    select
        ba_code,
        local_date,
        count(*) as hours_present
    from hourly
    group by 1, 2

),

daily as (

    select
        c.ba_code,
        c.local_date,

        count(c.demand_mwh)                               as hours_valid,
        count(*) filter (where c.is_net_load_valid)       as hours_net_load_valid,
        count(*) filter (where c.is_fuel_mix_valid)       as hours_fuel_mix_valid,
        count(*) filter (where c.has_fuel_missing_value
                            or c.has_fuel_negative_anomaly
                            or c.has_fuel_extreme_outlier
                            or c.has_vre_absent
                            or c.has_fuel_group_absent
                            or c.has_fuel_implausible_value)  as n_fuel_anomalous_hours,

        sum(c.demand_mwh) / 1000.0                        as demand_gwh,
        max(c.demand_mwh)                                 as peak_demand_mwh,
        arg_max(c.local_hour, c.demand_mwh)               as peak_demand_hour,
        avg(c.demand_mwh) / nullif(max(c.demand_mwh), 0)  as load_factor,

        min(c.net_load_mwh)                               as min_net_load_mwh,
        arg_min(c.local_hour, c.net_load_mwh)             as min_net_load_hour,

        sum(c.renewable_mwh)   / nullif(sum(c.total_fuel_mwh), 0)
                                                          as renewable_share,
        sum(c.carbon_free_mwh) / nullif(sum(c.total_fuel_mwh), 0)
                                                          as carbon_free_share,
        sum(c.solar_mwh)   / 1000.0                       as solar_gwh,
        sum(c.wind_mwh)    / 1000.0                       as wind_gwh,
        sum(c.storage_mwh) / 1000.0                       as storage_gwh

    from clean c
    group by 1, 2

)

, enriched as (

    select
        d.*,
        p.hours_present,
        datediff(
            'hour',
            timezone('UTC', timezone(b.timezone, cast(d.local_date as timestamp))),
            timezone('UTC', timezone(b.timezone,
                                     cast(d.local_date + interval 1 day as timestamp)))
        )                                                 as hours_expected,
        coalesce(a.n_anomalous_hours, 0)                  as n_anomalous_hours
    from daily d
inner join ba b
    on d.ba_code = b.ba_code
inner join presence p
    on  d.ba_code    = p.ba_code
    and d.local_date = p.local_date
left join anomalies a
    on  d.ba_code    = a.ba_code
    and d.local_date = a.local_date

)

select
    *,
    hours_valid = hours_expected                          as is_complete_day,
    -- Publication gate for net-load analytics: every expected hour has a
    -- VALID net load (fuel present, VRE intact, no fuel outliers).
    hours_net_load_valid = hours_expected                 as is_net_load_complete_day,
    -- Publication gate for GENERATION-SHARE analytics: a day where any fuel
    -- value is null/flagged/unmapped has an incomplete denominator, which
    -- silently inflates renewable share. Distinct from net-load
    -- completeness: a null coal value breaks shares but not demand−VRE.
    hours_fuel_mix_valid = hours_expected                 as is_fuel_mix_complete_day
from enriched

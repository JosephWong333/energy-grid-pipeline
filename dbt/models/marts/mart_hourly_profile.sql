
-- Only days complete for BOTH demand and net load, and only standard
-- 24-hour days: DST transition days would give hour 02 (spring) or hour 01
-- (fall) a different sample size than every other hour of that month, so
-- they are excluded from profile averages entirely (they remain in the
-- daily mart). One day set => every series averages over the same days.
with complete_days as (

    select ba_code, local_date
    from {{ ref('mart_grid_daily') }}
    where is_complete_day
      and is_net_load_complete_day
      and hours_expected = 24

),

hourly as (

    select h.*
    from {{ ref('fct_grid_hourly') }} h
    inner join complete_days d
        on  h.ba_code    = d.ba_code
        and h.local_date = d.local_date
    where not coalesce(h.has_negative_anomaly, false)
      and not coalesce(h.has_extreme_outlier, false)

)

select
    ba_code,
    date_trunc('month', local_date)        as month,
    local_hour,

    count(*)                               as n_hours,
    count(distinct local_date)             as n_days,
    count(net_load_mwh)                    as n_valid_net_load_hours,
    avg(demand_mwh)                        as avg_demand_mwh,
    avg(net_load_mwh)                      as avg_net_load_mwh,
    avg(solar_mwh)                         as avg_solar_mwh,
    avg(wind_mwh)                          as avg_wind_mwh,
    avg(natural_gas_mwh)                   as avg_natural_gas_mwh

from hourly
group by 1, 2, 3

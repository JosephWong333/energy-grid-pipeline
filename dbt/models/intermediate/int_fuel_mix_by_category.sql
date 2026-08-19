-- One row per (BA, hour) with generation rolled up by fuel GROUP.
--
-- Groups come from the fuel_types seed rather than hardcoded codes because
-- EIA changed its fuel vocabulary in 2024Q3 (SUN/SNB, WND/WNB, GEO, BAT,
-- OES). Grouping by seed lets a 2019-2026 backfill aggregate consistently
-- across the change.
--
-- Quality signals are CARRIED, never dropped:
--   * has_fuel_negative_anomaly / has_fuel_missing_value / has_fuel_extreme_outlier
--   * has_vre_missing_value  — a solar/wind VALUE was null this hour
--   * has_vre_absent         — a solar/wind ROW is missing even though this
--     BA is EXPECTED to report that group. Expectations are the UNION of a
--     seed declaration (balancing_authorities.expects_solar/expects_wind —
--     fail-closed: it catches a whole month of missing solar, which a
--     purely self-learned signal cannot, because the broken month would
--     teach it that absence is normal) and the month-observed signal (which
--     stays era-aware for vocabulary changes and catches gaps in BAs the
--     seed doesn't declare). A BA the seed marks false and that never
--     reports the group is legitimately zero.
--   * has_vre_negative_anomaly — a non-hybrid solar/wind row was negative;
--     net load must not be computed from a flagged-impossible VRE value.

with fuel as (

    select * from {{ ref('stg_eia__fuel_mix') }}

),

types as (

    select * from {{ ref('fuel_types') }}

),

joined as (

    select
        f.ba_code,
        f.period_utc,
        f.fuel_code,
        f.generation_mwh,
        f.is_negative_anomaly,
        f.is_extreme_outlier,
        f.is_missing_value,
        t.fuel_group,
        t.is_renewable,
        t.is_carbon_free
    from fuel f
    left join types t
        on f.fuel_code = t.fuel_code

),

hourly as (

    select
        ba_code,
        period_utc,

        count(*)                                                    as n_fuel_reports,
        count(*) filter (where fuel_group is null)                  as unmapped_fuel_reports,

        sum(generation_mwh) filter (where fuel_group = 'coal')        as coal_mwh,
        sum(generation_mwh) filter (where fuel_group = 'natural_gas') as natural_gas_mwh,
        sum(generation_mwh) filter (where fuel_group = 'oil')         as oil_mwh,
        sum(generation_mwh) filter (where fuel_group = 'nuclear')     as nuclear_mwh,
        sum(generation_mwh) filter (where fuel_group = 'hydro')       as hydro_mwh,
        sum(generation_mwh) filter (where fuel_group = 'solar')       as solar_mwh,
        sum(generation_mwh) filter (where fuel_group = 'wind')        as wind_mwh,
        sum(generation_mwh) filter (where fuel_group = 'geothermal')  as geothermal_mwh,
        sum(generation_mwh) filter (where fuel_group = 'storage')     as storage_mwh,
        sum(generation_mwh) filter (
            where fuel_group = 'other' or fuel_group is null)         as other_mwh,

        sum(generation_mwh) filter (where is_renewable)               as renewable_mwh,
        sum(generation_mwh) filter (where is_carbon_free)             as carbon_free_mwh,
        sum(generation_mwh)                                           as total_fuel_mwh,

        bool_or(fuel_group = 'solar')                                 as hour_has_solar,
        bool_or(fuel_group = 'wind')                                  as hour_has_wind,
        coalesce(bool_or(is_negative_anomaly) filter (
            where fuel_group in ('solar', 'wind')), false)            as has_vre_negative_anomaly,

        bool_or(is_negative_anomaly)                                  as has_fuel_negative_anomaly,
        bool_or(is_missing_value)                                     as has_fuel_missing_value,
        bool_or(is_extreme_outlier)                                   as has_fuel_extreme_outlier,
        coalesce(bool_or(is_missing_value) filter (
            where fuel_group in ('solar', 'wind')), false)            as has_vre_missing_value

    from joined
    group by 1, 2

),

-- Expected fuel groups per (BA, month): once a group is observed for a BA,
-- it is expected in EVERY subsequent month until a row in the
-- fuel_group_retirements seed explicitly ends the contract. A one-month
-- memory "learns" a sustained outage as normal by month two; persistence
-- from first observation cannot — the only way a group stops being
-- expected is a deliberate, reviewable seed change. First-seen months
-- stay era-aware for vocabulary changes (SNB simply has no first_seen
-- before 2024Q3), and the seed VRE declarations remain the fail-closed
-- floor for the chart-critical groups even in a warehouse's first month.
present_groups as (

    select distinct
        ba_code,
        period_utc,
        date_trunc('month', period_utc) as month,
        fuel_group
    from joined
    where fuel_group is not null

),

month_groups as (

    select ba_code, month, fuel_group, count(*) as n_hours
    from present_groups
    group by 1, 2, 3

),

group_first_seen as (

    -- Materiality floor: a group must be reported for a substantial slice of
    -- a month before it earns a standing expectation. MISO reported OIL for
    -- 24 hours in November 2021 and never again; without this floor that one
    -- blip invalidated every MISO hour for the next five years.
    select ba_code, fuel_group, min(month) as first_seen
    from month_groups
    where n_hours >= 100
    group by 1, 2

),

warehouse_months as (

    select distinct ba_code, month from month_groups

),

expected_groups as (

    select w.ba_code, w.month, g.fuel_group
    from group_first_seen g
    inner join warehouse_months w
        on  g.ba_code = w.ba_code
        and w.month >= g.first_seen
    left join {{ ref('fuel_group_retirements') }} r
        on  g.ba_code    = r.ba_code
        and g.fuel_group = r.fuel_group
    where r.ba_code is null or w.month < r.retired_month
    union
    select b.ba_code, m.month, x.fuel_group
    from {{ ref('balancing_authorities') }} b
    cross join (select distinct month from month_groups) m
    cross join (values ('solar'), ('wind')) as x(fuel_group)
    left join {{ ref('fuel_group_retirements') }} r
        on  b.ba_code    = r.ba_code
        and x.fuel_group = r.fuel_group
    where ((x.fuel_group = 'solar' and b.expects_solar)
        or (x.fuel_group = 'wind'  and b.expects_wind))
      -- A retirement contract silences the standing declaration too;
      -- otherwise retiring a declared VRE group would need edits to two
      -- seeds to take effect.
      and (r.ba_code is null or m.month < r.retired_month)

),

absent_per_hour as (

    select
        h.ba_code,
        h.period_utc,
        count(*)                                                      as n_groups_absent,
        bool_or(e.fuel_group in ('solar', 'wind'))                    as vre_group_absent
    from hourly h
    inner join expected_groups e
        on  h.ba_code = e.ba_code
        and date_trunc('month', h.period_utc) = e.month
    left join present_groups pg
        on  h.ba_code    = pg.ba_code
        and h.period_utc = pg.period_utc
        and e.fuel_group = pg.fuel_group
    where pg.fuel_group is null
    group by 1, 2

)

select
    h.* exclude (hour_has_solar, hour_has_wind),
    coalesce(a.vre_group_absent, false)                               as has_vre_absent,
    coalesce(a.n_groups_absent, 0) > 0                                as has_fuel_group_absent
from hourly h
left join absent_per_hour a
    on  h.ba_code    = a.ba_code
    and h.period_utc = a.period_utc

-- fct_grid_hourly: one row per (balancing authority, hour).
--
-- INCREMENTAL STRATEGY — per-BA watermarks
-- EIA restates recent hours, so every run reprocesses a trailing lookback
-- window (var: late_arrival_lookback_hours, default 96h) with delete+insert
-- on the (ba_code, period_utc) grain. The cutoff is computed PER balancing
-- authority, not globally: a global max(period_utc) would silently skip the
-- entire 2019-present history of a newly added BA (its old rows fall behind
-- authorities that are already current). With per-BA watermarks, a BA with
-- no rows in the target gets its full history on the next ordinary run.
--
-- COMPLETENESS — absence is not zero
-- EIA publishes demand well before fuel mix. Treating a not-yet-reported
-- fuel value as 0 would overstate net load for exactly the newest hours, so
-- net_load_mwh is only computed when the hour's fuel report exists
-- (is_net_load_valid); otherwise it is NULL and excluded from averages.
--
-- TIMEZONES
-- Raw periods are UTC. Local wall-clock time is derived here via each BA's
-- IANA zone from the seed — storing UTC and deriving local sidesteps DST
-- duplicate/missing-hour bugs at ingestion.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['ba_code', 'period_utc']
) }}

{% if is_incremental() %}
with ba_watermarks as (

    select ba_code, max(period_utc) as max_period
    from {{ this }}
    group by 1

),

metrics_source as (

    select m.*
    from {{ ref('int_grid_metrics_pivoted') }} m
    left join ba_watermarks w using (ba_code)
    where m.period_utc > coalesce(w.max_period, timestamp '1900-01-01')
        - interval '{{ var("late_arrival_lookback_hours", 96) }} hours'

),

fuel as (

    select f.*
    from {{ ref('int_fuel_mix_by_category') }} f
    left join ba_watermarks w using (ba_code)
    where f.period_utc > coalesce(w.max_period, timestamp '1900-01-01')
        - interval '{{ var("late_arrival_lookback_hours", 96) }} hours'

),
{% else %}
with metrics_source as (

    select * from {{ ref('int_grid_metrics_pivoted') }}

),

fuel as (

    select * from {{ ref('int_fuel_mix_by_category') }}

),
{% endif %}

-- The EIA-930 identity, evaluated ONCE and carried as booleans — in a
-- SHARED CTE so the incremental and full-refresh paths cannot drift.
-- identity_evaluable: all three metrics present (== region complete).
-- identity_residual_ok: residual within the publication band (50% of
-- demand, 5 GWh floor). Fail-closed: an hour where the identity CANNOT be
-- evaluated is just as unpublishable as one where it fails — "skip
-- validation when inputs are missing" is how a 400 GWh demand reading with
-- a deleted NG row walks into a chart.
metrics as (

    select
        *,
        (demand_mwh is not null
            and net_generation_mwh is not null
            and total_interchange_mwh is not null)          as identity_evaluable,
        (abs(demand_mwh - (net_generation_mwh - total_interchange_mwh))
            <= greatest(0.5 * abs(demand_mwh), 5000))       as identity_residual_ok
    from metrics_source

),

ba as (

    select * from {{ ref('balancing_authorities') }}

),

-- BA-relative plausibility: a single fuel group cannot physically exceed a
-- small multiple of the hour's OWN net generation — 400 GWh of solar in a
-- 25 GWh hour is absurd for CISO even though it clears a global unit-error
-- ceiling. Hour-local and BA-relative by construction, no history needed;
-- inert when net generation itself is missing (nothing to compare against).
plaus as (

    select
        f.ba_code,
        f.period_utc,
        (m.net_generation_mwh is not null
         and greatest(
                abs(coalesce(f.coal_mwh, 0)), abs(coalesce(f.natural_gas_mwh, 0)),
                abs(coalesce(f.oil_mwh, 0)),  abs(coalesce(f.nuclear_mwh, 0)),
                abs(coalesce(f.hydro_mwh, 0)), abs(coalesce(f.solar_mwh, 0)),
                abs(coalesce(f.wind_mwh, 0)), abs(coalesce(f.geothermal_mwh, 0)),
                abs(coalesce(f.storage_mwh, 0)), abs(coalesce(f.other_mwh, 0)))
             > 3 * greatest(abs(m.net_generation_mwh), 1))       as implausible
    from fuel f
    inner join metrics m
        on  f.ba_code    = m.ba_code
        and f.period_utc = m.period_utc

)

select
    m.ba_code,
    m.period_utc,
    timezone(b.timezone, timezone('UTC', m.period_utc))          as period_local,
    cast(timezone(b.timezone, timezone('UTC', m.period_utc)) as date)
                                                                  as local_date,
    extract(hour from timezone(b.timezone, timezone('UTC', m.period_utc)))
                                                                  as local_hour,

    m.demand_mwh,
    m.net_generation_mwh,
    m.total_interchange_mwh,

    f.coal_mwh,
    f.natural_gas_mwh,
    f.oil_mwh,
    f.nuclear_mwh,
    f.hydro_mwh,
    f.solar_mwh,
    f.wind_mwh,
    f.geothermal_mwh,
    f.storage_mwh,
    f.other_mwh,
    f.renewable_mwh,
    f.carbon_free_mwh,
    f.total_fuel_mwh,
    f.n_fuel_reports,
    f.unmapped_fuel_reports,

    -- Completeness: all three region metrics present, and the fuel report
    -- for this hour has arrived at all.
    (m.demand_mwh is not null
        and m.net_generation_mwh is not null
        and m.total_interchange_mwh is not null)                  as is_region_complete,

    -- Evaluated and absurd: distinguishes "the identity failed" from "the
    -- identity could not be checked" (both gate publication via
    -- is_grid_identity_valid below; only this one means proven-bad data).
    (m.identity_evaluable and not m.identity_residual_ok)        as has_grid_implausible_value,

    -- The publication precondition for anything demand-derived: the
    -- identity was evaluable AND held. Required by net load, completeness,
    -- daily clean aggregates, and therefore profiles.
    (m.identity_evaluable and m.identity_residual_ok)            as is_grid_identity_valid,
    (f.ba_code is not null and f.n_fuel_reports > 0)              as is_fuel_reported,
    (m.demand_mwh is not null
        and m.identity_evaluable and m.identity_residual_ok
        and f.ba_code is not null and f.n_fuel_reports > 0
        and not coalesce(f.has_vre_missing_value, false)
        and not coalesce(f.has_vre_absent, false)
        and not coalesce(f.has_vre_negative_anomaly, false)
        and not coalesce(f.has_fuel_extreme_outlier, false)
        and not coalesce(pl.implausible, false))                       as is_net_load_valid,

    -- Fuel-mix validity is a STRICTER, different concept than net-load
    -- validity: shares need the whole denominator, so a null COAL value —
    -- irrelevant to demand-minus-VRE — still invalidates the mix. Report
    -- present, every value populated, nothing flagged, every code mapped.
    (f.ba_code is not null and f.n_fuel_reports > 0
        and not coalesce(f.has_fuel_missing_value, false)
        and not coalesce(f.has_fuel_negative_anomaly, false)
        and not coalesce(f.has_fuel_extreme_outlier, false)
        and not coalesce(f.has_vre_absent, false)
        and not coalesce(f.has_fuel_group_absent, false)
        and not coalesce(pl.implausible, false)
        and coalesce(f.unmapped_fuel_reports, 0) = 0)             as is_fuel_mix_valid,

    -- Net load: demand minus variable renewables — but ONLY when the fuel
    -- report exists AND its solar/wind values are intact. Missing is not
    -- zero, whether the whole report is absent or just a VRE value is null.
    case
        when m.demand_mwh is not null
             and m.identity_evaluable and m.identity_residual_ok
             and f.ba_code is not null and f.n_fuel_reports > 0
             and not coalesce(f.has_vre_missing_value, false)
             and not coalesce(f.has_vre_absent, false)
             and not coalesce(f.has_vre_negative_anomaly, false)
             and not coalesce(f.has_fuel_extreme_outlier, false)
             and not coalesce(pl.implausible, false)
        then m.demand_mwh - coalesce(f.solar_mwh, 0) - coalesce(f.wind_mwh, 0)
    end                                                           as net_load_mwh,

    f.renewable_mwh   / nullif(f.total_fuel_mwh, 0)               as renewable_share,
    f.carbon_free_mwh / nullif(f.total_fuel_mwh, 0)               as carbon_free_share,

    m.has_negative_anomaly,
    m.has_extreme_outlier,
    m.has_missing_value,
    coalesce(f.has_fuel_negative_anomaly, false)                  as has_fuel_negative_anomaly,
    coalesce(f.has_fuel_missing_value, false)                     as has_fuel_missing_value,
    coalesce(f.has_vre_missing_value, false)                      as has_vre_missing_value,
    coalesce(f.has_vre_absent, false)                             as has_vre_absent,
    coalesce(f.has_fuel_extreme_outlier, false)                   as has_fuel_extreme_outlier,
    coalesce(f.has_vre_negative_anomaly, false)                   as has_vre_negative_anomaly,
    coalesce(f.has_fuel_group_absent, false)                      as has_fuel_group_absent,
    coalesce(pl.implausible, false)                               as has_fuel_implausible_value

from metrics m
left join plaus pl
    on  m.ba_code    = pl.ba_code
    and m.period_utc = pl.period_utc
-- Inner join: local-time analytics require a timezone, so the fact only
-- covers BAs present in the seed. A warn-level relationship test on staging
-- surfaces any BA reporting data before it has been added there.
inner join ba b
    on m.ba_code = b.ba_code
left join fuel f
    on  m.ba_code    = f.ba_code
    and m.period_utc = f.period_utc

{{ config(severity='error', warn_if='>0', error_if='>3') }}
-- Coverage against the SEED, not against whatever happens to be present:
-- every seeded balancing authority must have (a) metrics within 5 days,
-- (b) a fuel report within 7 days (fuel legitimately lags demand), and
-- (c) at least 20 usable hours in the last 72: fuel reported, valid net
-- load, valid fuel mix. In practice (c) is the binding rule. A BA that is
-- entirely absent appears here — grouping only existing rows would let a
-- missing BA vanish from its own freshness check, and one fresh BA can't
-- mask the others going silent.
--
-- Severity policy: a FEW BAs with a coverage problem is an upstream
-- availability event (a BA going dark or restating late at EIA), so it
-- WARNS and the rest keep publishing — the marts already carry
-- is_complete_day / is_fuel_mix_complete_day flags, so consumers can see
-- the gap. FOUR OR MORE of the 11 seeded BAs (a third of the fleet) is an
-- ERROR: that many independent BAs do not go dark together, so it means
-- something is broken on our side (a watermark, a route, a facet) and the
-- build must stop before publish. The threshold is 3, not 1, because the
-- Sept 5 and Sept 7 2026 nightly runs each saw TWO BAs trip at once from
-- upstream noise. Revisit if the seed grows.
-- Context: Sept 5-12 2026, ISNE's OIL fuel group went intermittently absent
-- at EIA, the single failing row blocked the whole publish for six nights.

with per_ba as (

    select
        ba_code,
        max(period_utc)                                            as newest_metrics,
        max(period_utc) filter (where is_fuel_reported)            as newest_fuel,
        -- Quantity, not mere existence: one current fuel row must not count
        -- as a working feed. Require a substantially-reported recent window.
        count(*) filter (where is_fuel_reported
                           and period_utc >= timezone('UTC', now())
                               - interval '72 hours')              as fuel_hours_72h,
        count(*) filter (where is_net_load_valid
                           and period_utc >= timezone('UTC', now())
                               - interval '72 hours')              as net_load_valid_72h,
        count(*) filter (where is_fuel_mix_valid
                           and period_utc >= timezone('UTC', now())
                               - interval '72 hours')              as fuel_mix_valid_72h
    from {{ ref('fct_grid_hourly') }}
    group by 1

)

select
    b.ba_code,
    p.newest_metrics,
    p.newest_fuel,
    case
        when p.ba_code is null then 'ba_entirely_absent'
        when p.newest_metrics < timezone('UTC', now()) - interval '5 days'
            then 'metrics_stale'
        when p.fuel_hours_72h < 20 then 'fuel_quantity_low'
        when p.net_load_valid_72h < 20 then 'net_load_unusable'
        when p.fuel_mix_valid_72h < 20 then 'fuel_mix_unusable'
        else 'fuel_stale_or_absent'
    end as issue
from {{ ref('balancing_authorities') }} b
left join per_ba p
    on b.ba_code = p.ba_code
where p.ba_code is null
   or p.newest_metrics < timezone('UTC', now()) - interval '5 days'
   or p.newest_fuel is null
   or p.newest_fuel < timezone('UTC', now()) - interval '7 days'
   or p.fuel_hours_72h < 20
   -- Reported is not usable: a BA whose every hour is flagged (the DUK
   -- misconfiguration class) must fail coverage, not haunt the marts.
   or p.net_load_valid_72h < 20
   or p.fuel_mix_valid_72h < 20

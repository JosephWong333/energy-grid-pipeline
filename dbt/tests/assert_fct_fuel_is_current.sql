-- Reconciliation, fuel side: every BA-hour the fuel intermediate holds must
-- look the same in the fact. A mismatch means the incremental fact fell
-- behind its inputs where the nightly's reprocess window can't reach: fuel
-- that landed after the window had moved past its hour (more than
-- fuel_heal_max_days behind, or hours stranded before the Oct 2026 fix), or
-- fuel-group flags that changed after the hour was built (seed edits, or a
-- BA starting to report a new fuel group).
-- Warn, not error: the hours are stale, not wrong, and the fix is one nightly
-- dispatch with dbt_full_refresh=true.
{{ config(severity='warn') }}

select
    f.ba_code,
    f.period_utc,
    case when not f.is_fuel_reported then 'fuel_missing_from_fct'
         else 'fuel_flags_stale' end as issue
from {{ ref('fct_grid_hourly') }} f
inner join {{ ref('int_fuel_mix_by_category') }} i
    on  f.ba_code    = i.ba_code
    and f.period_utc = i.period_utc
where not f.is_fuel_reported
   or f.has_fuel_group_absent <> i.has_fuel_group_absent
   or f.has_vre_absent        <> i.has_vre_absent

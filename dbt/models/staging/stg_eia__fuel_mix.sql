-- Staging: hourly net generation by fuel type.
--
-- Negative values mean net CONSUMPTION, which is legitimate in EIA-930:
--   * storage codes charge (PS/BAT/OES/SNB/WNB/UES)
--   * OTH is the storage bucket for BAs that report no BAT code at all --
--     CISO reports no BAT ever, and its OTH reaches -9,838 MWh, which is
--     its battery fleet charging
--   * solar/wind draw small auxiliary loads (inverters, trackers, turbine
--     heaters); CISO's worst observed is -74 MWh against ~15 GW of solar
-- So this flag catches only negatives too large to be consumption. Absurd
-- magnitudes are caught downstream by the BA-relative plausibility bound in
-- fct_grid_hourly. Floor set from evidence in the 2019-2026 backfill.

with source as (

    select * from {{ source('eia_raw', 'eia_fuel_mix') }}

)

select
    period_utc,
    respondent                                   as ba_code,
    fuel_code,
    value                                        as generation_mwh,

    coalesce(fuel_code not in ('PS', 'BAT', 'OES', 'SNB', 'WNB', 'UES', 'OTH')
             and value < -1000, false)           as is_negative_anomaly,
    coalesce(abs(value) > 500000, false)         as is_extreme_outlier,
    value is null                                as is_missing_value,

    _ingested_at

from source

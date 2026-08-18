-- Staging: rename, type, and quality-flag the region-data metrics.
--
-- Quality philosophy: FLAG, don't drop. Raw observations are preserved with
-- boolean flags so downstream models decide how to treat anomalies, and the
-- flags themselves are testable/monitorable.

with source as (

    select * from {{ source('eia_raw', 'eia_region_data') }}

)

select
    period_utc,
    respondent                                   as ba_code,
    metric_code,
    case metric_code
        when 'D'  then 'demand'
        when 'NG' then 'net_generation'
        when 'TI' then 'total_interchange'
        else 'unknown'
    end                                          as metric_name,
    value                                        as value_mwh,

    -- Negative demand or negative net generation is a reporting glitch.
    -- Negative *interchange* is legitimate (net imports), so it is excluded.
    coalesce(metric_code in ('D', 'NG') and value < 0, false)
                                                 as is_negative_anomaly,
    coalesce(abs(value) > 500000, false)         as is_extreme_outlier,
    value is null                                as is_missing_value,

    _ingested_at

from source

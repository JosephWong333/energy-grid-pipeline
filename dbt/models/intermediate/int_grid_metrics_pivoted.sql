-- Pivot the long metric rows (D / NG / TI) into one row per BA-hour.
-- Hours where an anomaly was flagged carry the flag forward rather than
-- being silently repaired.

select
    ba_code,
    period_utc,

    max(case when metric_code = 'D'  then value_mwh end)  as demand_mwh,
    max(case when metric_code = 'NG' then value_mwh end)  as net_generation_mwh,
    max(case when metric_code = 'TI' then value_mwh end)  as total_interchange_mwh,

    bool_or(is_negative_anomaly)                           as has_negative_anomaly,
    bool_or(is_extreme_outlier)                            as has_extreme_outlier,
    bool_or(is_missing_value)                              as has_missing_value

from {{ ref('stg_eia__grid_metrics') }}
group by 1, 2

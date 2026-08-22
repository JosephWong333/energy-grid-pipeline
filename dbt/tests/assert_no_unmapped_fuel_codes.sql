-- Warn when the feed contains fuel codes absent from the fuel_types seed.
-- They still flow into other_mwh, but the seed should be updated.

{{ config(severity = 'warn') }}

select ba_code, period_utc, unmapped_fuel_reports
from {{ ref('int_fuel_mix_by_category') }}
where unmapped_fuel_reports > 0

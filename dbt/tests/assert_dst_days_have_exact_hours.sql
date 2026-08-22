-- Pin the DST arithmetic to KNOWN transition dates, not just to the formula
-- that computes it: 2026-03-08 (US spring-forward) must expect exactly 23
-- local hours and 2025-11-02 (fall-back) exactly 25, for every US-zone BA
-- present on those dates. Fixtures carry deterministic, glitch-free slices
-- around both dates, so this is exercised in every CI run; after a real
-- backfill it pins the same dates in production history.

select ba_code, local_date, hours_expected
from {{ ref('mart_grid_daily') }}
where (local_date = date '2026-03-08' and hours_expected != 23)
   or (local_date = date '2025-11-02' and hours_expected != 25)

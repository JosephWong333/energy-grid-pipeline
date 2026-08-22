{% test non_negative(model, column_name) %}
-- Fails on any row where the column is negative. NULLs pass: missingness is
-- tracked separately by the is_missing_value flags.
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}

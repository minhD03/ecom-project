{% test not_half_null(model, column_name, threshold=0.5) %}

select null_pct
from (
    select
        sum(case when {{ column_name }} is null then 1 else 0 end)::float / nullif(count(*), 0) as null_pct
    from {{ model }}
) sub
where null_pct > {{ threshold }}

{% endtest %}
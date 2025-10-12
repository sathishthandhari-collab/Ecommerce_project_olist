{% test expect_z_score_within(model, column_name, threshold=3, partition_by=[], min_rows=30) %}
with base as (
  select
    {{ column_name }},
    {% for col in partition_by %} {{ col }}, {% endfor %}
    count(*) over ({% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}) as _n,
    avg({{ column_name }}) over ({% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}) as _mu,
    stddev({{ column_name }}) over ({% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}) as _sigma
  from {{ model }}
  where {{ column_name }} is not null
),
z as (
  select *,
         abs({{ column_name }} - _mu) / nullif(_sigma, 0) as z_score
  from base
  where _n >= {{ min_rows }}
)
select {{ column_name }},
       z_score
from z
where z_score > {{ threshold }}
{% endtest %}

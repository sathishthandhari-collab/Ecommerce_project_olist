{% macro calculate_z_score(column, partition_by=none) %}
  ABS(
    ({{ column }} - AVG({{ column }}) OVER (
      {% if partition_by %}PARTITION BY {{ partition_by }}{% endif %}
    )) / NULLIF(
      STDDEV({{ column }}) OVER (
        {% if partition_by %}PARTITION BY {{ partition_by }}{% endif %}
      ), 0
    )
  )
{% endmacro %}

{% macro data_quality_score(checks) %}
  (
    {% for check in checks %}
    
      CASE WHEN {{ check }} THEN 1 ELSE 0 END
    {%- if not loop.last %} + {% endif %}
{% endfor %}
  ) / {{ checks|length }}.0
{% endmacro %}

{% macro standardize_timestamp(timestamp_col) %}
  CONVERT_TIMEZONE('America/Sao_Paulo', 'UTC', {{ timestamp_col }}::TIMESTAMP_NTZ)
{% endmacro %}

{% macro generate_surrogate_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}

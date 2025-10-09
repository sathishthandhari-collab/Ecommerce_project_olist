{% macro generate_executive_time_spine(start_date, end_date, grain='month') %}
  WITH date_spine AS (
    SELECT 
      DATEADD('{{ grain }}', ROW_NUMBER() OVER (ORDER BY 1) - 1, '{{ start_date }}'::DATE) AS report_date
    FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('{{ grain }}', '{{ start_date }}'::DATE, '{{ end_date }}'::DATE) + 1))
  )
  SELECT 
    report_date,
    EXTRACT(year FROM report_date) AS report_year,
    EXTRACT(month FROM report_date) AS report_month,
    EXTRACT(quarter FROM report_date) AS report_quarter,
    CASE WHEN DAYOFWEEK(report_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
  FROM date_spine
{% endmacro %}

-- Calculate period-over-period metrics
{% macro calculate_period_over_period(metric_col, partition_cols=[], order_col='report_date', periods=1) %}
{{ metric_col }} - LAG({{ metric_col }}, {{ periods }}) OVER (
    {% if partition_cols %}PARTITION BY {{ partition_cols | join(', ') }}{% endif %}
    ORDER BY {{ order_col }}
  ) AS {{ metric_col }}_change,
  
  ({{ metric_col }} - LAG({{ metric_col }}, {{ periods }}) OVER (
    {% if partition_cols %}PARTITION BY {{ partition_cols | join(', ') }}{% endif %}
    ORDER BY {{ order_col }}
  )) / NULLIF(LAG({{ metric_col }}, {{ periods }}) OVER (
    {% if partition_cols %}PARTITION BY {{ partition_cols | join(', ') }}{% endif %}
    ORDER BY {{ order_col }}
  ), 0) * 100 AS {{ metric_col }}_change_pct
{% endmacro %}

-- Executive-level aggregation with statistical confidence
{% macro executive_aggregation(base_query, metric_configs) %}
  WITH base AS (
    {{ base_query }}
  ),
  
  aggregated AS (
    SELECT 
      report_date,
      {% for metric, config in metric_configs.items() %}
      {{ config.get('agg_func', 'SUM') }}({{ metric }}) AS {{ metric }},
      STDDEV({{ metric }}) AS {{ metric }}_stddev,
      COUNT({{ metric }}) AS {{ metric }}_sample_size,
      {% endfor %}
      COUNT(*) AS total_records
    FROM base
    GROUP BY report_date
  ),
  
  with_confidence AS (
    SELECT *,
      {% for metric, config in metric_configs.items() %}
      {{ metric }} - (1.96 * {{ metric }}_stddev / SQRT({{ metric }}_sample_size)) AS {{ metric }}_lower_ci,
      {{ metric }} + (1.96 * {{ metric }}_stddev / SQRT({{ metric }}_sample_size)) AS {{ metric }}_upper_ci,
      {% endfor %}
      CURRENT_TIMESTAMP AS last_updated
    FROM aggregated
  )
  
  SELECT * FROM with_confidence
{% endmacro %}
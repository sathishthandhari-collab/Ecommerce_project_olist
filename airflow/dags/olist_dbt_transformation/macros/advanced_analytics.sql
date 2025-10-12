{% macro calculate_clv_with_confidence(frequency_col, monetary_col, recency_col, confidence_level=0.95) %}
    
  WITH clv_base AS (
    SELECT 
      -- Basic CLV calculation (frequency * monetary / recency decay)
      ({{ frequency_col }} * {{ monetary_col }}) / (1 + ({{ recency_col }} / 365.0)) AS base_clv,
      
      -- Standard error calculation for confidence intervals
      SQRT(
        POWER({{ monetary_col }} * STDDEV({{ frequency_col }}) OVER(), 2) +
        POWER({{ frequency_col }} * STDDEV({{ monetary_col }}) OVER(), 2)
      ) / SQRT(COUNT(*) OVER()) AS standard_error
  ),
  
  clv_with_intervals AS (
    SELECT *,
      -- Confidence intervals using t-distribution approximation
      base_clv - (1.96 * standard_error) AS clv_lower_bound,
      base_clv + (1.96 * standard_error) AS clv_upper_bound,
      
      -- CLV segments based on confidence intervals
      CASE 
        WHEN base_clv >= 1000 THEN 'High Value'
        WHEN base_clv >= 500 THEN 'Medium Value'  
        WHEN base_clv >= 100 THEN 'Low Value'
        ELSE 'Minimal Value'
      END AS clv_segment
    FROM clv_base
  )
  SELECT * FROM clv_with_intervals

{% endmacro %}

-- Multi-dimensional anomaly scoring
{% macro calculate_anomaly_score(columns, weights=none) %}
    {%- if weights is none -%}
        {%- set weights = [1.0] * columns|length -%}  
    {%- endif -%}
  
    (
    {%- for col in columns -%}
        ({{ weights[loop.index0] }} * 
       ABS({{ col }} - AVG({{ col }}) OVER ()) / 
       NULLIF(STDDEV({{ col }}) OVER (), 0)
      )
        {%- if not loop.last %} + {% endif -%}
    {%- endfor -%}
    ) / {{ weights|sum }}
{% endmacro %}

-- Business health score calculation
{% macro calculate_health_score(metrics, weights) %}
    
  (
    {%- for metric, weight in weights.items() -%}
        ({{ weight }} * {{ metrics[metric] }})
        {%- if not loop.last %} + {% endif -%}
    {%- endfor -%}
  )
{% endmacro %}

-- Statistical significance test for cohorts
{% macro cohort_significance_test(treatment_col, control_col, alpha=0.05) %}
  WITH stats AS (
    SELECT 
      AVG({{ treatment_col }}) AS treatment_mean,
      AVG({{ control_col }}) AS control_mean,
      STDDEV({{ treatment_col }}) AS treatment_std,
      STDDEV({{ control_col }}) AS control_std,
      COUNT({{ treatment_col }}) AS treatment_n,
      COUNT({{ control_col }}) AS control_n
  ),
  
  significance AS (
    SELECT *,
      -- T-test calculation
      (treatment_mean - control_mean) / 
      SQRT((POWER(treatment_std, 2) / treatment_n) + (POWER(control_std, 2) / control_n)) AS t_statistic,
      
      -- Degrees of freedom (Welch's formula)
      POWER((POWER(treatment_std, 2) / treatment_n) + (POWER(control_std, 2) / control_n), 2) /
      ((POWER(treatment_std, 4) / (POWER(treatment_n, 2) * (treatment_n - 1))) + 
       (POWER(control_std, 4) / (POWER(control_n, 2) * (control_n - 1)))) AS degrees_freedom
    FROM stats
  )
  
  SELECT *,
    CASE WHEN ABS(t_statistic) > 1.96 THEN TRUE ELSE FALSE END AS is_significant
  FROM significance
{% endmacro %}
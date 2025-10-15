{#
    ADVANCED ANALYTICS MACROS: Statistical and business intelligence functions for sophisticated analysis
#}

{% macro calculate_clv_with_confidence(frequency_col, monetary_col, recency_col, confidence_level=0.95) %}
    {# 
    CUSTOMER LIFETIME VALUE WITH STATISTICAL CONFIDENCE: Advanced CLV calculation with uncertainty quantification
    
    PURPOSE: Calculates CLV with confidence intervals to support data-driven investment decisions
    
    PARAMETERS:
    - frequency_col: Purchase frequency metric (orders per time period)
    - monetary_col: Average monetary value per transaction
    - recency_col: Days since last purchase (recency decay factor)
    - confidence_level: Statistical confidence level for intervals (default 95%)
    
    RETURNS: CLV with upper/lower bounds and segment classification
    
    BUSINESS VALUE: Enables confident customer investment decisions with known uncertainty ranges
    #}
    
    WITH clv_base AS (
        SELECT
            -- CLV CORE CALCULATION: Frequency × Monetary Value adjusted for recency decay
            ({{ frequency_col }} * {{ monetary_col }}) / (1 + ({{ recency_col }} / 365.0)) AS base_clv,
            
            -- UNCERTAINTY QUANTIFICATION: Standard error calculation for confidence intervals
            SQRT(
                -- FREQUENCY UNCERTAINTY: Impact of purchase frequency variability on CLV
                POWER({{ monetary_col }} * STDDEV({{ frequency_col }}) OVER(), 2) +
                -- MONETARY UNCERTAINTY: Impact of transaction value variability on CLV  
                POWER({{ frequency_col }} * STDDEV({{ monetary_col }}) OVER(), 2)
            ) / SQRT(COUNT(*) OVER()) AS standard_error
    ),
    
    clv_with_intervals AS (
        SELECT *,
            -- CONFIDENCE INTERVAL BOUNDS: Statistical bounds for decision-making confidence
            base_clv - (1.96 * standard_error) AS clv_lower_bound,  -- 95% CI lower bound
            base_clv + (1.96 * standard_error) AS clv_upper_bound,  -- 95% CI upper bound
            
            -- BUSINESS SEGMENTATION: Value-based customer classification for strategic planning
            CASE
                WHEN base_clv >= 1000 THEN 'High Value'      -- Premium customer segment
                WHEN base_clv >= 500 THEN 'Medium Value'     -- Core customer segment  
                WHEN base_clv >= 100 THEN 'Low Value'        -- Base customer segment
                ELSE 'Minimal Value'                         -- Cost management segment
            END AS clv_segment
        FROM clv_base
    )
    
    SELECT * FROM clv_with_intervals
{% endmacro %}

{% macro calculate_anomaly_score(columns, weights=none) %}
    {# 
    MULTI-DIMENSIONAL ANOMALY DETECTION: Weighted composite anomaly scoring across multiple dimensions
    
    PURPOSE: Combines multiple anomaly indicators into single weighted score for prioritization
    
    PARAMETERS:
    - columns: List of columns to include in anomaly calculation
    - weights: Optional list of weights for each column (defaults to equal weighting)
    
    RETURNS: Weighted composite anomaly score for threat prioritization
    
    BUSINESS VALUE: Enables sophisticated fraud detection and operational anomaly identification
    #}
    
    {%- if weights is none -%}
        {%- set weights = [1.0] * columns|length -%}  {# Default to equal weighting #}
    {%- endif -%}

    (
        {%- for col in columns -%}
        -- WEIGHTED Z-SCORE COMPONENT: Individual column anomaly score with business importance weighting
        ({{ weights[loop.index0] }} *
            ABS({{ col }} - AVG({{ col }}) OVER ()) /
            NULLIF(STDDEV({{ col }}) OVER (), 0)
        )
        {%- if not loop.last %} + {% endif -%}  {# Sum weighted components #}
        {%- endfor -%}
    ) / {{ weights|sum }}  {# NORMALIZATION: Maintain 0-N scale regardless of number of dimensions #}
{% endmacro %}

{% macro calculate_health_score(metrics, weights) %}
    {# 
    BUSINESS HEALTH SCORING: Configurable weighted scoring system for business performance assessment
    
    PURPOSE: Combines multiple performance metrics into single health score with business-defined weights
    
    PARAMETERS:
    - metrics: Dictionary of metric names and their column references
    - weights: Dictionary of metric names and their business importance weights
    
    RETURNS: Weighted composite health score for performance assessment
    
    BUSINESS VALUE: Enables consistent performance evaluation across different business dimensions
    #}
    
    (
        {%- for metric, weight in weights.items() -%}
        -- WEIGHTED METRIC COMPONENT: Individual metric contribution to overall health score
        ({{ weight }} * {{ metrics[metric] }})
        {%- if not loop.last %} + {% endif -%}  {# Sum weighted metric contributions #}
        {%- endfor -%}
    )
{% endmacro %}

{% macro cohort_significance_test(treatment_col, control_col, alpha=0.05) %}
    {# 
    STATISTICAL SIGNIFICANCE TESTING: A/B test and cohort analysis with proper statistical validation
    
    PURPOSE: Determines if observed differences between groups are statistically significant
    
    PARAMETERS:
    - treatment_col: Metric values for treatment/test group
    - control_col: Metric values for control/baseline group  
    - alpha: Significance level threshold (default 5%)
    
    RETURNS: T-test results with significance determination for business decision support
    
    BUSINESS VALUE: Ensures A/B test and cohort analysis conclusions are statistically valid
    #}
    
    WITH stats AS (
        SELECT
            -- GROUP STATISTICS: Calculate descriptive statistics for both cohorts
            AVG({{ treatment_col }}) AS treatment_mean,      -- Treatment group average
            AVG({{ control_col }}) AS control_mean,          -- Control group average
            STDDEV({{ treatment_col }}) AS treatment_std,    -- Treatment group variability
            STDDEV({{ control_col }}) AS control_std,        -- Control group variability
            COUNT({{ treatment_col }}) AS treatment_n,       -- Treatment sample size
            COUNT({{ control_col }}) AS control_n            -- Control sample size
    ),
    
    significance AS (
        SELECT *,
            -- T-STATISTIC CALCULATION: Measure of difference relative to variability
            (treatment_mean - control_mean) /
            SQRT((POWER(treatment_std, 2) / treatment_n) + (POWER(control_std, 2) / control_n)) AS t_statistic,
            
            -- DEGREES OF FREEDOM: Using Welch's formula for unequal variances
            POWER((POWER(treatment_std, 2) / treatment_n) + (POWER(control_std, 2) / control_n), 2) /
            ((POWER(treatment_std, 4) / (POWER(treatment_n, 2) * (treatment_n - 1))) +
             (POWER(control_std, 4) / (POWER(control_n, 2) * (control_n - 1)))) AS degrees_freedom
        FROM stats
    )
    
    SELECT *,
        -- SIGNIFICANCE DETERMINATION: Compare t-statistic to critical value (±1.96 for 95% confidence)
        CASE WHEN ABS(t_statistic) > 1.96 THEN TRUE ELSE FALSE END AS is_significant
    FROM significance
{% endmacro %}
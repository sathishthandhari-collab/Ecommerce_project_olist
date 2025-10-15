{#
    DATA QUALITY MACROS: Core reusable functions for statistical analysis and data validation
#}

{% macro calculate_z_score(column, partition_by=none) %}
    {# 
    STATISTICAL ANOMALY DETECTION: Calculates Z-score (standard deviations from mean) for anomaly identification
    
    PURPOSE: Identifies outliers by measuring how many standard deviations a value is from the population mean
    
    PARAMETERS:
    - column: The numeric column to analyze for outliers
    - partition_by: Optional grouping column(s) for calculating Z-scores within segments
    
    RETURNS: Absolute Z-score value (always positive for easier threshold comparison)
    
    MATHEMATICAL FORMULA: |x - μ| / σ where:
    - x = individual value
    - μ = population/partition mean  
    - σ = population/partition standard deviation
    #}
    
    ABS(
        -- DEVIATION FROM MEAN: Calculate how far the value is from the average
        ({{ column }} - AVG({{ column }}) OVER (
            {% if partition_by %}PARTITION BY {{ partition_by }}{% endif %}
        )) / 
        -- STANDARDIZATION: Divide by standard deviation to normalize the scale
        NULLIF(
            STDDEV({{ column }}) OVER (
                {% if partition_by %}PARTITION BY {{ partition_by }}{% endif %}
            ), 0  -- Prevent division by zero for constant values
        )
    )
{% endmacro %}

{% macro data_quality_score(checks) %}
    {# 
    COMPREHENSIVE DATA QUALITY SCORING: Aggregates multiple validation rules into a single quality score
    
    PURPOSE: Provides a normalized score (0-1) indicating what percentage of quality checks pass
    
    PARAMETERS:
    - checks: List of SQL boolean expressions that should evaluate to TRUE for good data
    
    RETURNS: Decimal score between 0 (all checks failed) and 1 (all checks passed)
    
    BUSINESS VALUE: Enables data quality monitoring, SLA tracking, and automated quality gates
    #}
    
    (
        -- QUALITY RULE EVALUATION: Convert each boolean check to 1 (pass) or 0 (fail)
        {% for check in checks %}
        CASE WHEN {{ check }} THEN 1 ELSE 0 END
        {%- if not loop.last %} + {% endif %}  -- Sum all passing checks
        {% endfor %}
    ) / {{ checks|length }}.0  -- NORMALIZATION: Convert to percentage (0.0 - 1.0 scale)
{% endmacro %}

{% macro standardize_timestamp(timestamp_col) %}
    {# 
    TIMEZONE STANDARDIZATION: Converts Brazilian local time to UTC for consistent global analysis
    
    PURPOSE: Ensures all timestamps are in UTC timezone for consistent time-based analysis
    
    PARAMETERS:
    - timestamp_col: Column containing timestamp data in São Paulo timezone
    
    RETURNS: UTC timestamp for standardized temporal analysis
    
    BUSINESS CONTEXT: Brazilian e-commerce operates in America/Sao_Paulo timezone, 
    but analytics systems require UTC for global consistency
    #}
    
    -- TIMEZONE CONVERSION: São Paulo local time → UTC for global standardization
    CONVERT_TIMEZONE('America/Sao_Paulo', 'UTC', {{ timestamp_col }}::TIMESTAMP_NTZ)
{% endmacro %}

{% macro generate_surrogate_key(columns) %}
    {# 
    DIMENSIONAL MODELING KEY GENERATION: Creates stable, unique surrogate keys for dimension tables
    
    PURPOSE: Generates deterministic hash-based keys for dimensional modeling and efficient joins
    
    PARAMETERS:
    - columns: List of natural key columns to combine into surrogate key
    
    RETURNS: Hash-based surrogate key that remains stable across runs
    
    TECHNICAL IMPLEMENTATION: Leverages dbt_utils for industry-standard key generation
    #}
    
    -- SURROGATE KEY CREATION: Uses dbt_utils for standardized hash-based key generation
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}
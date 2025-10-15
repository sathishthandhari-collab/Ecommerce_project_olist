{#
    CUSTOM DATA TESTS: Advanced validation functions for business-specific data quality requirements
#}

{% test expect_z_score_within(model, column_name, threshold=3, partition_by=[], min_rows=30) %}
    {# 
    STATISTICAL OUTLIER DETECTION TEST: Identifies values that are statistical outliers within acceptable bounds
    
    PURPOSE: Validates that numeric values fall within acceptable statistical ranges (Z-score thresholds)
    
    PARAMETERS:
    - model: The dbt model/table to test
    - column_name: Numeric column to analyze for outliers
    - threshold: Maximum acceptable Z-score (default 3 = 99.7% of normal distribution)
    - partition_by: Optional list of columns to calculate Z-scores within groups
    - min_rows: Minimum sample size required for statistical validity (default 30)
    
    TEST LOGIC: Fails if any values exceed the Z-score threshold (indicating statistical outliers)
    
    BUSINESS VALUE: Detects data quality issues, pricing anomalies, and potential fraud
    #}

    with base as (
        select
            {{ column_name }},
            {% for col in partition_by %} {{ col }}, {% endfor %}
            -- STATISTICAL CONTEXT: Calculate population statistics within each partition
            count(*) over ({% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}) as _n,
            avg({{ column_name }}) over ({% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}) as _mu,
            stddev({{ column_name }}) over ({% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}) as _sigma
        from {{ model }}
        where {{ column_name }} is not null
    ),
    
    z as (
        select *,
            -- Z-SCORE CALCULATION: Standard deviations from mean
            abs({{ column_name }} - _mu) / nullif(_sigma, 0) as z_score
        from base
        -- STATISTICAL VALIDITY: Only test partitions with sufficient sample size
        where _n >= {{ min_rows }}
    )

    -- TEST FAILURE CONDITIONS: Return records that exceed the statistical threshold
    select {{ column_name }},
           z_score
    from z
    where z_score > {{ threshold }}
{% endtest %}

{% test expect_column_pair_values_A_to_be_smaller_than_B(model, column_A, column_B, or_equal=false) %}
    {# 
    LOGICAL RELATIONSHIP VALIDATION: Ensures one column value is smaller than another
    
    PURPOSE: Validates logical business relationships between numeric columns
    
    PARAMETERS:
    - model: The dbt model/table to test
    - column_A: First column (should be smaller)
    - column_B: Second column (should be larger)
    - or_equal: Whether equal values are acceptable (default false)
    
    COMMON USE CASES:
    - Start date < End date
    - Min value < Max value  
    - Cost < Price
    - Order date < Delivery date
    
    TEST LOGIC: Fails if column_A >= column_B (or > when or_equal=true)
    
    BUSINESS VALUE: Prevents logical inconsistencies that would break downstream analytics
    #}

    SELECT
        {{ column_A }},
        {{ column_B }},
        -- DESCRIPTIVE ERROR MESSAGE: Explains the validation rule that failed
        '{{ column_A }} should be {{ "less than or equal to" if or_equal else "less than" }} {{ column_B }}' AS failure_reason
    FROM {{ model }}
    WHERE
        {{ column_A }} IS NOT NULL
        AND {{ column_B }} IS NOT NULL
        -- LOGICAL VALIDATION: Check if the relationship is violated
        AND {{ column_A }} {{ ">=" if not or_equal else ">" }} {{ column_B }}
{% endtest %}

{% test expect_foreign_key_relationships_with_context(model, column_name, to, field, context_columns=[]) %}
    {# 
    ENHANCED REFERENTIAL INTEGRITY TEST: Foreign key validation with business context information
    
    PURPOSE: Validates referential integrity while providing rich context for debugging failures
    
    PARAMETERS:
    - model: Source model to validate
    - column_name: Foreign key column in source model
    - to: Target model that should contain the key
    - field: Primary key column in target model
    - context_columns: Additional columns to include in failure reports for debugging
    
    ENHANCED FEATURES:
    - Provides occurrence counts for duplicate key analysis
    - Includes context columns for rich error reporting
    - Generates descriptive failure messages for operational teams
    
    TEST LOGIC: Fails if any foreign keys don't exist in the target table
    
    BUSINESS VALUE: Ensures data integrity while providing actionable debugging information
    #}

    WITH source_data AS (
        SELECT
            {{ column_name }},
            {% for col in context_columns %}
            {{ col }},
            {% endfor %}
            -- FREQUENCY ANALYSIS: Count occurrences for duplicate detection
            COUNT(*) AS occurrence_count
        FROM {{ model }}
        WHERE {{ column_name }} IS NOT NULL
        GROUP BY
            {{ column_name }}
            {%- for col in context_columns -%}
            , {{ col }}
            {%- endfor %}
    ),
    
    target_data AS (
        -- TARGET KEY UNIVERSE: All valid foreign key values
        SELECT DISTINCT {{ field }}
        FROM {{ to }}
    ),
    
    orphaned_records AS (
        -- REFERENTIAL INTEGRITY VIOLATIONS: Foreign keys without matching targets
        SELECT s.*
        FROM source_data s
        LEFT JOIN target_data t ON s.{{ column_name }} = t.{{ field }}
        WHERE t.{{ field }} IS NULL
    )

    -- RICH FAILURE REPORTING: Return all orphaned records with business context
    SELECT
        {{ column_name }},
        {% for col in context_columns %}
        {{ col }},
        {% endfor %}
        occurrence_count,
        -- DESCRIPTIVE ERROR MESSAGE: Clear explanation of the integrity violation
        '{{ column_name }} not found in {{ to }}.{{ field }}' AS failure_reason
    FROM orphaned_records
{% endtest %}

{#
    CUSTOM TEST STRATEGY OVERVIEW:
    
    These custom tests extend dbt's built-in testing capabilities with:
    
    1. STATISTICAL VALIDATION: Z-score based outlier detection for numeric data quality
    
    2. LOGICAL RELATIONSHIP VALIDATION: Business rule enforcement for column relationships
    
    3. ENHANCED REFERENTIAL INTEGRITY: Foreign key validation with rich debugging context
    
    4. BUSINESS CONTEXT: Meaningful error messages and debugging information
    
    5. CONFIGURABLE THRESHOLDS: Parameterized tests that adapt to different business requirements
    
    TESTING PHILOSOPHY:
    - Fail fast with clear, actionable error messages
    - Provide rich context for debugging data quality issues
    - Support statistical validation beyond simple null/not null checks
    - Enable business rule validation specific to domain requirements
    
    OPERATIONAL IMPACT:
    - Prevents bad data from reaching downstream systems
    - Reduces time to diagnose and fix data quality issues
    - Enables automated data quality monitoring and alerting
    - Supports compliance and audit requirements with documented validation rules
#}
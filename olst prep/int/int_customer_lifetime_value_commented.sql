{{ config(
    -- PERFORMANCE OPTIMIZATION: Advanced incremental configuration for CLV calculation efficiency
    materialized='incremental',
    incremental_strategy='delete+insert',       -- Full refresh of changed customers for accuracy in CLV calculations
    on_schema_change='append_new_columns',      -- Handle schema evolution for new CLV features
    unique_key='customer_sk',                   -- Customer surrogate key for merge operations
    cluster_by=['clv_segment', 'customer_state'], -- Optimize for segment and geographic analysis
    schema = "dbt_olist_int"                    -- Intermediate layer for business intelligence
) }}

WITH customer_base AS (
    -- DATA FOUNDATION: Extract core customer metrics from 360-degree customer view
    SELECT
        -- DIMENSIONAL KEYS: Customer identification and geographic context
        customer_sk,                            -- Stable surrogate key
        customer_id,                            -- Business identifier
        customer_state,                         -- Geographic dimension
        region,                                 -- Regional classification

        -- HISTORICAL BEHAVIOR INPUTS: Core metrics for CLV modeling
        total_orders,                           -- Purchase frequency (historical)
        total_spent,                            -- Lifetime value delivered (historical)
        avg_order_value,                        -- Average transaction size
        days_since_last_order,                  -- Recency indicator
        customer_lifespan_days,                 -- Observed relationship duration
        avg_days_between_orders,                -- Purchase frequency pattern
        churn_probability                       -- Predictive churn risk from customer 360 model
    FROM {{ ref('int_customer_360') }}

    -- INCREMENTAL PROCESSING: Only recalculate CLV for customers with updated base metrics
    {% if is_incremental() %}
    WHERE dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
),

clv_calculations AS (
    -- CORE CLV MATHEMATICS: Frequency, lifetime, and value predictions
    SELECT *,

        -- ANNUAL PURCHASE FREQUENCY: Standardized frequency metric for CLV formula
        CASE
            WHEN customer_lifespan_days > 0
            THEN (total_orders::FLOAT / customer_lifespan_days) * 365    -- Annualized frequency from historical data
            ELSE total_orders::FLOAT                                     -- Single-purchase customers: use order count
        END AS annual_purchase_frequency,

        -- ESTIMATED CUSTOMER LIFETIME: Predictive lifetime based on churn probability
        CASE
            WHEN churn_probability > 0
            THEN -1 / LN(churn_probability) * 365                       -- Expected lifetime in days using churn model
            ELSE 1095                                                   -- Default 3 years for customers with very low churn
        END AS estimated_lifetime_days,

        -- HISTORICAL CLV: Actual value delivered to date
        total_spent AS historical_clv,

        -- PREDICTED CLV BASE CALCULATION: Core CLV formula (AOV × Frequency × Lifetime)
        avg_order_value *
        (CASE WHEN customer_lifespan_days > 0 
              THEN (total_orders::FLOAT / customer_lifespan_days) * 365    -- Annual frequency
              ELSE total_orders END) *
        (CASE WHEN churn_probability > 0 
              THEN -1 / LN(churn_probability) / 365                      -- Expected lifetime in years
              ELSE 3 END) AS predicted_clv_base                          -- CLV prediction
    FROM customer_base
),

clv_with_confidence AS (
    -- STATISTICAL CONFIDENCE MODELING: Uncertainty quantification for CLV predictions
    SELECT *,

        -- STANDARD ERROR CALCULATION: Propagated uncertainty from component estimates
        SQRT(
            POWER(avg_order_value * 0.2, 2) +              -- 20% uncertainty in average order value
            POWER(annual_purchase_frequency * 0.3, 2) +     -- 30% uncertainty in purchase frequency
            POWER(estimated_lifetime_days / 365 * 0.4, 2)   -- 40% uncertainty in customer lifetime
        ) AS clv_standard_error,

        -- 95% CONFIDENCE INTERVALS: Statistical bounds for CLV predictions
        predicted_clv_base - (1.96 * SQRT(                 -- Lower bound (95% confidence)
            POWER(avg_order_value * 0.2, 2) +
            POWER(annual_purchase_frequency * 0.3, 2) +
            POWER(estimated_lifetime_days / 365 * 0.4, 2)
        )) AS clv_lower_bound,

        predicted_clv_base + (1.96 * SQRT(                 -- Upper bound (95% confidence)
            POWER(avg_order_value * 0.2, 2) +
            POWER(annual_purchase_frequency * 0.3, 2) +
            POWER(estimated_lifetime_days / 365 * 0.4, 2)
        )) AS clv_upper_bound
    FROM clv_calculations
),

clv_segmentation AS (
    -- BUSINESS SEGMENTATION: CLV-based customer classification and strategic prioritization
    SELECT *,

        -- CLV TIER SEGMENTATION: Value-based customer classification for resource allocation
        CASE
            WHEN predicted_clv_base >= 2000 THEN 'Very High Value'      -- Premium customers: highest investment priority
            WHEN predicted_clv_base >= 1000 THEN 'High Value'           -- Key customers: retention focus
            WHEN predicted_clv_base >= 500 THEN 'Medium Value'          -- Core customers: growth opportunity
            WHEN predicted_clv_base >= 200 THEN 'Low Value'             -- Base customers: efficiency focus
            ELSE 'Very Low Value'                                       -- Marginal customers: cost management
        END AS clv_segment,

        -- PREDICTION CONFIDENCE ASSESSMENT: Model reliability indicator
        CASE
            WHEN clv_standard_error / NULLIF(predicted_clv_base, 0) < 0.2 THEN 'High Confidence'    -- <20% coefficient of variation
            WHEN clv_standard_error / NULLIF(predicted_clv_base, 0) < 0.5 THEN 'Medium Confidence'  -- 20-50% coefficient of variation
            ELSE 'Low Confidence'                                                                    -- >50% coefficient of variation
        END AS prediction_confidence,

        -- GROWTH POTENTIAL ANALYSIS: Future value vs historical value comparison
        CASE
            WHEN predicted_clv_base / NULLIF(historical_clv, 0) > 2 THEN 'High Growth Potential'     -- 2x+ future potential
            WHEN predicted_clv_base / NULLIF(historical_clv, 0) > 1.5 THEN 'Medium Growth Potential' -- 1.5-2x future potential
            ELSE 'Limited Growth Potential'                                                          -- <1.5x future potential
        END AS growth_potential,

        -- STRATEGIC INVESTMENT PRIORITIZATION: Action-oriented customer classification
        CASE
            -- RETENTION STRATEGY: High-value customers with reliable predictions
            WHEN clv_segment IN ('Very High Value', 'High Value') AND prediction_confidence = 'High Confidence'
            THEN 'Priority 1 - Retain'
            
            -- GROWTH STRATEGY: Medium-value customers with high upside potential
            WHEN clv_segment = 'Medium Value' AND growth_potential = 'High Growth Potential'
            THEN 'Priority 2 - Grow'
            
            -- WIN-BACK STRATEGY: Low-value customers at high churn risk
            WHEN clv_segment IN ('Low Value', 'Very Low Value') AND churn_probability > 0.7
            THEN 'Priority 3 - Win Back'
            
            -- MONITORING STRATEGY: All other customers require standard treatment
            ELSE 'Priority 4 - Monitor'
        END AS investment_priority
    FROM clv_with_confidence
),

final AS (
    -- FINAL ASSEMBLY: Complete CLV intelligence for business decision-making
    SELECT
        -- DIMENSIONAL IDENTIFIERS: Keys for joins and geographic analysis
        customer_sk,                            -- Surrogate key for efficient joins
        customer_id,                            -- Natural business key
        customer_state,                         -- Geographic dimension
        region,                                 -- Regional classification

        -- HISTORICAL PERFORMANCE METRICS: Observed customer behavior
        total_orders,                           -- Historical purchase count
        total_spent AS historical_clv,          -- Actual value delivered to date
        avg_order_value,                        -- Historical transaction size
        annual_purchase_frequency,              -- Annualized purchase rate

        -- CLV PREDICTIONS: Forward-looking value estimates
        predicted_clv_base AS predicted_clv,    -- Point estimate of future CLV
        clv_lower_bound,                        -- 95% confidence interval lower bound
        clv_upper_bound,                        -- 95% confidence interval upper bound
        clv_standard_error,                     -- Prediction uncertainty measure

        -- BUSINESS SEGMENTATION: Strategic customer classifications
        clv_segment,                            -- Value-based tier classification
        prediction_confidence,                  -- Model reliability assessment
        growth_potential,                       -- Future value opportunity assessment
        investment_priority,                    -- Strategic action recommendation

        -- MODEL INPUTS: CLV calculation components for transparency
        estimated_lifetime_days,                -- Predicted customer lifespan
        churn_probability,                      -- Churn risk input from customer 360

        -- AUDIT METADATA: Data lineage and processing tracking
        CURRENT_TIMESTAMP AS dbt_loaded_at,     -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id -- dbt run identifier
    FROM clv_segmentation
)

-- FINAL OUTPUT: Complete customer lifetime value intelligence with confidence intervals and strategic segmentation
SELECT * FROM final
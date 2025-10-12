{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    unique_key='customer_sk',
    cluster_by=['clv_segment', 'customer_state'],
    schema = "dbt_olist_int",
    tags = ['intermediate']
) }}

WITH customer_base AS (
    SELECT 
        customer_sk,
        customer_id,
        customer_state,
        region,
        total_orders,
        total_spent,
        avg_order_value,
        days_since_last_order,
        customer_lifespan_days,
        avg_days_between_orders,
        churn_probability
    FROM {{ ref('int_customer_360') }}
    
    {% if is_incremental() %}
    WHERE dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
),

clv_calculations AS (
    SELECT *,
        -- Purchase frequency (annual)
        CASE 
            WHEN customer_lifespan_days > 0 
            THEN (total_orders::FLOAT / customer_lifespan_days) * 365 
            ELSE total_orders::FLOAT
        END AS annual_purchase_frequency,
        
        -- Customer lifetime (estimated)
        CASE 
            WHEN churn_probability > 0 
            THEN -1 / LN(churn_probability) * 365  -- Expected lifetime in days
            ELSE 1095  -- Default 3 years for low churn probability
        END AS estimated_lifetime_days,
        
        -- Historical CLV (actual value delivered)
        total_spent AS historical_clv,
        
        -- Predicted CLV components
        avg_order_value * 
        (CASE WHEN customer_lifespan_days > 0 THEN (total_orders::FLOAT / customer_lifespan_days) * 365 ELSE total_orders END) * 
        (CASE WHEN churn_probability > 0 THEN -1 / LN(churn_probability) / 365 ELSE 3 END) AS predicted_clv_base

    FROM customer_base
),

clv_with_confidence AS (
    SELECT *,
        -- Standard error calculation for CLV prediction
        SQRT(
            POWER(avg_order_value * 0.2, 2) +  -- 20% uncertainty in AOV
            POWER(annual_purchase_frequency * 0.3, 2) +  -- 30% uncertainty in frequency
            POWER(estimated_lifetime_days / 365 * 0.4, 2)  -- 40% uncertainty in lifetime
        ) AS clv_standard_error,
        
        -- Confidence intervals (95%)
        predicted_clv_base - (1.96 * SQRT(
            POWER(avg_order_value * 0.2, 2) + 
            POWER(annual_purchase_frequency * 0.3, 2) + 
            POWER(estimated_lifetime_days / 365 * 0.4, 2)
        )) AS clv_lower_bound,
        
        predicted_clv_base + (1.96 * SQRT(
            POWER(avg_order_value * 0.2, 2) + 
            POWER(annual_purchase_frequency * 0.3, 2) + 
            POWER(estimated_lifetime_days / 365 * 0.4, 2)
        )) AS clv_upper_bound

    FROM clv_calculations
),

clv_segmentation AS (
    SELECT *,
        -- CLV segments based on predicted value
        CASE 
            WHEN predicted_clv_base >= 2000 THEN 'Very High Value'
            WHEN predicted_clv_base >= 1000 THEN 'High Value'
            WHEN predicted_clv_base >= 500 THEN 'Medium Value'
            WHEN predicted_clv_base >= 200 THEN 'Low Value' 
            ELSE 'Very Low Value'
        END AS clv_segment,
        
        -- Confidence level in prediction
        CASE 
            WHEN clv_standard_error / NULLIF(predicted_clv_base, 0) < 0.2 THEN 'High Confidence'
            WHEN clv_standard_error / NULLIF(predicted_clv_base, 0) < 0.5 THEN 'Medium Confidence'
            ELSE 'Low Confidence'
        END AS prediction_confidence,
        
        -- ROI potential
        CASE 
            WHEN predicted_clv_base / NULLIF(historical_clv, 0) > 2 THEN 'High Growth Potential'
            WHEN predicted_clv_base / NULLIF(historical_clv, 0) > 1.5 THEN 'Medium Growth Potential'  
            ELSE 'Limited Growth Potential'
        END AS growth_potential,
        
        -- Investment priority
        CASE 
            WHEN clv_segment IN ('Very High Value', 'High Value') AND prediction_confidence = 'High Confidence' 
            THEN 'Priority 1 - Retain'
            WHEN clv_segment = 'Medium Value' AND growth_potential = 'High Growth Potential'
            THEN 'Priority 2 - Grow'
            WHEN clv_segment IN ('Low Value', 'Very Low Value') AND churn_probability > 0.7
            THEN 'Priority 3 - Win Back'
            ELSE 'Priority 4 - Monitor'
        END AS investment_priority

    FROM clv_with_confidence
),

final AS (
    SELECT 
        customer_sk,
        customer_id,
        customer_state,
        region,
        
        -- Historical metrics
        total_orders,
        total_spent AS historical_clv,
        avg_order_value,
        annual_purchase_frequency,
        
        -- CLV predictions
        predicted_clv_base AS predicted_clv,
        clv_lower_bound,
        clv_upper_bound,
        clv_standard_error,
        
        -- Business segments
        clv_segment,
        prediction_confidence,
        growth_potential,
        investment_priority,
        
        -- Model inputs
        estimated_lifetime_days,
        churn_probability,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_tz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id

    FROM clv_segmentation
)

SELECT * FROM final
{% if target.name == 'dev' %}
    LIMIT {{ var('dev_sample_size_agg') }}
{% endif %}
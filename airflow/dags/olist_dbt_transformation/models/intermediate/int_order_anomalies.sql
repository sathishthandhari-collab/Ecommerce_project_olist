{{ config(
    materialized='incremental', 
    unique_key='order_sk',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    cluster_by=['anomaly_date', 'anomaly_severity'],
    schema = "dbt_olist_int",
    tags = ['intermediate']
) }}

WITH order_context AS (
    SELECT 
        o.order_sk,
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.delivery_days,
        o.is_on_time_delivery,
        
        -- Customer context
        c.customer_state,
        c.region,
        c360.total_orders AS customer_order_count,
        c360.avg_order_value AS customer_avg_order_value,
        c360.days_since_last_order,
        c360.customer_risk_level,
        
        -- Order financial metrics
        SUM(p.payment_value)::float AS total_order_value,
        COUNT(DISTINCT p.payment_type) AS payment_methods_count,
        AVG(p.payment_installments) AS avg_installments,
        MAX(p.payment_installments) AS max_installments,
        SUM(CASE WHEN p.payment_risk_category = 'High Risk' THEN 1 ELSE 0 END) AS high_risk_payments,
        
        -- Order complexity metrics
        COUNT(DISTINCT oi.product_id) AS unique_products,
        COUNT(DISTINCT oi.seller_id) AS unique_sellers,
        SUM(oi.item_price) AS total_item_value,
        SUM(oi.freight_value) AS total_freight_value,
        
        -- Product mix analysis
        COUNT(DISTINCT pr.category_group) AS category_diversity,
        SUM(CASE WHEN pr.is_bulky_item THEN 1 ELSE 0 END) AS bulky_items_count,
        AVG(pr.weight_z_score) AS avg_product_weight_anomaly,
        
        -- Geographic complexity
        COUNT(DISTINCT s.seller_state) AS seller_states_count,
        AVG(s.business_environment_score) AS avg_seller_business_score,
        
        -- Review context (if available)
        AVG(r.review_score)::float AS avg_review_score,
        COUNT(r.review_id) AS review_count

    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
    LEFT JOIN {{ ref('int_customer_360') }} c360 ON c.customer_sk = c360.customer_sk
    LEFT JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
    LEFT JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id  
    LEFT JOIN {{ ref('stg_products') }} pr ON oi.product_id = pr.product_id
    LEFT JOIN {{ ref('stg_sellers') }} s ON oi.seller_id = s.seller_id
    LEFT JOIN {{ ref('stg_reviews') }} r ON o.order_id = r.order_id
    
    {% if is_incremental() %}
    WHERE o.dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
    
    GROUP BY 
        o.order_sk, o.order_id, o.customer_id, o.order_status, o.order_purchase_timestamp,
        o.delivery_days, o.is_on_time_delivery, c.customer_state, c.region,
        c360.total_orders, c360.avg_order_value, c360.days_since_last_order, c360.customer_risk_level
),

statistical_context AS (
    -- Calculate population statistics for anomaly detection
    SELECT 
        AVG(total_order_value) AS avg_order_value_population,
        STDDEV(total_order_value) AS stddev_order_value,
        AVG(unique_products) AS avg_products_per_order,
        STDDEV(unique_products) AS stddev_products_per_order,
        AVG(unique_sellers) AS avg_sellers_per_order, 
        STDDEV(unique_sellers) AS stddev_sellers_per_order,
        AVG(delivery_days) AS avg_delivery_days_population,
        STDDEV(delivery_days) AS stddev_delivery_days,
        AVG(avg_installments) AS avg_installments_population,
        STDDEV(avg_installments) AS stddev_installments
    FROM order_context
),

anomaly_detection AS (
    SELECT 
        oc.*,
        sc.*,
        
        -- Financial anomalies (Z-scores)
        ABS(oc.total_order_value - sc.avg_order_value_population) / NULLIF(sc.stddev_order_value, 0) AS order_value_z_score,
        
        -- Complexity anomalies
        ABS(oc.unique_products - sc.avg_products_per_order) / NULLIF(sc.stddev_products_per_order, 0) AS product_count_z_score,
        ABS(oc.unique_sellers - sc.avg_sellers_per_order) / NULLIF(sc.stddev_sellers_per_order, 0) AS seller_count_z_score,
        
        -- Timing anomalies
        ABS(oc.delivery_days - sc.avg_delivery_days_population) / NULLIF(sc.stddev_delivery_days, 0) AS delivery_time_z_score,
        
        -- Payment behavior anomalies
        ABS(oc.avg_installments - sc.avg_installments_population) / NULLIF(sc.stddev_installments, 0) AS installment_z_score,
        
        -- Customer behavior anomalies (compared to their own history)
        CASE 
            WHEN oc.customer_avg_order_value > 0 
            THEN ABS(oc.total_order_value - oc.customer_avg_order_value) / oc.customer_avg_order_value
            ELSE 0
        END AS customer_behavior_deviation,
        
        -- First-time customer with high-value order
        CASE 
            WHEN oc.customer_order_count = 1 AND oc.total_order_value > sc.avg_order_value_population * 3
            THEN 1 ELSE 0
        END AS first_time_high_value_flag,
        
        -- Geographic anomalies
        CASE 
            WHEN oc.seller_states_count > 3 THEN 1 ELSE 0  -- Multiple states unusual
        END AS geographic_complexity_flag,
        
        -- Payment method anomalies
        CASE 
            WHEN oc.payment_methods_count > 2 OR oc.max_installments > 20 THEN 1 ELSE 0
        END AS payment_complexity_flag,
        
        -- Product mix anomalies
        CASE 
            WHEN oc.category_diversity > 5 OR oc.bulky_items_count > 5 THEN 1 ELSE 0
        END AS product_mix_anomaly_flag

    FROM order_context oc
    CROSS JOIN statistical_context sc
),

anomaly_scoring AS (
    SELECT *,
        -- Composite anomaly score (weighted)
        (
            (0.35 * LEAST(order_value_z_score, 5)) +  -- Cap extreme z-scores
            (0.20 * LEAST(product_count_z_score, 5)) +
            (0.15 * LEAST(seller_count_z_score, 5)) +
            (0.10 * LEAST(delivery_time_z_score, 5)) +
            (0.10 * LEAST(installment_z_score, 5)) +
            (0.05 * customer_behavior_deviation * 5) +  -- Scale to match z-scores
            (0.05 * (first_time_high_value_flag + geographic_complexity_flag + 
                     payment_complexity_flag + product_mix_anomaly_flag))
        ) AS composite_anomaly_score,
        
        -- Individual anomaly flags
        CASE WHEN order_value_z_score > {{ var('anomaly_z_threshold', 3.0) }} THEN TRUE ELSE FALSE END AS is_value_anomaly,
        CASE WHEN product_count_z_score > {{ var('anomaly_z_threshold', 3.0) }} THEN TRUE ELSE FALSE END AS is_complexity_anomaly,
        CASE WHEN delivery_time_z_score > {{ var('anomaly_z_threshold', 3.0) }} THEN TRUE ELSE FALSE END AS is_delivery_anomaly,
        CASE WHEN customer_behavior_deviation > 2.0 THEN TRUE ELSE FALSE END AS is_customer_behavior_anomaly,
        
        -- Time-based patterns
        EXTRACT(hour FROM order_purchase_timestamp) AS order_hour,
        EXTRACT(dow FROM order_purchase_timestamp) AS order_day_of_week,
        
        -- Unusual timing flags
        CASE 
            WHEN EXTRACT(hour FROM order_purchase_timestamp) BETWEEN 2 AND 5 THEN TRUE  -- Late night orders
            ELSE FALSE 
        END AS is_unusual_time,
        
        CASE 
            WHEN EXTRACT(dow FROM order_purchase_timestamp) IN (1, 7) THEN TRUE  -- Weekend orders
            ELSE FALSE 
        END AS is_weekend_order

    FROM anomaly_detection
),

anomaly_classification AS (
    SELECT *,
        -- Severity classification
        CASE 
            WHEN composite_anomaly_score >= 4.0 THEN 'Critical'
            WHEN composite_anomaly_score >= 3.0 THEN 'High'  
            WHEN composite_anomaly_score >= 2.0 THEN 'Medium'
            WHEN composite_anomaly_score >= 1.0 THEN 'Low'
            ELSE 'Normal'
        END AS anomaly_severity,
        
        -- Anomaly type categorization
        CASE 
            WHEN is_value_anomaly AND first_time_high_value_flag = 1 THEN 'Potential Fraud - New Customer High Value'
            WHEN is_value_anomaly AND customer_risk_level = 'High Risk' THEN 'Potential Fraud - High Risk Customer'
            WHEN is_complexity_anomaly AND is_unusual_time THEN 'Suspicious Complexity'
            WHEN is_delivery_anomaly AND NOT is_on_time_delivery THEN 'Logistics Issue'
            WHEN is_customer_behavior_anomaly THEN 'Behavioral Deviation'
            WHEN composite_anomaly_score >= 2.0 THEN 'Multiple Factors'
            ELSE 'Normal Order'
        END AS anomaly_type,
        
        -- Business impact assessment
        CASE 
            WHEN composite_anomaly_score >= 3.0 AND total_order_value > 1000 THEN 'High Impact'
            WHEN composite_anomaly_score >= 2.0 AND customer_order_count = 1 THEN 'Customer Experience Risk'
            WHEN composite_anomaly_score >= 2.0 THEN 'Medium Impact'
            ELSE 'Low Impact'
        END AS business_impact,
        
        -- Recommended action
        CASE 
            WHEN anomaly_severity = 'Critical' THEN 'Immediate Review Required'
            WHEN anomaly_severity = 'High' AND customer_risk_level = 'High Risk' THEN 'Fraud Team Review'
            WHEN anomaly_severity = 'High' THEN 'Manager Review'
            WHEN anomaly_severity = 'Medium' THEN 'Automated Alert'
            ELSE 'Monitor'
        END AS recommended_action

    FROM anomaly_scoring
),

final AS (
    SELECT 
        order_sk,
        order_id, 
        customer_id,
        order_status,
        DATE(order_purchase_timestamp) AS anomaly_date,
        order_purchase_timestamp,
        
        -- Order characteristics
        total_order_value,
        unique_products,
        unique_sellers,
        delivery_days,
        customer_order_count,
        
        -- Anomaly metrics
        composite_anomaly_score,
        order_value_z_score,
        product_count_z_score,
        customer_behavior_deviation,
        
        -- Anomaly classification
        anomaly_severity,
        anomaly_type,
        business_impact,
        recommended_action,
        
        -- Individual flags
        is_value_anomaly,
        is_complexity_anomaly,
        is_delivery_anomaly,
        is_customer_behavior_anomaly,
        is_unusual_time,
        is_weekend_order,
        
        -- Context
        customer_risk_level,
        region,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_tz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id

    FROM anomaly_classification
    WHERE composite_anomaly_score >= 1.0  -- Only store anomalous orders
)

SELECT * FROM final
{% if target.name == 'dev' %}
    LIMIT {{ var('dev_sample_size_agg') }}
{% endif %}
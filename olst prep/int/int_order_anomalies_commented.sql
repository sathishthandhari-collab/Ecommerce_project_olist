{{ config(
    -- PERFORMANCE OPTIMIZATION: Incremental configuration for comprehensive anomaly detection
    materialized='incremental',
    unique_key='order_sk',                      -- Order surrogate key for merge operations
    incremental_strategy='delete+insert',       -- Full refresh for accurate statistical context
    on_schema_change='append_new_columns',      -- Handle schema evolution for new anomaly features
    cluster_by=['anomaly_date', 'anomaly_severity'], -- Optimize for temporal and severity analysis
    schema = "dbt_olist_int"                    -- Intermediate layer for business intelligence
) }}

WITH order_context AS (
    -- COMPREHENSIVE ORDER INTELLIGENCE: Multi-dimensional order analysis combining all business contexts
    SELECT
        -- CORE ORDER IDENTIFIERS: Primary keys and dimensional context
        o.order_sk,                             -- Stable surrogate key
        o.order_id,                             -- Natural business identifier
        o.customer_id,                          -- Customer relationship
        o.order_status,                         -- Current order state
        o.order_purchase_timestamp,             -- Transaction timestamp
        o.delivery_days,                        -- Fulfillment performance
        o.is_on_time_delivery,                  -- Service level achievement

        -- CUSTOMER INTELLIGENCE CONTEXT: Behavioral and risk profiling from customer 360
        c.customer_state,                       -- Geographic context
        c.region,                               -- Regional classification
        c360.total_orders AS customer_order_count,      -- Customer purchase history volume
        c360.avg_order_value AS customer_avg_order_value, -- Customer's typical order size
        c360.days_since_last_order,             -- Customer activity recency
        c360.customer_risk_level,               -- Customer risk classification

        -- FINANCIAL TRANSACTION ANALYSIS: Payment behavior and complexity metrics
        SUM(p.payment_value)::float AS total_order_value,          -- Total order value
        COUNT(DISTINCT p.payment_type) AS payment_methods_count,   -- Payment complexity indicator
        AVG(p.payment_installments) AS avg_installments,           -- Payment terms preference
        MAX(p.payment_installments) AS max_installments,           -- Maximum installment terms
        SUM(CASE WHEN p.payment_risk_category = 'High Risk' THEN 1 ELSE 0 END) AS high_risk_payments, -- Risk payment count

        -- ORDER COMPOSITION COMPLEXITY: Product and seller diversity analysis
        COUNT(DISTINCT oi.product_id) AS unique_products,          -- Product variety per order
        COUNT(DISTINCT oi.seller_id) AS unique_sellers,            -- Seller diversity per order
        SUM(oi.item_price) AS total_item_value,                    -- Item value component
        SUM(oi.freight_value) AS total_freight_value,              -- Shipping cost component

        -- PRODUCT MIX INTELLIGENCE: Category and logistics complexity
        COUNT(DISTINCT pr.category_group) AS category_diversity,   -- Product category breadth
        SUM(CASE WHEN pr.is_bulky_item THEN 1 ELSE 0 END) AS bulky_items_count, -- Logistics complexity items
        AVG(pr.weight_z_score) AS avg_product_weight_anomaly,      -- Product weight anomaly average

        -- GEOGRAPHIC DISTRIBUTION ANALYSIS: Cross-state seller complexity
        COUNT(DISTINCT s.seller_state) AS seller_states_count,     -- Geographic seller diversity
        AVG(s.business_environment_score) AS avg_seller_business_score, -- Seller quality average

        -- CUSTOMER SATISFACTION CONTEXT: Review and feedback context (if available)
        AVG(r.review_score)::float AS avg_review_score,            -- Order satisfaction rating
        COUNT(r.review_id) AS review_count                         -- Feedback volume
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
    LEFT JOIN {{ ref('int_customer_360') }} c360 ON c.customer_sk = c360.customer_sk
    LEFT JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
    LEFT JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id
    LEFT JOIN {{ ref('stg_products') }} pr ON oi.product_id = pr.product_id
    LEFT JOIN {{ ref('stg_sellers') }} s ON oi.seller_id = s.seller_id
    LEFT JOIN {{ ref('stg_reviews') }} r ON o.order_id = r.order_id

    -- INCREMENTAL PROCESSING: Only analyze orders with recent data updates
    {% if is_incremental() %}
    WHERE o.dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}

    -- ORDER-LEVEL AGGREGATION: Comprehensive order profiling
    GROUP BY
        o.order_sk, o.order_id, o.customer_id, o.order_status, o.order_purchase_timestamp,
        o.delivery_days, o.is_on_time_delivery, c.customer_state, c.region,
        c360.total_orders, c360.avg_order_value, c360.days_since_last_order, c360.customer_risk_level
),

statistical_context AS (
    -- POPULATION STATISTICS: Baseline metrics for anomaly detection using entire order population
    SELECT
        -- FINANCIAL DISTRIBUTION PARAMETERS: Order value statistical foundation
        AVG(total_order_value) AS avg_order_value_population,      -- Population mean order value
        STDDEV(total_order_value) AS stddev_order_value,           -- Order value standard deviation

        -- COMPLEXITY DISTRIBUTION PARAMETERS: Order composition statistical foundation
        AVG(unique_products) AS avg_products_per_order,            -- Population mean products per order
        STDDEV(unique_products) AS stddev_products_per_order,      -- Product count standard deviation
        AVG(unique_sellers) AS avg_sellers_per_order,              -- Population mean sellers per order
        STDDEV(unique_sellers) AS stddev_sellers_per_order,        -- Seller count standard deviation

        -- OPERATIONAL DISTRIBUTION PARAMETERS: Service delivery statistical foundation
        AVG(delivery_days) AS avg_delivery_days_population,        -- Population mean delivery time
        STDDEV(delivery_days) AS stddev_delivery_days,             -- Delivery time standard deviation

        -- PAYMENT BEHAVIOR PARAMETERS: Financial terms statistical foundation
        AVG(avg_installments) AS avg_installments_population,      -- Population mean installments
        STDDEV(avg_installments) AS stddev_installments            -- Installment count standard deviation
    FROM order_context
),

anomaly_detection AS (
    -- STATISTICAL ANOMALY IDENTIFICATION: Z-score based deviation detection across multiple dimensions
    SELECT
        oc.*,
        sc.*,

        -- FINANCIAL ANOMALIES: Order value deviation analysis using Z-scores
        ABS(oc.total_order_value - sc.avg_order_value_population) / NULLIF(sc.stddev_order_value, 0) AS order_value_z_score,

        -- ORDER COMPLEXITY ANOMALIES: Product and seller diversity deviation analysis
        ABS(oc.unique_products - sc.avg_products_per_order) / NULLIF(sc.stddev_products_per_order, 0) AS product_count_z_score,
        ABS(oc.unique_sellers - sc.avg_sellers_per_order) / NULLIF(sc.stddev_sellers_per_order, 0) AS seller_count_z_score,

        -- OPERATIONAL TIMING ANOMALIES: Delivery performance deviation analysis
        ABS(oc.delivery_days - sc.avg_delivery_days_population) / NULLIF(sc.stddev_delivery_days, 0) AS delivery_time_z_score,

        -- PAYMENT BEHAVIOR ANOMALIES: Financial terms deviation analysis
        ABS(oc.avg_installments - sc.avg_installments_population) / NULLIF(sc.stddev_installments, 0) AS installment_z_score,

        -- CUSTOMER BEHAVIORAL ANOMALIES: Individual vs historical pattern analysis
        CASE
            WHEN oc.customer_avg_order_value > 0
            THEN ABS(oc.total_order_value - oc.customer_avg_order_value) / oc.customer_avg_order_value  -- Relative deviation from customer's norm
            ELSE 0
        END AS customer_behavior_deviation,

        -- BUSINESS RULE ANOMALIES: Specific pattern-based anomaly flags
        CASE
            WHEN oc.customer_order_count = 1 AND oc.total_order_value > sc.avg_order_value_population * 3
            THEN 1 ELSE 0                       -- First-time customer with unusually high-value order
        END AS first_time_high_value_flag,

        CASE
            WHEN oc.seller_states_count > 3 THEN 1 ELSE 0  -- Orders spanning multiple states (unusual distribution complexity)
        END AS geographic_complexity_flag,

        CASE
            WHEN oc.payment_methods_count > 2 OR oc.max_installments > 20 THEN 1 ELSE 0  -- Complex payment arrangements
        END AS payment_complexity_flag,

        CASE
            WHEN oc.category_diversity > 5 OR oc.bulky_items_count > 5 THEN 1 ELSE 0     -- Unusual product mix complexity
        END AS product_mix_anomaly_flag
    FROM order_context oc
    CROSS JOIN statistical_context sc
),

anomaly_scoring AS (
    -- COMPOSITE ANOMALY SCORING: Weighted multi-dimensional anomaly assessment
    SELECT *,

        -- WEIGHTED COMPOSITE ANOMALY SCORE: Business-priority weighted scoring (0-5+ scale)
        (
            (0.35 * LEAST(order_value_z_score, 5)) +           -- 35% weight: Financial anomalies (capped at 5σ)
            (0.20 * LEAST(product_count_z_score, 5)) +         -- 20% weight: Product complexity
            (0.15 * LEAST(seller_count_z_score, 5)) +          -- 15% weight: Seller complexity
            (0.10 * LEAST(delivery_time_z_score, 5)) +         -- 10% weight: Delivery timing
            (0.10 * LEAST(installment_z_score, 5)) +           -- 10% weight: Payment terms
            (0.05 * customer_behavior_deviation * 5) +         -- 5% weight: Customer behavior (scaled)
            (0.05 * (first_time_high_value_flag + geographic_complexity_flag +
                     payment_complexity_flag + product_mix_anomaly_flag))  -- 5% weight: Business rule flags
        ) AS composite_anomaly_score,

        -- INDIVIDUAL ANOMALY TYPE FLAGS: Specific deviation identification
        CASE WHEN order_value_z_score > {{ var('anomaly_z_threshold', 3.0) }} THEN TRUE ELSE FALSE END AS is_value_anomaly,
        CASE WHEN product_count_z_score > {{ var('anomaly_z_threshold', 3.0) }} THEN TRUE ELSE FALSE END AS is_complexity_anomaly,
        CASE WHEN delivery_time_z_score > {{ var('anomaly_z_threshold', 3.0) }} THEN TRUE ELSE FALSE END AS is_delivery_anomaly,
        CASE WHEN customer_behavior_deviation > 2.0 THEN TRUE ELSE FALSE END AS is_customer_behavior_anomaly,

        -- TEMPORAL PATTERN ANALYSIS: Time-based anomaly detection
        EXTRACT(hour FROM order_purchase_timestamp) AS order_hour,        -- Hour of day analysis
        EXTRACT(dow FROM order_purchase_timestamp) AS order_day_of_week,   -- Day of week analysis

        -- UNUSUAL TIMING FLAGS: Time-based business rule anomalies
        CASE
            WHEN EXTRACT(hour FROM order_purchase_timestamp) BETWEEN 2 AND 5 THEN TRUE  -- Late night orders (2-5 AM)
            ELSE FALSE
        END AS is_unusual_time,

        CASE
            WHEN EXTRACT(dow FROM order_purchase_timestamp) IN (1, 7) THEN TRUE         -- Weekend orders (Sunday=1, Saturday=7)
            ELSE FALSE
        END AS is_weekend_order
    FROM anomaly_detection
),

anomaly_classification AS (
    -- BUSINESS-FRIENDLY ANOMALY CLASSIFICATION: Severity and type categorization for actionability
    SELECT *,

        -- SEVERITY CLASSIFICATION: Business impact-based severity assessment
        CASE
            WHEN composite_anomaly_score >= 4.0 THEN 'Critical'    -- Immediate investigation required
            WHEN composite_anomaly_score >= 3.0 THEN 'High'        -- High priority investigation
            WHEN composite_anomaly_score >= 2.0 THEN 'Medium'      -- Standard review process
            WHEN composite_anomaly_score >= 1.0 THEN 'Low'         -- Monitoring and tracking
            ELSE 'Normal'                                           -- No anomaly detected
        END AS anomaly_severity,

        -- ANOMALY TYPE CATEGORIZATION: Business context-driven classification
        CASE
            WHEN is_value_anomaly AND first_time_high_value_flag = 1 THEN 'Potential Fraud - New Customer High Value'
            WHEN is_value_anomaly AND customer_risk_level = 'High Risk' THEN 'Potential Fraud - High Risk Customer'
            WHEN is_complexity_anomaly AND is_unusual_time THEN 'Suspicious Complexity'
            WHEN is_delivery_anomaly AND NOT is_on_time_delivery THEN 'Logistics Issue'
            WHEN is_customer_behavior_anomaly THEN 'Behavioral Deviation'
            WHEN composite_anomaly_score >= 2.0 THEN 'Multiple Factors'
            ELSE 'Normal Order'
        END AS anomaly_type,

        -- BUSINESS IMPACT ASSESSMENT: Strategic importance and urgency classification
        CASE
            WHEN composite_anomaly_score >= 3.0 AND total_order_value > 1000 THEN 'High Impact'           -- High-value critical anomalies
            WHEN composite_anomaly_score >= 2.0 AND customer_order_count = 1 THEN 'Customer Experience Risk' -- New customer experience risk
            WHEN composite_anomaly_score >= 2.0 THEN 'Medium Impact'                                       -- Standard anomaly impact
            ELSE 'Low Impact'                                                                               -- Minimal business concern
        END AS business_impact,

        -- RECOMMENDED ACTION: Process-driven response classification
        CASE
            WHEN anomaly_severity = 'Critical' THEN 'Immediate Review Required'    -- Urgent manual investigation
            WHEN anomaly_severity = 'High' AND customer_risk_level = 'High Risk' THEN 'Fraud Team Review' -- Specialized team escalation
            WHEN anomaly_severity = 'High' THEN 'Manager Review'                   -- Management attention
            WHEN anomaly_severity = 'Medium' THEN 'Automated Alert'                -- System notification
            ELSE 'Monitor'                                                          -- Passive tracking
        END AS recommended_action
    FROM anomaly_scoring
),

final AS (
    -- FINAL ASSEMBLY: Complete order anomaly intelligence for operational decision-making
    SELECT
        -- CORE IDENTIFIERS: Keys and dimensional context
        order_sk,                               -- Surrogate key for efficient joins
        order_id,                               -- Natural business key
        customer_id,                            -- Customer relationship
        order_status,                           -- Current order state
        DATE(order_purchase_timestamp) AS anomaly_date,  -- Date dimension for analysis
        order_purchase_timestamp,               -- Precise timestamp

        -- ORDER CHARACTERISTICS: Core business metrics
        total_order_value,                      -- Financial magnitude
        unique_products,                        -- Order complexity
        unique_sellers,                         -- Distribution complexity
        delivery_days,                          -- Fulfillment performance
        customer_order_count,                   -- Customer context

        -- ANOMALY METRICS: Statistical deviation measurements
        composite_anomaly_score,                -- Overall anomaly intensity (0-5+ scale)
        order_value_z_score,                    -- Financial deviation magnitude
        product_count_z_score,                  -- Complexity deviation magnitude
        customer_behavior_deviation,            -- Individual customer pattern deviation

        -- BUSINESS CLASSIFICATION: Actionable anomaly categorization
        anomaly_severity,                       -- Critical/High/Medium/Low/Normal
        anomaly_type,                           -- Fraud/Logistics/Behavioral/etc.
        business_impact,                        -- High/Medium/Low impact assessment
        recommended_action,                     -- Process-driven response guidance

        -- DETAILED ANOMALY FLAGS: Granular anomaly type identification
        is_value_anomaly,                       -- Financial deviation flag
        is_complexity_anomaly,                  -- Order complexity flag
        is_delivery_anomaly,                    -- Logistics performance flag
        is_customer_behavior_anomaly,           -- Individual pattern deviation flag
        is_unusual_time,                        -- Temporal pattern flag
        is_weekend_order,                       -- Day-of-week pattern flag

        -- CONTEXTUAL INTELLIGENCE: Supporting business context
        customer_risk_level,                    -- Customer risk profile
        region,                                 -- Geographic context

        -- AUDIT METADATA: Data lineage and processing tracking
        CURRENT_TIMESTAMP AS dbt_loaded_at,     -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id -- dbt run identifier
    FROM anomaly_classification
    -- ANOMALY FILTER: Only store orders that exhibit anomalous behavior (composite_anomaly_score >= 1.0)
    WHERE composite_anomaly_score >= 1.0
)

-- FINAL OUTPUT: Complete order anomaly intelligence with multi-dimensional scoring, business classification, and recommended actions
SELECT * FROM final
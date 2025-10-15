{{ config(
    -- INCREMENTAL CONFIGURATION: Optimized for performance with smart merging strategy
    materialized='incremental',
    unique_key='customer_sk',                    -- Customer surrogate key for merge operations
    incremental_strategy='delete+insert',       -- Full refresh of changed customers to ensure accuracy
    on_schema_change='append_new_columns',      -- Handle schema evolution gracefully
    cluster_by=['customer_state', 'lifecycle_stage'], -- Optimize queries by geography and lifecycle
    schema = "dbt_olist_int"                    -- Dedicated intermediate layer schema
) }}

WITH customer_orders AS (
    -- FOUNDATION DATA ASSEMBLY: Joins all customer-related data sources to create comprehensive view
    SELECT
        -- DIMENSIONAL KEYS: Stable identifiers for customer analysis
        c.customer_sk,                          -- Surrogate key for efficient joins
        c.customer_id,                          -- Natural business key
        c.customer_state,                       -- Geographic dimension
        c.region,                               -- Regional classification
        c.state_tier,                           -- Business tier classification

        -- ORDER VOLUME METRICS: Core transaction behavior indicators
        COUNT(DISTINCT o.order_id) AS total_orders,              -- Total purchase count
        SUM(p.payment_value)::float AS total_spent,              -- Lifetime value (actual)
        AVG(p.payment_value)::float AS avg_order_value,          -- Average order size
        STDDEV(p.payment_value) AS order_value_volatility,       -- Spending consistency

        -- TEMPORAL BEHAVIOR ANALYSIS: Customer lifecycle timing patterns
        MIN(o.order_purchase_timestamp) AS first_order_date,     -- Customer acquisition date
        MAX(o.order_purchase_timestamp) AS last_order_date,      -- Last activity date
        DATEDIFF('day', MIN(o.order_purchase_timestamp), MAX(o.order_purchase_timestamp)) AS customer_lifespan_days, -- Active period
        DATEDIFF('day', MAX(o.order_purchase_timestamp), CURRENT_DATE) AS days_since_last_order, -- Recency measure

        -- PURCHASE FREQUENCY CALCULATION: Advanced timing analysis
        CASE
            WHEN COUNT(DISTINCT o.order_id) > 1
            THEN DATEDIFF('day', MIN(o.order_purchase_timestamp), MAX(o.order_purchase_timestamp)) /
                 NULLIF(COUNT(DISTINCT o.order_id) - 1, 0)      -- Average days between purchases
            ELSE NULL                                           -- Single-purchase customers
        END AS avg_days_between_orders,

        -- DELIVERY EXPERIENCE METRICS: Service quality indicators
        AVG(CASE WHEN o.is_on_time_delivery THEN 1.0 ELSE 0.0 END)::float AS on_time_delivery_rate, -- Service level experience
        AVG(o.delivery_days) AS avg_delivery_days,             -- Typical delivery time

        -- PAYMENT BEHAVIOR ANALYSIS: Financial preferences and risk indicators
        AVG(p.payment_installments) AS avg_installments,       -- Payment term preferences
        COUNT(DISTINCT p.payment_type) AS payment_methods_used, -- Payment diversity
        SUM(CASE WHEN p.payment_risk_category = 'High Risk' THEN 1 ELSE 0 END) AS high_risk_payments, -- Risk transactions

        -- PRODUCT ENGAGEMENT METRICS: Purchase diversity and seller relationships
        COUNT(DISTINCT pr.product_category_name) AS categories_purchased, -- Category breadth
        COUNT(DISTINCT oi.seller_id) AS sellers_used,          -- Seller relationship diversity

        -- REVIEW ENGAGEMENT ANALYSIS: Customer feedback behavior
        COUNT(DISTINCT r.review_id) AS reviews_given,          -- Feedback frequency
        AVG(r.review_score) AS avg_review_score,               -- Satisfaction sentiment
        SUM(CASE WHEN r.is_comprehensive_review THEN 1 ELSE 0 END) AS detailed_reviews_count -- Engagement depth
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
    LEFT JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
    LEFT JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id
    LEFT JOIN {{ ref('stg_products') }} pr ON oi.product_id = pr.product_id
    LEFT JOIN {{ ref('stg_reviews') }} r ON o.order_id = r.order_id

    -- INCREMENTAL PROCESSING: Only process customers with recent data changes
    {% if is_incremental() %}
    WHERE c.dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}

    -- AGGREGATION GROUPING: Customer-level summaries
    GROUP BY c.customer_sk, c.customer_id, c.customer_state, c.region, c.state_tier
),

behavioral_scoring AS (
    -- ADVANCED ANALYTICS LAYER: RFM analysis and behavioral classification
    SELECT *,

        -- RFM ANALYSIS: Recency, Frequency, Monetary scoring (1-5 scale)
        NTILE(5) OVER (ORDER BY days_since_last_order DESC) AS recency_score,    -- 5=most recent, 1=least recent
        NTILE(5) OVER (ORDER BY total_orders ASC) AS frequency_score,            -- 5=most frequent, 1=least frequent  
        NTILE(5) OVER (ORDER BY total_spent ASC) AS monetary_score,              -- 5=highest value, 1=lowest value

        -- ENGAGEMENT CLASSIFICATION: Customer interaction depth assessment
        CASE
            WHEN reviews_given / NULLIF(total_orders, 0) >= 0.5 THEN 'High Engagement'      -- Reviews >50% of orders
            WHEN reviews_given / NULLIF(total_orders, 0) >= 0.2 THEN 'Medium Engagement'    -- Reviews 20-50% of orders
            WHEN reviews_given > 0 THEN 'Low Engagement'                                     -- Some reviews
            ELSE 'No Engagement'                                                             -- No feedback given
        END AS engagement_level,

        -- CUSTOMER SOPHISTICATION: Purchase complexity and diversity analysis
        CASE
            WHEN categories_purchased >= 5 AND payment_methods_used >= 2 THEN 'Sophisticated'  -- High diversity across dimensions
            WHEN categories_purchased >= 3 OR sellers_used >= 3 THEN 'Moderate'                -- Some diversity
            ELSE 'Basic'                                                                        -- Limited scope customers
        END AS customer_sophistication,

        -- RISK ASSESSMENT: Payment behavior risk categorization
        CASE
            WHEN high_risk_payments / NULLIF(total_orders, 0) > 0.3 THEN 'High Risk'     -- >30% high-risk transactions
            WHEN high_risk_payments > 0 THEN 'Medium Risk'                                -- Some high-risk activity
            ELSE 'Low Risk'                                                               -- Clean payment history
        END AS customer_risk_level,

        -- SERVICE QUALITY EXPERIENCE: Delivery performance impact on customer
        CASE
            WHEN on_time_delivery_rate < 0.7 THEN 'Poor Experience'      -- <70% on-time delivery
            WHEN on_time_delivery_rate < 0.9 THEN 'Average Experience'   -- 70-90% on-time delivery
            ELSE 'Excellent Experience'                                   -- >90% on-time delivery
        END AS delivery_experience_quality
    FROM customer_orders
),

lifecycle_analysis AS (
    -- PREDICTIVE ANALYTICS: Lifecycle staging and churn modeling
    SELECT *,

        -- CUSTOMER LIFECYCLE CLASSIFICATION: Business-critical segmentation
        CASE
            -- HIGH VALUE SEGMENTS: Retain and grow
            WHEN days_since_last_order <= 30 AND total_orders >= 3 THEN 'Champion'                    -- Recent, frequent, loyal
            WHEN days_since_last_order <= 60 AND total_orders >= 2 AND avg_review_score >= 4 THEN 'Loyal Customer' -- Satisfied repeat customers
            WHEN days_since_last_order <= 90 AND total_orders >= 2 THEN 'Potential Loyalist'         -- Building loyalty
            
            -- ACQUISITION SEGMENTS: Nurture and convert
            WHEN days_since_last_order <= 30 AND total_orders = 1 THEN 'New Customer'                -- First-time buyers
            WHEN days_since_last_order <= 90 THEN 'Recent Customer'                                   -- Single recent purchase
            
            -- AT-RISK SEGMENTS: Reactivation campaigns critical
            WHEN days_since_last_order <= 180 THEN 'At Risk'                                          -- Moderate churn risk
            WHEN days_since_last_order <= 365 THEN 'Cannot Lose Them'                               -- High churn risk, high value
            
            -- LOST SEGMENTS: Win-back or write-off
            ELSE 'Lost Customer'                                                                       -- Very high churn probability
        END AS lifecycle_stage,

        -- CHURN PROBABILITY MODELING: Simplified logistic regression for churn prediction
        1 / (1 + EXP(-(
            -2.5 +                                                          -- Base intercept
            (days_since_last_order * 0.01) +                               -- Recency impact (positive = higher churn)
            (total_orders * -0.3) +                                        -- Frequency impact (negative = lower churn)
            (avg_review_score * -0.2) +                                    -- Satisfaction impact (negative = lower churn)  
            (CASE WHEN on_time_delivery_rate < 0.8 THEN 1 ELSE 0 END * 0.5) -- Poor experience impact (positive = higher churn)
        ))) AS churn_probability,

        -- NEXT PURCHASE PREDICTION: Behavioral forecasting using historical patterns
        CASE
            WHEN avg_days_between_orders IS NOT NULL
            THEN DATEADD('day', avg_days_between_orders, last_order_date)   -- Predict based on historical frequency
            ELSE NULL                                                       -- No pattern available for single-purchase customers
        END AS predicted_next_purchase_date
    FROM behavioral_scoring
),

final_customer_360 AS (
    -- FINAL ASSEMBLY: Complete customer intelligence with derived insights
    SELECT
        -- DIMENSIONAL IDENTIFIERS: Keys for joins and analysis
        customer_sk,                            -- Surrogate key
        customer_id,                            -- Business key
        customer_state,                         -- Geographic dimension
        region,                                 -- Regional grouping
        state_tier,                             -- Market tier classification

        -- CORE TRANSACTION METRICS: Fundamental business measures
        total_orders,                           -- Order frequency
        total_spent,                            -- Lifetime value (historical)
        avg_order_value,                        -- Order size indicator
        order_value_volatility,                 -- Spending consistency

        -- TEMPORAL INTELLIGENCE: Time-based behavioral patterns
        first_order_date,                       -- Customer acquisition date
        last_order_date,                        -- Last activity date
        customer_lifespan_days,                 -- Active relationship duration
        days_since_last_order,                  -- Recency measure
        avg_days_between_orders,                -- Purchase frequency pattern

        -- OPERATIONAL EXPERIENCE METRICS: Service quality impact
        on_time_delivery_rate,                  -- Service level experienced
        avg_delivery_days,                      -- Typical fulfillment time
        avg_installments,                       -- Payment preference
        payment_methods_used,                   -- Payment diversity
        high_risk_payments,                     -- Risk transaction count

        -- ENGAGEMENT AND DIVERSITY INDICATORS: Relationship depth measures
        categories_purchased,                   -- Product category breadth
        sellers_used,                           -- Seller relationship diversity
        reviews_given,                          -- Feedback participation
        avg_review_score,                       -- Satisfaction level
        detailed_reviews_count,                 -- Engagement depth

        -- ADVANCED BEHAVIORAL SCORING: RFM and classification results
        recency_score,                          -- RFM: Recency quintile (1-5)
        frequency_score,                        -- RFM: Frequency quintile (1-5)
        monetary_score,                         -- RFM: Monetary quintile (1-5)
        engagement_level,                       -- Feedback engagement classification
        customer_sophistication,                -- Purchase complexity classification
        customer_risk_level,                    -- Payment risk assessment
        delivery_experience_quality,            -- Service experience classification

        -- PREDICTIVE ANALYTICS: Forward-looking business intelligence
        lifecycle_stage,                        -- Strategic customer segment
        churn_probability,                      -- Predictive churn risk (0-1)
        predicted_next_purchase_date,           -- Forecasted next transaction

        -- DERIVED BUSINESS FLAGS: Operational decision support indicators
        CASE WHEN order_value_volatility > 100 THEN TRUE ELSE FALSE END AS has_volatile_spending,        -- Inconsistent spending pattern
        CASE WHEN total_orders / NULLIF(customer_lifespan_days, 0) * 365 > 6 THEN TRUE ELSE FALSE END AS high_frequency_customer, -- High-velocity buyer

        -- AUDIT METADATA: Data lineage and processing information
        CURRENT_TIMESTAMP AS dbt_loaded_at,     -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id -- dbt run identifier for tracking
    FROM lifecycle_analysis
)

-- FINAL OUTPUT: Complete 360-degree customer intelligence for business decision-making
SELECT * FROM final_customer_360
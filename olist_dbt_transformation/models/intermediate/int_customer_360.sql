{{ config(
  materialized='incremental',
  unique_key='customer_sk',
  incremental_strategy='delete+insert',
  on_schema_change='append_new_columns',
  cluster_by=['customer_state', 'lifecycle_stage'],
  schema = "dbt_olist_int",
  tags = ['intermediate']
) }}

WITH customer_orders AS (
    SELECT 
        c.customer_sk,
        c.customer_id,
        c.customer_state,
        c.region,
        c.state_tier,
        
        -- Order aggregations
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(p.payment_value)::float AS total_spent,
        AVG(p.payment_value)::float AS avg_order_value,
        STDDEV(p.payment_value) AS order_value_volatility,
        
        -- Temporal patterns
        MIN(o.order_purchase_timestamp) AS first_order_date,
        MAX(o.order_purchase_timestamp) AS last_order_date,
        DATEDIFF('day', MIN(o.order_purchase_timestamp), MAX(o.order_purchase_timestamp)) AS customer_lifespan_days,
        DATEDIFF('day', MAX(o.order_purchase_timestamp), CURRENT_DATE) AS days_since_last_order,
        
        -- Average days between orders
        CASE 
            WHEN COUNT(DISTINCT o.order_id) > 1 
            THEN DATEDIFF('day', MIN(o.order_purchase_timestamp), MAX(o.order_purchase_timestamp)) / 
                 NULLIF(COUNT(DISTINCT o.order_id) - 1, 0)
            ELSE NULL 
        END AS avg_days_between_orders,
        
        -- Delivery performance 
        AVG(CASE WHEN o.is_on_time_delivery THEN 1.0 ELSE 0.0 END)::float AS on_time_delivery_rate,
        AVG(o.delivery_days) AS avg_delivery_days,
        
        -- Payment behavior
        AVG(p.payment_installments) AS avg_installments,
        COUNT(DISTINCT p.payment_type) AS payment_methods_used,
        SUM(CASE WHEN p.payment_risk_category = 'High Risk' THEN 1 ELSE 0 END) AS high_risk_payments,
        
        -- Product diversity
        COUNT(DISTINCT pr.product_category_name) AS categories_purchased,
        COUNT(DISTINCT oi.seller_id) AS sellers_used,
        
        -- Review behavior
        COUNT(DISTINCT r.review_id) AS reviews_given,
        AVG(r.review_score) AS avg_review_score,
        SUM(CASE WHEN r.is_comprehensive_review THEN 1 ELSE 0 END) AS detailed_reviews_count

    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
    LEFT JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id  
    LEFT JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id
    LEFT JOIN {{ ref('stg_products') }} pr ON oi.product_id = pr.product_id
    LEFT JOIN {{ ref('stg_reviews') }} r ON o.order_id = r.order_id
    
    {% if is_incremental() %}
    WHERE c.dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
    
    GROUP BY c.customer_sk, c.customer_id, c.customer_state, c.region, c.state_tier
),

behavioral_scoring AS (
    SELECT *,
        -- RFM Analysis
        NTILE(5) OVER (ORDER BY days_since_last_order DESC) AS recency_score,
        NTILE(5) OVER (ORDER BY total_orders ASC) AS frequency_score,  
        NTILE(5) OVER (ORDER BY total_spent ASC) AS monetary_score,
        
        -- Engagement scoring
        CASE 
            WHEN reviews_given / NULLIF(total_orders, 0) >= 0.5 THEN 'High Engagement'
            WHEN reviews_given / NULLIF(total_orders, 0) >= 0.2 THEN 'Medium Engagement'
            WHEN reviews_given > 0 THEN 'Low Engagement'
            ELSE 'No Engagement'
        END AS engagement_level,
        
        -- Customer sophistication  
        CASE 
            WHEN categories_purchased >= 5 AND payment_methods_used >= 2 THEN 'Sophisticated'
            WHEN categories_purchased >= 3 OR sellers_used >= 3 THEN 'Moderate'
            ELSE 'Basic'
        END AS customer_sophistication,
        
        -- Risk assessment
        CASE 
            WHEN high_risk_payments / NULLIF(total_orders, 0) > 0.3 THEN 'High Risk'
            WHEN high_risk_payments > 0 THEN 'Medium Risk' 
            ELSE 'Low Risk'
        END AS customer_risk_level,
        
        -- Quality indicators
        CASE 
            WHEN on_time_delivery_rate < 0.7 THEN 'Poor Experience'
            WHEN on_time_delivery_rate < 0.9 THEN 'Average Experience'
            ELSE 'Excellent Experience'  
        END AS delivery_experience_quality

    FROM customer_orders
),

lifecycle_analysis AS (
    SELECT *,
        -- Customer lifecycle stage
        CASE 
            WHEN days_since_last_order <= 30 AND total_orders >= 3 THEN 'Champion'
            WHEN days_since_last_order <= 60 AND total_orders >= 2 AND avg_review_score >= 4 THEN 'Loyal Customer'
            WHEN days_since_last_order <= 90 AND total_orders >= 2 THEN 'Potential Loyalist'  
            WHEN days_since_last_order <= 30 AND total_orders = 1 THEN 'New Customer'
            WHEN days_since_last_order <= 90 THEN 'Recent Customer'
            WHEN days_since_last_order <= 180 THEN 'At Risk'
            WHEN days_since_last_order <= 365 THEN 'Cannot Lose Them'
            ELSE 'Lost Customer'
        END AS lifecycle_stage,
        
        -- Churn probability (simplified logistic regression approach)
        1 / (1 + EXP(-(
            -2.5 +  -- Intercept
            (days_since_last_order * 0.01) +  -- Recency factor
            (total_orders * -0.3) +  -- Frequency factor  
            (avg_review_score * -0.2) +  -- Satisfaction factor
            (CASE WHEN on_time_delivery_rate < 0.8 THEN 1 ELSE 0 END * 0.5)  -- Experience factor
        ))) AS churn_probability,
        
        -- Next purchase prediction
        CASE 
            WHEN avg_days_between_orders IS NOT NULL 
            THEN DATEADD('day', avg_days_between_orders, last_order_date)
            ELSE NULL 
        END AS predicted_next_purchase_date

    FROM behavioral_scoring
),

final_customer_360 AS (
    SELECT 
        customer_sk,
        customer_id,
        customer_state,
        region,
        state_tier,
        
        -- Order metrics
        total_orders,
        total_spent,
        avg_order_value,
        order_value_volatility,
        
        -- Temporal patterns
        first_order_date,
        last_order_date, 
        customer_lifespan_days,
        days_since_last_order,
        avg_days_between_orders,
        
        -- Performance metrics
        on_time_delivery_rate,
        avg_delivery_days,
        avg_installments,
        payment_methods_used,
        high_risk_payments,
        
        -- Diversity metrics
        categories_purchased,
        sellers_used,
        reviews_given,
        avg_review_score,
        detailed_reviews_count,
        
        -- Behavioral scores
        recency_score,
        frequency_score, 
        monetary_score,
        engagement_level,
        customer_sophistication,
        customer_risk_level,
        delivery_experience_quality,
        
        -- Advanced analytics
        lifecycle_stage,
        churn_probability,
        predicted_next_purchase_date,
        
        -- Statistical indicators
        CASE WHEN order_value_volatility > 100 THEN TRUE ELSE FALSE END AS has_volatile_spending,
        CASE WHEN total_orders / NULLIF(customer_lifespan_days, 0) * 365 > 6 THEN TRUE ELSE FALSE END AS high_frequency_customer,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_tz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id

    FROM lifecycle_analysis
)

SELECT * FROM final_customer_360
{% if target.name == 'dev' %}
    LIMIT {{ var('dev_sample_size_agg') }}
{% endif %}
{{ config(
    materialized='incremental',
    unique_key='seller_sk',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    cluster_by=['health_tier', 'seller_state'],
    schema = "dbt_olist_int",
    tags = ['intermediate']
) }}

WITH seller_base AS (
    SELECT 
        s.seller_sk,
        s.seller_id,
        s.seller_state,
        s.seller_city,
        s.state_business_tier,
        s.geographic_region,
        s.business_environment_score,
        s.has_logistics_advantage
    FROM {{ ref('stg_sellers') }} s
    
    {% if is_incremental() %}
    WHERE s.dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
),

seller_order_metrics AS (
    SELECT 
        sb.*,
        
        -- Volume metrics
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        SUM(oi.total_item_value)::float AS total_gmv,
        AVG(oi.total_item_value) AS avg_order_value,
        COUNT(DISTINCT DATE(o.order_purchase_timestamp)) AS active_days,
        
        -- Time-based metrics  
        MIN(o.order_purchase_timestamp) AS first_order_date,
        MAX(o.order_purchase_timestamp) AS last_order_date,
        DATEDIFF('day', MIN(o.order_purchase_timestamp), MAX(o.order_purchase_timestamp)) AS seller_tenure_days,
        DATEDIFF('day', MAX(o.order_purchase_timestamp), CURRENT_DATE) AS days_since_last_order,
        
        -- Performance metrics
        AVG(CASE WHEN o.is_on_time_delivery THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
        AVG(o.delivery_days)::float AS avg_delivery_days,
        STDDEV(o.delivery_days) AS delivery_consistency,
        
        -- Order fulfillment
        SUM(CASE WHEN o.is_cancelled THEN 1 ELSE 0 END) AS cancelled_orders,
        SUM(CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
        
        -- Product portfolio  
        COUNT(DISTINCT oi.product_id) AS unique_products_sold,
        COUNT(DISTINCT pr.category_group) AS category_diversity,
        AVG(pr.product_weight_g) AS avg_product_weight,
        SUM(CASE WHEN pr.is_bulky_item THEN 1 ELSE 0 END) AS bulky_items_sold,
        
        -- Pricing analysis
        AVG(oi.item_price) AS avg_item_price,
        STDDEV(oi.item_price) AS price_volatility,
        SUM(CASE WHEN oi.is_price_anomaly THEN 1 ELSE 0 END) AS price_anomaly_count,
        
        -- Geographic reach
        COUNT(DISTINCT c.customer_state) AS customer_states_reached,
        COUNT(DISTINCT c.region) AS regions_served

    FROM seller_base sb
    LEFT JOIN {{ ref('stg_order_items') }} oi ON sb.seller_id = oi.seller_id
    LEFT JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
    LEFT JOIN {{ ref('stg_products') }} pr ON oi.product_id = pr.product_id  
    LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
    
    GROUP BY 
        sb.seller_sk, sb.seller_id, sb.seller_state, sb.seller_city,
        sb.state_business_tier, sb.geographic_region, sb.business_environment_score,
        sb.has_logistics_advantage
),

seller_customer_satisfaction AS (
    SELECT 
        som.seller_sk,
        
        -- Review metrics
        COUNT(r.review_id) AS total_reviews,
        AVG(r.review_score)::float AS avg_review_score,
        STDDEV(r.review_score) AS review_score_consistency,
        
        -- Review distribution
        SUM(CASE WHEN r.review_score = 5 THEN 1 ELSE 0 END) AS five_star_reviews,
        SUM(CASE WHEN r.review_score = 1 THEN 1 ELSE 0 END) AS one_star_reviews,
        SUM(CASE WHEN r.is_comprehensive_review THEN 1 ELSE 0 END) AS detailed_reviews,
        
        -- Customer satisfaction indicators
        AVG(CASE WHEN r.is_satisfied THEN 1.0 ELSE 0.0 END) AS customer_satisfaction_rate,
        AVG(CASE WHEN r.is_dissatisfied THEN 1.0 ELSE 0.0 END) AS customer_dissatisfaction_rate,
        
        -- Response metrics
        AVG(CASE WHEN r.has_response THEN 1.0 ELSE 0.0 END) AS review_response_rate,
        AVG(r.response_time_hours) AS avg_response_time_hours

    FROM seller_order_metrics som
    LEFT JOIN {{ ref('stg_order_items') }} oi ON som.seller_id = oi.seller_id
    LEFT JOIN {{ ref('stg_reviews') }} r ON oi.order_id = r.order_id
    
    GROUP BY som.seller_sk
),

health_score_calculation AS (
    SELECT 
        som.*,
        scs.total_reviews,
        scs.avg_review_score,
        scs.customer_satisfaction_rate,
        scs.customer_dissatisfaction_rate,
        scs.review_response_rate,
        scs.avg_response_time_hours,
        
        -- Delivery Performance Score (0-100)
        CASE 
            WHEN som.total_orders = 0 THEN 0
            ELSE LEAST(100, (
                (som.on_time_delivery_rate * 40) +  -- 40 points for on-time delivery
                (GREATEST(0, (10 - som.avg_delivery_days)) * 3) +  -- Up to 30 points for speed
                (CASE WHEN som.delivery_consistency <= 2 THEN 20 ELSE GREATEST(0, (20 - som.delivery_consistency * 2)) END) +  -- Up to 20 points for consistency
                (GREATEST(0, (100 - som.cancelled_orders * 100.0 / som.total_orders)) * 0.1)  -- Up to 10 points for low cancellation
            ))
        END AS delivery_performance_score,
        
        -- Customer Satisfaction Score (0-100)  
        CASE 
            WHEN scs.total_reviews = 0 THEN 50.00  -- Neutral score for no reviews
            ELSE LEAST(100, (
                (scs.avg_review_score * 15) +  -- Up to 75 points for avg rating
                (scs.customer_satisfaction_rate * 20) +  -- Up to 20 points for satisfaction rate
                (GREATEST(0, (100 - scs.customer_dissatisfaction_rate * 100)) * 0.05)  -- Up to 5 points for low dissatisfaction
            ))::float
        END AS customer_satisfaction_score,
        
        -- Order Volume Score (0-100)
        CASE 
            WHEN som.total_orders = 0 THEN 0
            ELSE LEAST(100, (
                (ln(som.total_orders + 1) * 15) +  -- Logarithmic scaling for order volume
                (ln(som.unique_customers + 1) * 10) +  -- Customer diversity
                (GREATEST(0, LEAST(50, som.active_days * 365.0 / GREATEST(som.seller_tenure_days, 1))) * 0.5) +  -- Activity consistency
                (GREATEST(0, LEAST(25, som.total_gmv / 1000)) * 1)  -- GMV contribution
            ))
        END AS order_volume_score,
        
        -- Quality Score (0-100)
        CASE 
            WHEN som.total_orders = 0 THEN 0.00
            ELSE LEAST(100, (
                (GREATEST(0, (100 - som.price_anomaly_count * 100.0 / som.total_orders)) * 0.3) +  -- Pricing consistency
                (som.unique_products_sold / GREATEST(som.total_orders, 1) * 100 * 0.2) +  -- Product diversity
                (som.category_diversity * 10) +  -- Category breadth  
                (CASE WHEN scs.review_response_rate > 0 THEN 20 ELSE 0 END) +  -- Customer service
                (som.business_environment_score * 5)  -- Location advantage
            ))::float
        END AS quality_score

    FROM seller_order_metrics som
    LEFT JOIN seller_customer_satisfaction scs ON som.seller_sk = scs.seller_sk
),

weighted_health_score AS (
    SELECT *,
        -- Composite health score using configurable weights
        (
            (delivery_performance_score * {{ var('health_score_weights', {}).get('delivery_performance', 0.35) }}) +
            (customer_satisfaction_score * {{ var('health_score_weights', {}).get('customer_satisfaction', 0.25) }}) +
            (order_volume_score * {{ var('health_score_weights', {}).get('order_volume', 0.20) }}) +
            (quality_score * {{ var('health_score_weights', {}).get('quality_score', 0.20) }})
        ) AS composite_health_score,
        
        -- Risk indicators
        CASE 
            WHEN days_since_last_order > 90 THEN 'Inactive Seller'
            WHEN customer_dissatisfaction_rate > 0.3 THEN 'High Complaint Risk'
            WHEN on_time_delivery_rate < 0.7 THEN 'Delivery Risk'
            WHEN cancelled_orders > total_orders * 0.1 THEN 'Fulfillment Risk'
            ELSE 'Low Risk'
        END AS primary_risk_factor,
        
        -- Growth indicators
        CASE 
            WHEN total_orders / GREATEST(seller_tenure_days / 30.0, 1) > 50 THEN 'High Growth'
            WHEN total_orders / GREATEST(seller_tenure_days / 30.0, 1) > 20 THEN 'Medium Growth'  
            WHEN total_orders / GREATEST(seller_tenure_days / 30.0, 1) > 5 THEN 'Slow Growth'
            ELSE 'Declining'
        END AS growth_trajectory

    FROM health_score_calculation
),

final AS (
    SELECT 
        seller_sk,
        seller_id,
        seller_state,
        seller_city,
        geographic_region,
        business_environment_score,
        has_logistics_advantage,
        
        -- Performance metrics
        total_orders,
        total_gmv,
        unique_customers,
        unique_products_sold,
        seller_tenure_days,
        days_since_last_order,
        
        -- Key performance indicators
        on_time_delivery_rate,
        avg_delivery_days,
        avg_review_score,
        customer_satisfaction_rate,
        
        -- Health scores
        delivery_performance_score,
        customer_satisfaction_score, 
        order_volume_score,
        quality_score,
        composite_health_score,
        
        -- Health tier classification
        CASE 
            WHEN composite_health_score >= 80 THEN 'Excellent'
            WHEN composite_health_score >= 65 THEN 'Good'
            WHEN composite_health_score >= 50 THEN 'Average'
            WHEN composite_health_score >= 35 THEN 'Below Average'
            ELSE 'Poor'
        END AS health_tier,
        
        -- Business insights
        primary_risk_factor,
        growth_trajectory,
        
        -- Recommended actions
        CASE 
            WHEN composite_health_score >= 80 THEN 'Reward and Promote'
            WHEN composite_health_score >= 65 AND growth_trajectory = 'High Growth' THEN 'Invest in Growth'
            WHEN composite_health_score >= 50 THEN 'Provide Support'
            WHEN composite_health_score >= 35 THEN 'Performance Improvement Plan'
            ELSE 'Consider Termination'
        END AS recommended_action,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_tz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id

    FROM weighted_health_score
)

SELECT * FROM final
{% if target.name == 'dev' %}
    LIMIT {{ var('dev_sample_size_agg') }}
{% endif %}
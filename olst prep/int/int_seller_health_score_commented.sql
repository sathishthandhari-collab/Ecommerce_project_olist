{{ config(
    -- PERFORMANCE OPTIMIZATION: Incremental configuration for comprehensive seller performance analysis
    materialized='incremental',
    unique_key='seller_sk',                     -- Seller surrogate key for merge operations
    incremental_strategy='delete+insert',       -- Full refresh of changed sellers for accuracy
    on_schema_change='append_new_columns',      -- Handle schema evolution for new health metrics
    cluster_by=['health_tier', 'seller_state'], -- Optimize for performance analysis and geographic queries
    schema = "dbt_olist_int"                    -- Intermediate layer for business intelligence
) }}

WITH seller_base AS (
    -- FOUNDATION DATA: Core seller dimensional information and business context
    SELECT
        -- SELLER IDENTIFIERS: Keys and dimensional attributes
        s.seller_sk,                            -- Stable surrogate key
        s.seller_id,                            -- Natural business identifier
        s.seller_state,                         -- Geographic location
        s.seller_city,                          -- City-level location
        s.state_business_tier,                  -- Economic tier classification (Tier 1/2/3)
        s.geographic_region,                    -- Regional grouping
        s.business_environment_score,           -- Economic environment rating (1-5)
        s.has_logistics_advantage               -- Infrastructure advantage flag
    FROM {{ ref('stg_sellers') }} s

    -- INCREMENTAL PROCESSING: Only analyze sellers with recent activity changes
    {% if is_incremental() %}
    WHERE s.dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
),

seller_order_metrics AS (
    -- COMPREHENSIVE PERFORMANCE ANALYSIS: Multi-dimensional seller activity aggregation
    SELECT
        sb.*,

        -- VOLUME AND SCALE METRICS: Business size and reach indicators
        COUNT(DISTINCT o.order_id) AS total_orders,             -- Total transaction volume
        COUNT(DISTINCT o.customer_id) AS unique_customers,      -- Customer base breadth  
        SUM(oi.total_item_value)::float AS total_gmv,          -- Gross merchandise value
        AVG(oi.total_item_value) AS avg_order_value,           -- Average transaction size
        COUNT(DISTINCT DATE(o.order_purchase_timestamp)) AS active_days, -- Activity frequency

        -- TEMPORAL ACTIVITY ANALYSIS: Seller lifecycle and engagement patterns
        MIN(o.order_purchase_timestamp) AS first_order_date,    -- Platform onboarding date
        MAX(o.order_purchase_timestamp) AS last_order_date,     -- Most recent activity
        DATEDIFF('day', MIN(o.order_purchase_timestamp), MAX(o.order_purchase_timestamp)) AS seller_tenure_days, -- Platform relationship duration
        DATEDIFF('day', MAX(o.order_purchase_timestamp), CURRENT_DATE) AS days_since_last_order, -- Activity recency

        -- DELIVERY PERFORMANCE METRICS: Service quality and reliability indicators
        AVG(CASE WHEN o.is_on_time_delivery THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,    -- Service level achievement
        AVG(o.delivery_days)::float AS avg_delivery_days,      -- Average fulfillment time
        STDDEV(o.delivery_days) AS delivery_consistency,       -- Delivery time variability

        -- ORDER FULFILLMENT ANALYSIS: Completion and failure tracking
        SUM(CASE WHEN o.is_cancelled THEN 1 ELSE 0 END) AS cancelled_orders,      -- Failed transactions
        SUM(CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders, -- Successful completions

        -- PRODUCT PORTFOLIO METRICS: Catalog breadth and diversity analysis
        COUNT(DISTINCT oi.product_id) AS unique_products_sold,  -- Product catalog size
        COUNT(DISTINCT pr.category_group) AS category_diversity, -- Category specialization vs diversification
        AVG(pr.product_weight_g) AS avg_product_weight,        -- Logistics complexity indicator
        SUM(CASE WHEN pr.is_bulky_item THEN 1 ELSE 0 END) AS bulky_items_sold, -- Specialized logistics requirements

        -- PRICING STRATEGY ANALYSIS: Competitive positioning and consistency
        AVG(oi.item_price) AS avg_item_price,                  -- Pricing level
        STDDEV(oi.item_price) AS price_volatility,             -- Pricing consistency
        SUM(CASE WHEN oi.is_price_anomaly THEN 1 ELSE 0 END) AS price_anomaly_count, -- Unusual pricing incidents

        -- GEOGRAPHIC REACH ANALYSIS: Market penetration and expansion
        COUNT(DISTINCT c.customer_state) AS customer_states_reached, -- Geographic penetration
        COUNT(DISTINCT c.region) AS regions_served             -- Regional market presence
    FROM seller_base sb
    LEFT JOIN {{ ref('stg_order_items') }} oi ON sb.seller_id = oi.seller_id
    LEFT JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
    LEFT JOIN {{ ref('stg_products') }} pr ON oi.product_id = pr.product_id
    LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id

    -- AGGREGATION GROUPING: Seller-level performance summaries
    GROUP BY
        sb.seller_sk, sb.seller_id, sb.seller_state, sb.seller_city,
        sb.state_business_tier, sb.geographic_region, sb.business_environment_score,
        sb.has_logistics_advantage
),

seller_customer_satisfaction AS (
    -- CUSTOMER SATISFACTION INTELLIGENCE: Review and feedback analysis
    SELECT
        som.seller_sk,

        -- REVIEW VOLUME AND ENGAGEMENT METRICS: Feedback quantity and seller responsiveness
        COUNT(r.review_id) AS total_reviews,                   -- Total feedback received
        AVG(r.review_score)::float AS avg_review_score,        -- Average satisfaction rating
        STDDEV(r.review_score) AS review_score_consistency,    -- Rating variability

        -- SATISFACTION DISTRIBUTION ANALYSIS: Granular sentiment breakdown
        SUM(CASE WHEN r.review_score = 5 THEN 1 ELSE 0 END) AS five_star_reviews,    -- Excellent ratings
        SUM(CASE WHEN r.review_score = 1 THEN 1 ELSE 0 END) AS one_star_reviews,     -- Poor ratings
        SUM(CASE WHEN r.is_comprehensive_review THEN 1 ELSE 0 END) AS detailed_reviews, -- High-engagement feedback

        -- CUSTOMER SATISFACTION RATES: Binary satisfaction classification
        AVG(CASE WHEN r.is_satisfied THEN 1.0 ELSE 0.0 END) AS customer_satisfaction_rate,     -- Positive experience rate (4-5 stars)
        AVG(CASE WHEN r.is_dissatisfied THEN 1.0 ELSE 0.0 END) AS customer_dissatisfaction_rate, -- Negative experience rate (1-2 stars)

        -- CUSTOMER SERVICE RESPONSIVENESS: Seller engagement with feedback
        AVG(CASE WHEN r.has_response THEN 1.0 ELSE 0.0 END) AS review_response_rate,  -- Response frequency
        AVG(r.response_time_hours) AS avg_response_time_hours  -- Response speed
    FROM seller_order_metrics som
    LEFT JOIN {{ ref('stg_order_items') }} oi ON som.seller_id = oi.seller_id
    LEFT JOIN {{ ref('stg_reviews') }} r ON oi.order_id = r.order_id
    GROUP BY som.seller_sk
),

health_score_calculation AS (
    -- HEALTH SCORE COMPUTATION: Multi-dimensional performance scoring (0-100 scale)
    SELECT
        som.*,
        scs.total_reviews,
        scs.avg_review_score,
        scs.customer_satisfaction_rate,
        scs.customer_dissatisfaction_rate,
        scs.review_response_rate,
        scs.avg_response_time_hours,

        -- DELIVERY PERFORMANCE SCORE (0-100): Weighted delivery excellence assessment
        CASE
            WHEN som.total_orders = 0 THEN 0
            ELSE LEAST(100, (
                (som.on_time_delivery_rate * 40) +             -- 40 points: On-time delivery rate
                (GREATEST(0, (10 - som.avg_delivery_days)) * 3) + -- 30 points: Speed (bonus for <10 days)
                (CASE WHEN som.delivery_consistency <= 2 
                      THEN 20 
                      ELSE GREATEST(0, (20 - som.delivery_consistency * 2)) END) + -- 20 points: Consistency
                (GREATEST(0, (100 - som.cancelled_orders * 100.0 / som.total_orders)) * 0.1) -- 10 points: Low cancellation rate
            ))
        END AS delivery_performance_score,

        -- CUSTOMER SATISFACTION SCORE (0-100): Review-based satisfaction assessment
        CASE
            WHEN scs.total_reviews = 0 THEN 50.00             -- Neutral score for new sellers with no reviews
            ELSE LEAST(100, (
                (scs.avg_review_score * 15) +                  -- 75 points: Average rating (5-star scale × 15)
                (scs.customer_satisfaction_rate * 20) +        -- 20 points: High satisfaction rate
                (GREATEST(0, (100 - scs.customer_dissatisfaction_rate * 100)) * 0.05) -- 5 points: Low dissatisfaction penalty
            ))::float
        END AS customer_satisfaction_score,

        -- ORDER VOLUME SCORE (0-100): Business scale and consistency assessment with logarithmic scaling
        CASE
            WHEN som.total_orders = 0 THEN 0
            ELSE LEAST(100, (
                (ln(som.total_orders + 1) * 15) +              -- Logarithmic order volume scaling
                (ln(som.unique_customers + 1) * 10) +          -- Customer base diversity
                (GREATEST(0, LEAST(50, som.active_days * 365.0 / GREATEST(som.seller_tenure_days, 1))) * 0.5) + -- Activity consistency
                (GREATEST(0, LEAST(25, som.total_gmv / 1000)) * 1) -- GMV contribution (scaled)
            ))
        END AS order_volume_score,

        -- QUALITY SCORE (0-100): Operational excellence and business sophistication
        CASE
            WHEN som.total_orders = 0 THEN 0.00
            ELSE LEAST(100, (
                (GREATEST(0, (100 - som.price_anomaly_count * 100.0 / som.total_orders)) * 0.3) + -- 30 points: Pricing consistency
                (som.unique_products_sold / GREATEST(som.total_orders, 1) * 100 * 0.2) + -- 20 points: Product diversity per order
                (som.category_diversity * 10) +                -- Up to 50+ points: Category breadth
                (CASE WHEN scs.review_response_rate > 0 THEN 20 ELSE 0 END) + -- 20 points: Customer service engagement
                (som.business_environment_score * 5)           -- Up to 25 points: Location advantage
            ))::float
        END AS quality_score
    FROM seller_order_metrics som
    LEFT JOIN seller_customer_satisfaction scs ON som.seller_sk = scs.seller_sk
),

weighted_health_score AS (
    -- COMPOSITE HEALTH SCORE: Configurable weighted combination of performance dimensions
    SELECT *,

        -- WEIGHTED COMPOSITE SCORE: Configurable business-priority weighting
        (
            (delivery_performance_score * {{ var('health_score_weights', {}).get('delivery_performance', 0.35) }}) +      -- 35% weight: Delivery excellence
            (customer_satisfaction_score * {{ var('health_score_weights', {}).get('customer_satisfaction', 0.25) }}) +     -- 25% weight: Customer satisfaction
            (order_volume_score * {{ var('health_score_weights', {}).get('order_volume', 0.20) }}) +                      -- 20% weight: Business scale
            (quality_score * {{ var('health_score_weights', {}).get('quality_score', 0.20) }})                           -- 20% weight: Operational quality
        ) AS composite_health_score,

        -- PRIMARY RISK FACTOR IDENTIFICATION: Critical business risk classification
        CASE
            WHEN days_since_last_order > 90 THEN 'Inactive Seller'           -- Platform engagement risk
            WHEN customer_dissatisfaction_rate > 0.3 THEN 'High Complaint Risk' -- Customer satisfaction risk
            WHEN on_time_delivery_rate < 0.7 THEN 'Delivery Risk'            -- Service quality risk
            WHEN cancelled_orders > total_orders * 0.1 THEN 'Fulfillment Risk' -- Order completion risk
            ELSE 'Low Risk'                                                   -- Healthy seller profile
        END AS primary_risk_factor,

        -- GROWTH TRAJECTORY ANALYSIS: Business momentum and scaling pattern
        CASE
            WHEN total_orders / GREATEST(seller_tenure_days / 30.0, 1) > 50 THEN 'High Growth'    -- >50 orders/month average
            WHEN total_orders / GREATEST(seller_tenure_days / 30.0, 1) > 20 THEN 'Medium Growth'  -- 20-50 orders/month average
            WHEN total_orders / GREATEST(seller_tenure_days / 30.0, 1) > 5 THEN 'Slow Growth'     -- 5-20 orders/month average
            ELSE 'Declining'                                                                        -- <5 orders/month average
        END AS growth_trajectory
    FROM health_score_calculation
),

final AS (
    -- FINAL ASSEMBLY: Complete seller health intelligence for business decision-making
    SELECT
        -- DIMENSIONAL IDENTIFIERS: Keys and geographic context
        seller_sk,                              -- Surrogate key for efficient joins
        seller_id,                              -- Natural business key  
        seller_state,                           -- Geographic location
        seller_city,                            -- City-level location
        geographic_region,                      -- Regional classification
        business_environment_score,             -- Economic environment rating
        has_logistics_advantage,                -- Infrastructure advantage

        -- CORE PERFORMANCE METRICS: Fundamental business measures
        total_orders,                           -- Transaction volume
        total_gmv,                              -- Gross merchandise value
        unique_customers,                       -- Customer base size
        unique_products_sold,                   -- Catalog breadth
        seller_tenure_days,                     -- Platform relationship duration
        days_since_last_order,                  -- Activity recency

        -- KEY PERFORMANCE INDICATORS: Critical success metrics
        on_time_delivery_rate,                  -- Service level performance
        avg_delivery_days,                      -- Fulfillment speed
        avg_review_score,                       -- Customer satisfaction
        customer_satisfaction_rate,             -- Positive experience rate

        -- HEALTH SCORE COMPONENTS: Detailed performance dimension scoring
        delivery_performance_score,             -- Delivery excellence (0-100)
        customer_satisfaction_score,            -- Satisfaction excellence (0-100)
        order_volume_score,                     -- Business scale (0-100)
        quality_score,                          -- Operational quality (0-100)
        composite_health_score,                 -- Overall health (0-100)

        -- HEALTH TIER CLASSIFICATION: Business-friendly performance categorization
        CASE
            WHEN composite_health_score >= 80 THEN 'Excellent'      -- Top-performing sellers
            WHEN composite_health_score >= 65 THEN 'Good'           -- Strong performers
            WHEN composite_health_score >= 50 THEN 'Average'        -- Acceptable performance
            WHEN composite_health_score >= 35 THEN 'Below Average'  -- Needs improvement
            ELSE 'Poor'                                             -- Requires intervention
        END AS health_tier,

        -- BUSINESS INTELLIGENCE: Strategic insights and risk assessment
        primary_risk_factor,                    -- Critical risk identification
        growth_trajectory,                      -- Business momentum assessment

        -- STRATEGIC RECOMMENDATIONS: Action-oriented business guidance
        CASE
            WHEN composite_health_score >= 80 THEN 'Reward and Promote'                        -- Showcase and incentivize excellence
            WHEN composite_health_score >= 65 AND growth_trajectory = 'High Growth' THEN 'Invest in Growth' -- Support expansion
            WHEN composite_health_score >= 50 THEN 'Provide Support'                          -- Coaching and resources
            WHEN composite_health_score >= 35 THEN 'Performance Improvement Plan'             -- Formal improvement program
            ELSE 'Consider Termination'                                                        -- Platform quality protection
        END AS recommended_action,

        -- AUDIT METADATA: Data lineage and processing tracking
        CURRENT_TIMESTAMP AS dbt_loaded_at,     -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id -- dbt run identifier for tracking
    FROM weighted_health_score
)

-- FINAL OUTPUT: Complete seller health intelligence with multi-dimensional scoring and strategic recommendations
SELECT * FROM final
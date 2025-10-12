{{ config(
    materialized='table',
    cluster_by=['health_tier', 'seller_state'],
    schema='dbt_olist_marts',
    tags=['marts']
    ) }}

WITH current_seller_state AS (
    SELECT 
        shs.*,
        ROW_NUMBER() OVER (PARTITION BY shs.seller_sk ORDER BY shs.dbt_loaded_at DESC) AS rn
    FROM {{ ref('int_seller_health_score') }} shs
),

latest_sellers AS (
    SELECT * FROM current_seller_state WHERE rn = 1
),

-- Performance tier analysis
tier_performance AS (
    SELECT 
        ls.health_tier,
        ls.seller_state,
        ls.geographic_region,
        
        -- Volume metrics
        COUNT(DISTINCT ls.seller_sk) AS seller_count,
        COUNT(DISTINCT CASE WHEN ls.days_since_last_order <= 30 THEN ls.seller_sk END) AS active_sellers,
        
        -- Financial performance
        SUM(ls.total_gmv) AS total_tier_gmv,
        AVG(ls.total_gmv) AS avg_seller_gmv,
        SUM(ls.total_orders) AS total_tier_orders,
        AVG(ls.total_orders) AS avg_seller_orders,
        
        -- Quality metrics
        AVG(ls.composite_health_score) AS avg_health_score,
        AVG(ls.on_time_delivery_rate) AS avg_delivery_rate,
        AVG(ls.customer_satisfaction_rate) AS avg_satisfaction_rate,
        AVG(ls.avg_review_score) AS avg_review_score,
        
        -- Risk assessment
        COUNT(DISTINCT CASE WHEN ls.primary_risk_factor != 'Low Risk' THEN ls.seller_sk END) AS at_risk_sellers,
        COUNT(DISTINCT CASE WHEN ls.growth_trajectory = 'Declining' THEN ls.seller_sk END) AS declining_sellers,
        COUNT(DISTINCT CASE WHEN ls.days_since_last_order > 90 THEN ls.seller_sk END) AS inactive_sellers,
        
        -- Growth analysis
        COUNT(DISTINCT CASE WHEN ls.growth_trajectory = 'High Growth' THEN ls.seller_sk END) AS high_growth_sellers,
        COUNT(DISTINCT CASE WHEN ls.growth_trajectory = 'Medium Growth' THEN ls.seller_sk END) AS medium_growth_sellers
        
    FROM latest_sellers ls
    GROUP BY ls.health_tier, ls.seller_state, ls.geographic_region
),

-- Individual seller analysis with benchmarking
seller_analysis AS (
    SELECT 
        ls.seller_sk,
        ls.seller_id,
        ls.seller_state,
        ls.seller_city,
        ls.geographic_region,
        ls.business_environment_score,
        ls.has_logistics_advantage,
        
        -- Performance metrics
        ls.total_orders,
        ls.total_gmv,
        ls.unique_customers,
        ls.unique_products_sold,
        ls.seller_tenure_days,
        ls.days_since_last_order,
        
        -- Quality indicators
        ls.on_time_delivery_rate,
        ls.avg_delivery_days,
        ls.avg_review_score,
        ls.customer_satisfaction_rate,
        
        -- Health scoring
        ls.delivery_performance_score,
        ls.customer_satisfaction_score,
        ls.order_volume_score, 
        ls.quality_score,
        ls.composite_health_score,
        ls.health_tier,
        
        -- Risk and growth
        ls.primary_risk_factor,
        ls.growth_trajectory,
        ls.recommended_action,
        
        -- Benchmarking against tier peers
        MEDIAN(ls.composite_health_score) OVER (PARTITION BY ls.health_tier) AS health_score_median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ls.composite_health_score) OVER (PARTITION BY ls.health_tier) AS health_score_75th,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ls.composite_health_score) OVER (PARTITION BY ls.health_tier) AS health_score_25th,
        
        -- Performance vs peers
        CASE 
            WHEN ls.composite_health_score > PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ls.composite_health_score) OVER (PARTITION BY ls.health_tier) THEN 'Top Performer'
            WHEN ls.composite_health_score > MEDIAN(ls.composite_health_score) OVER (PARTITION BY ls.health_tier) THEN 'Above Average'
            WHEN ls.composite_health_score > PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ls.composite_health_score) OVER (PARTITION BY ls.health_tier) THEN 'Below Average'
            ELSE 'Bottom Performer'
        END AS peer_performance,
        
        -- Revenue contribution analysis
        ls.total_gmv / SUM(ls.total_gmv) OVER (PARTITION BY ls.seller_state) * 100 AS state_gmv_contribution_pct,
        RANK() OVER (PARTITION BY ls.seller_state ORDER BY ls.total_gmv DESC) AS gmv_rank_in_state,
        
        -- Customer satisfaction relative to market
        CASE 
            WHEN ls.avg_review_score >= 4.5 THEN 'Excellent'
            WHEN ls.avg_review_score >= 4.0 THEN 'Good' 
            WHEN ls.avg_review_score >= 3.5 THEN 'Average'
            WHEN ls.avg_review_score >= 3.0 THEN 'Below Average'
            ELSE 'Poor'
        END AS satisfaction_tier,
        
        -- Delivery performance benchmarking
        CASE 
            WHEN ls.on_time_delivery_rate >= 0.95 THEN 'Excellent'
            WHEN ls.on_time_delivery_rate >= 0.90 THEN 'Good'
            WHEN ls.on_time_delivery_rate >= 0.80 THEN 'Average' 
            WHEN ls.on_time_delivery_rate >= 0.70 THEN 'Below Average'
            ELSE 'Poor'
        END AS delivery_tier
        
    FROM latest_sellers ls
),

-- Management action priorities
action_priorities AS (
    SELECT 
        sa.*,
        
        -- Priority scoring for management intervention
        CASE 
            WHEN sa.health_tier = 'Poor' AND sa.total_gmv > 10000 THEN 100  -- High revenue, poor performance
            WHEN sa.health_tier = 'Poor' THEN 90
            WHEN sa.health_tier = 'Below Average' AND sa.growth_trajectory = 'Declining' THEN 85
            WHEN sa.health_tier = 'Below Average' AND sa.peer_performance = 'Bottom Performer' THEN 80
            WHEN sa.health_tier = 'Average' AND sa.primary_risk_factor != 'Low Risk' THEN 70
            WHEN sa.health_tier = 'Good' AND sa.growth_trajectory = 'High Growth' THEN 60  -- Investment opportunity
            WHEN sa.health_tier = 'Excellent' THEN 50  -- Recognition/retention
            ELSE 40
        END AS management_priority_score,
        
        -- Specific intervention recommendations
        CASE 
            WHEN sa.health_tier = 'Poor' AND sa.delivery_tier = 'Poor' THEN 'Logistics Training & Support Required'
            WHEN sa.health_tier = 'Poor' AND sa.satisfaction_tier = 'Poor' THEN 'Customer Service Improvement Program'  
            WHEN sa.health_tier = 'Below Average' AND sa.days_since_last_order > 60 THEN 'Re-engagement Campaign'
            WHEN sa.health_tier = 'Average' AND sa.growth_trajectory = 'High Growth' THEN 'Growth Support Program'
            WHEN sa.health_tier = 'Good' AND sa.peer_performance = 'Top Performer' THEN 'Best Practices Documentation'
            WHEN sa.health_tier = 'Excellent' THEN 'Retention & Recognition Program'
            ELSE 'Standard Monitoring'
        END AS specific_intervention,
        
        -- Business impact assessment
        CASE 
            WHEN sa.state_gmv_contribution_pct > 10 THEN 'High Business Impact'
            WHEN sa.state_gmv_contribution_pct > 5 THEN 'Medium Business Impact'
            ELSE 'Low Business Impact'  
        END AS business_impact_tier,
        
        -- Investment recommendation
        CASE 
            WHEN sa.health_tier IN ('Good', 'Excellent') AND sa.growth_trajectory = 'High Growth' THEN 'Invest for Growth'
            WHEN sa.health_tier = 'Poor' AND sa.total_gmv > 5000 THEN 'Invest to Recover'
            WHEN sa.health_tier = 'Poor' AND sa.total_gmv < 1000 THEN 'Consider Termination'
            ELSE 'Maintain Current Support'
        END AS investment_recommendation
        
    FROM seller_analysis sa
),

final AS (
    SELECT 
        -- Seller identification  
        ap.seller_sk,
        ap.seller_id,
        ap.seller_state,
        ap.seller_city,
        ap.geographic_region,
        
        -- Performance metrics
        ap.total_orders,
        ap.total_gmv,
        ap.unique_customers,
        ap.seller_tenure_days,
        ap.days_since_last_order,
        
        -- Quality scores
        ap.composite_health_score,
        ap.health_tier,
        ap.delivery_tier,
        ap.satisfaction_tier,
        ap.peer_performance,
        
        -- Benchmarking
        ap.gmv_rank_in_state,
        ap.state_gmv_contribution_pct,
        ap.health_score_median,
        ap.health_score_75th,
        
        -- Management guidance
        ap.management_priority_score,
        ap.recommended_action,
        ap.specific_intervention,
        ap.business_impact_tier,
        ap.investment_recommendation,
        
        -- Risk indicators
        ap.primary_risk_factor,
        ap.growth_trajectory,
        
        -- Context for decision making
        CASE 
            WHEN ap.management_priority_score >= 90 THEN 'URGENT ACTION REQUIRED'
            WHEN ap.management_priority_score >= 80 THEN 'HIGH PRIORITY'
            WHEN ap.management_priority_score >= 70 THEN 'MEDIUM PRIORITY'  
            WHEN ap.management_priority_score >= 60 THEN 'GROWTH OPPORTUNITY'
            ELSE 'MONITOR'
        END AS management_flag,
        
        -- Performance trend indicators  
        CASE 
            WHEN ap.days_since_last_order <= 7 THEN 'Very Active'
            WHEN ap.days_since_last_order <= 30 THEN 'Active'
            WHEN ap.days_since_last_order <= 90 THEN 'Moderate Activity'
            ELSE 'Low Activity'
        END AS activity_status,
        
        -- Metadata
        CURRENT_DATE AS report_date,
        CURRENT_TIMESTAMP AS last_updated,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM action_priorities ap
)

SELECT * FROM final
ORDER BY management_priority_score DESC, total_gmv DESC
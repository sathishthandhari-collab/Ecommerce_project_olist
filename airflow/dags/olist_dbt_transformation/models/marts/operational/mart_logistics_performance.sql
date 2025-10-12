{{ config(
    materialized='table',
    cluster_by=['report_date', 'region'],
    schema='dbt_olist_marts',
    tags=['marts']
) }}

WITH delivery_performance_base AS (
    SELECT 
        DATE_TRUNC('week', c360.last_order_date) AS report_date,
        c360.region,
        c360.customer_state,
        
        -- Order volume metrics
        COUNT(DISTINCT c360.customer_sk) AS customers_with_orders,
        SUM(c360.total_orders) AS total_orders,
        AVG(c360.avg_order_value) AS avg_order_value,
        
        -- Delivery performance metrics
        AVG(c360.on_time_delivery_rate) AS avg_on_time_delivery_rate,
        AVG(c360.avg_delivery_days) AS avg_delivery_days,
        
        -- Customer satisfaction impact
        AVG(c360.avg_review_score) AS avg_customer_satisfaction,
        COUNT(DISTINCT CASE WHEN c360.delivery_experience_quality = 'Poor Experience' THEN c360.customer_sk END) AS poor_experience_customers,
        COUNT(DISTINCT CASE WHEN c360.delivery_experience_quality = 'Excellent Experience' THEN c360.customer_sk END) AS excellent_experience_customers
        
    FROM {{ ref('int_customer_360') }} c360
    WHERE c360.last_order_date >= CURRENT_DATE - INTERVAL '12 weeks'
      AND c360.total_orders > 0
    GROUP BY DATE_TRUNC('week', c360.last_order_date), c360.region, c360.customer_state
),

seller_logistics_performance AS (
    SELECT 
        DATE_TRUNC('week', shs.dbt_loaded_at) AS report_date,
        shs.geographic_region AS region,
        shs.seller_state,
        
        -- Seller delivery metrics
        COUNT(DISTINCT shs.seller_sk) AS active_sellers,
        AVG(shs.on_time_delivery_rate) AS seller_avg_on_time_rate,
        AVG(shs.avg_delivery_days) AS seller_avg_delivery_days,
        AVG(shs.delivery_performance_score) AS avg_delivery_score,
        
        -- Performance distribution
        COUNT(DISTINCT CASE WHEN shs.on_time_delivery_rate >= 0.95 THEN shs.seller_sk END) AS excellent_delivery_sellers,
        COUNT(DISTINCT CASE WHEN shs.on_time_delivery_rate < 0.80 THEN shs.seller_sk END) AS poor_delivery_sellers,
        
        -- Business impact
        SUM(shs.total_gmv) AS total_gmv,
        SUM(CASE WHEN shs.on_time_delivery_rate < 0.80 THEN shs.total_gmv ELSE 0 END) AS poor_delivery_gmv,
        
        -- Logistics advantages
        COUNT(DISTINCT CASE WHEN shs.has_logistics_advantage THEN shs.seller_sk END) AS sellers_with_logistics_advantage
        
    FROM {{ ref('int_seller_health_score') }} shs
    WHERE shs.dbt_loaded_at >= CURRENT_DATE - INTERVAL '12 weeks'
      AND shs.total_orders > 0
    GROUP BY DATE_TRUNC('week', shs.dbt_loaded_at), shs.geographic_region, shs.seller_state
),

combined_logistics_metrics AS (
    SELECT 
        COALESCE(dpb.report_date, slp.report_date) AS report_date,
        COALESCE(dpb.region, slp.region) AS region,
        COALESCE(dpb.customer_state, slp.seller_state) AS state,
        
        -- Customer perspective
        dpb.customers_with_orders,
        dpb.total_orders,
        dpb.avg_order_value,
        dpb.avg_on_time_delivery_rate AS customer_experienced_on_time_rate,
        dpb.avg_delivery_days AS customer_experienced_delivery_days,
        dpb.avg_customer_satisfaction,
        dpb.poor_experience_customers,
        dpb.excellent_experience_customers,
        
        -- Seller capability perspective  
        slp.active_sellers,
        slp.seller_avg_on_time_rate,
        slp.seller_avg_delivery_days,
        slp.avg_delivery_score,
        slp.excellent_delivery_sellers,
        slp.poor_delivery_sellers,
        slp.total_gmv,
        slp.poor_delivery_gmv,
        slp.sellers_with_logistics_advantage,
        
        -- Performance gap analysis
        ABS(dpb.avg_on_time_delivery_rate - slp.seller_avg_on_time_rate) AS delivery_rate_gap,
        ABS(dpb.avg_delivery_days - slp.seller_avg_delivery_days) AS delivery_days_gap
        
    FROM delivery_performance_base dpb
    FULL OUTER JOIN seller_logistics_performance slp 
        ON dpb.report_date = slp.report_date 
        AND dpb.region = slp.region 
        AND dpb.customer_state = slp.seller_state
),

logistics_kpis AS (
    SELECT 
        clm.*,
        
        -- Calculated KPIs
        (clm.poor_experience_customers::FLOAT / NULLIF(clm.customers_with_orders, 0)) * 100 AS poor_experience_rate,
        (clm.excellent_experience_customers::FLOAT / NULLIF(clm.customers_with_orders, 0)) * 100 AS excellent_experience_rate,
        (clm.poor_delivery_sellers::FLOAT / NULLIF(clm.active_sellers, 0)) * 100 AS poor_delivery_seller_rate,
        (clm.poor_delivery_gmv / NULLIF(clm.total_gmv, 0)) * 100 AS poor_delivery_gmv_impact,
        (clm.sellers_with_logistics_advantage::FLOAT / NULLIF(clm.active_sellers, 0)) * 100 AS logistics_advantage_coverage,
        
        -- Regional performance vs national benchmarks
        clm.customer_experienced_on_time_rate - AVG(clm.customer_experienced_on_time_rate) OVER (PARTITION BY clm.report_date) AS vs_national_on_time_rate,
        clm.customer_experienced_delivery_days - AVG(clm.customer_experienced_delivery_days) OVER (PARTITION BY clm.report_date) AS vs_national_delivery_days,
        clm.avg_customer_satisfaction - AVG(clm.avg_customer_satisfaction) OVER (PARTITION BY clm.report_date) AS vs_national_satisfaction,
        
        -- Trend analysis (4-week moving average)
        AVG(clm.customer_experienced_on_time_rate) OVER (
            PARTITION BY clm.region, clm.state 
            ORDER BY clm.report_date 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS on_time_rate_4week_avg,
        
        AVG(clm.customer_experienced_delivery_days) OVER (
            PARTITION BY clm.region, clm.state 
            ORDER BY clm.report_date 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW  
        ) AS delivery_days_4week_avg,
        
        -- Performance classification
        CASE 
            WHEN clm.customer_experienced_on_time_rate >= 0.95 THEN 'Excellent'
            WHEN clm.customer_experienced_on_time_rate >= 0.90 THEN 'Good'
            WHEN clm.customer_experienced_on_time_rate >= 0.85 THEN 'Average'
            WHEN clm.customer_experienced_on_time_rate >= 0.75 THEN 'Below Average'
            ELSE 'Poor'
        END AS delivery_performance_tier,
        
        CASE 
            WHEN clm.avg_customer_satisfaction >= 4.5 THEN 'Excellent'
            WHEN clm.avg_customer_satisfaction >= 4.0 THEN 'Good'
            WHEN clm.avg_customer_satisfaction >= 3.5 THEN 'Average' 
            WHEN clm.avg_customer_satisfaction >= 3.0 THEN 'Below Average'
            ELSE 'Poor'
        END AS satisfaction_tier
        
    FROM combined_logistics_metrics clm
    WHERE clm.report_date IS NOT NULL
),

final AS (
    SELECT 
        -- Time and location dimensions
        lk.report_date,
        EXTRACT(year FROM lk.report_date) AS report_year,
        EXTRACT(week FROM lk.report_date) AS report_week,
        lk.region,
        lk.state,
        
        -- Volume metrics
        lk.customers_with_orders,
        lk.total_orders,
        lk.active_sellers,
        lk.total_gmv,
        
        -- Core delivery performance
        lk.customer_experienced_on_time_rate,
        lk.customer_experienced_delivery_days, 
        lk.seller_avg_on_time_rate,
        lk.avg_delivery_score,
        lk.delivery_performance_tier,
        
        -- Customer impact
        lk.avg_customer_satisfaction,
        lk.satisfaction_tier,
        lk.poor_experience_rate,
        lk.excellent_experience_rate,
        
        -- Seller capability
        lk.poor_delivery_seller_rate,
        lk.poor_delivery_gmv_impact,
        lk.logistics_advantage_coverage,
        
        -- Benchmarking
        lk.vs_national_on_time_rate,
        lk.vs_national_delivery_days,
        lk.vs_national_satisfaction,
        
        -- Trend indicators
        lk.on_time_rate_4week_avg,
        lk.delivery_days_4week_avg,
        
        -- Performance gaps
        lk.delivery_rate_gap,
        lk.delivery_days_gap,
        
        -- Strategic recommendations
        CASE 
            WHEN lk.poor_delivery_gmv_impact > 20 THEN 'URGENT: High-revenue sellers underperforming'
            WHEN lk.poor_experience_rate > 15 THEN 'HIGH PRIORITY: Customer experience at risk'
            WHEN lk.vs_national_on_time_rate < -0.05 THEN 'PRIORITY: Region underperforming vs national average'
            WHEN lk.logistics_advantage_coverage < 30 THEN 'OPPORTUNITY: Expand logistics partnerships'
            ELSE 'STABLE: Monitor performance'
        END AS strategic_recommendation,
        
        -- Operational alerts
        CASE 
            WHEN lk.delivery_performance_tier = 'Poor' THEN TRUE
            WHEN lk.poor_experience_rate > 20 THEN TRUE
            WHEN lk.vs_national_on_time_rate < -0.10 THEN TRUE
            ELSE FALSE
        END AS requires_immediate_attention,
        
        -- Investment priorities
        CASE 
            WHEN lk.total_gmv > 100000 AND lk.delivery_performance_tier IN ('Poor', 'Below Average') THEN 'High ROI Infrastructure Investment'
            WHEN lk.logistics_advantage_coverage < 50 AND lk.active_sellers > 50 THEN 'Logistics Partnership Expansion' 
            WHEN lk.poor_delivery_seller_rate > 25 THEN 'Seller Training & Support'
            ELSE 'Monitor & Maintain'
        END AS investment_priority,
        
        -- Metadata
        CURRENT_TIMESTAMP AS last_updated,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM logistics_kpis lk
)

SELECT * FROM final
ORDER BY report_date DESC, region, state
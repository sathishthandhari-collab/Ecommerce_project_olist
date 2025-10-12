{{ config(
    materialized='table',
    cluster_by=['report_date', 'clv_segment'],
    contract={
        "enforced": true
    },
    schema='dbt_olist_marts',
    tags=['marts']
) }}

WITH current_snapshot AS (
    -- Latest customer state for strategic analysis
    SELECT
        c360.customer_sk,
        c360.customer_id,
        c360.customer_state,
        c360.region,
        c360.lifecycle_stage,
        c360.total_orders,
        c360.total_spent,
        c360.avg_order_value,
        c360.churn_probability,
        c360.engagement_level,
        c360.customer_sophistication,
        c360.days_since_last_order,
        clv.clv_segment,
        clv.predicted_clv,
        clv.investment_priority,
        clv.growth_potential,
        clv.prediction_confidence,
        ROW_NUMBER() OVER (PARTITION BY c360.customer_sk ORDER BY c360.dbt_loaded_at DESC) as rn
    FROM {{ ref('int_customer_360') }} c360
    LEFT JOIN {{ ref('int_customer_lifetime_value') }} clv ON c360.customer_sk = clv.customer_sk
),

latest_customer_state AS (
    SELECT * FROM current_snapshot WHERE rn = 1
),

-- Strategic customer segments analysis
segment_performance AS (
    SELECT
        CURRENT_DATE AS report_date,
        lcs.clv_segment,
        lcs.lifecycle_stage,
        lcs.investment_priority,
        lcs.region,

        -- Volume metrics
        COUNT(DISTINCT lcs.customer_sk) AS customer_count,
        COUNT(DISTINCT CASE WHEN lcs.days_since_last_order <= 30 THEN lcs.customer_sk END) AS active_customers,

        -- Financial metrics
        SUM(lcs.total_spent) AS total_historical_revenue,
        SUM(lcs.predicted_clv) AS total_predicted_clv,
        SUM(lcs.predicted_clv - lcs.total_spent) AS total_future_value_potential,
        AVG(lcs.predicted_clv) AS avg_predicted_clv,
        AVG(lcs.total_spent) AS avg_historical_spend,
        AVG(lcs.avg_order_value) AS avg_order_value,

        -- Behavioral metrics
        AVG(lcs.total_orders) AS avg_orders_per_customer,
        AVG(lcs.churn_probability) AS avg_churn_probability,
        AVG(lcs.days_since_last_order) AS avg_days_since_last_order,

        -- Risk metrics
        COUNT(DISTINCT CASE WHEN lcs.churn_probability > 0.7 THEN lcs.customer_sk END) AS high_churn_risk_customers,
        COUNT(DISTINCT CASE WHEN lcs.days_since_last_order > 180 THEN lcs.customer_sk END) AS dormant_customers,

        -- Engagement analysis
        COUNT(DISTINCT CASE WHEN lcs.engagement_level = 'High Engagement' THEN lcs.customer_sk END) AS highly_engaged_customers,
        COUNT(DISTINCT CASE WHEN lcs.customer_sophistication = 'Sophisticated' THEN lcs.customer_sk END) AS sophisticated_customers,

        -- Confidence metrics
        COUNT(DISTINCT CASE WHEN lcs.prediction_confidence = 'High Confidence' THEN lcs.customer_sk END) AS high_confidence_predictions

    FROM latest_customer_state lcs
    WHERE lcs.clv_segment IS NOT NULL
    GROUP BY lcs.clv_segment, lcs.lifecycle_stage, lcs.investment_priority, lcs.region
),

-- Strategic recommendations per segment
segment_recommendations AS (
    SELECT
        sp.*,

        -- Segment health indicators
        (sp.active_customers::FLOAT / NULLIF(sp.customer_count, 0)) * 100 AS active_customer_rate,
        (sp.high_churn_risk_customers::FLOAT / NULLIF(sp.customer_count, 0)) * 100 AS churn_risk_rate,
        (sp.highly_engaged_customers::FLOAT / NULLIF(sp.customer_count, 0)) * 100 AS engagement_rate,
        (sp.high_confidence_predictions::FLOAT / NULLIF(sp.customer_count, 0)) * 100 AS prediction_confidence_rate,

        -- ROI potential analysis
        sp.total_future_value_potential / NULLIF(sp.total_historical_revenue, 0) AS roi_potential_multiplier,

        -- Strategic priorities
        CASE
            WHEN sp.clv_segment IN ('Very High Value', 'High Value') AND sp.avg_churn_probability < 0.3
                THEN 'Retention - VIP Treatment'
            WHEN sp.clv_segment IN ('Very High Value', 'High Value') AND sp.avg_churn_probability >= 0.3
                THEN 'Urgent Intervention - Save High Value'
            WHEN sp.clv_segment = 'Medium Value' AND sp.avg_churn_probability < 0.5
                THEN 'Growth - Increase Engagement'
            WHEN sp.clv_segment IN ('Low Value', 'Very Low Value') AND sp.avg_churn_probability > 0.7
                THEN 'Reactivation Campaign - Last Chance'
            ELSE 'Monitor - Standard Treatment'
        END AS strategic_recommendation,

        -- Investment allocation scoring (0-100)
        CASE
            WHEN sp.clv_segment = 'Very High Value' THEN 100
            WHEN sp.clv_segment = 'High Value' THEN 85
            WHEN sp.clv_segment = 'Medium Value' THEN 65
            WHEN sp.clv_segment = 'Low Value' THEN 40
            ELSE 20
        END AS investment_priority_score,

        -- Campaign readiness flags
        CASE WHEN (sp.high_churn_risk_customers::FLOAT / NULLIF(sp.customer_count, 0)) * 100 > 60 THEN TRUE ELSE FALSE END AS ready_for_engagement_campaigns,
        CASE WHEN (sp.high_churn_risk_customers::FLOAT / NULLIF(sp.customer_count, 0)) * 100 > 40 THEN TRUE ELSE FALSE END AS needs_retention_campaigns,
        CASE WHEN (sp.high_confidence_predictions::FLOAT / NULLIF(sp.customer_count, 0)) * 100 > 70 THEN TRUE ELSE FALSE END AS high_prediction_confidence

    FROM segment_performance sp
),

-- Get dominant segment per region using window function approach
regional_dominant_segment AS (
    SELECT 
        sr.report_date,
        sr.region,
        sr.clv_segment,
        sr.customer_count,
        ROW_NUMBER() OVER (
            PARTITION BY sr.report_date, sr.region 
            ORDER BY sr.customer_count DESC
        ) as segment_rank
    FROM segment_recommendations sr
),

-- Geographic strategy overlay
geographic_strategy AS (
    SELECT
        sr.report_date,
        sr.region,

        -- Regional aggregations
        SUM(sr.customer_count) AS regional_customer_count,
        SUM(sr.total_predicted_clv) AS regional_predicted_clv,
        SUM(sr.total_future_value_potential) AS regional_growth_potential,
        AVG(sr.investment_priority_score) AS avg_investment_priority_score,

        -- Regional strategy
        CASE
            WHEN AVG(sr.investment_priority_score) > 80 THEN 'Premium Market - High Investment'
            WHEN AVG(sr.investment_priority_score) > 60 THEN 'Growth Market - Scaled Investment'
            WHEN AVG(sr.investment_priority_score) > 40 THEN 'Maintenance Market - Efficient Operations'
            ELSE 'Harvest Market - Cost Optimization'
        END AS regional_strategy

    FROM segment_recommendations sr
    GROUP BY sr.report_date, sr.region
),

final AS (
    SELECT
        -- Time and geographic dimensions
        sr.report_date,
        sr.region,
        gs.regional_strategy,
        rds.clv_segment AS regional_dominant_segment,

        -- Segment identification
        sr.clv_segment,
        sr.lifecycle_stage,
        sr.investment_priority,

        -- Volume metrics
        sr.customer_count,
        sr.active_customers,
        sr.active_customer_rate,

        -- Financial performance
        sr.total_historical_revenue,
        sr.total_predicted_clv,
        sr.total_future_value_potential,
        sr.avg_predicted_clv,
        sr.avg_historical_spend,
        sr.roi_potential_multiplier,

        -- Customer behavior insights
        sr.avg_churn_probability,
        sr.churn_risk_rate,
        sr.engagement_rate,
        sr.avg_days_since_last_order,

        -- Strategic guidance
        sr.strategic_recommendation,
        sr.investment_priority_score,

        -- Campaign readiness
        sr.ready_for_engagement_campaigns,
        sr.needs_retention_campaigns,
        sr.high_prediction_confidence,

        -- Segment ranking within region
        RANK() OVER (PARTITION BY sr.region ORDER BY sr.total_predicted_clv DESC) AS clv_rank_in_region,
        RANK() OVER (PARTITION BY sr.region ORDER BY sr.customer_count DESC) AS volume_rank_in_region,

        -- Executive summary flags
        CASE
            WHEN sr.clv_segment IN ('Very High Value', 'High Value') AND sr.churn_risk_rate > 50
                THEN 'CRITICAL: High-value churn risk'
            WHEN sr.clv_segment = 'Medium Value' AND sr.engagement_rate < 30
                THEN 'OPPORTUNITY: Underengaged growth segment'
            WHEN sr.roi_potential_multiplier > 3.0
                THEN 'OPPORTUNITY: High ROI potential'
            ELSE 'STABLE'
        END AS executive_alert,

        -- Metadata
        CURRENT_TIMESTAMP::timestamp_tz AS last_updated,
        '{{ invocation_id }}' AS dbt_invocation_id

    FROM segment_recommendations sr
    LEFT JOIN geographic_strategy gs 
        ON sr.region = gs.region AND sr.report_date = gs.report_date
    LEFT JOIN regional_dominant_segment rds 
        ON sr.region = rds.region AND sr.report_date = rds.report_date 
        AND rds.segment_rank = 1
)

SELECT * FROM final
ORDER BY clv_rank_in_region, region, clv_segment

{{ config(
    materialized='table',
    cluster_by=['report_date', 'metric_category'],
    contract={
        "enforced": true
    },
    schema='dbt_olist_marts',
    tags=['marts']
) }}

WITH time_spine AS (
    {{ generate_executive_time_spine('2016-01-01', 'CURRENT_DATE', 'month') }}
),

-- Revenue and Financial Metrics
revenue_metrics AS (
    SELECT 
        DATE_TRUNC('month', c360.last_order_date)::date AS report_date,
        'Financial Performance' AS metric_category,
        'Total Revenue' AS metric_name,
        SUM(c360.total_spent) AS metric_value,
        COUNT(DISTINCT c360.customer_sk) AS contributing_entities,
        AVG(c360.avg_order_value) AS avg_entity_value,
        STDDEV(c360.total_spent) AS metric_volatility
    FROM {{ ref('int_customer_360') }} c360
    WHERE c360.last_order_date >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', c360.last_order_date)::date
    
    UNION ALL
    
    SELECT 
        DATE_TRUNC('month', c360.last_order_date)::date AS report_date,
        'Financial Performance' AS metric_category,
        'Average Order Value' AS metric_name,
        AVG(c360.avg_order_value) AS metric_value,
        COUNT(DISTINCT c360.customer_sk) AS contributing_entities,
        AVG(c360.avg_order_value) AS avg_entity_value,
        STDDEV(c360.avg_order_value) AS metric_volatility
    FROM {{ ref('int_customer_360') }} c360
    WHERE c360.last_order_date >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', c360.last_order_date)::date
),

-- Customer Lifecycle Metrics
customer_metrics AS (
    SELECT 
        DATE_TRUNC('month', c360.last_order_date)::date AS report_date,
        'Customer Performance' AS metric_category,
        'Active Customers' AS metric_name,
        COUNT(DISTINCT CASE WHEN c360.days_since_last_order <= 30 THEN c360.customer_sk END) AS metric_value,
        COUNT(DISTINCT c360.customer_sk) AS contributing_entities,
        AVG(c360.total_orders) AS avg_entity_value,
        STDDEV(c360.total_orders) AS metric_volatility
    FROM {{ ref('int_customer_360') }} c360
    WHERE c360.last_order_date >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', c360.last_order_date)::date
    
    UNION ALL
    
    SELECT 
        DATE_TRUNC('month', clv.dbt_loaded_at)::date AS report_date,
        'Customer Performance' AS metric_category,
        'Predicted CLV (High Confidence)' AS metric_name,
        SUM(CASE WHEN clv.prediction_confidence = 'High Confidence' THEN clv.predicted_clv ELSE 0 END) AS metric_value,
        COUNT(DISTINCT CASE WHEN clv.prediction_confidence = 'High Confidence' THEN clv.customer_sk END) AS contributing_entities,
        AVG(CASE WHEN clv.prediction_confidence = 'High Confidence' THEN clv.predicted_clv END) AS avg_entity_value,
        STDDEV(CASE WHEN clv.prediction_confidence = 'High Confidence' THEN clv.predicted_clv END) AS metric_volatility
    FROM {{ ref('int_customer_lifetime_value') }} clv
    WHERE clv.dbt_loaded_at >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', clv.dbt_loaded_at)::date
    
    UNION ALL
    
    -- Churn Risk Analysis
    SELECT 
        DATE_TRUNC('month', c360.dbt_loaded_at)::date AS report_date,
        'Customer Performance' AS metric_category,
        'High Churn Risk Customers' AS metric_name,
        COUNT(DISTINCT CASE WHEN c360.churn_probability > {{ var('churn_risk_threshold', 0.7) }} THEN c360.customer_sk END) AS metric_value,
        COUNT(DISTINCT c360.customer_sk) AS contributing_entities,
        AVG(c360.churn_probability) AS avg_entity_value,
        STDDEV(c360.churn_probability) AS metric_volatility
    FROM {{ ref('int_customer_360') }} c360
    WHERE c360.dbt_loaded_at >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', c360.dbt_loaded_at)::date
),

-- Operational Excellence Metrics
operational_metrics AS (
    SELECT 
        DATE_TRUNC('month', shs.dbt_loaded_at)::date AS report_date,
        'Operational Excellence' AS metric_category,
        'Seller Health Score (Average)' AS metric_name,
        AVG(shs.composite_health_score) AS metric_value,
        COUNT(DISTINCT shs.seller_sk) AS contributing_entities,
        AVG(shs.on_time_delivery_rate) AS avg_entity_value,
        STDDEV(shs.composite_health_score) AS metric_volatility
    FROM {{ ref('int_seller_health_score') }} shs
    WHERE shs.dbt_loaded_at >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', shs.dbt_loaded_at)::date
    
    UNION ALL
    
    -- Quality Metrics
    SELECT 
        DATE_TRUNC('month', shs.dbt_loaded_at)::date AS report_date,
        'Operational Excellence' AS metric_category,
        'Poor Health Sellers' AS metric_name,
        COUNT(DISTINCT CASE WHEN shs.composite_health_score < {{ var('health_score_alert_threshold', 35) }} THEN shs.seller_sk END) AS metric_value,
        COUNT(DISTINCT shs.seller_sk) AS contributing_entities,
        AVG(shs.avg_review_score) AS avg_entity_value,
        STDDEV(shs.avg_review_score) AS metric_volatility
    FROM {{ ref('int_seller_health_score') }} shs
    WHERE shs.dbt_loaded_at >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', shs.dbt_loaded_at)::date
),

-- Risk and Security Metrics
risk_metrics AS (
    SELECT 
        DATE_TRUNC('month', oa.anomaly_date)::date AS report_date,
        'Risk Management' AS metric_category,
        'Critical Anomalies Detected' AS metric_name,
        COUNT(DISTINCT CASE WHEN oa.anomaly_severity = 'Critical' THEN oa.order_sk END) AS metric_value,
        COUNT(DISTINCT oa.order_sk) AS contributing_entities,
        AVG(oa.composite_anomaly_score) AS avg_entity_value,
        STDDEV(oa.composite_anomaly_score) AS metric_volatility
    FROM {{ ref('int_order_anomalies') }} oa
    WHERE oa.anomaly_date >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', oa.anomaly_date)::date
    
    UNION ALL
    
    SELECT 
        DATE_TRUNC('month', oa.anomaly_date)::date AS report_date,
        'Risk Management' AS metric_category,
        'Average Anomaly Score' AS metric_name,
        AVG(oa.composite_anomaly_score) AS metric_value,
        COUNT(DISTINCT oa.order_sk) AS contributing_entities,
        SUM(oa.total_order_value) AS avg_entity_value,
        STDDEV(oa.total_order_value) AS metric_volatility
    FROM {{ ref('int_order_anomalies') }} oa
    WHERE oa.anomaly_date >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', oa.anomaly_date)::date
),

-- Combine all metrics
all_metrics AS (
    SELECT * FROM revenue_metrics
    UNION ALL
    SELECT * FROM customer_metrics  
    UNION ALL
    SELECT * FROM operational_metrics
    UNION ALL
    SELECT * FROM risk_metrics
),

-- Add time series analysis
metrics_with_trends AS (
    SELECT 
        am.*,
        -- Period-over-period analysis
        {{ calculate_period_over_period('metric_value', ['metric_category', 'metric_name']) }},
        
        -- Moving averages for trend analysis
        AVG(am.metric_value) OVER (
            PARTITION BY am.metric_category, am.metric_name 
            ORDER BY am.report_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS metric_3month_avg,
        
        AVG(am.metric_value) OVER (
            PARTITION BY am.metric_category, am.metric_name 
            ORDER BY am.report_date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS metric_12month_avg,
        
        -- Statistical confidence intervals
        am.metric_value - (1.96 * am.metric_volatility / SQRT(GREATEST(am.contributing_entities, 1))) AS metric_lower_ci,
        am.metric_value + (1.96 * am.metric_volatility / SQRT(GREATEST(am.contributing_entities, 1))) AS metric_upper_ci,
        
        -- Business context flags
        CASE 
            WHEN am.metric_category = 'Financial Performance' AND am.metric_value < LAG(am.metric_value, 1) OVER (PARTITION BY am.metric_category, am.metric_name ORDER BY am.report_date) 
            THEN 'Revenue Decline Alert'
            WHEN am.metric_category = 'Risk Management' AND am.metric_value > LAG(am.metric_value, 1) OVER (PARTITION BY am.metric_category, am.metric_name ORDER BY am.report_date) * 1.2
            THEN 'Risk Spike Alert' 
            WHEN am.metric_category = 'Customer Performance' AND am.metric_name = 'High Churn Risk Customers' AND am.metric_value > 100
            THEN 'Churn Risk Alert'
            ELSE 'Normal'
        END AS alert_status
        
    FROM all_metrics am
    WHERE am.report_date IS NOT NULL
),

final AS (
    SELECT 
        -- Time dimensions
        mwt.report_date,
        EXTRACT(year FROM mwt.report_date) AS report_year,
        EXTRACT(month FROM mwt.report_date) AS report_month,
        EXTRACT(quarter FROM mwt.report_date) AS report_quarter,
        
        -- Metric identification
        mwt.metric_category,
        mwt.metric_name,
        
        -- Core metrics
        mwt.metric_value,
        mwt.contributing_entities,
        mwt.avg_entity_value,
        mwt.metric_volatility,
        
        -- Trend analysis
        mwt.metric_value_change,
        mwt.metric_value_change_pct,
        mwt.metric_3month_avg,
        mwt.metric_12month_avg,
        
        -- Statistical confidence
        mwt.metric_lower_ci,
        mwt.metric_upper_ci,
        
        -- Executive alerts
        mwt.alert_status,
        
        -- Performance indicators
        CASE 
            WHEN mwt.metric_value_change_pct > 10 THEN 'Strong Growth'
            WHEN mwt.metric_value_change_pct > 5 THEN 'Growth'  
            WHEN mwt.metric_value_change_pct > -5 THEN 'Stable'
            WHEN mwt.metric_value_change_pct > -15 THEN 'Declining'
            ELSE 'Critical Decline'
        END AS performance_indicator,
        
        -- Data quality metrics
        CASE 
            WHEN mwt.contributing_entities >= 100 THEN 'High'
            WHEN mwt.contributing_entities >= 30 THEN 'Medium'
            ELSE 'Low'
        END AS data_confidence_level,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_tz AS last_updated,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM metrics_with_trends mwt
    WHERE mwt.report_date >= CURRENT_DATE - INTERVAL '{{ var("executive_report_lookback_days", 90) }} days'
      OR mwt.report_date = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')  -- Always include last complete month
)

SELECT * FROM final
ORDER BY report_date DESC, metric_category, metric_name
{{ config(
    -- PERFORMANCE OPTIMIZATION: Executive dashboard materialized as table for fast query performance
    materialized='table',
    cluster_by=['report_date', 'metric_category'],     -- Optimize for time series and category filtering
    contract={
        "enforced": true                               -- Schema contract enforcement for data governance
    },
    schema='dbt_olist_mart_prod'                      -- Production mart schema for executive consumption
) }}

WITH time_spine AS (
    -- TIME DIMENSION FOUNDATION: Creates complete date range for consistent executive reporting
    {{ generate_executive_time_spine('2016-01-01', 'CURRENT_DATE', 'month') }}
    -- MACRO FUNCTIONALITY: Generates monthly date spine for consistent time series analysis
),

-- FINANCIAL PERFORMANCE INTELLIGENCE: Core revenue and growth metrics for C-suite visibility
revenue_metrics AS (
    SELECT
        -- TIME AGGREGATION: Monthly rollups for executive-level reporting granularity
        DATE_TRUNC('month', c360.last_order_date)::date AS report_date,
        
        -- METRIC CATEGORIZATION: Organized for executive dashboard structure
        'Financial Performance' AS metric_category,
        'Total Revenue' AS metric_name,
        
        -- CORE BUSINESS METRICS: Primary financial indicators
        SUM(c360.total_spent) AS metric_value,                    -- Total customer lifetime revenue
        COUNT(DISTINCT c360.customer_sk) AS contributing_entities, -- Number of customers contributing
        AVG(c360.avg_order_value) AS avg_entity_value,            -- Average revenue per customer
        STDDEV(c360.total_spent) AS metric_volatility             -- Revenue distribution variability

    FROM {{ ref('int_customer_360') }} c360
    WHERE c360.last_order_date >= '2016-01-01'                   -- Historical data boundary
    GROUP BY DATE_TRUNC('month', c360.last_order_date)::date

    UNION ALL

    -- AVERAGE ORDER VALUE TRACKING: Executive view of transaction size trends
    SELECT
        DATE_TRUNC('month', c360.last_order_date)::date AS report_date,
        'Financial Performance' AS metric_category,
        'Average Order Value' AS metric_name,
        
        AVG(c360.avg_order_value) AS metric_value,               -- Market-wide AOV trend
        COUNT(DISTINCT c360.customer_sk) AS contributing_entities,
        AVG(c360.avg_order_value) AS avg_entity_value,
        STDDEV(c360.avg_order_value) AS metric_volatility

    FROM {{ ref('int_customer_360') }} c360
    WHERE c360.last_order_date >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', c360.last_order_date)::date
),

-- CUSTOMER PERFORMANCE INTELLIGENCE: Strategic customer metrics for growth planning
customer_metrics AS (
    SELECT
        DATE_TRUNC('month', c360.last_order_date)::date AS report_date,
        'Customer Performance' AS metric_category,
        'Active Customers' AS metric_name,
        
        -- ACTIVE CUSTOMER DEFINITION: Recently engaged customers (≤30 days)
        COUNT(DISTINCT CASE WHEN c360.days_since_last_order <= 30 THEN c360.customer_sk END) AS metric_value,
        COUNT(DISTINCT c360.customer_sk) AS contributing_entities,
        AVG(c360.total_orders) AS avg_entity_value,
        STDDEV(c360.total_orders) AS metric_volatility

    FROM {{ ref('int_customer_360') }} c360
    WHERE c360.last_order_date >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', c360.last_order_date)::date

    UNION ALL

    -- CUSTOMER LIFETIME VALUE PREDICTION: High-confidence CLV for strategic planning
    SELECT
        DATE_TRUNC('month', clv.dbt_loaded_at)::date AS report_date,
        'Customer Performance' AS metric_category,
        'Predicted CLV (High Confidence)' AS metric_name,
        
        -- HIGH-CONFIDENCE CLV FOCUS: Only include statistically reliable predictions
        SUM(CASE WHEN clv.prediction_confidence = 'High Confidence' THEN clv.predicted_clv ELSE 0 END) AS metric_value,
        COUNT(DISTINCT CASE WHEN clv.prediction_confidence = 'High Confidence' THEN clv.customer_sk END) AS contributing_entities,
        AVG(CASE WHEN clv.prediction_confidence = 'High Confidence' THEN clv.predicted_clv END) AS avg_entity_value,
        STDDEV(CASE WHEN clv.prediction_confidence = 'High Confidence' THEN clv.predicted_clv END) AS metric_volatility

    FROM {{ ref('int_customer_lifetime_value') }} clv
    WHERE clv.dbt_loaded_at >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', clv.dbt_loaded_at)::date

    UNION ALL

    -- CHURN RISK MONITORING: Early warning system for customer retention
    SELECT
        DATE_TRUNC('month', c360.dbt_loaded_at)::date AS report_date,
        'Customer Performance' AS metric_category,
        'High Churn Risk Customers' AS metric_name,
        
        -- CONFIGURABLE RISK THRESHOLD: Executive-defined churn risk boundary
        COUNT(DISTINCT CASE WHEN c360.churn_probability > {{ var('churn_risk_threshold', 0.7) }} THEN c360.customer_sk END) AS metric_value,
        COUNT(DISTINCT c360.customer_sk) AS contributing_entities,
        AVG(c360.churn_probability) AS avg_entity_value,
        STDDEV(c360.churn_probability) AS metric_volatility

    FROM {{ ref('int_customer_360') }} c360
    WHERE c360.dbt_loaded_at >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', c360.dbt_loaded_at)::date
),

-- OPERATIONAL EXCELLENCE MONITORING: Platform quality and seller performance metrics
operational_metrics AS (
    SELECT
        DATE_TRUNC('month', shs.dbt_loaded_at)::date AS report_date,
        'Operational Excellence' AS metric_category,
        'Seller Health Score (Average)' AS metric_name,
        
        -- COMPOSITE HEALTH ASSESSMENT: Overall seller ecosystem quality
        AVG(shs.composite_health_score) AS metric_value,          -- Platform-wide seller quality
        COUNT(DISTINCT shs.seller_sk) AS contributing_entities,   -- Active seller count
        AVG(shs.on_time_delivery_rate) AS avg_entity_value,       -- Service quality indicator
        STDDEV(shs.composite_health_score) AS metric_volatility   -- Quality consistency measure

    FROM {{ ref('int_seller_health_score') }} shs
    WHERE shs.dbt_loaded_at >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', shs.dbt_loaded_at)::date

    UNION ALL

    -- POOR PERFORMANCE MONITORING: Early warning for seller quality issues
    SELECT
        DATE_TRUNC('month', shs.dbt_loaded_at)::date AS report_date,
        'Operational Excellence' AS metric_category,
        'Poor Health Sellers' AS metric_name,
        
        -- CONFIGURABLE QUALITY THRESHOLD: Executive-defined minimum seller standards
        COUNT(DISTINCT CASE WHEN shs.composite_health_score < {{ var('health_score_alert_threshold', 35) }} THEN shs.seller_sk END) AS metric_value,
        COUNT(DISTINCT shs.seller_sk) AS contributing_entities,
        AVG(shs.avg_review_score) AS avg_entity_value,
        STDDEV(shs.avg_review_score) AS metric_volatility

    FROM {{ ref('int_seller_health_score') }} shs
    WHERE shs.dbt_loaded_at >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', shs.dbt_loaded_at)::date
),

-- RISK MANAGEMENT INTELLIGENCE: Security and anomaly detection metrics for executive oversight
risk_metrics AS (
    SELECT
        DATE_TRUNC('month', oa.anomaly_date)::date AS report_date,
        'Risk Management' AS metric_category,
        'Critical Anomalies Detected' AS metric_name,
        
        -- CRITICAL THREAT IDENTIFICATION: Highest priority security alerts
        COUNT(DISTINCT CASE WHEN oa.anomaly_severity = 'Critical' THEN oa.order_sk END) AS metric_value,
        COUNT(DISTINCT oa.order_sk) AS contributing_entities,     -- Total anomalous transactions
        AVG(oa.composite_anomaly_score) AS avg_entity_value,      -- Average threat intensity
        STDDEV(oa.composite_anomaly_score) AS metric_volatility   -- Threat pattern consistency

    FROM {{ ref('int_order_anomalies') }} oa
    WHERE oa.anomaly_date >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', oa.anomaly_date)::date

    UNION ALL

    -- OVERALL RISK EXPOSURE: General security posture monitoring
    SELECT
        DATE_TRUNC('month', oa.anomaly_date)::date AS report_date,
        'Risk Management' AS metric_category,
        'Average Anomaly Score' AS metric_name,
        
        -- PLATFORM RISK TEMPERATURE: Overall security threat level
        AVG(oa.composite_anomaly_score) AS metric_value,          -- Mean threat intensity
        COUNT(DISTINCT oa.order_sk) AS contributing_entities,     -- Anomaly transaction volume
        SUM(oa.total_order_value) AS avg_entity_value,            -- Financial exposure
        STDDEV(oa.total_order_value) AS metric_volatility         -- Financial risk distribution

    FROM {{ ref('int_order_anomalies') }} oa
    WHERE oa.anomaly_date >= '2016-01-01'
    GROUP BY DATE_TRUNC('month', oa.anomaly_date)::date
),

-- METRIC CONSOLIDATION: Union all executive KPI categories into unified structure
all_metrics AS (
    SELECT * FROM revenue_metrics
    UNION ALL
    SELECT * FROM customer_metrics
    UNION ALL
    SELECT * FROM operational_metrics
    UNION ALL
    SELECT * FROM risk_metrics
),

-- EXECUTIVE ANALYTICS LAYER: Advanced time series analysis and trend identification
metrics_with_trends AS (
    SELECT
        am.*,
        
        -- PERIOD-OVER-PERIOD ANALYSIS: Month-over-month change detection
        {{ calculate_period_over_period('metric_value', ['metric_category', 'metric_name']) }},
        
        -- MOVING AVERAGE TREND ANALYSIS: Smoothed trend identification for strategic planning
        AVG(am.metric_value) OVER (
            PARTITION BY am.metric_category, am.metric_name
            ORDER BY am.report_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW             -- 3-month moving average
        ) AS metric_3month_avg,

        AVG(am.metric_value) OVER (
            PARTITION BY am.metric_category, am.metric_name
            ORDER BY am.report_date
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW            -- 12-month moving average
        ) AS metric_12month_avg,

        -- STATISTICAL CONFIDENCE INTERVALS: Executive decision confidence bounds
        am.metric_value - (1.96 * am.metric_volatility / SQRT(GREATEST(am.contributing_entities, 1))) AS metric_lower_ci,
        am.metric_value + (1.96 * am.metric_volatility / SQRT(GREATEST(am.contributing_entities, 1))) AS metric_upper_ci,

        -- EXECUTIVE ALERT SYSTEM: Automated C-suite notification triggers
        CASE
            -- REVENUE PERFORMANCE ALERTS: Financial decline detection
            WHEN am.metric_category = 'Financial Performance' 
            AND am.metric_value < LAG(am.metric_value, 1) OVER (PARTITION BY am.metric_category, am.metric_name ORDER BY am.report_date)
            THEN 'Revenue Decline Alert'
            
            -- SECURITY THREAT ALERTS: Risk escalation detection
            WHEN am.metric_category = 'Risk Management' 
            AND am.metric_value > LAG(am.metric_value, 1) OVER (PARTITION BY am.metric_category, am.metric_name ORDER BY am.report_date) * 1.2
            THEN 'Risk Spike Alert'
            
            -- CUSTOMER RETENTION ALERTS: Churn risk escalation
            WHEN am.metric_category = 'Customer Performance' 
            AND am.metric_name = 'High Churn Risk Customers' 
            AND am.metric_value > 100
            THEN 'Churn Risk Alert'
            
            ELSE 'Normal'                                         -- Baseline operational state
        END AS alert_status

    FROM all_metrics am
    WHERE am.report_date IS NOT NULL                              -- Data quality filter
),

final AS (
    -- FINAL EXECUTIVE DASHBOARD STRUCTURE: Complete KPI intelligence for C-suite consumption
    SELECT
        -- TEMPORAL DIMENSIONS: Multi-granularity time analysis for executive flexibility
        mwt.report_date,
        EXTRACT(year FROM mwt.report_date) AS report_year,
        EXTRACT(month FROM mwt.report_date) AS report_month,
        EXTRACT(quarter FROM mwt.report_date) AS report_quarter,

        -- BUSINESS METRIC CLASSIFICATION: Organized structure for executive dashboards
        mwt.metric_category,                                      -- Primary grouping (Financial, Customer, Operational, Risk)
        mwt.metric_name,                                          -- Specific KPI identifier

        -- CORE PERFORMANCE INDICATORS: Primary executive metrics
        mwt.metric_value,                                         -- Current period value
        mwt.contributing_entities,                                -- Sample size/confidence indicator
        mwt.avg_entity_value,                                     -- Per-entity performance
        mwt.metric_volatility,                                    -- Stability/predictability measure

        -- EXECUTIVE TREND ANALYSIS: Strategic planning intelligence
        mwt.metric_value_change,                                  -- Absolute period-over-period change
        mwt.metric_value_change_pct,                              -- Percentage period-over-period change
        mwt.metric_3month_avg,                                    -- Short-term trend smoothing
        mwt.metric_12month_avg,                                   -- Long-term trend analysis

        -- STATISTICAL DECISION SUPPORT: Confidence bounds for executive decisions
        mwt.metric_lower_ci,                                      -- 95% confidence interval lower bound
        mwt.metric_upper_ci,                                      -- 95% confidence interval upper bound

        -- EXECUTIVE ALERT SYSTEM: Automated C-suite notifications
        mwt.alert_status,                                         -- Alert classification

        -- STRATEGIC PERFORMANCE INDICATORS: Business health assessment for executive review
        CASE
            WHEN mwt.metric_value_change_pct > 10 THEN 'Strong Growth'      -- Exceptional performance
            WHEN mwt.metric_value_change_pct > 5 THEN 'Growth'              -- Positive momentum
            WHEN mwt.metric_value_change_pct > -5 THEN 'Stable'             -- Steady state
            WHEN mwt.metric_value_change_pct > -15 THEN 'Declining'         -- Performance concern
            ELSE 'Critical Decline'                                         -- Executive intervention required
        END AS performance_indicator,

        -- DATA CONFIDENCE ASSESSMENT: Statistical reliability for executive decisions
        CASE
            WHEN mwt.contributing_entities >= 100 THEN 'High'               -- Highly reliable for decision making
            WHEN mwt.contributing_entities >= 30 THEN 'Medium'              -- Moderately reliable
            ELSE 'Low'                                                      -- Use with caution
        END AS data_confidence_level,

        -- AUDIT AND GOVERNANCE: Executive reporting metadata
        CURRENT_TIMESTAMP AS last_updated,                        -- Data freshness indicator
        '{{ invocation_id }}' AS dbt_invocation_id                -- Data lineage tracking

    FROM metrics_with_trends mwt
    WHERE 
        -- EXECUTIVE TIME HORIZON: Configurable lookback period for strategic focus
        mwt.report_date >= CURRENT_DATE - INTERVAL '{{ var("executive_report_lookback_days", 90) }} days'
        -- HISTORICAL CONTEXT: Always include last complete month for trend analysis
        OR mwt.report_date = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
)

-- FINAL OUTPUT: Complete executive KPI intelligence optimized for C-suite decision making
SELECT * FROM final
ORDER BY report_date DESC, metric_category, metric_name
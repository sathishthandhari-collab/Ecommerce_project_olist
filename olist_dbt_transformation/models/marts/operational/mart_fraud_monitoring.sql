{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key='alert_id',
    incremental_strategy='append',
    cluster_by=['alert_date', 'anomaly_severity'],
    schema='dbt_olist_marts',
    tags=['marts']
) }}

WITH active_anomalies AS (
    SELECT 
        oa.order_sk,
        oa.order_id,
        oa.customer_id,
        oa.anomaly_date,
        oa.order_purchase_timestamp,
        oa.total_order_value,
        oa.composite_anomaly_score,
        oa.anomaly_severity,
        oa.anomaly_type,
        oa.business_impact,
        oa.recommended_action,
        oa.is_value_anomaly,
        oa.is_complexity_anomaly,
        oa.is_customer_behavior_anomaly,
        oa.is_unusual_time,
        oa.customer_risk_level,
        oa.region,
        
        -- Customer context from 360 view
        c360.lifecycle_stage,
        c360.total_orders AS customer_lifetime_orders,
        c360.churn_probability,
        c360.customer_sophistication,
        
        -- Enhanced risk scoring
        CASE 
            WHEN oa.anomaly_severity = 'Critical' AND oa.total_order_value > 1000 THEN 100
            WHEN oa.anomaly_severity = 'Critical' THEN 90
            WHEN oa.anomaly_severity = 'High' AND oa.customer_risk_level = 'High Risk' THEN 85
            WHEN oa.anomaly_severity = 'High' THEN 75
            WHEN oa.anomaly_severity = 'Medium' AND oa.is_customer_behavior_anomaly THEN 65
            WHEN oa.anomaly_severity = 'Medium' THEN 55
            ELSE 45
        END AS risk_score,
        
        -- Priority classification
        CASE 
            WHEN oa.anomaly_severity = 'Critical' AND oa.customer_risk_level = 'High Risk' THEN 'P0 - Immediate'
            WHEN oa.anomaly_severity = 'Critical' THEN 'P1 - Urgent' 
            WHEN oa.anomaly_severity = 'High' AND oa.total_order_value > 500 THEN 'P1 - Urgent'
            WHEN oa.anomaly_severity = 'High' THEN 'P2 - High'
            WHEN oa.anomaly_severity = 'Medium' AND oa.is_unusual_time THEN 'P2 - High'
            ELSE 'P3 - Standard'
        END AS priority_level
        
    FROM {{ ref('int_order_anomalies') }} oa
    LEFT JOIN {{ ref('int_customer_360') }} c360 ON oa.customer_id = c360.customer_id
    
    {% if is_incremental() %}
    WHERE oa.dbt_loaded_at > (SELECT COALESCE(MAX(alert_created_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

-- Alert generation with business context
fraud_alerts AS (
    SELECT 
        {{ generate_surrogate_key(['aa.order_sk', 'aa.anomaly_date']) }} AS alert_id,
        aa.order_sk,
        aa.order_id,
        aa.customer_id,
        aa.anomaly_date AS alert_date,
        aa.order_purchase_timestamp,
        
        -- Alert classification
        'Fraud Detection' AS alert_category,
        aa.anomaly_type AS alert_type,
        aa.priority_level,
        aa.risk_score,
        
        -- Financial impact
        aa.total_order_value,
        CASE 
            WHEN aa.risk_score > 80 THEN aa.total_order_value * 0.9  -- 90% likely fraudulent
            WHEN aa.risk_score > 60 THEN aa.total_order_value * 0.6  -- 60% likely fraudulent
            ELSE aa.total_order_value * 0.3  -- 30% likely fraudulent
        END AS estimated_fraud_amount,
        
        -- Alert details
        aa.composite_anomaly_score,
        aa.anomaly_severity,
        aa.recommended_action,
        
        -- Risk factors breakdown
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN aa.is_value_anomaly THEN 'Unusual Order Value' END,
            CASE WHEN aa.is_complexity_anomaly THEN 'Complex Order Pattern' END, 
            CASE WHEN aa.is_customer_behavior_anomaly THEN 'Abnormal Customer Behavior' END,
            CASE WHEN aa.is_unusual_time THEN 'Suspicious Timing' END,
            CASE WHEN aa.customer_risk_level = 'High Risk' THEN 'High Risk Customer' END,
            CASE WHEN aa.lifecycle_stage = 'New Customer' AND aa.total_order_value > 500 THEN 'New Customer High Value' END
        ) AS risk_factors,
        
        -- Customer intelligence  
        aa.customer_risk_level,
        aa.lifecycle_stage,
        aa.customer_lifetime_orders,
        aa.churn_probability,
        aa.customer_sophistication,
        aa.region,
        
        -- Alert status and workflow
        'OPEN' AS alert_status,
        NULL AS assigned_analyst,
        NULL AS resolution_notes,
        NULL AS resolved_at,
        
        -- SLA targets
        CASE 
            WHEN aa.priority_level = 'P0 - Immediate' THEN DATEADD('minute', 15, CURRENT_TIMESTAMP)
            WHEN aa.priority_level = 'P1 - Urgent' THEN DATEADD('hour', 2, CURRENT_TIMESTAMP)  
            WHEN aa.priority_level = 'P2 - High' THEN DATEADD('hour', 8, CURRENT_TIMESTAMP)
            ELSE DATEADD('day', 1, CURRENT_TIMESTAMP)
        END AS sla_target,
        
        -- Business context for investigation
        CASE 
            WHEN aa.total_order_value > 1000 AND aa.customer_lifetime_orders = 1 THEN 'New customer with high-value order - verify identity'
            WHEN aa.is_customer_behavior_anomaly AND aa.churn_probability < 0.2 THEN 'Loyal customer unusual behavior - possible account compromise'
            WHEN aa.is_unusual_time AND aa.region NOT IN ('Southeast', 'South') THEN 'Late night order from low-activity region'
            WHEN aa.customer_risk_level = 'High Risk' AND aa.composite_anomaly_score > 4.0 THEN 'Known high-risk customer with extreme anomaly'
            ELSE 'Standard fraud investigation required'
        END AS investigation_guidance,
        
        -- Metadata
        CURRENT_TIMESTAMP AS alert_created_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM active_anomalies aa
    WHERE aa.composite_anomaly_score >= {{ var('fraud_alert_threshold', 3.0) }}
)

SELECT * FROM fraud_alerts
ORDER BY risk_score DESC, alert_created_at DESC
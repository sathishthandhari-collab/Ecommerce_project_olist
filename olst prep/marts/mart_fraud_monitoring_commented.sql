{{ config(
    -- SECURITY MONITORING OPTIMIZATION: Incremental append-only for fraud alert tracking
    materialized='incremental',
    unique_key='alert_id',                              -- Unique fraud alert identifier
    incremental_strategy='append',                      -- Append-only for complete alert history
    cluster_by=['alert_date', 'anomaly_severity'],     -- Optimize for temporal and severity queries
    schema='dbt_olist_mart_prod'                        -- Production schema for fraud operations
) }}

WITH active_anomalies AS (
    -- ANOMALY INTELLIGENCE FOUNDATION: Comprehensive order anomaly analysis with customer context
    SELECT
        -- CORE ANOMALY IDENTIFIERS: Primary keys and dimensional context
        oa.order_sk,                                    -- Order surrogate key
        oa.order_id,                                    -- Natural business identifier
        oa.customer_id,                                 -- Customer relationship
        oa.anomaly_date,                                -- Date of anomaly detection
        oa.order_purchase_timestamp,                    -- Original transaction timestamp
        oa.total_order_value,                           -- Financial exposure amount

        -- STATISTICAL ANOMALY ASSESSMENT: Multi-dimensional threat analysis
        oa.composite_anomaly_score,                     -- Overall anomaly intensity (0-5+ scale)
        oa.anomaly_severity,                            -- Business-friendly severity classification
        oa.anomaly_type,                                -- Specific anomaly pattern type
        oa.business_impact,                             -- Strategic impact classification
        oa.recommended_action,                          -- Process-driven response guidance

        -- GRANULAR ANOMALY FLAGS: Detailed deviation type identification
        oa.is_value_anomaly,                            -- Unusual transaction amounts
        oa.is_complexity_anomaly,                       -- Unusual order patterns
        oa.is_customer_behavior_anomaly,                -- Individual behavioral deviations
        oa.is_unusual_time,                             -- Temporal pattern anomalies
        oa.customer_risk_level,                         -- Customer risk profile
        oa.region,                                      -- Geographic context

        -- CUSTOMER INTELLIGENCE CONTEXT: 360-degree customer profiling for fraud assessment
        c360.lifecycle_stage,                           -- Customer maturity classification
        c360.total_orders AS customer_lifetime_orders,  -- Historical transaction volume
        c360.churn_probability,                         -- Customer retention likelihood
        c360.customer_sophistication,                   -- Purchase complexity classification

        -- ENHANCED RISK SCORING: Business-context weighted fraud probability
        CASE
            -- CRITICAL HIGH-VALUE SCENARIOS: Maximum threat priority
            WHEN oa.anomaly_severity = 'Critical' AND oa.total_order_value > 1000 THEN 100    -- Critical + high value = maximum risk
            WHEN oa.anomaly_severity = 'Critical' THEN 90                                     -- Critical severity baseline

            -- HIGH RISK SCENARIOS: Elevated threat levels requiring urgent attention
            WHEN oa.anomaly_severity = 'High' AND oa.customer_risk_level = 'High Risk' THEN 85 -- High anomaly + high-risk customer
            WHEN oa.anomaly_severity = 'High' THEN 75                                          -- High severity baseline

            -- MEDIUM RISK SCENARIOS: Standard investigation priority
            WHEN oa.anomaly_severity = 'Medium' AND oa.is_customer_behavior_anomaly THEN 65    -- Medium + behavioral deviation
            WHEN oa.anomaly_severity = 'Medium' THEN 55                                        -- Medium severity baseline

            ELSE 45                                     -- Low severity baseline
        END AS risk_score,

        -- OPERATIONAL PRIORITY CLASSIFICATION: SLA-driven response prioritization
        CASE
            -- P0 IMMEDIATE RESPONSE: Critical threats requiring immediate action
            WHEN oa.anomaly_severity = 'Critical' AND oa.customer_risk_level = 'High Risk' THEN 'P0 - Immediate'
            WHEN oa.anomaly_severity = 'Critical' THEN 'P1 - Urgent'

            -- P1 URGENT RESPONSE: High-value or high-severity threats
            WHEN oa.anomaly_severity = 'High' AND oa.total_order_value > 500 THEN 'P1 - Urgent'
            WHEN oa.anomaly_severity = 'High' THEN 'P2 - High'

            -- P2 HIGH PRIORITY: Medium severity with concerning patterns
            WHEN oa.anomaly_severity = 'Medium' AND oa.is_unusual_time THEN 'P2 - High'
            
            ELSE 'P3 - Standard'                        -- Standard investigation workflow
        END AS priority_level

    FROM {{ ref('int_order_anomalies') }} oa
    LEFT JOIN {{ ref('int_customer_360') }} c360 ON oa.customer_id = c360.customer_id

    -- INCREMENTAL PROCESSING: Only process new anomalies since last fraud alert generation
    {% if is_incremental() %}
    WHERE oa.dbt_loaded_at > (SELECT COALESCE(MAX(alert_created_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

-- FRAUD ALERT GENERATION: Comprehensive fraud detection alerts with business context and workflow integration
fraud_alerts AS (
    SELECT
        -- UNIQUE ALERT IDENTIFICATION: Composite key for fraud alert tracking
        {{ generate_surrogate_key(['aa.order_sk', 'aa.anomaly_date']) }} AS alert_id,

        -- CORE TRANSACTION IDENTIFIERS: Links to operational systems
        aa.order_sk,                                    -- Data warehouse order key
        aa.order_id,                                    -- Business system order identifier
        aa.customer_id,                                 -- Customer account identifier
        aa.anomaly_date AS alert_date,                  -- Alert generation date
        aa.order_purchase_timestamp,                    -- Original transaction timestamp

        -- ALERT CLASSIFICATION SYSTEM: Structured alert categorization for operations
        'Fraud Detection' AS alert_category,            -- High-level alert type
        aa.anomaly_type AS alert_type,                  -- Specific fraud pattern detected
        aa.priority_level,                              -- SLA-driven priority classification
        aa.risk_score,                                  -- Quantitative risk assessment (0-100)

        -- FINANCIAL IMPACT ASSESSMENT: Economic exposure and loss estimation
        aa.total_order_value,                           -- Gross transaction value at risk
        CASE
            -- PROBABILISTIC LOSS ESTIMATION: Risk-weighted expected fraud amount
            WHEN aa.risk_score > 80 THEN aa.total_order_value * 0.9     -- 90% probability of fraud
            WHEN aa.risk_score > 60 THEN aa.total_order_value * 0.6     -- 60% probability of fraud
            ELSE aa.total_order_value * 0.3                             -- 30% probability of fraud
        END AS estimated_fraud_amount,

        -- TECHNICAL ANOMALY DETAILS: Statistical and rule-based detection results
        aa.composite_anomaly_score,                     -- Multi-dimensional anomaly intensity
        aa.anomaly_severity,                            -- Business-friendly severity level
        aa.recommended_action,                          -- Process-driven investigation guidance

        -- RISK FACTOR ANALYSIS: Structured enumeration of specific fraud indicators
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN aa.is_value_anomaly THEN 'Unusual Order Value' END,
            CASE WHEN aa.is_complexity_anomaly THEN 'Complex Order Pattern' END,
            CASE WHEN aa.is_customer_behavior_anomaly THEN 'Abnormal Customer Behavior' END,
            CASE WHEN aa.is_unusual_time THEN 'Suspicious Timing' END,
            CASE WHEN aa.customer_risk_level = 'High Risk' THEN 'High Risk Customer' END,
            CASE WHEN aa.lifecycle_stage = 'New Customer' AND aa.total_order_value > 500 THEN 'New Customer High Value' END
        ) AS risk_factors,

        -- CUSTOMER INTELLIGENCE PROFILE: Comprehensive customer context for fraud investigation
        aa.customer_risk_level,                         -- Pre-existing customer risk classification
        aa.lifecycle_stage,                             -- Customer maturity and engagement level
        aa.customer_lifetime_orders,                    -- Historical transaction volume
        aa.churn_probability,                           -- Customer retention likelihood
        aa.customer_sophistication,                     -- Purchase behavior complexity
        aa.region,                                      -- Geographic risk context

        -- WORKFLOW MANAGEMENT SYSTEM: Alert processing and case management integration
        'OPEN' AS alert_status,                         -- Initial alert state
        NULL AS assigned_analyst,                       -- Fraud analyst assignment
        NULL AS resolution_notes,                       -- Investigation outcome documentation
        NULL AS resolved_at,                           -- Case closure timestamp

        -- SERVICE LEVEL AGREEMENT TARGETS: Response time requirements by priority
        CASE
            WHEN aa.priority_level = 'P0 - Immediate' THEN DATEADD('minute', 15, CURRENT_TIMESTAMP)  -- 15-minute response
            WHEN aa.priority_level = 'P1 - Urgent' THEN DATEADD('hour', 2, CURRENT_TIMESTAMP)       -- 2-hour response
            WHEN aa.priority_level = 'P2 - High' THEN DATEADD('hour', 8, CURRENT_TIMESTAMP)         -- 8-hour response
            ELSE DATEADD('day', 1, CURRENT_TIMESTAMP)                                               -- 24-hour response
        END AS sla_target,

        -- INVESTIGATION GUIDANCE SYSTEM: Context-driven investigation recommendations
        CASE
            -- NEW CUSTOMER HIGH-VALUE PATTERNS: Identity verification priority
            WHEN aa.total_order_value > 1000 AND aa.customer_lifetime_orders = 1 
            THEN 'New customer with high-value order - verify identity'

            -- LOYAL CUSTOMER BEHAVIORAL DEVIATION: Account compromise investigation
            WHEN aa.is_customer_behavior_anomaly AND aa.churn_probability < 0.2 
            THEN 'Loyal customer unusual behavior - possible account compromise'

            -- GEOGRAPHIC AND TEMPORAL RISK PATTERNS: Regional fraud pattern analysis
            WHEN aa.is_unusual_time AND aa.region NOT IN ('Southeast', 'South') 
            THEN 'Late night order from low-activity region'

            -- HIGH-RISK CUSTOMER EXTREME ANOMALIES: Enhanced due diligence required
            WHEN aa.customer_risk_level = 'High Risk' AND aa.composite_anomaly_score > 4.0 
            THEN 'Known high-risk customer with extreme anomaly'

            ELSE 'Standard fraud investigation required'  -- Default investigation workflow
        END AS investigation_guidance,

        -- AUDIT AND GOVERNANCE: Alert creation metadata for compliance and tracking
        CURRENT_TIMESTAMP AS alert_created_at,          -- Alert generation timestamp
        '{{ invocation_id }}' AS dbt_invocation_id      -- Data lineage tracking identifier

    FROM active_anomalies aa
    -- FRAUD ALERT THRESHOLD: Configurable anomaly score threshold for alert generation
    WHERE aa.composite_anomaly_score >= {{ var('fraud_alert_threshold', 3.0) }}
)

-- FINAL OUTPUT: Complete fraud monitoring intelligence optimized for operational fraud detection and investigation workflows
SELECT * FROM fraud_alerts
ORDER BY risk_score DESC, alert_created_at DESC
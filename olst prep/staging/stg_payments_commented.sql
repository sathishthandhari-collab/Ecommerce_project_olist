-- EXECUTION DEPENDENCY: Ensures proper sequencing with order processing
-- depends_on: {{ ref('stg_orders') }}

{{ config(
    -- INCREMENTAL PROCESSING: Handles large payment datasets efficiently
    materialized='incremental',
    unique_key='payment_sk',            -- Defines merge key for updates
    on_schema_change='append_new_columns', -- Handles schema evolution
    schema='DBT_OLIST_STAGING'          -- Dedicated staging schema
) }}

{%- set quality_checks = [
    -- BUSINESS VALIDATION RULES: Critical payment data integrity checks
    'order_id IS NOT NULL',                                                    -- Order relationship required
    'payment_sequential IS NOT NULL',                                          -- Payment sequence required
    'payment_type IS NOT NULL',                                               -- Payment method required
    'payment_installments >= 1',                                              -- Minimum one installment
    'payment_value > 0',                                                      -- Positive payment amount
    'payment_type IN (\'credit_card\', \'debit_card\', \'voucher\', \'boleto\')'  -- Valid payment methods only
] -%}

WITH source_data AS (
    -- SOURCE DATA EXTRACTION: Retrieves raw payment data from configured source
    SELECT *
    FROM {{ source('src_olist_raw', 'raw_olist_payments') }}
    -- INCREMENTAL FILTER: Only processes payments for new orders
    WHERE order_id IN {{ get_new_order_ids() }}
),

enhanced_payments AS (
    SELECT
        -- COMPOSITE SURROGATE KEY: Unique identifier combining order and payment sequence
        {{ generate_surrogate_key(['order_id', 'payment_sequential']) }} AS payment_sk,

        -- NATURAL BUSINESS KEYS: Original identifiers for traceability
        order_id,                       -- Foreign key to orders dimension
        payment_sequential,             -- Payment sequence within order (multiple payments per order)

        -- PAYMENT METHOD STANDARDIZATION: Ensures consistent categorization
        LOWER(TRIM(payment_type)) AS payment_type,      -- Normalize payment method names
        payment_installments,                           -- Number of payment installments
        payment_value::FLOAT AS payment_value,          -- Payment amount with standardized precision

        -- PAYMENT METHOD CATEGORIZATION: High-level groupings for analysis
        CASE
            WHEN payment_type IN ('credit_card', 'debit_card') THEN 'Card'     -- Electronic card payments
            WHEN payment_type = 'boleto' THEN 'Bank Transfer'                  -- Brazilian bank transfer system
            WHEN payment_type = 'voucher' THEN 'Voucher'                       -- Store credit/vouchers
            ELSE 'Other'                                                       -- Catch-all for new methods
        END AS payment_method_category,

        -- INSTALLMENT ANALYSIS: Payment term categorization
        CASE
            WHEN payment_installments = 1 THEN 'Full Payment'                  -- Single payment
            WHEN payment_installments BETWEEN 2 AND 6 THEN 'Short Term'        -- 2-6 months
            WHEN payment_installments BETWEEN 7 AND 12 THEN 'Medium Term'      -- 7-12 months
            WHEN payment_installments > 12 THEN 'Long Term'                    -- 12+ months
            ELSE 'Unknown'                                                     -- Invalid installment data
        END AS installment_category,

        -- RISK ASSESSMENT: Credit risk categorization based on method and terms
        CASE
            WHEN payment_type = 'credit_card' AND payment_installments > 10 THEN 'High Risk'    -- Long-term credit
            WHEN payment_type = 'credit_card' AND payment_installments > 6 THEN 'Medium Risk'   -- Medium-term credit
            ELSE 'Low Risk'                                                                      -- All other scenarios
        END AS payment_risk_category,

        -- STATISTICAL ANOMALY DETECTION: Identifies unusual payment patterns
        {{ calculate_z_score('payment_value', 'payment_type') }} AS value_z_score_by_type,      -- Value deviation by payment method
        {{ calculate_z_score('payment_installments', 'payment_type') }} AS installment_z_score, -- Installment deviation by method

        -- DERIVED FINANCIAL METRICS: Calculated business insights
        payment_value / NULLIF(payment_installments, 0) AS installment_amount, -- Amount per installment

        -- BUSINESS ANOMALY FLAGS: Special conditions for investigation
        CASE
            WHEN payment_value > 5000 AND payment_type != 'credit_card' THEN TRUE  -- High-value non-credit payments
            ELSE FALSE
        END AS is_high_value_non_credit,

        CASE
            WHEN payment_installments > 24 THEN TRUE                               -- Extremely long payment terms
            ELSE FALSE
        END AS is_extreme_installments,

        CASE
            WHEN {{ calculate_z_score('payment_value', 'payment_type') }} > {{ var('anomaly_z_score_threshold') }}
            THEN TRUE
            ELSE FALSE
        END AS is_value_anomaly,        -- Statistical value outlier

        -- COMPREHENSIVE DATA QUALITY ASSESSMENT: Aggregates all validation rules
        {{ data_quality_score(quality_checks) }} AS data_quality_score,

        -- AUDIT METADATA: Processing lineage and tracking
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,  -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id          -- Links to specific dbt run
    FROM source_data
),

final AS (
    -- FINAL STRUCTURED OUTPUT: Payment fact table ready for analytics
    SELECT
        -- KEYS AND IDENTIFIERS
        payment_sk,                     -- Surrogate key for joins
        order_id,                       -- Foreign key to orders
        payment_sequential,             -- Payment sequence number
        
        -- PAYMENT DETAILS
        payment_type,                   -- Standardized payment method
        payment_installments,           -- Number of installments
        payment_value,                  -- Payment amount
        
        -- BUSINESS CLASSIFICATIONS
        payment_method_category,        -- High-level method grouping
        installment_category,           -- Payment term classification
        payment_risk_category,          -- Risk assessment category
        
        -- STATISTICAL ANALYSIS
        value_z_score_by_type,          -- Value deviation metric
        installment_z_score,            -- Installment deviation metric
        installment_amount,             -- Calculated installment amount
        
        -- ANOMALY FLAGS
        is_high_value_non_credit,       -- Business exception flag
        is_extreme_installments,        -- Unusual terms flag
        is_value_anomaly,               -- Statistical outlier flag
        
        -- QUALITY AND AUDIT
        data_quality_score,             -- Overall quality metric
        dbt_loaded_at,                  -- Processing metadata
        dbt_invocation_id               -- Run metadata
    FROM enhanced_payments
    -- QUALITY GATE: Excludes payments not meeting minimum quality standards
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

-- FINAL OUTPUT: Clean, enriched payments fact table
SELECT * FROM final
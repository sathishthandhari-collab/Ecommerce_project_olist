-- depends_on: {{ ref('stg_orders') }}

{{ config(
    materialized='incremental',
    unique_key='payment_sk', 
    on_schema_change='append_new_columns',
    schema='DBT_OLIST_STAGING',
    tags=['staging']
) }}

{%- set quality_checks = [
    'order_id IS NOT NULL',
    'payment_sequential IS NOT NULL',
    'payment_type IS NOT NULL',
    'payment_installments >= 1',
    'payment_value > 0',
    'payment_type IN (\'credit_card\', \'debit_card\', \'voucher\', \'boleto\')'
] -%}

WITH source_data AS (
    SELECT * 
    FROM {{ source('src_olist_raw', 'raw_olist_payments') }}
    WHERE order_id IN {{ get_new_order_ids() }}
),

enhanced_payments AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key(['order_id', 'payment_sequential']) }} AS payment_sk,
        
        -- Natural keys
        order_id,
        payment_sequential,
        
        -- Payment details
        LOWER(TRIM(payment_type)) AS payment_type,
        payment_installments,
        payment_value::FLOAT AS payment_value,
        
        -- Payment method categorization
        CASE 
            WHEN payment_type IN ('credit_card', 'debit_card') THEN 'Card'
            WHEN payment_type = 'boleto' THEN 'Bank Transfer'
            WHEN payment_type = 'voucher' THEN 'Voucher'
            ELSE 'Other'
        END AS payment_method_category,
        
        -- Installment analysis
        CASE 
            WHEN payment_installments = 1 THEN 'Full Payment'
            WHEN payment_installments BETWEEN 2 AND 6 THEN 'Short Term'
            WHEN payment_installments BETWEEN 7 AND 12 THEN 'Medium Term' 
            WHEN payment_installments > 12 THEN 'Long Term'
            ELSE 'Unknown'
        END AS installment_category,
        
        -- Risk assessment
        CASE 
            WHEN payment_type = 'credit_card' AND payment_installments > 10 THEN 'High Risk'
            WHEN payment_type = 'credit_card' AND payment_installments > 6 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS payment_risk_category,
        
        -- Statistical analysis
        {{ calculate_z_score('payment_value', 'payment_type') }} AS value_z_score_by_type,
        {{ calculate_z_score('payment_installments', 'payment_type') }} AS installment_z_score,
        
        -- Business insights
        payment_value / NULLIF(payment_installments, 0) AS installment_amount,
        
        -- Anomaly flags
        CASE 
            WHEN payment_value > 5000 AND payment_type != 'credit_card' THEN TRUE
            ELSE FALSE 
        END AS is_high_value_non_credit,
        
        CASE 
            WHEN payment_installments > 24 THEN TRUE 
            ELSE FALSE 
        END AS is_extreme_installments,
        
        CASE 
            WHEN {{ calculate_z_score('payment_value', 'payment_type') }} > {{ var('anomaly_z_score_threshold') }}
            THEN TRUE 
            ELSE FALSE 
        END AS is_value_anomaly,
        
        -- Data quality assessment
        {{ data_quality_score(quality_checks) }} AS data_quality_score,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM source_data
),

final AS (
    SELECT 
        payment_sk,
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        payment_method_category,
        installment_category,
        payment_risk_category,
        value_z_score_by_type,
        installment_z_score,
        installment_amount,
        is_high_value_non_credit,
        is_extreme_installments,
        is_value_anomaly,
        data_quality_score,
        dbt_loaded_at,
        dbt_invocation_id
    FROM enhanced_payments
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

SELECT * FROM final
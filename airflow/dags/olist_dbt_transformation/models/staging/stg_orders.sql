{{ config(
    materialized='incremental',
    unique_key='order_sk',
    on_schema_change='append_new_columns',
    schema='DBT_OLIST_STAGING',
    tags=['staging']
) }}

{%- set quality_checks = [
    'order_purchase_timestamp IS NOT NULL',
    'order_status IS NOT NULL', 
    'customer_id IS NOT NULL',
    'order_purchase_timestamp <= CURRENT_TIMESTAMP',
    'COALESCE(order_delivered_customer_date, order_purchase_timestamp) >= order_purchase_timestamp',
    'order_status IN (\'approved\', \'canceled\', \'delivered\', \'invoiced\', \'processing\', \'shipped\', \'unavailable\')'
] -%}

WITH source_data AS (
    SELECT * 
    FROM {{ source('src_olist_raw', 'raw_olist_orders') }}
    {% if is_incremental() %}
        WHERE _loaded_at > (SELECT MAX(ingestion_loaded_at) FROM {{ this }})
    {% endif %}
    
),

enhanced_orders AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key(['order_id']) }} AS order_sk,
        
        -- Natural keys
        order_id,
        customer_id,
        
        -- Order status and timing 
        UPPER(TRIM(order_status)) AS order_status,
        {{ standardize_timestamp('order_purchase_timestamp') }} AS order_purchase_timestamp,
        {{ standardize_timestamp('order_approved_at') }} AS order_approved_at,
        {{ standardize_timestamp('order_delivered_carrier_date') }} AS order_delivered_carrier_date,
        {{ standardize_timestamp('order_delivered_customer_date') }} AS order_delivered_customer_date,
        {{ standardize_timestamp('order_estimated_delivery_date') }} AS order_estimated_delivery_date,
        
        -- {# Derived timing metrics #}
        DATEDIFF('day', 
            {{ standardize_timestamp('order_purchase_timestamp') }},
            {{ standardize_timestamp('order_delivered_customer_date') }}
        ) AS delivery_days,
        
        DATEDIFF('day',
            {{ standardize_timestamp('order_delivered_customer_date') }},
            {{ standardize_timestamp('order_estimated_delivery_date') }}
        ) AS delivery_vs_estimate_days,
        
        -- {# Business flags #}
        CASE 
            WHEN order_status = 'delivered' 
                AND order_delivered_customer_date <= order_estimated_delivery_date 
            THEN TRUE 
            ELSE FALSE 
        END AS is_on_time_delivery,
        
        CASE 
            WHEN order_status IN ('canceled', 'unavailable') THEN TRUE 
            ELSE FALSE 
        END AS is_cancelled,
        
        -- {# Data quality assessment #}
        {{ data_quality_score(quality_checks) }} AS data_quality_score,
        
        -- {# Individual quality flags for debugging #}
        {% for check in quality_checks %}
            CASE WHEN {{ check }} THEN TRUE ELSE FALSE END AS quality_{{ loop.index }}{{ "," if not loop.last }}
        {% endfor %},
        
        -- {# Metadata #}
        _loaded_at AS ingestion_loaded_at,
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM source_data
),

-- Contract enforcement
final AS (
    SELECT 
        order_sk,
        order_id,
        customer_id,  
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        delivery_days,
        delivery_vs_estimate_days,
        is_on_time_delivery,
        is_cancelled,
        data_quality_score,
        ingestion_loaded_at,
        dbt_loaded_at,
        dbt_invocation_id
    FROM enhanced_orders
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
    )

SELECT * FROM final
    {% if target.name == 'dev' %}
        LIMIT {{ var('dev_sample_size') }}
    {% endif %}
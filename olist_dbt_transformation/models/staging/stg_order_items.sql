-- depends_on: {{ ref('stg_orders') }}
{{ config(
    materialized='incremental',
    unique_key='order_item_sk',
    on_schema_change='append_new_columns',
    schema='DBT_OLIST_STAGING',
    tags=['staging']
) }}

{%- set quality_checks = [
    'order_id IS NOT NULL',
    'order_item_id IS NOT NULL',
    'product_id IS NOT NULL', 
    'seller_id IS NOT NULL',
    'price >= 0',
    'freight_value >= 0',
    'shipping_limit_date IS NOT NULL'
] -%}


WITH source_data AS (
    SELECT * 
    FROM {{ source('src_olist_raw', 'raw_olist_order_items') }}
    WHERE order_id IN {{ get_new_order_ids() }}
    {% if target.name == 'dev' %}
    LIMIT {{ var('dev_sample_size') }}
    {% endif %}
),

enhanced_order_items AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key(['order_id', 'order_item_id']) }} AS order_item_sk,
        
        -- Natural keys
        order_id,
        order_item_id,
        product_id,
        seller_id,
        
        -- Financial metrics
        price::number(10,2) AS item_price,
        freight_value::number(10,2) AS freight_value,
        (price + freight_value)::number(10,2) AS total_item_value,
        
        -- Timing
        {{ standardize_timestamp('shipping_limit_date') }} AS shipping_limit_date,
        
        -- Statistical analysis for pricing anomalies
        {{ calculate_z_score('price', 'product_id') }} AS price_z_score,
        {{ calculate_z_score('freight_value', 'seller_id') }} AS freight_z_score,
        
        -- Business flags
        CASE WHEN price = 0.00 THEN TRUE ELSE FALSE END AS is_free_item,
        CASE WHEN freight_value = 0.00 THEN TRUE ELSE FALSE END AS is_free_shipping,
        
        -- Anomaly detection
        CASE 
            WHEN {{ calculate_z_score('price', 'product_id') }} > {{ var('anomaly_z_score_threshold') }}
            THEN TRUE ELSE FALSE 
        END AS is_price_anomaly,
        
        CASE 
            WHEN {{ calculate_z_score('freight_value', 'seller_id') }} > {{ var('anomaly_z_score_threshold') }}
            THEN TRUE ELSE FALSE 
        END AS is_freight_anomaly,
        
        -- Data quality assessment
        {{ data_quality_score(quality_checks) }} AS data_quality_score,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM source_data
),

final AS (
    SELECT 
        order_item_sk,
        order_id,
        order_item_id,
        product_id,
        seller_id,
        item_price,
        freight_value,
        total_item_value,
        shipping_limit_date,
        price_z_score,
        freight_z_score,
        is_free_item,
        is_free_shipping,
        is_price_anomaly,
        is_freight_anomaly,
        data_quality_score,
        dbt_loaded_at,
        dbt_invocation_id
    FROM enhanced_order_items
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

SELECT * FROM final
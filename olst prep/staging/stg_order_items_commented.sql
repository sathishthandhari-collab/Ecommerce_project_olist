-- DEPENDENCY MANAGEMENT: Ensures proper execution order in the DAG
-- depends_on: {{ ref('stg_orders') }}

{{ config(
    -- PERFORMANCE OPTIMIZATION: Incremental processing for large datasets
    materialized='incremental',
    unique_key='order_item_sk',          -- Defines merge key for incremental updates
    on_schema_change='append_new_columns', -- Handles schema evolution automatically
    schema='DBT_OLIST_STAGING'           -- Organizes models in staging schema
) }}

{%- set quality_checks = [
    -- BUSINESS RULES: Essential validations for order items data integrity
    'order_id IS NOT NULL',             -- Links to parent order (referential integrity)
    'order_item_id IS NOT NULL',        -- Line item sequence number required
    'product_id IS NOT NULL',           -- Product reference required
    'seller_id IS NOT NULL',            -- Seller reference required
    'price >= 0',                       -- Non-negative pricing validation
    'freight_value >= 0',               -- Non-negative shipping cost validation
    'shipping_limit_date IS NOT NULL'   -- Logistics deadline required
] -%}

WITH source_data AS (
    -- SOURCE DATA EXTRACTION: Pulls raw order items from configured source
    SELECT *
    FROM {{ source('src_olist_raw', 'raw_olist_order_items') }}
    -- INCREMENTAL FILTERING: Only processes items from new orders for efficiency
    WHERE order_id IN {{ get_new_order_ids() }}
),

enhanced_order_items AS (
    SELECT
        -- SURROGATE KEY GENERATION: Creates composite key for unique item identification
        {{ generate_surrogate_key(['order_id', 'order_item_id']) }} AS order_item_sk,

        -- NATURAL BUSINESS KEYS: Preserves original identifiers for business traceability
        order_id,                       -- Parent order identifier
        order_item_id,                  -- Line item sequence number
        product_id,                     -- Product catalog reference
        seller_id,                      -- Seller/vendor identifier

        -- FINANCIAL DATA STANDARDIZATION: Ensures consistent precision and formatting
        price::number(10,2) AS item_price,                    -- Item unit price (2 decimal places)
        freight_value::number(10,2) AS freight_value,         -- Shipping cost per item
        (price + freight_value)::number(10,2) AS total_item_value, -- Complete item cost to customer

        -- LOGISTICS TIMING: Critical shipping deadline information
        {{ standardize_timestamp('shipping_limit_date') }} AS shipping_limit_date,

        -- STATISTICAL ANOMALY DETECTION: Identifies unusual pricing patterns
        {{ calculate_z_score('price', 'product_id') }} AS price_z_score,     -- Price deviation within product
        {{ calculate_z_score('freight_value', 'seller_id') }} AS freight_z_score, -- Freight deviation by seller

        -- BUSINESS CLASSIFICATION FLAGS: Identifies special pricing conditions
        CASE WHEN price = 0.00 THEN TRUE ELSE FALSE END AS is_free_item,      -- Promotional/free items
        CASE WHEN freight_value = 0.00 THEN TRUE ELSE FALSE END AS is_free_shipping, -- Free shipping offers

        -- ANOMALY DETECTION FLAGS: Statistical outliers for investigation
        CASE
            WHEN {{ calculate_z_score('price', 'product_id') }} > {{ var('anomaly_z_score_threshold') }}
            THEN TRUE ELSE FALSE
        END AS is_price_anomaly,        -- Unusually high/low price for this product

        CASE
            WHEN {{ calculate_z_score('freight_value', 'seller_id') }} > {{ var('anomaly_z_score_threshold') }}
            THEN TRUE ELSE FALSE
        END AS is_freight_anomaly,      -- Unusual freight cost for this seller

        -- COMPREHENSIVE QUALITY SCORING: Aggregates all validation rules
        {{ data_quality_score(quality_checks) }} AS data_quality_score,

        -- AUDIT METADATA: Processing lineage and timestamps
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,  -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id          -- Links to specific dbt run
    FROM source_data
),

final AS (
    -- FINAL COLUMN SELECTION: Structured output for downstream consumption
    SELECT
        -- KEYS AND IDENTIFIERS
        order_item_sk,                  -- Surrogate key for joins
        order_id,                       -- Foreign key to orders
        order_item_id,                  -- Line item sequence
        product_id,                     -- Foreign key to products
        seller_id,                      -- Foreign key to sellers
        
        -- FINANCIAL METRICS
        item_price,                     -- Standardized unit price
        freight_value,                  -- Standardized shipping cost
        total_item_value,               -- Complete customer cost
        
        -- LOGISTICS DATA
        shipping_limit_date,            -- Shipping deadline
        
        -- STATISTICAL ANALYSIS
        price_z_score,                  -- Price deviation metric
        freight_z_score,                -- Freight deviation metric
        
        -- BUSINESS FLAGS
        is_free_item,                   -- Promotional item indicator
        is_free_shipping,               -- Free shipping indicator
        is_price_anomaly,               -- Price outlier flag
        is_freight_anomaly,             -- Freight outlier flag
        
        -- QUALITY AND AUDIT
        data_quality_score,             -- Overall quality metric
        dbt_loaded_at,                  -- Processing metadata
        dbt_invocation_id               -- Run metadata
    FROM enhanced_order_items
    -- QUALITY GATE: Excludes poor quality records from downstream processing
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

-- FINAL OUTPUT: Clean, enriched order items fact table
SELECT * FROM final
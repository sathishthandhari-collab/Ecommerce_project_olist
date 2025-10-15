{{ config(
    -- INCREMENTAL CONFIGURATION: Enables efficient processing of large datasets
    materialized='incremental',
    unique_key='order_sk',              -- Defines the key for incremental updates
    on_schema_change='append_new_columns',  -- Handles schema evolution gracefully
    schema='DBT_OLIST_STAGING'          -- Places model in dedicated staging schema
) }}

{%- set quality_checks = [
    -- DATA INTEGRITY RULES: Critical validations for order data quality
    'order_purchase_timestamp IS NOT NULL',                              -- Core business event timestamp
    'order_status IS NOT NULL',                                         -- Order state information required
    'customer_id IS NOT NULL',                                          -- Customer relationship validation
    'order_purchase_timestamp <= CURRENT_TIMESTAMP',                    -- Prevents future-dated orders
    'COALESCE(order_delivered_customer_date, order_purchase_timestamp) >= order_purchase_timestamp', -- Logical date sequence
    'order_status IN (\'approved\', \'canceled\', \'delivered\', \'invoiced\', \'processing\', \'shipped\', \'unavailable\')' -- Valid status values
] -%}

WITH source_data AS (
    -- SOURCE DATA INGESTION: Retrieves raw order data from configured source
    SELECT *
    FROM {{ source('src_olist_raw', 'raw_olist_orders') }}
    
    -- INCREMENTAL PROCESSING: Only processes new data since last run
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(ingestion_loaded_at) FROM {{ this }})
    {% endif %}
),

enhanced_orders AS (
    SELECT
        -- SURROGATE KEY CREATION: Generates stable unique identifier for dimension modeling
        {{ generate_surrogate_key(['order_id']) }} AS order_sk,

        -- NATURAL BUSINESS KEYS: Preserves original identifiers for business users
        order_id,                       -- Primary business identifier
        customer_id,                    -- Links to customer dimension

        -- STATUS AND TIMING DATA STANDARDIZATION
        UPPER(TRIM(order_status)) AS order_status,  -- Normalizes status values

        -- TIMESTAMP STANDARDIZATION: Ensures consistent datetime handling across all timestamps
        {{ standardize_timestamp('order_purchase_timestamp') }} AS order_purchase_timestamp,      -- Customer order placement
        {{ standardize_timestamp('order_approved_at') }} AS order_approved_at,                    -- Payment approval
        {{ standardize_timestamp('order_delivered_carrier_date') }} AS order_delivered_carrier_date, -- Handed to carrier
        {{ standardize_timestamp('order_delivered_customer_date') }} AS order_delivered_customer_date, -- Customer receipt
        {{ standardize_timestamp('order_estimated_delivery_date') }} AS order_estimated_delivery_date, -- Original estimate

        -- DERIVED TIMING METRICS: Business-critical performance indicators
        DATEDIFF('day',
            {{ standardize_timestamp('order_purchase_timestamp') }},
            {{ standardize_timestamp('order_delivered_customer_date') }}
        ) AS delivery_days,             -- Total fulfillment time

        DATEDIFF('day',
            {{ standardize_timestamp('order_delivered_customer_date') }},
            {{ standardize_timestamp('order_estimated_delivery_date') }}
        ) AS delivery_vs_estimate_days, -- Performance vs promise (negative = late)

        -- BUSINESS PERFORMANCE FLAGS: Key operational metrics
        CASE
            WHEN order_status = 'delivered'
            AND order_delivered_customer_date <= order_estimated_delivery_date
            THEN TRUE
            ELSE FALSE
        END AS is_on_time_delivery,     -- Service level achievement

        CASE
            WHEN order_status IN ('canceled', 'unavailable') THEN TRUE
            ELSE FALSE
        END AS is_cancelled,            -- Failed order flag

        -- COMPREHENSIVE DATA QUALITY ASSESSMENT: Aggregates all validation rules
        {{ data_quality_score(quality_checks) }} AS data_quality_score,

        -- INDIVIDUAL QUALITY FLAGS: Detailed validation results for debugging
        {% for check in quality_checks %}
        CASE WHEN {{ check }} THEN TRUE ELSE FALSE END AS quality_{{ loop.index }}{{ "," if not loop.last }}
        {% endfor %},

        -- AUDIT AND LINEAGE METADATA: Tracks data processing history
        _loaded_at AS ingestion_loaded_at,                  -- Source system timestamp
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,  -- dbt processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id          -- Links to specific dbt run
    FROM source_data
),

-- CONTRACT ENFORCEMENT: Final data validation and column selection
final AS (
    SELECT
        -- DIMENSIONAL MODELING KEYS
        order_sk,                       -- Surrogate key for efficient joins
        order_id,                       -- Natural business key
        customer_id,                    -- Foreign key to customer dimension
        
        -- CORE ORDER ATTRIBUTES
        order_status,                   -- Current order state
        
        -- STANDARDIZED TIMESTAMPS
        order_purchase_timestamp,       -- Customer action timestamp
        order_approved_at,              -- Payment processing timestamp
        order_delivered_carrier_date,   -- Logistics handoff timestamp
        order_delivered_customer_date,  -- Final delivery timestamp
        order_estimated_delivery_date,  -- Original promise timestamp
        
        -- DERIVED BUSINESS METRICS
        delivery_days,                  -- Total fulfillment time
        delivery_vs_estimate_days,      -- Performance vs promise
        
        -- OPERATIONAL FLAGS
        is_on_time_delivery,            -- Service level indicator
        is_cancelled,                   -- Order failure indicator
        
        -- QUALITY AND AUDIT
        data_quality_score,             -- Overall quality assessment
        ingestion_loaded_at,            -- Source data timestamp
        dbt_loaded_at,                  -- Processing timestamp
        dbt_invocation_id               -- Run linkage
    FROM enhanced_orders
    -- QUALITY GATE: Only includes records meeting minimum quality standards
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

-- FINAL MODEL OUTPUT: Clean, enriched order dimension ready for analytics
SELECT * FROM final
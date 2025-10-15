{{ config(
    -- PERFORMANCE OPTIMIZATION: Incremental processing for large product catalog
    materialized='incremental',
    unique_key='product_sk',             -- Defines merge key for updates
    on_schema_change='append_new_columns', -- Handles schema evolution automatically
    schema='DBT_OLIST_STAGING'           -- Places model in dedicated staging schema
) }}

{%- set quality_checks = [
    -- PRODUCT DATA VALIDATION RULES: Essential product information integrity
    'product_id IS NOT NULL',                              -- Primary business identifier required
    'product_category_name IS NOT NULL',                   -- Categorization required for analysis
    'product_weight_g > 0 OR product_weight_g IS NULL',    -- Positive weight or missing (acceptable)
    'product_length_cm > 0 OR product_length_cm IS NULL',  -- Positive length or missing
    'product_height_cm > 0 OR product_height_cm IS NULL',  -- Positive height or missing
    'product_width_cm > 0 OR product_width_cm IS NULL',    -- Positive width or missing
    'product_photos_qty >= 0 OR product_photos_qty IS NULL' -- Non-negative photo count or missing
] -%}

WITH source_data AS (
    -- SOURCE DATA EXTRACTION: Pulls raw product data from configured source
    SELECT *
    FROM {{ source('src_olist_raw', 'raw_olist_products') }}
    -- INCREMENTAL FILTERING: Only processes products related to new orders
    WHERE product_id IN {{ get_related_product_ids() }}
),

-- STATISTICAL CONTEXT CALCULATION: Computes population statistics for anomaly detection
statistical_context AS (
    SELECT
        -- WEIGHT STATISTICS: Population mean and standard deviation for weight analysis
        AVG(product_weight_g)::number(10,2) AS avg_weight,
        STDDEV(product_weight_g)::number(10,2) AS stddev_weight,
        -- PHOTO STATISTICS: Population statistics for photo quantity analysis
        AVG(product_photos_qty)::number(10,2) AS avg_photos,
        STDDEV(product_photos_qty)::number(10,2) AS stddev_photos
    FROM {{ source('src_olist_raw', 'raw_olist_products') }}
    -- BROADER DATASET: Uses entire product catalog for statistical validity
    WHERE product_weight_g IS NOT NULL
    OR product_photos_qty IS NOT NULL
),

enhanced_products AS (
    SELECT
        -- SURROGATE KEY GENERATION: Creates unique identifier for dimension modeling
        {{ generate_surrogate_key(['product_id']) }} AS product_sk,

        -- NATURAL BUSINESS KEY: Preserves original product identifier
        product_id,

        -- PRODUCT ATTRIBUTE STANDARDIZATION: Handles missing and inconsistent data
        COALESCE(NULLIF(TRIM(product_category_name), ''), 'uncategorized') AS product_category_name, -- Default for missing categories
        COALESCE(product_name_lenght, 0) AS product_name_length,              -- Handle null name lengths
        COALESCE(product_description_lenght, 0) AS product_description_length, -- Handle null description lengths
        COALESCE(product_photos_qty, 0) AS product_photos_qty,                -- Default zero photos

        -- PHYSICAL DIMENSIONS: Raw dimensional data preservation
        product_weight_g,               -- Weight in grams
        product_length_cm,              -- Length in centimeters
        product_height_cm,              -- Height in centimeters  
        product_width_cm,               -- Width in centimeters

        -- CALCULATED VOLUMETRIC METRICS: Derived logistics insights
        CASE
            WHEN product_length_cm > 0 AND product_height_cm > 0 AND product_width_cm > 0
            THEN (product_length_cm * product_height_cm * product_width_cm) / 1000000.0  -- Convert to cubic meters
            ELSE NULL
        END AS product_volume_cubic_meters,

        -- CATEGORY GROUPING: High-level business categorization for analysis
        CASE
            WHEN product_category_name IN ('beleza_saude', 'perfumaria', 'fraldas_higiene')
            THEN 'Health & Beauty'
            WHEN product_category_name IN ('esporte_lazer', 'casa_construcao', 'jardim_ferramentas_jardim')
            THEN 'Home & Garden'
            WHEN product_category_name IN ('informatica_acessorios', 'eletronicos', 'telefonia')
            THEN 'Electronics'
            WHEN product_category_name IN ('moveis_decoracao', 'cama_mesa_banho', 'utilidades_domesticas')
            THEN 'Home & Furniture'
            ELSE 'Other'
        END AS category_group,

        -- STATISTICAL ANOMALY DETECTION: Z-score analysis using population statistics
        CASE
            WHEN product_weight_g IS NOT NULL AND sc.stddev_weight > 0
            THEN (ABS(product_weight_g - sc.avg_weight) / sc.stddev_weight)::FLOAT  -- Weight z-score
            ELSE NULL
        END AS weight_z_score,

        CASE
            WHEN product_photos_qty IS NOT NULL AND sc.stddev_photos > 0
            THEN (ABS(product_photos_qty - sc.avg_photos) / sc.stddev_photos)::FLOAT  -- Photo count z-score
            ELSE NULL
        END AS photos_z_score,

        -- BATCH TRACKING: Identifies which products are in current processing batch
        CASE WHEN product_id IN {{ get_related_product_ids() }} THEN TRUE ELSE FALSE END AS in_current_batch,

        -- LOGISTICS FLAGS: Shipping complexity indicators
        CASE
            WHEN product_weight_g > 30000 OR  -- Over 30kg
            GREATEST(product_length_cm, product_height_cm, product_width_cm) > 100  -- Any dimension over 1m
            THEN TRUE ELSE FALSE
        END AS is_bulky_item,

        -- COMPREHENSIVE DATA QUALITY ASSESSMENT: Aggregates validation rules
        {{ data_quality_score(quality_checks) }} AS data_quality_score,

        -- COMPLETENESS INDICATOR: Flags products with full dimensional data
        CASE
            WHEN product_length_cm IS NOT NULL
            AND product_name_lenght IS NOT NULL
            AND product_photos_qty IS NOT NULL
            AND product_weight_g IS NOT NULL
            AND product_width_cm IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS has_complete_dimensions,

        -- AUDIT METADATA: Processing lineage and tracking
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,  -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id          -- Links to specific dbt run
    FROM source_data
    -- STATISTICAL CONTEXT JOIN: Adds population statistics to each record
    CROSS JOIN statistical_context sc
),

final AS (
    -- FINAL STRUCTURED OUTPUT: Product dimension ready for analytics
    SELECT
        -- KEYS AND IDENTIFIERS
        product_sk,                     -- Surrogate key for efficient joins
        product_id,                     -- Natural business key
        
        -- CORE PRODUCT ATTRIBUTES
        product_category_name,          -- Standardized category
        product_name_length,            -- Text length metric
        product_description_length,     -- Content richness indicator
        product_photos_qty,             -- Visual content count
        
        -- PHYSICAL PROPERTIES
        product_weight_g,               -- Weight for logistics
        product_length_cm,              -- Length dimension
        product_height_cm,              -- Height dimension
        product_width_cm,               -- Width dimension
        product_volume_cubic_meters,    -- Calculated volume
        
        -- BUSINESS CLASSIFICATIONS
        category_group,                 -- High-level grouping
        
        -- STATISTICAL ANALYSIS
        weight_z_score,                 -- Weight anomaly indicator
        photos_z_score,                 -- Photo count anomaly indicator
        
        -- OPERATIONAL FLAGS
        in_current_batch,               -- Batch processing indicator
        is_bulky_item,                  -- Logistics complexity flag
        has_complete_dimensions,        -- Data completeness flag
        
        -- QUALITY AND AUDIT
        data_quality_score,             -- Overall quality metric
        dbt_loaded_at,                  -- Processing metadata
        dbt_invocation_id               -- Run metadata
    FROM enhanced_products
    -- QUALITY GATE: Only includes products meeting minimum quality standards
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

-- FINAL OUTPUT: Clean, enriched product dimension
SELECT * FROM final
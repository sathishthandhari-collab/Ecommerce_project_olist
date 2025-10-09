{{ config(
    materialized='incremental',
    unique_key='product_sk',
    on_schema_change='append_new_columns',
    schema='DBT_OLIST_STAGING',
    tags=['staging']
) }}

{%- set quality_checks = [
    'product_id IS NOT NULL',
    'product_category_name IS NOT NULL',
    'product_weight_g > 0 OR product_weight_g IS NULL',
    'product_length_cm > 0 OR product_length_cm IS NULL',
    'product_height_cm > 0 OR product_height_cm IS NULL', 
    'product_width_cm > 0 OR product_width_cm IS NULL',
    'product_photos_qty >= 0 OR product_photos_qty IS NULL'
] -%}

WITH source_data AS (
    SELECT * 
    FROM {{ source('src_olist_raw', 'raw_olist_products') }}
    WHERE product_id IN {{ get_related_product_ids() }}
),

-- Get statistical context for z-score calculations (need broader dataset)
statistical_context AS (
    SELECT 
        AVG(product_weight_g)::number(10,2) AS avg_weight,
        STDDEV(product_weight_g)::number(10,2) AS stddev_weight,
        AVG(product_photos_qty)::number(10,2) AS avg_photos,
        STDDEV(product_photos_qty)::number(10,2) AS stddev_photos
    FROM {{ source('src_olist_raw', 'raw_olist_products') }}
    WHERE product_weight_g IS NOT NULL 
       OR product_photos_qty IS NOT NULL
),

enhanced_products AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key(['product_id']) }} AS product_sk,
        
        -- Natural key
        product_id,
        
        -- Product attributes
        COALESCE(NULLIF(TRIM(product_category_name), ''), 'uncategorized') AS product_category_name,
        COALESCE(product_name_lenght, 0) AS product_name_length,
        COALESCE(product_description_lenght, 0) AS product_description_length,
        COALESCE(product_photos_qty, 0) AS product_photos_qty,
        
        -- Physical dimensions
        product_weight_g,
        product_length_cm,
        product_height_cm, 
        product_width_cm,
        
        -- Calculated metrics
        CASE 
            WHEN product_length_cm > 0 AND product_height_cm > 0 AND product_width_cm > 0
            THEN (product_length_cm * product_height_cm * product_width_cm) / 1000000.0
            ELSE NULL
        END AS product_volume_cubic_meters,
        
        -- Category analysis
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
        
        -- Statistical analysis using broader context
        CASE 
            WHEN product_weight_g IS NOT NULL AND sc.stddev_weight > 0
            THEN (ABS(product_weight_g - sc.avg_weight) / sc.stddev_weight)::FLOAT
            ELSE NULL
        END AS weight_z_score,
        
        CASE 
            WHEN product_photos_qty IS NOT NULL AND sc.stddev_photos > 0  
            THEN (ABS(product_photos_qty - sc.avg_photos) / sc.stddev_photos)::FLOAT
            ELSE NULL
        END AS photos_z_score,
        
        -- Order relationship context (NEW)
        CASE WHEN product_id IN {{ get_related_product_ids() }} THEN TRUE ELSE FALSE END AS in_current_batch,
        
        -- Business flags
        CASE 
            WHEN product_weight_g > 30000 OR
                 GREATEST(product_length_cm, product_height_cm, product_width_cm) > 100
            THEN TRUE ELSE FALSE 
        END AS is_bulky_item,
        
        -- Data quality assessment
        {{ data_quality_score(quality_checks) }} AS data_quality_score,

        CASE
            WHEN product_length_cm     IS NOT NULL
            AND product_name_lenght   IS NOT NULL
            AND product_photos_qty    IS NOT NULL
            AND product_weight_g      IS NOT NULL
            AND product_width_cm      IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS has_complete_dimensions,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM source_data
    CROSS JOIN statistical_context sc
),

final AS (
    SELECT 
        product_sk,
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        product_volume_cubic_meters,
        category_group,
        weight_z_score,
        photos_z_score,
        in_current_batch,
        is_bulky_item,
        data_quality_score,
        dbt_loaded_at,
        dbt_invocation_id,
        has_complete_dimensions
    FROM enhanced_products
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

SELECT * FROM final
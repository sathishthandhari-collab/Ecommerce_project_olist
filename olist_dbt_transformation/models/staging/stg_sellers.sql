-- depends_on: {{ ref('stg_order_items') }}
{{ config(
    materialized='incremental',
    schema='DBT_OLIST_STAGING',
    on_schema_change='append_new_columns',
    tags=['staging']
) }}

{%- set quality_checks = [
    'seller_id IS NOT NULL',
    'seller_zip_code_prefix IS NOT NULL',
    'seller_city IS NOT NULL', 
    'seller_state IS NOT NULL',
    'LENGTH(seller_zip_code_prefix) = 5',
    'LENGTH(seller_state) = 2'
] -%}

WITH source_data AS (
    SELECT * FROM {{ source('src_olist_raw', 'raw_olist_sellers') }}
    where seller_id in {{get_related_seller_ids()}}
),

enhanced_sellers AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key(['seller_id']) }} AS seller_sk,
        
        -- Natural key
        seller_id,
        
        -- Standardized location data  
        LPAD(seller_zip_code_prefix, 5, '0') AS seller_zip_code_prefix,
        UPPER(TRIM(seller_city)) AS seller_city,
        UPPER(TRIM(seller_state)) AS seller_state,
        
        -- Geographic analysis
        CASE 
            WHEN seller_state IN ('SP', 'RJ', 'MG', 'RS') THEN 'Tier 1'
            WHEN seller_state IN ('PR', 'SC', 'GO', 'PE', 'BA') THEN 'Tier 2' 
            ELSE 'Tier 3'
        END AS state_business_tier,
        
        CASE 
            WHEN seller_state IN ('SP', 'RJ') THEN 'Southeast Core'
            WHEN seller_state IN ('MG', 'ES') THEN 'Southeast Extended'
            WHEN seller_state IN ('RS', 'SC', 'PR') THEN 'South'
            WHEN seller_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Central-West'
            WHEN seller_state IN ('BA', 'PE', 'CE', 'PB', 'RN', 'AL', 'SE', 'MA', 'PI') THEN 'Northeast'
            ELSE 'North'
        END AS geographic_region,
        
        -- Economic zone classification
        CASE 
            WHEN seller_city IN ('SAO PAULO', 'RIO DE JANEIRO', 'BELO HORIZONTE', 'PORTO ALEGRE', 'BRASILIA')
            THEN 'Major Metro'
            WHEN seller_state IN ('SP', 'RJ', 'RS', 'MG') AND seller_city != seller_state
            THEN 'Secondary City'
            ELSE 'Smaller Market'  
        END AS market_size,
        
        -- Business environment scoring
        CASE 
            WHEN seller_state = 'SP' THEN 5  -- Highest business activity
            WHEN seller_state IN ('RJ', 'MG', 'RS') THEN 4
            WHEN seller_state IN ('PR', 'SC', 'GO', 'PE') THEN 3
            WHEN seller_state IN ('BA', 'CE', 'DF', 'ES') THEN 2
            ELSE 1
        END AS business_environment_score,
        
        -- Logistics advantages
        CASE 
            WHEN seller_state IN ('SP', 'RJ') THEN TRUE  -- Major logistics hubs
            ELSE FALSE
        END AS has_logistics_advantage,
        
        -- Data quality flags
        CASE WHEN REGEXP_LIKE(seller_zip_code_prefix, '^[0-9]{5}$') THEN TRUE ELSE FALSE END AS valid_zip_format,
        CASE WHEN REGEXP_LIKE(seller_state, '^[A-Z]{2}$') THEN TRUE ELSE FALSE END AS valid_state_format,
        
        -- Data quality assessment  
        {{ data_quality_score(quality_checks) }} AS data_quality_score,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM source_data
),

final AS (
    SELECT 
        seller_sk,
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        state_business_tier,
        geographic_region,
        market_size,
        business_environment_score,
        has_logistics_advantage,
        valid_zip_format,
        valid_state_format,
        data_quality_score,
        dbt_loaded_at,
        dbt_invocation_id
    FROM enhanced_sellers
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

SELECT * FROM final
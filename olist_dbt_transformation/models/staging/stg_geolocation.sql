-- depends_on: {{ ref('stg_orders') }}

{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    schema='DBT_OLIST_STAGING',
    tags=['staging']
) }}

{%- set quality_checks = [
    'geolocation_zip_code_prefix IS NOT NULL',
    'geolocation_lat IS NOT NULL',
    'geolocation_lng IS NOT NULL',
    'geolocation_city IS NOT NULL',
    'geolocation_state IS NOT NULL',
    'geolocation_lat BETWEEN -35 AND 10',
    'geolocation_lng BETWEEN -75 AND -30',
    'LENGTH(geolocation_zip_code_prefix) = 5'
] -%}

WITH source_data AS (
    SELECT * FROM {{ source('src_olist_raw', 'raw_olist_geolocation') }}
),

-- Deduplicate geolocation data (keep most common city/state for each zip)
deduplicated_geo AS (
    SELECT 
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,
        COUNT(*) as occurrence_count,
        ROW_NUMBER() OVER (
            PARTITION BY geolocation_zip_code_prefix 
            ORDER BY COUNT(*) DESC, geolocation_city
        ) AS rn
    FROM source_data
    GROUP BY 1,2,3,4,5
),

enhanced_geolocation AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key(['geolocation_zip_code_prefix']) }} AS geo_sk,
        
        -- Location identifiers
        LPAD(geolocation_zip_code_prefix, 5, '0') AS zip_code_prefix,
        UPPER(TRIM(geolocation_city)) AS city,
        UPPER(TRIM(geolocation_state)) AS state,
        
        -- Coordinates
        geolocation_lat AS latitude,
        geolocation_lng AS longitude,
        
        -- Regional classifications
        CASE 
            WHEN geolocation_state IN ('SP', 'RJ', 'ES', 'MG') THEN 'Southeast'
            WHEN geolocation_state IN ('PR', 'SC', 'RS') THEN 'South'  
            WHEN geolocation_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Central-West'
            WHEN geolocation_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
            WHEN geolocation_state IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') THEN 'North'
            ELSE 'Unknown'
        END AS region,
        
        -- Urban vs Rural classification (simplified by state and coordinates)
        CASE 
            WHEN geolocation_city IN ('SAO PAULO', 'RIO DE JANEIRO', 'BRASILIA', 'SALVADOR', 'FORTALEZA')
            THEN 'Major Metropolitan'
            WHEN geolocation_state IN ('SP', 'RJ') 
            THEN 'Metropolitan Area'
            WHEN geolocation_state IN ('MG', 'RS', 'PR', 'SC', 'GO', 'PE', 'BA')
            THEN 'Urban'
            ELSE 'Rural/Small City'
        END AS urbanization_level,
        
        -- Economic zones (simplified)
        CASE 
            WHEN geolocation_state IN ('SP', 'RJ') THEN 'High Development'
            WHEN geolocation_state IN ('MG', 'RS', 'PR', 'SC') THEN 'Medium-High Development'
            WHEN geolocation_state IN ('GO', 'ES', 'PE', 'BA', 'CE') THEN 'Medium Development'
            ELSE 'Lower Development'
        END AS economic_zone,
        
        -- Coordinate validation
        CASE 
            WHEN geolocation_lat BETWEEN -35 AND 10 AND 
                 geolocation_lng BETWEEN -75 AND -30 
            THEN TRUE 
            ELSE FALSE 
        END AS valid_coordinates,
        
        -- Distance from major economic centers (Sao Paulo as reference)
        -- Simplified distance calculation using Haversine approximation
        ROUND(
            111.32 * SQRT(
                POWER(geolocation_lat - (-23.5505), 2) + 
                POWER((geolocation_lng - (-46.6333)) * COS(geolocation_lat * PI()/180), 2)
            )
        ) AS distance_from_sao_paulo_km,
        
        -- Logistics complexity
        CASE 
            WHEN geolocation_state IN ('AM', 'AC', 'RR', 'AP') THEN 'Very High'  -- Amazon region
            WHEN geolocation_state IN ('PA', 'TO', 'MA', 'PI') THEN 'High'
            WHEN geolocation_state IN ('MT', 'GO', 'MS', 'MG') THEN 'Medium'
            WHEN geolocation_state IN ('SP', 'RJ', 'ES') THEN 'Low'
            ELSE 'Medium'
        END AS logistics_complexity,
        
        -- Data quality assessment
        {{ data_quality_score(quality_checks) }} AS data_quality_score,
        
        occurrence_count,  -- How many times this zip appeared in source
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM deduplicated_geo
    WHERE rn = 1  -- Keep only the most common city/state for each zip
),

final AS (
    SELECT 
        geo_sk,
        zip_code_prefix,
        city,
        state,
        latitude,
        longitude,
        region,
        urbanization_level,
        economic_zone,
        valid_coordinates,
        distance_from_sao_paulo_km,
        logistics_complexity,
        data_quality_score,
        occurrence_count,
        dbt_loaded_at,
        dbt_invocation_id
    FROM enhanced_geolocation
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

SELECT * FROM final

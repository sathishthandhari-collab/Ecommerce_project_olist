-- EXECUTION DEPENDENCY: Ensures geolocation processing happens after core order data
-- depends_on: {{ ref('stg_orders') }}

{{ config(
    -- INCREMENTAL PROCESSING: Optimized for large geolocation datasets
    materialized='incremental',
    schema='DBT_OLIST_STAGING'          -- Dedicated staging schema
    -- NOTE: No unique_key specified due to deduplication logic in the model
) }}

{%- set quality_checks = [
    -- GEOGRAPHIC DATA VALIDATION RULES: Essential location data integrity
    'geolocation_zip_code_prefix IS NOT NULL',     -- ZIP code required for location matching
    'geolocation_lat IS NOT NULL',                 -- Latitude required for mapping
    'geolocation_lng IS NOT NULL',                 -- Longitude required for mapping
    'geolocation_city IS NOT NULL',                -- City name required for analysis
    'geolocation_state IS NOT NULL',               -- State required for regional analysis
    'geolocation_lat BETWEEN -35 AND 10',          -- Valid latitude range for Brazil
    'geolocation_lng BETWEEN -75 AND -30',         -- Valid longitude range for Brazil
    'LENGTH(geolocation_zip_code_prefix) = 5'      -- Brazilian ZIP code format validation
] -%}

WITH source_data AS (
    -- SOURCE DATA EXTRACTION: Pulls complete geolocation dataset
    SELECT * FROM {{ source('src_olist_raw', 'raw_olist_geolocation') }}
    -- NOTE: No incremental filtering - geolocation is a reference dataset
),

-- DATA DEDUPLICATION: Resolves multiple entries per ZIP code to single authoritative record
deduplicated_geo AS (
    SELECT
        geolocation_zip_code_prefix,    -- ZIP code (primary grouping key)
        geolocation_lat,                -- Latitude coordinate
        geolocation_lng,                -- Longitude coordinate
        geolocation_city,               -- City name
        geolocation_state,              -- State code
        COUNT(*) as occurrence_count,   -- Frequency of this combination in source data
        -- RANKING LOGIC: Prefer most common city/state combination for each ZIP
        ROW_NUMBER() OVER (
            PARTITION BY geolocation_zip_code_prefix
            ORDER BY COUNT(*) DESC, geolocation_city  -- Most frequent first, then alphabetical
        ) AS rn
    FROM source_data
    GROUP BY 1,2,3,4,5
),

enhanced_geolocation AS (
    SELECT
        -- SURROGATE KEY GENERATION: Creates unique identifier for geographic locations
        {{ generate_surrogate_key(['geolocation_zip_code_prefix']) }} AS geo_sk,

        -- STANDARDIZED LOCATION IDENTIFIERS: Consistent formatting for matching
        LPAD(geolocation_zip_code_prefix, 5, '0') AS zip_code_prefix,  -- Zero-pad ZIP codes
        UPPER(TRIM(geolocation_city)) AS city,                         -- Normalize city names
        UPPER(TRIM(geolocation_state)) AS state,                       -- Normalize state codes

        -- COORDINATE DATA: Geographic positioning for mapping and distance calculations
        geolocation_lat AS latitude,    -- Decimal latitude
        geolocation_lng AS longitude,   -- Decimal longitude

        -- REGIONAL CLASSIFICATION: Brazilian geographic regions for macro analysis
        CASE
            WHEN geolocation_state IN ('SP', 'RJ', 'ES', 'MG') THEN 'Southeast'        -- Economic core
            WHEN geolocation_state IN ('PR', 'SC', 'RS') THEN 'South'                  -- Industrial region
            WHEN geolocation_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Central-West'     -- Interior/capital
            WHEN geolocation_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'  -- Northeast region
            WHEN geolocation_state IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') THEN 'North'  -- Amazon region
            ELSE 'Unknown'
        END AS region,

        -- URBANIZATION CLASSIFICATION: Market size and development level indicators
        CASE
            WHEN geolocation_city IN ('SAO PAULO', 'RIO DE JANEIRO', 'BRASILIA', 'SALVADOR', 'FORTALEZA')
            THEN 'Major Metropolitan'      -- Top 5 metropolitan areas
            WHEN geolocation_state IN ('SP', 'RJ')
            THEN 'Metropolitan Area'       -- Other cities in major economic states
            WHEN geolocation_state IN ('MG', 'RS', 'PR', 'SC', 'GO', 'PE', 'BA')
            THEN 'Urban'                   -- Secondary urban centers
            ELSE 'Rural/Small City'        -- Rural and small urban areas
        END AS urbanization_level,

        -- ECONOMIC DEVELOPMENT ZONES: Investment and infrastructure level indicators
        CASE
            WHEN geolocation_state IN ('SP', 'RJ') THEN 'High Development'           -- Highest economic activity
            WHEN geolocation_state IN ('MG', 'RS', 'PR', 'SC') THEN 'Medium-High Development'  -- Strong secondary markets
            WHEN geolocation_state IN ('GO', 'ES', 'PE', 'BA', 'CE') THEN 'Medium Development'  -- Developing markets
            ELSE 'Lower Development'       -- Emerging and frontier markets
        END AS economic_zone,

        -- COORDINATE VALIDATION: Ensures coordinates fall within Brazil's boundaries
        CASE
            WHEN geolocation_lat BETWEEN -35 AND 10 AND    -- Brazil's latitude range
            geolocation_lng BETWEEN -75 AND -30            -- Brazil's longitude range
            THEN TRUE
            ELSE FALSE
        END AS valid_coordinates,

        -- DISTANCE CALCULATION: Distance from São Paulo (economic center) using Haversine approximation
        ROUND(
            111.32 * SQRT(                                 -- Convert degrees to kilometers
                POWER(geolocation_lat - (-23.5505), 2) +  -- São Paulo latitude: -23.5505
                POWER((geolocation_lng - (-46.6333)) * COS(geolocation_lat * PI()/180), 2)  -- São Paulo longitude: -46.6333, adjusted for latitude
            )
        ) AS distance_from_sao_paulo_km,

        -- LOGISTICS COMPLEXITY ASSESSMENT: Shipping difficulty based on infrastructure
        CASE
            WHEN geolocation_state IN ('AM', 'AC', 'RR', 'AP') THEN 'Very High'  -- Amazon region - remote, limited infrastructure
            WHEN geolocation_state IN ('PA', 'TO', 'MA', 'PI') THEN 'High'       -- Northern/interior - challenging logistics
            WHEN geolocation_state IN ('MT', 'GO', 'MS', 'MG') THEN 'Medium'     -- Interior - moderate challenges
            WHEN geolocation_state IN ('SP', 'RJ', 'ES') THEN 'Low'              -- Southeast - excellent infrastructure
            ELSE 'Medium'                                                         -- Default for other regions
        END AS logistics_complexity,

        -- COMPREHENSIVE DATA QUALITY ASSESSMENT: Aggregates all validation rules
        {{ data_quality_score(quality_checks) }} AS data_quality_score,

        -- DATA SOURCE QUALITY INDICATOR: Shows reliability of this geographic mapping
        occurrence_count,               -- How many times this ZIP/location combo appeared in source

        -- AUDIT METADATA: Processing lineage and tracking
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,  -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id          -- Links to specific dbt run
    FROM deduplicated_geo
    -- DEDUPLICATION FILTER: Only keeps the most authoritative record per ZIP code
    WHERE rn = 1 -- Most common city/state combination for each ZIP
),

final AS (
    -- FINAL STRUCTURED OUTPUT: Geographic dimension ready for analytics
    SELECT
        -- KEYS AND IDENTIFIERS
        geo_sk,                         -- Surrogate key for joins
        zip_code_prefix,                -- Standardized ZIP code
        
        -- LOCATION IDENTIFIERS
        city,                           -- Normalized city name
        state,                          -- Normalized state code
        latitude,                       -- Geographic latitude
        longitude,                      -- Geographic longitude
        
        -- BUSINESS CLASSIFICATIONS
        region,                         -- Brazilian region grouping
        urbanization_level,             -- Urban vs rural classification
        economic_zone,                  -- Development level indicator
        
        -- OPERATIONAL INDICATORS
        valid_coordinates,              -- Coordinate validation flag
        distance_from_sao_paulo_km,     -- Distance from economic center
        logistics_complexity,           -- Shipping difficulty indicator
        
        -- QUALITY METRICS
        data_quality_score,             -- Overall quality assessment
        occurrence_count,               -- Source data reliability indicator
        
        -- AUDIT METADATA
        dbt_loaded_at,                  -- Processing metadata
        dbt_invocation_id               -- Run metadata
    FROM enhanced_geolocation
    -- QUALITY GATE: Only includes locations meeting minimum quality standards
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

-- FINAL OUTPUT: Clean, enhanced geolocation dimension with business intelligence
SELECT * FROM final
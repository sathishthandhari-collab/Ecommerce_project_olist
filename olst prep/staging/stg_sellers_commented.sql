-- EXECUTION DEPENDENCY: Ensures this model runs after related order items are processed
-- depends_on: {{ ref('stg_order_items') }}

{{ config(
    -- INCREMENTAL CONFIGURATION: Optimizes performance for large seller datasets
    materialized='incremental'
    -- NOTE: No unique_key specified - relies on natural deduplication in source
) }}

{%- set quality_checks = [
    -- DATA COMPLETENESS RULES: Essential seller information validation
    'seller_id IS NOT NULL',                    -- Primary business identifier required
    'seller_zip_code_prefix IS NOT NULL',       -- Location data required for analytics
    'seller_city IS NOT NULL',                  -- Geographic analysis dependency
    'seller_state IS NOT NULL',                 -- Regional classification dependency
    'LENGTH(seller_zip_code_prefix) = 5',       -- Brazilian ZIP format validation
    'LENGTH(seller_state) = 2'                  -- Brazilian state code validation
] -%}

WITH source_data AS (
    -- SOURCE EXTRACTION: Retrieves raw seller data from configured source
    SELECT * FROM {{ source('src_olist_raw', 'raw_olist_sellers') }}
    -- INCREMENTAL FILTERING: Only processes sellers who have new order activity
    where seller_id in {{get_related_seller_ids()}}
),

enhanced_sellers AS (
    SELECT
        -- SURROGATE KEY CREATION: Generates stable unique identifier for dimension modeling
        {{ generate_surrogate_key(['seller_id']) }} AS seller_sk,

        -- NATURAL BUSINESS KEY: Preserves original seller identifier
        seller_id,

        -- GEOGRAPHIC DATA STANDARDIZATION: Ensures consistent location formatting
        LPAD(seller_zip_code_prefix, 5, '0') AS seller_zip_code_prefix,  -- Zero-pad ZIP codes
        UPPER(TRIM(seller_city)) AS seller_city,                         -- Normalize city names
        UPPER(TRIM(seller_state)) AS seller_state,                       -- Normalize state codes

        -- BUSINESS TIER CLASSIFICATION: Economic importance categorization
        CASE
            WHEN seller_state IN ('SP', 'RJ', 'MG', 'RS') THEN 'Tier 1'  -- Major economic centers
            WHEN seller_state IN ('PR', 'SC', 'GO', 'PE', 'BA') THEN 'Tier 2'  -- Secondary markets
            ELSE 'Tier 3'                                                  -- Emerging markets
        END AS state_business_tier,

        -- REGIONAL GROUPINGS: Geographic clustering for analysis
        CASE
            WHEN seller_state IN ('SP', 'RJ') THEN 'Southeast Core'           -- Economic powerhouses
            WHEN seller_state IN ('MG', 'ES') THEN 'Southeast Extended'       -- Extended southeast region
            WHEN seller_state IN ('RS', 'SC', 'PR') THEN 'South'              -- Industrial south
            WHEN seller_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Central-West' -- Interior/capital region
            WHEN seller_state IN ('BA', 'PE', 'CE', 'PB', 'RN', 'AL', 'SE', 'MA', 'PI') THEN 'Northeast' -- Northeast region
            ELSE 'North'                                                       -- Amazon and northern states
        END AS geographic_region,

        -- MARKET SIZE CLASSIFICATION: Urban vs rural market analysis
        CASE
            WHEN seller_city IN ('SAO PAULO', 'RIO DE JANEIRO', 'BELO HORIZONTE', 'PORTO ALEGRE', 'BRASILIA')
            THEN 'Major Metro'                  -- Top metropolitan areas
            WHEN seller_state IN ('SP', 'RJ', 'RS', 'MG') AND seller_city != seller_state
            THEN 'Secondary City'               -- Other cities in major states
            ELSE 'Smaller Market'               -- Rural and small urban areas
        END AS market_size,

        -- BUSINESS ENVIRONMENT SCORING: Economic activity ranking (1-5 scale)
        CASE
            WHEN seller_state = 'SP' THEN 5    -- São Paulo: Highest business activity
            WHEN seller_state IN ('RJ', 'MG', 'RS') THEN 4  -- Major secondary markets
            WHEN seller_state IN ('PR', 'SC', 'GO', 'PE') THEN 3  -- Tertiary markets
            WHEN seller_state IN ('BA', 'CE', 'DF', 'ES') THEN 2  -- Developing markets
            ELSE 1                              -- Emerging markets
        END AS business_environment_score,

        -- LOGISTICS ADVANTAGE FLAGS: Infrastructure and distribution benefits
        CASE
            WHEN seller_state IN ('SP', 'RJ') THEN TRUE -- Major logistics hubs with best infrastructure
            ELSE FALSE                                   -- All other locations
        END AS has_logistics_advantage,

        -- TECHNICAL DATA VALIDATION FLAGS: Format compliance checks
        CASE WHEN REGEXP_LIKE(seller_zip_code_prefix, '^[0-9]{5}$') THEN TRUE ELSE FALSE END AS valid_zip_format,
        CASE WHEN REGEXP_LIKE(seller_state, '^[A-Z]{2}$') THEN TRUE ELSE FALSE END AS valid_state_format,

        -- COMPREHENSIVE QUALITY ASSESSMENT: Aggregates all validation rules
        {{ data_quality_score(quality_checks) }} AS data_quality_score,

        -- AUDIT AND LINEAGE METADATA: Processing tracking information
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,  -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id          -- Links to specific dbt run
    FROM source_data
),

final AS (
    -- FINAL COLUMN SELECTION: Structured seller dimension output
    SELECT
        -- KEYS AND IDENTIFIERS
        seller_sk,                      -- Surrogate key for efficient joins
        seller_id,                      -- Natural business key
        
        -- STANDARDIZED GEOGRAPHIC DATA
        seller_zip_code_prefix,         -- Formatted ZIP code
        seller_city,                    -- Normalized city name
        seller_state,                   -- Normalized state code
        
        -- BUSINESS CLASSIFICATIONS
        state_business_tier,            -- Economic tier (1/2/3)
        geographic_region,              -- Regional grouping
        market_size,                    -- Urban/rural classification
        business_environment_score,     -- Economic activity score (1-5)
        has_logistics_advantage,        -- Infrastructure flag
        
        -- TECHNICAL VALIDATION FLAGS
        valid_zip_format,               -- ZIP code format compliance
        valid_state_format,             -- State code format compliance
        
        -- QUALITY AND AUDIT
        data_quality_score,             -- Overall quality metric
        dbt_loaded_at,                  -- Processing metadata
        dbt_invocation_id               -- Run metadata
    FROM enhanced_sellers
    -- QUALITY GATE: Only includes sellers meeting minimum quality standards
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

-- FINAL OUTPUT: Clean, enriched seller dimension for analytics
SELECT * FROM final
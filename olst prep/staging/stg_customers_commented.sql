-- DEPENDENCY DECLARATION: Establishes that this model depends on stg_orders
-- This ensures proper order of execution in the DAG
-- depends_on: {{ ref('stg_orders') }}

{{ config(
    -- INCREMENTAL CONFIGURATION: Sets up incremental processing to handle large datasets efficiently
    materialized='incremental',
    -- SCHEMA OVERRIDE: Places this staging table in the dedicated staging schema
    schema='DBT_OLIST_STAGING'
) }}

{%- set quality_checks = [
    -- DATA QUALITY RULES: Defines validation rules that each record must pass
    'customer_id IS NOT NULL',                     -- Primary business key validation
    'customer_unique_id IS NOT NULL',             -- Unique identifier validation
    'customer_zip_code_prefix IS NOT NULL',       -- Address completeness check
    'customer_city IS NOT NULL',                  -- Geographic data validation
    'customer_state IS NOT NULL',                 -- State information required
    'LENGTH(customer_zip_code_prefix) = 5',       -- Brazilian ZIP code format validation
    'LENGTH(customer_state) = 2'                  -- Brazilian state code format validation
] -%}

WITH source_data AS (
    -- SOURCE DATA EXTRACTION: Pulls raw customer data from the configured source
    SELECT * FROM {{ source('src_olist_raw', 'raw_olist_customers') }}
    -- INCREMENTAL FILTER: Only processes customers related to new orders to optimize performance
    WHERE customer_id IN {{ get_related_customer_ids() }}
),

enhanced_customers AS (
    SELECT
        -- SURROGATE KEY GENERATION: Creates a unique, stable identifier for dimension modeling
        {{ generate_surrogate_key(['customer_id']) }} AS customer_sk,

        -- NATURAL KEY PRESERVATION: Maintains original business identifiers
        customer_id,                                -- Order-level customer identifier
        customer_unique_id,                        -- Person-level unique identifier (handles multiple customer_ids per person)

        -- DATA STANDARDIZATION: Ensures consistent formatting across all records
        LPAD(customer_zip_code_prefix, 5, '0') AS customer_zip_code_prefix,  -- Left-pad ZIP codes with zeros
        UPPER(TRIM(customer_city)) AS customer_city,                         -- Normalize city names
        UPPER(TRIM(customer_state)) AS customer_state,                       -- Normalize state codes

        -- BUSINESS CLASSIFICATION: Creates analytical groupings for downstream analysis
        CASE
            -- TIER 1: Highest economic activity states
            WHEN customer_state IN ('SP', 'RJ', 'MG', 'RS') THEN 'Major States'
            -- TIER 2: Secondary economic centers
            WHEN customer_state IN ('PR', 'SC', 'GO', 'PE') THEN 'Secondary States'
            -- TIER 3: All other states
            ELSE 'Other States'
        END AS state_tier,

        -- REGIONAL GROUPINGS: Geographic clustering for regional analysis
        CASE
            WHEN customer_state IN ('SP', 'RJ') THEN 'Southeast'           -- Economic powerhouses
            WHEN customer_state IN ('RS', 'SC', 'PR') THEN 'South'         -- Industrial south
            WHEN customer_state IN ('MG', 'GO', 'MT', 'MS') THEN 'Central' -- Interior regions
            ELSE 'Other'                                                    -- Northern and northeastern states
        END AS region,

        -- DATA VALIDATION FLAGS: Technical quality indicators for monitoring
        CASE WHEN REGEXP_LIKE(customer_zip_code_prefix, '^[0-9]{5}$') THEN TRUE ELSE FALSE END AS valid_zip_format,
        CASE WHEN REGEXP_LIKE(customer_state, '^[A-Z]{2}$') THEN TRUE ELSE FALSE END AS valid_state_format,

        -- COMPREHENSIVE QUALITY SCORING: Aggregates all quality checks into a single score
        {{ data_quality_score(quality_checks) }} AS data_quality_score,

        -- AUDIT METADATA: Tracks data lineage and processing information
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,  -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id          -- Links to specific dbt run
    FROM source_data
),

final AS (
    -- COLUMN SELECTION: Explicitly selects columns for the final output table
    SELECT
        customer_sk,                    -- Surrogate key for joins
        customer_id,                    -- Business key
        customer_unique_id,             -- Person-level identifier
        customer_zip_code_prefix,       -- Standardized ZIP code
        customer_city,                  -- Standardized city name
        customer_state,                 -- Standardized state code
        state_tier,                     -- Business classification
        region,                         -- Geographic grouping
        valid_zip_format,               -- Technical quality flag
        valid_state_format,             -- Technical quality flag
        data_quality_score,             -- Aggregate quality metric
        dbt_loaded_at,                  -- Processing metadata
        dbt_invocation_id               -- Run metadata
    FROM enhanced_customers
    -- QUALITY FILTER: Only includes records that meet minimum quality standards
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

-- FINAL OUTPUT: Returns the cleaned, enhanced customer dimension
SELECT * FROM final
-- depends_on: {{ ref('stg_orders') }}

{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    schema='DBT_OLIST_STAGING',
    tags=['staging']
) }}

{%- set quality_checks = [
    'customer_id IS NOT NULL',
    'customer_unique_id IS NOT NULL',
    'customer_zip_code_prefix IS NOT NULL',
    'customer_city IS NOT NULL',
    'customer_state IS NOT NULL',
    'LENGTH(customer_zip_code_prefix) = 5',
    'LENGTH(customer_state) = 2'
] -%}

WITH source_data AS (
    SELECT * FROM {{ source('src_olist_raw', 'raw_olist_customers') }}
    WHERE customer_id IN {{ get_related_customer_ids() }}
),

enhanced_customers AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key(['customer_id']) }} AS customer_sk,
        
        -- Natural keys (PII handling)
        customer_id,
        customer_unique_id,
        
        -- Standardized location data
        LPAD(customer_zip_code_prefix, 5, '0') AS customer_zip_code_prefix,
        UPPER(TRIM(customer_city)) AS customer_city,
        UPPER(TRIM(customer_state)) AS customer_state,
        
        -- Geographic groupings for analysis
        CASE 
            WHEN customer_state IN ('SP', 'RJ', 'MG', 'RS') THEN 'Major States'
            WHEN customer_state IN ('PR', 'SC', 'GO', 'PE') THEN 'Secondary States'
            ELSE 'Other States'
        END AS state_tier,
        
        CASE 
            WHEN customer_state IN ('SP', 'RJ') THEN 'Southeast'
            WHEN customer_state IN ('RS', 'SC', 'PR') THEN 'South'
            WHEN customer_state IN ('MG', 'GO', 'MT', 'MS') THEN 'Central'
            ELSE 'Other'
        END AS region,
        
        -- Data quality flags
        CASE WHEN REGEXP_LIKE(customer_zip_code_prefix, '^[0-9]{5}$') THEN TRUE ELSE FALSE END AS valid_zip_format,
        CASE WHEN REGEXP_LIKE(customer_state, '^[A-Z]{2}$') THEN TRUE ELSE FALSE END AS valid_state_format,
        
        -- Data quality assessment
        {{ data_quality_score(quality_checks) }} AS data_quality_score,
        
        -- Metadata
    
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM source_data
),

final AS (
    SELECT 
        customer_sk,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        state_tier,
        region,
        valid_zip_format,
        valid_state_format,
        data_quality_score,
        dbt_loaded_at,
        dbt_invocation_id
    FROM enhanced_customers
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

SELECT * FROM final
    {% if target.name == 'dev' %}
        LIMIT {{ var('dev_sample_size') }}
    {% endif %}
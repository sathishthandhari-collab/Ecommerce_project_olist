-- depends_on: {{ ref('stg_orders') }}

{{ config(
    materialized='incremental',
    unique_key='review_sk',
    on_schema_change='append_new_columns',
    schema='DBT_OLIST_STAGING',
    tags=['staging']
) }}

{%- set quality_checks = [
    'review_id IS NOT NULL',
    'order_id IS NOT NULL', 
    'review_score BETWEEN 1 AND 5',
    'review_creation_date IS NOT NULL'
] -%}

WITH source_data AS (
    SELECT * 
    FROM {{ source('src_olist_raw', 'raw_olist_reviews') }}
    WHERE order_id IN {{ get_new_order_ids() }}
),

enhanced_reviews AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key(['review_id', 'order_id']) }} AS review_sk,
        
        -- Natural keys
        review_id,
        order_id,
        
        -- Review metrics
        review_score,
        {{ standardize_timestamp('review_creation_date') }} AS review_creation_date,
        {{ standardize_timestamp('review_answer_timestamp') }} AS review_answer_timestamp,
        
        -- Text analysis
        COALESCE(TRIM(review_comment_title), '') AS review_comment_title,
        COALESCE(TRIM(review_comment_message), '') AS review_comment_message,
        
        -- Text metrics
        LENGTH(COALESCE(review_comment_title, '')) AS title_length,
        LENGTH(COALESCE(review_comment_message, '')) AS message_length,
        
        -- Sentiment proxy analysis (basic keyword detection)
        CASE 
            WHEN review_score >= 4 THEN 'Positive'
            WHEN review_score = 3 THEN 'Neutral'
            WHEN review_score <= 2 THEN 'Negative'
        END AS sentiment_category,
        
        -- Text engagement indicators
        CASE 
            WHEN LENGTH(COALESCE(review_comment_message, '')) > 100 THEN 'Detailed'
            WHEN LENGTH(COALESCE(review_comment_message, '')) > 20 THEN 'Brief'
            WHEN LENGTH(COALESCE(review_comment_message, '')) > 0 THEN 'Minimal'
            ELSE 'No Comment'
        END AS comment_detail_level,
        
        -- Response analysis
        CASE WHEN review_answer_timestamp IS NOT NULL THEN TRUE ELSE FALSE END AS has_response,
        
        CASE 
            WHEN review_answer_timestamp IS NOT NULL 
            THEN DATEDIFF('hour', review_creation_date, review_answer_timestamp)
            ELSE NULL 
        END AS response_time_hours,
        
        -- Review quality indicators
        CASE 
            WHEN LENGTH(COALESCE(review_comment_message, '')) > 50 
                 AND review_comment_title IS NOT NULL 
                 AND review_comment_title != ''
            THEN TRUE 
            ELSE FALSE 
        END AS is_comprehensive_review,
        
        -- Business flags
        CASE WHEN review_score = 1 THEN TRUE ELSE FALSE END AS is_very_negative,
        CASE WHEN review_score = 5 THEN TRUE ELSE FALSE END AS is_very_positive,
        CASE WHEN review_score IN (1,2) THEN TRUE ELSE FALSE END AS is_dissatisfied,
        CASE WHEN review_score IN (4,5) THEN TRUE ELSE FALSE END AS is_satisfied,
        
        -- Statistical analysis
        {{ calculate_z_score('review_score') }} AS score_z_score,
        
        -- Data quality assessment
        {{ data_quality_score(quality_checks) }} AS data_quality_score,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM source_data
),

final AS (
    SELECT 
        review_sk,
        review_id,
        order_id,
        review_score,
        review_creation_date,
        review_answer_timestamp,
        review_comment_title,
        review_comment_message,
        title_length,
        message_length,
        sentiment_category,
        comment_detail_level,
        has_response,
        response_time_hours,
        is_comprehensive_review,
        is_very_negative,
        is_very_positive, 
        is_dissatisfied,
        is_satisfied,
        score_z_score,
        data_quality_score,
        dbt_loaded_at,
        dbt_invocation_id
    FROM enhanced_reviews
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

SELECT * FROM final
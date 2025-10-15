-- EXECUTION DEPENDENCY: Ensures reviews are processed after order data is available
-- depends_on: {{ ref('stg_orders') }}

{{ config(
    -- INCREMENTAL PROCESSING: Efficient handling of growing review dataset
    materialized='incremental',
    unique_key='review_sk',             -- Defines merge key for updates
    on_schema_change='append_new_columns', -- Handles schema evolution
    schema='DBT_OLIST_STAGING'          -- Dedicated staging schema
) }}

{%- set quality_checks = [
    -- REVIEW DATA VALIDATION RULES: Core review integrity requirements
    'review_id IS NOT NULL',           -- Unique review identifier required
    'order_id IS NOT NULL',            -- Order relationship required
    'review_score BETWEEN 1 AND 5',    -- Valid rating scale (1-5 stars)
    'review_creation_date IS NOT NULL' -- Review timestamp required
] -%}

WITH source_data AS (
    -- SOURCE DATA EXTRACTION: Pulls raw review data from configured source
    SELECT *
    FROM {{ source('src_olist_raw', 'raw_olist_reviews') }}
    -- INCREMENTAL FILTERING: Only processes reviews for new orders
    WHERE order_id IN {{ get_new_order_ids() }}
),

enhanced_reviews AS (
    SELECT
        -- COMPOSITE SURROGATE KEY: Unique identifier for reviews
        {{ generate_surrogate_key(['review_id', 'order_id']) }} AS review_sk,

        -- NATURAL BUSINESS KEYS: Original identifiers for traceability
        review_id,                      -- Unique review identifier
        order_id,                       -- Foreign key to orders dimension

        -- CORE REVIEW METRICS: Standardized rating and timing data
        review_score,                   -- Customer rating (1-5 scale)
        {{ standardize_timestamp('review_creation_date') }} AS review_creation_date,    -- When review was created
        {{ standardize_timestamp('review_answer_timestamp') }} AS review_answer_timestamp, -- When business responded

        -- TEXT CONTENT STANDARDIZATION: Clean and consistent text handling
        COALESCE(TRIM(review_comment_title), '') AS review_comment_title,       -- Clean title text
        COALESCE(TRIM(review_comment_message), '') AS review_comment_message,   -- Clean message text

        -- TEXT ANALYTICS METRICS: Content richness indicators
        LENGTH(COALESCE(review_comment_title, '')) AS title_length,             -- Title character count
        LENGTH(COALESCE(review_comment_message, '')) AS message_length,         -- Message character count

        -- SENTIMENT ANALYSIS: Score-based sentiment categorization
        CASE
            WHEN review_score >= 4 THEN 'Positive'     -- 4-5 stars = positive sentiment
            WHEN review_score = 3 THEN 'Neutral'       -- 3 stars = neutral sentiment
            WHEN review_score <= 2 THEN 'Negative'     -- 1-2 stars = negative sentiment
        END AS sentiment_category,

        -- TEXT ENGAGEMENT CLASSIFICATION: Comment depth analysis
        CASE
            WHEN LENGTH(COALESCE(review_comment_message, '')) > 100 THEN 'Detailed'  -- Comprehensive feedback
            WHEN LENGTH(COALESCE(review_comment_message, '')) > 20 THEN 'Brief'      -- Short feedback
            WHEN LENGTH(COALESCE(review_comment_message, '')) > 0 THEN 'Minimal'     -- Very short feedback
            ELSE 'No Comment'                                                        -- Score only
        END AS comment_detail_level,

        -- BUSINESS RESPONSE ANALYSIS: Customer service engagement tracking
        CASE WHEN review_answer_timestamp IS NOT NULL THEN TRUE ELSE FALSE END AS has_response,

        CASE
            WHEN review_answer_timestamp IS NOT NULL
            THEN DATEDIFF('hour', review_creation_date, review_answer_timestamp)    -- Response time in hours
            ELSE NULL
        END AS response_time_hours,

        -- REVIEW QUALITY INDICATORS: Content comprehensiveness assessment
        CASE
            WHEN LENGTH(COALESCE(review_comment_message, '')) > 50
            AND review_comment_title IS NOT NULL
            AND review_comment_title != ''
            THEN TRUE                           -- High-quality review with title and detailed message
            ELSE FALSE
        END AS is_comprehensive_review,

        -- BUSINESS IMPACT FLAGS: Critical satisfaction indicators
        CASE WHEN review_score = 1 THEN TRUE ELSE FALSE END AS is_very_negative,    -- Worst possible rating
        CASE WHEN review_score = 5 THEN TRUE ELSE FALSE END AS is_very_positive,    -- Best possible rating
        CASE WHEN review_score IN (1,2) THEN TRUE ELSE FALSE END AS is_dissatisfied, -- Poor experience
        CASE WHEN review_score IN (4,5) THEN TRUE ELSE FALSE END AS is_satisfied,    -- Good experience

        -- STATISTICAL ANALYSIS: Score distribution analysis
        {{ calculate_z_score('review_score') }} AS score_z_score,  -- Rating deviation from mean

        -- COMPREHENSIVE DATA QUALITY ASSESSMENT: Aggregates all validation rules
        {{ data_quality_score(quality_checks) }} AS data_quality_score,

        -- AUDIT METADATA: Processing lineage and tracking
        CURRENT_TIMESTAMP::timestamp_ntz AS dbt_loaded_at,  -- Processing timestamp
        '{{ invocation_id }}' AS dbt_invocation_id          -- Links to specific dbt run
    FROM source_data
),

final AS (
    -- FINAL STRUCTURED OUTPUT: Review fact table ready for analytics
    SELECT
        -- KEYS AND IDENTIFIERS
        review_sk,                      -- Surrogate key for joins
        review_id,                      -- Natural business key
        order_id,                       -- Foreign key to orders
        
        -- CORE REVIEW DATA
        review_score,                   -- Customer rating (1-5)
        review_creation_date,           -- Review timestamp
        review_answer_timestamp,        -- Response timestamp
        
        -- TEXT CONTENT
        review_comment_title,           -- Review title
        review_comment_message,         -- Review message
        title_length,                   -- Title length metric
        message_length,                 -- Message length metric
        
        -- SENTIMENT AND ENGAGEMENT ANALYSIS
        sentiment_category,             -- Positive/Neutral/Negative
        comment_detail_level,           -- Text engagement level
        has_response,                   -- Business response flag
        response_time_hours,            -- Response time metric
        is_comprehensive_review,        -- Quality indicator
        
        -- BUSINESS IMPACT FLAGS
        is_very_negative,               -- Critical satisfaction flag
        is_very_positive,               -- Excellence indicator
        is_dissatisfied,                -- Poor experience flag
        is_satisfied,                   -- Good experience flag
        
        -- STATISTICAL ANALYSIS
        score_z_score,                  -- Rating deviation metric
        
        -- QUALITY AND AUDIT
        data_quality_score,             -- Overall quality metric
        dbt_loaded_at,                  -- Processing metadata
        dbt_invocation_id               -- Run metadata
    FROM enhanced_reviews
    -- QUALITY GATE: Only includes reviews meeting minimum quality standards
    WHERE data_quality_score >= {{ var('data_quality_min_score') }}
)

-- FINAL OUTPUT: Clean, enriched reviews fact table with sentiment and engagement analysis
SELECT * FROM final
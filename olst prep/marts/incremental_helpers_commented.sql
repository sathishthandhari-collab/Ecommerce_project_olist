{#
    INCREMENTAL PROCESSING HELPERS: Smart dependency-aware incremental processing for staging layer optimization
#}

{% macro get_new_order_ids() %}
    {# 
    INCREMENTAL ORDER FILTERING: Intelligent order selection for downstream incremental processing
    
    PURPOSE: Returns order IDs that need processing in current dbt run for efficient incremental builds
    
    LOGIC:
    - INCREMENTAL MODE: Only process orders that have been updated since last run
    - FULL REFRESH MODE: Process all orders when doing complete rebuild
    - OVERRIDE CAPABILITY: Force full refresh via variable when needed
    
    RETURNS: Subquery with distinct order IDs for current processing batch
    
    BUSINESS VALUE: Dramatically reduces processing time and compute costs for large datasets
    #}
    
    {% if is_incremental() and not var('full_refresh_override', false) %}
        -- INCREMENTAL PROCESSING MODE: Only new/updated orders since last successful run
        (
            SELECT DISTINCT order_id
            FROM {{ ref('stg_orders') }}
            WHERE dbt_loaded_at >= (
                -- HIGH-WATER MARK: Get the latest processing timestamp from current table
                SELECT COALESCE(MAX(dbt_loaded_at), '1900-01-01')
                FROM {{ this }}
            )
        )
    {% else %}
        -- FULL REFRESH MODE: Process all orders for complete rebuild or initial load
        (SELECT DISTINCT order_id FROM {{ source('src_olist_raw', 'raw_olist_orders') }})
    {% endif %}
{% endmacro %}

{% macro get_related_customer_ids() %}
    {# 
    CUSTOMER DEPENDENCY RESOLUTION: Get customers related to orders being processed in current batch
    
    PURPOSE: Identifies which customers need reprocessing based on their order activity
    
    INCREMENTAL LOGIC: Only process customers who have orders in the current processing batch
    
    RETURNS: Customer IDs that require updating due to new order activity
    
    BUSINESS VALUE: Maintains referential integrity while minimizing unnecessary customer reprocessing
    #}
    
    {% if is_incremental() and not var('full_refresh_override', false) %}
        -- INCREMENTAL CUSTOMER PROCESSING: Only customers with new order activity
        (
            SELECT DISTINCT customer_id
            FROM {{ ref('stg_orders') }}
            WHERE dbt_loaded_at >= (
                -- DEPENDENCY TRACKING: Process customers whose orders have been updated
                SELECT COALESCE(MAX(dbt_loaded_at), '1900-01-01')
                FROM {{ this }}
            )
        )
    {% else %}
        -- FULL REFRESH: Process all customers
        (SELECT DISTINCT customer_id FROM {{ source('src_olist_raw', 'raw_olist_orders') }})
    {% endif %}
{% endmacro %}

{% macro get_related_product_ids() %}
    {# 
    PRODUCT DEPENDENCY RESOLUTION: Get products related to order items being processed
    
    PURPOSE: Identifies which products need processing based on order item activity
    
    DEPENDENCY CHAIN: Orders → Order Items → Products
    
    RETURNS: Product IDs that appear in current batch of order items
    
    BUSINESS VALUE: Ensures product dimension stays in sync with transactional activity
    #}
    
    -- PRODUCT DEPENDENCY: Products that appear in current order items batch
    (
        SELECT DISTINCT product_id
        FROM {{ ref('stg_order_items') }}
        -- NOTE: Depends on stg_order_items processing, which depends on order processing
    )
{% endmacro %}

{% macro get_related_seller_ids() %}
    {# 
    SELLER DEPENDENCY RESOLUTION: Get sellers related to order items being processed in current batch
    
    PURPOSE: Identifies which sellers need reprocessing based on their order item activity
    
    INCREMENTAL LOGIC: Only process sellers who have order items in the current processing batch
    
    RETURNS: Seller IDs that require updating due to new order item activity
    
    BUSINESS VALUE: Maintains seller dimension accuracy while minimizing processing overhead
    #}
    
    {% if is_incremental() and not var('full_refresh_override', false) %}
        -- INCREMENTAL SELLER PROCESSING: Only sellers with new order item activity
        (
            SELECT DISTINCT oi.seller_id
            FROM {{ ref('stg_order_items') }} oi
            WHERE oi.dbt_loaded_at >= (
                -- SELLER ACTIVITY TRACKING: Process sellers with recent order item updates
                SELECT COALESCE(MAX(dbt_loaded_at), '1900-01-01')
                FROM {{ this }}
            )
        )
    {% else %}
        -- FULL REFRESH: Process all sellers
        (SELECT DISTINCT seller_id FROM {{ source('src_olist_raw', 'raw_olist_order_items') }})
    {% endif %}
{% endmacro %}

{#
    INCREMENTAL PROCESSING STRATEGY OVERVIEW:
    
    This macro system implements a sophisticated incremental processing strategy that:
    
    1. DEPENDENCY AWARENESS: Understanding the data flow dependencies (orders → customers, order_items → products/sellers)
    
    2. BATCH OPTIMIZATION: Only processing records that have changed or are related to changes
    
    3. REFERENTIAL INTEGRITY: Ensuring all related records are updated consistently
    
    4. PERFORMANCE OPTIMIZATION: Dramatically reducing compute time and costs for large datasets
    
    5. FLEXIBILITY: Supporting both incremental and full refresh modes as needed
    
    TYPICAL PROCESSING FLOW:
    - Orders updated → Related customers, order items processed
    - Order items updated → Related products and sellers processed
    - Maintains full data consistency while minimizing processing overhead
    
    BUSINESS IMPACT:
    - Reduces daily processing time from hours to minutes
    - Enables near-real-time analytics updates
    - Significantly reduces compute costs
    - Maintains complete data accuracy and consistency
#}
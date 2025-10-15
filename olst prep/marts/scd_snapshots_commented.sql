{#
    SLOWLY CHANGING DIMENSIONS (SCD) SNAPSHOTS: Historical dimension tracking for data warehousing
    
    These snapshots implement SCD Type 2 patterns to track historical changes in dimension attributes
#}

{% snapshot snap_dim_customer %}
    {# 
    CUSTOMER DIMENSION SNAPSHOT: Tracks historical changes in customer attributes over time
    
    PURPOSE: Maintains complete history of customer geographic and classification changes
    
    SCD STRATEGY: Check-based detection monitors specific columns for changes
    
    TRACKED ATTRIBUTES: Geographic location and business classifications that affect analytics
    
    BUSINESS VALUE: Enables historical customer analysis and trend identification over time
    #}

    {{
        config(
            -- SNAPSHOT CONFIGURATION: SCD Type 2 implementation parameters
            target_schema='dbt_snapshots',                    -- Dedicated schema for historical data
            unique_key='customer_sk',                         -- Primary key for dimension records
            strategy='check',                                 -- Monitor specific columns for changes
            check_cols=['customer_state', 'state_tier', 'region', 'customer_city'],  -- Tracked attributes
            invalidate_hard_deletes=true,                     -- Handle deleted customers properly
            updated_at='dbt_loaded_at',                       -- Timestamp column for change detection
        )
    }}

    SELECT
        -- DIMENSIONAL KEYS: Stable identifiers for customer dimension
        customer_sk,                                         -- Surrogate key for efficient joins
        customer_id,                                         -- Natural business key
        customer_unique_id,                                  -- Alternative identifier

        -- TRACKED DIMENSION ATTRIBUTES: Geographic and business classification changes monitored
        customer_zip_code_prefix,                            -- Geographic location (granular)
        customer_city,                                       -- City-level location (SCD tracked)
        customer_state,                                      -- State-level location (SCD tracked)
        state_tier,                                          -- Business tier classification (SCD tracked)
        region,                                              -- Regional classification (SCD tracked)

        -- DATA QUALITY AND LINEAGE: Metadata for tracking data quality and processing history
        data_quality_score,                                  -- Quality assessment at snapshot time
        dbt_loaded_at,                                       -- Original processing timestamp
        dbt_invocation_id,                                   -- Processing run identifier

        -- SNAPSHOT METADATA: Historical tracking information
        CURRENT_TIMESTAMP AS snapshot_created_at             -- When this snapshot record was created

    FROM {{ ref('stg_customers') }}
{% endsnapshot %}

{% snapshot snap_dim_product %}
    {# 
    PRODUCT DIMENSION SNAPSHOT: Tracks historical changes in product characteristics and classifications
    
    PURPOSE: Maintains complete history of product attributes for inventory and catalog analysis
    
    TRACKED CHANGES: Category changes, physical characteristic updates, and business classifications
    
    BUSINESS VALUE: Enables product lifecycle analysis and category migration tracking
    #}

    {{
        config(
            -- SCD TYPE 2 CONFIGURATION: Product attribute change tracking
            target_schema='dbt_snapshots',
            unique_key='product_sk',
            strategy='check',
            -- MONITORED ATTRIBUTES: Product characteristics that affect business analysis
            check_cols=['product_category_name', 'category_group', 'is_bulky_item', 'product_weight_g', 'product_volume_cubic_meters'],
            invalidate_hard_deletes=true,
            updated_at='dbt_loaded_at',
        )
    }}

    SELECT
        -- PRODUCT IDENTIFIERS: Keys for product dimension tracking
        product_sk,                                          -- Surrogate key
        product_id,                                          -- Natural business key

        -- TRACKED PRODUCT ATTRIBUTES: Business-critical characteristics monitored for changes
        product_category_name,                               -- Primary categorization (SCD tracked)
        category_group,                                      -- High-level grouping (SCD tracked)
        product_name_length,                                 -- Content richness indicator
        product_description_length,                          -- Marketing content quality
        product_photos_qty,                                  -- Visual content quantity

        -- PHYSICAL CHARACTERISTICS: Logistics and operational attributes
        product_weight_g,                                    -- Weight for shipping (SCD tracked)
        product_length_cm,                                   -- Dimensional data
        product_height_cm,                                   -- Dimensional data
        product_width_cm,                                    -- Dimensional data
        product_volume_cubic_meters,                         -- Calculated volume (SCD tracked)

        -- BUSINESS CLASSIFICATIONS: Operational categorizations
        is_bulky_item,                                       -- Logistics complexity flag (SCD tracked)
        weight_z_score,                                      -- Statistical outlier indicator
        photos_z_score,                                      -- Content quality indicator

        -- METADATA AND LINEAGE: Quality and processing tracking
        data_quality_score,                                  -- Data quality at snapshot time
        dbt_loaded_at,                                       -- Processing timestamp
        dbt_invocation_id,                                   -- Run identifier
        
        CURRENT_TIMESTAMP AS snapshot_created_at             -- Historical record creation time

    FROM {{ ref('stg_products') }}
{% endsnapshot %}

{% snapshot snap_dim_seller %}
    {# 
    SELLER DIMENSION SNAPSHOT: Tracks historical changes in seller business environment and classifications
    
    PURPOSE: Maintains seller performance and business context history for marketplace analytics
    
    TRACKED CHANGES: Geographic relocations, business tier changes, and environmental score updates
    
    BUSINESS VALUE: Enables seller development tracking and marketplace evolution analysis
    #}

    {{
        config(
            -- SELLER CHANGE TRACKING CONFIGURATION
            target_schema='dbt_snapshots',
            unique_key='seller_sk',
            strategy='check',
            -- BUSINESS-CRITICAL SELLER ATTRIBUTES: Changes that affect marketplace analysis
            check_cols=['seller_state', 'state_business_tier', 'geographic_region', 'business_environment_score', 'seller_city'],
            invalidate_hard_deletes=true,
            updated_at='dbt_loaded_at',
        )
    }}

    SELECT
        -- SELLER IDENTIFIERS: Dimension keys
        seller_sk,                                           -- Surrogate key
        seller_id,                                           -- Natural business key

        -- TRACKED SELLER ATTRIBUTES: Geographic and business environment changes
        seller_zip_code_prefix,                              -- Precise location
        seller_city,                                         -- City location (SCD tracked)
        seller_state,                                        -- State location (SCD tracked)
        state_business_tier,                                 -- Economic tier classification (SCD tracked)
        geographic_region,                                   -- Regional classification (SCD tracked)
        business_environment_score,                          -- Economic environment rating (SCD tracked)
        has_logistics_advantage,                             -- Infrastructure advantage flag

        -- QUALITY AND LINEAGE METADATA
        data_quality_score,                                  -- Quality assessment
        dbt_loaded_at,                                       -- Processing timestamp
        dbt_invocation_id,                                   -- Processing run ID
        
        CURRENT_TIMESTAMP AS snapshot_created_at             -- Snapshot creation time

    FROM {{ ref('stg_sellers') }}
{% endsnapshot %}

{#
    SCD SNAPSHOT STRATEGY OVERVIEW:
    
    These snapshots implement a comprehensive SCD Type 2 strategy for key business dimensions:
    
    1. CUSTOMER GEOGRAPHY TRACKING: Monitor customer geographic changes that affect regional analysis
    
    2. PRODUCT EVOLUTION TRACKING: Track product category changes and physical characteristic updates
    
    3. SELLER BUSINESS ENVIRONMENT TRACKING: Monitor seller location and business context changes
    
    4. AUTOMATED CHANGE DETECTION: Check-based strategy automatically detects and records changes
    
    5. COMPLETE HISTORICAL PRESERVATION: Maintains full history with valid_from/valid_to dates
    
    BUSINESS BENEFITS:
    - Historical trend analysis: "How has our customer base shifted geographically?"
    - Product lifecycle tracking: "When did this product change categories?"
    - Seller marketplace evolution: "How has seller distribution changed over time?"
    - Compliance and auditing: Complete audit trail of all dimensional changes
    - Time-travel analytics: Query data "as it was" at any historical point
    
    TECHNICAL IMPLEMENTATION:
    - Efficient storage with only tracked columns monitored for changes
    - Proper handling of deleted records with invalidate_hard_deletes
    - Timestamp-based change detection for reliable incremental processing
    - Rich metadata preservation for debugging and lineage tracking
    
    QUERY PATTERNS ENABLED:
    - Point-in-time analysis: Customer segments as of specific dates
    - Change impact analysis: Effect of product re-categorization on sales
    - Historical cohort analysis: Customer behavior before/after geographic moves
    - Marketplace evolution: Seller geographic expansion patterns over time
#}
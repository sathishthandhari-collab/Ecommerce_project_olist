{% snapshot snap_dim_seller %}

{{
  config(
    target_schema='dbt_snapshots',
    unique_key='seller_sk',
    strategy='check',
    check_cols=['seller_state', 'state_business_tier', 'geographic_region', 'business_environment_score', 'seller_city'],
    invalidate_hard_deletes=true,
    updated_at='dbt_loaded_at',
  )
}}

SELECT
  -- Surrogate key
  seller_sk,
  seller_id,
  
  -- Tracked seller attributes (SCD Type 2)
  seller_zip_code_prefix,
  seller_city,
  seller_state,
  state_business_tier,
  geographic_region,
  business_environment_score,
  has_logistics_advantage,
  
  -- Quality indicators
  data_quality_score,
  dbt_loaded_at,
  dbt_invocation_id,
  
  -- Snapshot metadata
  CURRENT_TIMESTAMP AS snapshot_created_at

FROM {{ ref('stg_sellers') }}

{% endsnapshot %}
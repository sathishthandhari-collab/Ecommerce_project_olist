{% snapshot snap_dim_customer %}

{{
  config(
    target_schema='dbt_snapshots',
    unique_key='customer_sk',
    strategy='check',
    check_cols=['customer_state', 'state_tier', 'region', 'customer_city'],
    invalidate_hard_deletes=true,
    updated_at='dbt_loaded_at',
  )
}}

SELECT
  -- Surrogate key for dimension
  customer_sk,
  customer_id,
  customer_unique_id,
  
  -- Tracked dimension attributes (SCD Type 2)
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  state_tier,
  region,
  
  -- Metadata for tracking
  data_quality_score,
  dbt_loaded_at,
  dbt_invocation_id,
  
  -- Snapshot metadata
  CURRENT_TIMESTAMP AS snapshot_created_at

FROM {{ ref('stg_customers') }}

{% endsnapshot %}
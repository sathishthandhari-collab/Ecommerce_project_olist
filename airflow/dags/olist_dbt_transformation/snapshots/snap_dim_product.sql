{% snapshot snap_dim_product %}

{{
  config(
    target_schema='dbt_snapshots',
    unique_key='product_sk',
    strategy='check',
    check_cols=['product_category_name', 'category_group', 'is_bulky_item', 'product_weight_g', 'product_volume_cubic_meters'],
    invalidate_hard_deletes=true,
    updated_at='dbt_loaded_at',
  )
}}

SELECT
  -- Surrogate key
  product_sk,
  product_id,
  
  -- Tracked product attributes (SCD Type 2)
  product_category_name,
  category_group,
  product_name_length,
  product_description_length,
  product_photos_qty,
  
  -- Physical characteristics
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm,
  product_volume_cubic_meters,
  
  -- Business classifications
  is_bulky_item,
  weight_z_score,
  photos_z_score,
  
  -- Quality tracking
  data_quality_score,
  dbt_loaded_at,
  dbt_invocation_id,
  
  -- Snapshot metadata
  CURRENT_TIMESTAMP AS snapshot_created_at

FROM {{ ref('stg_products') }}

{% endsnapshot %}
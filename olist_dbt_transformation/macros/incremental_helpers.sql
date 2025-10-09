{# Get new order IDs from current batch #}
{% macro get_new_order_ids() %}
    {% if is_incremental() and not var('full_refresh_override', false) %}
    (
      SELECT DISTINCT order_id 
      FROM {{ ref('stg_orders') }}
      WHERE dbt_loaded_at >= (
        SELECT COALESCE(MAX(dbt_loaded_at), '1900-01-01') 
        FROM {{ this }}
      )
    )
  {% else %}
    {# Full refresh mode - process all orders #}
    (SELECT DISTINCT order_id FROM {{ source('src_olist_raw', 'raw_olist_orders') }})
  {% endif %}
{% endmacro %}


{# Get customer IDs related to new orders #}
{% macro get_related_customer_ids() %}
    {% if is_incremental() and not var('full_refresh_override', false) %}
    (
      SELECT DISTINCT customer_id
      FROM {{ ref('stg_orders') }} 
      WHERE dbt_loaded_at >= (
        SELECT COALESCE(MAX(dbt_loaded_at), '1900-01-01')
        FROM {{ this }}
      )
    )
  {% else %}
    (SELECT DISTINCT customer_id FROM {{ source('src_olist_raw', 'raw_olist_orders') }})
  {% endif %}  
{% endmacro %}


{# Get product IDs related to new orders #}
{% macro get_related_product_ids() %}
    (
      SELECT DISTINCT product_id
      FROM {{ ref('stg_order_items') }}
      )

{% endmacro %}


{# Get seller IDs related to new orders #}
{% macro get_related_seller_ids() %}  
    {% if is_incremental() and not var('full_refresh_override', false) %}
    (
      SELECT DISTINCT oi.seller_id
      FROM {{ ref('stg_order_items') }} oi
      WHERE oi.dbt_loaded_at >= (
        SELECT COALESCE(MAX(dbt_loaded_at), '1900-01-01')
        FROM {{ this }}
      )
    )
  {% else %}
    (SELECT DISTINCT seller_id FROM {{ source('src_olist_raw', 'raw_olist_order_items') }})
  {% endif %}
{% endmacro %}


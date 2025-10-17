{{ config(
    materialized='incremental',
    unique_key='sales_analysis_sk',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    cluster_by=['analysis_month', 'customer_state'],
    schema = "dbt_olist_int",
    tags = ['intermediate', 'sales_analysis']
) }}

WITH orders_base AS (
    SELECT 
        o.order_sk,
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.delivery_days,
        o.is_on_time_delivery,
        o.is_cancelled,
        
        -- Time dimensions
        DATE_TRUNC('month', o.order_purchase_timestamp) AS analysis_month,
        EXTRACT(year FROM o.order_purchase_timestamp) AS analysis_year,
        EXTRACT(quarter FROM o.order_purchase_timestamp) AS analysis_quarter,
        EXTRACT(month FROM o.order_purchase_timestamp) AS month_number,
        TO_CHAR(o.order_purchase_timestamp, 'YYYY-MM') AS year_month,
        DAYNAME(o.order_purchase_timestamp) AS day_of_week,
        EXTRACT(hour FROM o.order_purchase_timestamp) AS purchase_hour
    FROM {{ ref('stg_orders') }} o
    WHERE o.order_status NOT IN ('unavailable')
    
    {% if is_incremental() %}
    AND o.dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
),

order_financial_metrics AS (
    SELECT 
        ob.*,
        s.seller_id,
        
        -- Customer geographic dimensions
        c.customer_state,
        c.region AS customer_region,
        c.state_tier AS customer_state_tier,
        
        -- Seller geographic dimensions  
        s.seller_state,
        s.geographic_region AS seller_region,
        s.state_business_tier AS seller_state_tier,
        s.business_environment_score,
        
        -- Product dimensions
        pr.product_category_name,
        pr.category_group,
        pr.is_bulky_item,
        
        -- Financial aggregations per order
        SUM(oi.total_item_value) AS total_order_value,
        SUM(oi.item_price) AS total_product_revenue,
        SUM(oi.freight_value) AS total_shipping_revenue,
        AVG(oi.total_item_value) AS avg_item_value,
        COUNT(DISTINCT oi.product_id) AS unique_products_in_order,
        COUNT(DISTINCT oi.seller_id) AS unique_sellers_in_order,
        COUNT(oi.order_item_id) AS total_items_in_order,
        
        -- Payment context
        SUM(p.payment_value) AS total_payment_value,
        AVG(p.payment_installments) AS avg_installments,
        COUNT(DISTINCT p.payment_type) AS payment_methods_used,
        MAX(CASE WHEN p.payment_type = 'credit_card' THEN 1 ELSE 0 END) AS used_credit_card,
        MAX(CASE WHEN p.payment_risk_category = 'High Risk' THEN 1 ELSE 0 END) AS has_high_risk_payment,
        
        -- Order complexity indicators
        CASE 
            WHEN COUNT(DISTINCT oi.seller_id) > 1 THEN 'Multi-Seller'
            ELSE 'Single-Seller'
        END AS order_complexity,
        
        CASE 
            WHEN COUNT(DISTINCT pr.category_group) > 3 THEN 'High Diversity'
            WHEN COUNT(DISTINCT pr.category_group) > 1 THEN 'Medium Diversity'
            ELSE 'Single Category'
        END AS product_diversity
        
    FROM orders_base ob
    LEFT JOIN {{ ref('stg_customers') }} c ON ob.customer_id = c.customer_id
    LEFT JOIN {{ ref('stg_order_items') }} oi ON ob.order_id = oi.order_id
    LEFT JOIN {{ ref('stg_sellers') }} s ON oi.seller_id = s.seller_id
    LEFT JOIN {{ ref('stg_products') }} pr ON oi.product_id = pr.product_id
    LEFT JOIN {{ ref('stg_payments') }} p ON ob.order_id = p.order_id
    
    GROUP BY 
        ob.order_sk, ob.order_id, ob.customer_id, ob.order_status, ob.order_purchase_timestamp,
        ob.delivery_days, ob.is_on_time_delivery, ob.is_cancelled, ob.analysis_month,
        ob.analysis_year, ob.analysis_quarter, ob.month_number, ob.year_month,
        s.seller_id, 
        ob.day_of_week, ob.purchase_hour, c.customer_state, c.region, c.state_tier,
        s.seller_state, s.geographic_region, s.state_business_tier, s.business_environment_score,
        pr.product_category_name, pr.category_group, pr.is_bulky_item
),

-- Monthly aggregations by state
monthly_state_analysis AS (
    SELECT 
        analysis_month,
        analysis_year,
        analysis_quarter,
        customer_state,
        customer_region,
        customer_state_tier,
        
        -- Volume metrics
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(total_items_in_order) AS total_items_sold,
        
        -- Financial metrics
        SUM(total_order_value) AS total_gmv,
        SUM(total_product_revenue) AS total_product_revenue,
        SUM(total_shipping_revenue) AS total_shipping_revenue,
        AVG(total_order_value) AS avg_order_value,
        MEDIAN(total_order_value) AS median_order_value,
        STDDEV(total_order_value) AS order_value_volatility,
        
        -- Customer behavior
        AVG(unique_products_in_order) AS avg_products_per_order,
        AVG(unique_sellers_in_order) AS avg_sellers_per_order,
        AVG(avg_installments) AS avg_installments_used,
        
        -- Performance metrics
        AVG(CASE WHEN is_on_time_delivery THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
        AVG(delivery_days) AS avg_delivery_days,
        AVG(CASE WHEN is_cancelled THEN 1.0 ELSE 0.0 END) AS cancellation_rate,
        
        -- Payment insights
        AVG(CASE WHEN used_credit_card = 1 THEN 1.0 ELSE 0.0 END) AS credit_card_usage_rate,
        AVG(CASE WHEN has_high_risk_payment = 1 THEN 1.0 ELSE 0.0 END) AS high_risk_payment_rate,
        
        -- Order complexity
        AVG(CASE WHEN order_complexity = 'Multi-Seller' THEN 1.0 ELSE 0.0 END) AS multi_seller_order_rate,
        AVG(CASE WHEN product_diversity = 'High Diversity' THEN 1.0 ELSE 0.0 END) AS high_diversity_order_rate
        
    FROM order_financial_metrics
    WHERE total_order_value > 0 AND NOT is_cancelled
    GROUP BY analysis_month, analysis_year, analysis_quarter, customer_state, customer_region, customer_state_tier
),

-- Monthly aggregations by product category  
monthly_category_analysis AS (
    SELECT 
        analysis_month,
        analysis_year,
        analysis_quarter,
        product_category_name,
        category_group,
        
        -- Volume metrics
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(total_items_in_order) AS total_items_sold,
        
        -- Financial metrics
        SUM(total_order_value) AS total_gmv,
        SUM(total_product_revenue) AS total_product_revenue,
        SUM(total_shipping_revenue) AS total_shipping_revenue,
        AVG(total_order_value) AS avg_order_value,
        
        -- Category-specific insights
        AVG(CASE WHEN is_bulky_item THEN 1.0 ELSE 0.0 END) AS bulky_item_rate,
        AVG(unique_sellers_in_order) AS avg_sellers_per_order,
        AVG(delivery_days) AS avg_delivery_days,
        AVG(CASE WHEN is_on_time_delivery THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
        
        -- Customer acquisition
        COUNT(DISTINCT CASE WHEN analysis_month = (
            SELECT MIN(analysis_month) 
            FROM order_financial_metrics ofm2 
            WHERE ofm2.customer_id = order_financial_metrics.customer_id
        ) THEN customer_id END) AS new_customers_acquired,
        
        -- Market share calculation (within month)
        SUM(total_order_value) / SUM(SUM(total_order_value)) OVER (PARTITION BY analysis_month) AS category_market_share
        
    FROM order_financial_metrics
    WHERE total_order_value > 0 AND NOT is_cancelled AND product_category_name IS NOT NULL
    GROUP BY analysis_month, analysis_year, analysis_quarter, product_category_name, category_group
),

-- Monthly aggregations by seller region
monthly_seller_region_analysis AS (
    SELECT 
        analysis_month,
        analysis_year,
        analysis_quarter,
        seller_region,
        seller_state_tier,
        
        -- Seller performance metrics
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS customers_served,
        COUNT(DISTINCT seller_id) AS active_sellers,
        SUM(total_order_value) AS total_gmv,
        AVG(total_order_value) AS avg_order_value,
        
        -- Cross-region analysis
        COUNT(DISTINCT CASE WHEN customer_region != seller_region THEN order_id END) AS cross_region_orders,
        COUNT(DISTINCT CASE WHEN customer_state != seller_state THEN order_id END) AS cross_state_orders,
        
        -- Seller region efficiency
        AVG(delivery_days) AS avg_delivery_days,
        AVG(CASE WHEN is_on_time_delivery THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
        AVG(business_environment_score) AS avg_business_environment_score,
        
        -- Revenue concentration
        SUM(total_order_value) / COUNT(DISTINCT seller_id) AS revenue_per_seller
        
    FROM order_financial_metrics ofm
    WHERE total_order_value > 0 AND NOT is_cancelled AND seller_region IS NOT NULL
    GROUP BY analysis_month, analysis_year, analysis_quarter, seller_region, seller_state_tier
),

-- Temporal analysis (day of week, hour patterns)
temporal_sales_patterns AS (
    SELECT 
        analysis_month,
        day_of_week,
        purchase_hour,
        
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_order_value) AS total_gmv,
        AVG(total_order_value) AS avg_order_value,
        
        -- Temporal patterns
        CASE 
            WHEN day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        
        CASE 
            WHEN purchase_hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN purchase_hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN purchase_hour BETWEEN 18 AND 22 THEN 'Evening'
            ELSE 'Night'
        END AS time_period,
        
        -- Weekend vs weekday performance
        AVG(CASE WHEN day_of_week IN ('Saturday', 'Sunday') THEN total_order_value ELSE 0 END) AS weekend_avg_order_value,
        AVG(CASE WHEN day_of_week NOT IN ('Saturday', 'Sunday') THEN total_order_value ELSE 0 END) AS weekday_avg_order_value
        
    FROM order_financial_metrics
    WHERE total_order_value > 0 AND NOT is_cancelled
    GROUP BY analysis_month, day_of_week, purchase_hour
),

-- Comprehensive sales analysis combining all dimensions
comprehensive_sales_analysis AS (
    SELECT 
        -- Generate unique key for each analysis record
        {{ generate_surrogate_key(['analysis_month', 'customer_state', 'customer_region', 'product_category_name', 'seller_region']) }} AS sales_analysis_sk,
        
        -- Time dimensions
        ofm.analysis_month,
        ofm.analysis_year,
        ofm.analysis_quarter,
        ofm.month_number,
        ofm.year_month,
        
        -- Geographic dimensions
        ofm.customer_state,
        ofm.customer_region,
        ofm.customer_state_tier,
        ofm.seller_state,
        ofm.seller_region,
        ofm.seller_state_tier,
        
        -- Product dimensions
        ofm.product_category_name,
        ofm.category_group,
        
        -- Aggregated metrics
        COUNT(DISTINCT ofm.order_id) AS total_orders,
        COUNT(DISTINCT ofm.customer_id) AS unique_customers,
        SUM(ofm.total_items_in_order) AS total_items_sold,
        
        -- Financial metrics
        SUM(ofm.total_order_value) AS total_gmv,
        SUM(ofm.total_product_revenue) AS total_product_revenue,
        SUM(ofm.total_shipping_revenue) AS total_shipping_revenue,
        AVG(ofm.total_order_value) AS avg_order_value,
        MEDIAN(ofm.total_order_value) AS median_order_value,
        STDDEV(ofm.total_order_value) AS order_value_volatility,
        
        -- Performance metrics
        AVG(CASE WHEN ofm.is_on_time_delivery THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
        AVG(ofm.delivery_days) AS avg_delivery_days,
        AVG(CASE WHEN ofm.is_cancelled THEN 1.0 ELSE 0.0 END) AS cancellation_rate,
        
        -- Customer behavior
        AVG(ofm.unique_products_in_order) AS avg_products_per_order,
        AVG(ofm.unique_sellers_in_order) AS avg_sellers_per_order,
        AVG(ofm.avg_installments) AS avg_installments_used,
        
        -- Market insights
        SUM(ofm.total_order_value) / SUM(SUM(ofm.total_order_value)) OVER (PARTITION BY ofm.analysis_month) AS market_share_by_month,
        SUM(ofm.total_order_value) / SUM(SUM(ofm.total_order_value)) OVER (PARTITION BY ofm.analysis_month, ofm.customer_region) AS regional_market_share,
        
        -- Cross-regional trade
        COUNT(DISTINCT CASE WHEN ofm.customer_region != ofm.seller_region THEN ofm.order_id END) AS cross_region_orders,
        AVG(CASE WHEN ofm.customer_region != ofm.seller_region THEN ofm.delivery_days END) AS cross_region_delivery_days,
        
        -- Business environment impact
        AVG(ofm.business_environment_score) AS avg_business_environment_score,
        
        -- Order complexity analysis
        AVG(CASE WHEN ofm.order_complexity = 'Multi-Seller' THEN 1.0 ELSE 0.0 END) AS multi_seller_order_rate,
        AVG(CASE WHEN ofm.product_diversity = 'High Diversity' THEN 1.0 ELSE 0.0 END) AS high_diversity_order_rate,
        
        -- Payment method analysis
        AVG(CASE WHEN ofm.used_credit_card = 1 THEN 1.0 ELSE 0.0 END) AS credit_card_usage_rate,
        AVG(CASE WHEN ofm.has_high_risk_payment = 1 THEN 1.0 ELSE 0.0 END) AS high_risk_payment_rate,
        
        -- Growth metrics (Month-over-Month comparison)
        LAG(COUNT(DISTINCT ofm.order_id), 1) OVER (
            PARTITION BY ofm.customer_state, ofm.product_category_name 
            ORDER BY ofm.analysis_month
        ) AS orders_previous_month,
        
        LAG(SUM(ofm.total_order_value), 1) OVER (
            PARTITION BY ofm.customer_state, ofm.product_category_name 
            ORDER BY ofm.analysis_month
        ) AS gmv_previous_month
        
    FROM order_financial_metrics ofm
    WHERE ofm.total_order_value > 0 AND NOT ofm.is_cancelled
    GROUP BY 
        ofm.analysis_month, ofm.analysis_year, ofm.analysis_quarter, ofm.month_number, ofm.year_month,
        ofm.customer_state, ofm.customer_region, ofm.customer_state_tier,
        ofm.seller_state, ofm.seller_region, ofm.seller_state_tier,
        ofm.product_category_name, ofm.category_group
),

-- Final analysis with calculated growth rates
final_sales_analysis AS (
    SELECT 
        *,
        
        -- Calculate growth rates
        CASE 
            WHEN orders_previous_month > 0 
            THEN ((total_orders - orders_previous_month)::FLOAT / orders_previous_month) * 100
            ELSE NULL 
        END AS orders_mom_growth_pct,
        
        CASE 
            WHEN gmv_previous_month > 0 
            THEN ((total_gmv - gmv_previous_month)::FLOAT / gmv_previous_month) * 100
            ELSE NULL 
        END AS gmv_mom_growth_pct,
        
        -- Performance tier classification
        CASE 
            WHEN on_time_delivery_rate >= 0.9 THEN 'Excellent Delivery'
            WHEN on_time_delivery_rate >= 0.8 THEN 'Good Delivery'
            WHEN on_time_delivery_rate >= 0.7 THEN 'Average Delivery'
            ELSE 'Poor Delivery'
        END AS delivery_performance_tier,
        
        -- Market size classification
        CASE 
            WHEN total_gmv >= 100000 THEN 'Large Market'
            WHEN total_gmv >= 50000 THEN 'Medium Market'
            WHEN total_gmv >= 10000 THEN 'Small Market'
            ELSE 'Micro Market'
        END AS market_size_tier,
        
        -- Customer engagement level
        CASE 
            WHEN avg_products_per_order >= 3 THEN 'High Engagement'
            WHEN avg_products_per_order >= 2 THEN 'Medium Engagement'
            ELSE 'Low Engagement'
        END AS customer_engagement_tier,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_tz AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM comprehensive_sales_analysis
)

SELECT * FROM final_sales_analysis
{% if target.name == 'dev' %}
    LIMIT {{ var('dev_sample_size_agg', 10000) }}
{% endif %}
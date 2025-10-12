{{ config(
    materialized='table',
    cluster_by=['report_date', 'financial_category'],
    contract={
        "enforced": true
    },
    schema='dbt_olist_marts',
    tags=['marts']
) }}

WITH time_spine AS (
    {{ generate_executive_time_spine('2016-01-01', 'CURRENT_DATE', 'day') }}
),

-- Core financial transactions with enriched context
financial_base AS (
    SELECT 
        o.order_purchase_timestamp::date AS transaction_date,
        o.order_id,
        o.customer_id,
        o.order_status,
        o.is_cancelled,
        
        -- Customer context
        c360.customer_state,
        c360.region,
        c360.lifecycle_stage,
        c360.customer_sophistication,
        c360.total_orders AS customer_lifetime_orders,
        
        -- Order financial metrics
        SUM(oi.total_item_value)::float AS gross_merchandise_value,
        SUM(oi.item_price)::float AS product_revenue,
        SUM(oi.freight_value)::float AS shipping_revenue,
        COUNT(DISTINCT oi.product_id) AS unique_products_ordered,
        COUNT(DISTINCT oi.seller_id) AS unique_sellers_in_order,
        
        -- Payment characteristics  
        SUM(p.payment_value) AS total_payment_value,
        AVG(p.payment_installments) AS avg_installments,
        COUNT(DISTINCT p.payment_type) AS payment_methods_used,
        
        -- Payment method breakdown
        SUM(CASE WHEN p.payment_type = 'credit_card' THEN p.payment_value ELSE 0 END) AS credit_card_volume,
        SUM(CASE WHEN p.payment_type = 'debit_card' THEN p.payment_value ELSE 0 END) AS debit_card_volume,
        SUM(CASE WHEN p.payment_type = 'boleto' THEN p.payment_value ELSE 0 END) AS boleto_volume,
        SUM(CASE WHEN p.payment_type = 'voucher' THEN p.payment_value ELSE 0 END) AS voucher_volume,
        
        -- Risk indicators
        SUM(CASE WHEN p.payment_risk_category = 'High Risk' THEN p.payment_value ELSE 0 END) AS high_risk_payment_volume,
        COUNT(CASE WHEN oa.anomaly_severity = 'Critical' THEN 1 END) AS critical_anomalies_count,
        
        -- Seller economics
        s.seller_state,
        s.geographic_region AS seller_region,
        shs.composite_health_score,
        shs.health_tier AS seller_health_tier
        
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id  
    LEFT JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
    LEFT JOIN {{ ref('int_customer_360') }} c360 ON o.customer_id = c360.customer_id
    LEFT JOIN {{ ref('stg_sellers') }} s ON oi.seller_id = s.seller_id
    LEFT JOIN {{ ref('int_seller_health_score') }} shs ON oi.seller_id = shs.seller_id
    LEFT JOIN {{ ref('int_order_anomalies') }} oa ON o.order_sk = oa.order_sk
    
    WHERE o.order_purchase_timestamp >= '2016-01-01'
      AND o.order_status NOT IN ('unavailable', 'canceled')
    
    GROUP BY 
        o.order_purchase_timestamp::date, o.order_id, o.customer_id, o.order_status, o.is_cancelled,
        c360.customer_state, c360.region, c360.lifecycle_stage, c360.customer_sophistication, c360.total_orders,
        s.seller_state, s.geographic_region, shs.composite_health_score, shs.health_tier
),

-- Daily financial performance aggregations
daily_financial_metrics AS (
    SELECT 
        fb.transaction_date AS report_date,
        
        -- Revenue & GMV metrics
        SUM(fb.gross_merchandise_value) AS daily_gmv,
        SUM(fb.product_revenue) AS daily_product_revenue,
        SUM(fb.shipping_revenue) AS daily_shipping_revenue,
        SUM(fb.total_payment_value) AS daily_payment_volume,
        
        -- Order metrics
        COUNT(DISTINCT fb.order_id) AS daily_orders,
        COUNT(DISTINCT fb.customer_id) AS daily_active_customers,
        COUNT(DISTINCT CASE WHEN fb.customer_lifetime_orders = 1 THEN fb.customer_id END) AS daily_new_customers,
        
        -- Average order metrics
        AVG(fb.gross_merchandise_value) AS avg_order_value,
        AVG(fb.unique_products_ordered) AS avg_products_per_order,
        AVG(fb.avg_installments) AS avg_payment_installments,
        
        -- Payment method performance
        SUM(fb.credit_card_volume) AS daily_credit_card_volume,
        SUM(fb.debit_card_volume) AS daily_debit_card_volume,
        SUM(fb.boleto_volume) AS daily_boleto_volume,
        SUM(fb.voucher_volume) AS daily_voucher_volume,
        
        -- Payment mix percentages
        (SUM(fb.credit_card_volume) / NULLIF(SUM(fb.total_payment_value), 0)) * 100 AS credit_card_mix_pct,
        (SUM(fb.debit_card_volume) / NULLIF(SUM(fb.total_payment_value), 0)) * 100 AS debit_card_mix_pct,
        (SUM(fb.boleto_volume) / NULLIF(SUM(fb.total_payment_value), 0)) * 100 AS boleto_mix_pct,
        
        -- Risk metrics
        SUM(fb.high_risk_payment_volume) AS daily_high_risk_volume,
        (SUM(fb.high_risk_payment_volume) / NULLIF(SUM(fb.total_payment_value), 0)) * 100 AS high_risk_volume_pct,
        SUM(fb.critical_anomalies_count) AS daily_critical_anomalies,
        
        -- Geographic distribution
        COUNT(DISTINCT fb.customer_state) AS states_with_orders,
        COUNT(DISTINCT fb.seller_state) AS seller_states_active,
        
        -- Customer lifecycle mix
        COUNT(DISTINCT CASE WHEN fb.lifecycle_stage = 'New Customer' THEN fb.customer_id END) AS new_customer_orders,
        COUNT(DISTINCT CASE WHEN fb.lifecycle_stage = 'Repeat Customer' THEN fb.customer_id END) AS repeat_customer_orders,
        COUNT(DISTINCT CASE WHEN fb.lifecycle_stage = 'VIP Customer' THEN fb.customer_id END) AS vip_customer_orders
        
    FROM financial_base fb
    GROUP BY fb.transaction_date
),

-- Weekly and monthly rollups with financial ratios
weekly_financial_performance AS (
    SELECT 
        DATE_TRUNC('week', dfm.report_date) AS report_date,
        'Weekly' AS reporting_period,
        'Financial Performance' AS financial_category,
        
        -- Revenue metrics (weekly aggregation)
        SUM(dfm.daily_gmv)::float AS total_gmv,
        SUM(dfm.daily_product_revenue)::float AS total_product_revenue,  
        SUM(dfm.daily_shipping_revenue)::float AS total_shipping_revenue,
        AVG(dfm.daily_gmv)::float AS avg_daily_gmv,
        
        -- Growth metrics (week-over-week)
        SUM(dfm.daily_gmv) - LAG(SUM(dfm.daily_gmv), 1) OVER (ORDER BY DATE_TRUNC('week', dfm.report_date)) AS gmv_wow_change,
        ((SUM(dfm.daily_gmv) - LAG(SUM(dfm.daily_gmv), 1) OVER (ORDER BY DATE_TRUNC('week', dfm.report_date))) / 
         NULLIF(LAG(SUM(dfm.daily_gmv), 1) OVER (ORDER BY DATE_TRUNC('week', dfm.report_date)), 0)) * 100::float AS gmv_wow_change_pct,
        
        -- Order economics
        SUM(dfm.daily_orders) AS total_orders,
        AVG(dfm.avg_order_value) AS weekly_avg_order_value,
        SUM(dfm.daily_active_customers) AS unique_customers_served,
        
        -- Customer acquisition
        SUM(dfm.daily_new_customers) AS new_customers_acquired,
        (SUM(dfm.daily_new_customers)::FLOAT / NULLIF(SUM(dfm.daily_active_customers), 0)) * 100 AS new_customer_mix_pct,
        
        -- Payment method performance  
        AVG(dfm.credit_card_mix_pct) AS avg_credit_card_mix,
        AVG(dfm.debit_card_mix_pct) AS avg_debit_card_mix,
        AVG(dfm.boleto_mix_pct) AS avg_boleto_mix,
        
        -- Risk and quality metrics
        AVG(dfm.high_risk_volume_pct) AS avg_high_risk_pct,
        SUM(dfm.daily_critical_anomalies) AS total_critical_anomalies,
        
        -- Market penetration
        AVG(dfm.states_with_orders) AS avg_geographic_reach,
        AVG(dfm.seller_states_active) AS avg_seller_geographic_diversity,
        
        -- Customer lifecycle performance
        SUM(dfm.new_customer_orders) AS new_customer_order_volume,
        SUM(dfm.repeat_customer_orders) AS repeat_customer_order_volume,  
        SUM(dfm.vip_customer_orders) AS vip_customer_order_volume
        
    FROM daily_financial_metrics dfm
    WHERE dfm.report_date >= CURRENT_DATE - INTERVAL '52 weeks'
    GROUP BY DATE_TRUNC('week', dfm.report_date)
),

monthly_financial_performance AS (
    SELECT 
        DATE_TRUNC('month', dfm.report_date) AS report_date,
        'Monthly' AS reporting_period,
        'Financial Performance' AS financial_category,
        
        -- Monthly revenue metrics
        SUM(dfm.daily_gmv) AS total_gmv,
        SUM(dfm.daily_product_revenue) AS total_product_revenue,
        SUM(dfm.daily_shipping_revenue) AS total_shipping_revenue,
        AVG(dfm.daily_gmv) AS avg_daily_gmv,
        
        -- Month-over-month growth
        SUM(dfm.daily_gmv) - LAG(SUM(dfm.daily_gmv), 1) OVER (ORDER BY DATE_TRUNC('month', dfm.report_date)) AS gmv_mom_change,
        ((SUM(dfm.daily_gmv) - LAG(SUM(dfm.daily_gmv), 1) OVER (ORDER BY DATE_TRUNC('month', dfm.report_date))) / 
         NULLIF(LAG(SUM(dfm.daily_gmv), 1) OVER (ORDER BY DATE_TRUNC('month', dfm.report_date)), 0)) * 100 AS gmv_mom_change_pct,
        
        -- Year-over-year comparison  
        SUM(dfm.daily_gmv) - LAG(SUM(dfm.daily_gmv), 12) OVER (ORDER BY DATE_TRUNC('month', dfm.report_date)) AS gmv_yoy_change,
        ((SUM(dfm.daily_gmv) - LAG(SUM(dfm.daily_gmv), 12) OVER (ORDER BY DATE_TRUNC('month', dfm.report_date))) / 
         NULLIF(LAG(SUM(dfm.daily_gmv), 12) OVER (ORDER BY DATE_TRUNC('month', dfm.report_date)), 0)) * 100 AS gmv_yoy_change_pct,
        
        -- Unit economics
        SUM(dfm.daily_orders) AS total_orders,
        SUM(dfm.daily_gmv) / NULLIF(SUM(dfm.daily_orders), 0) AS monthly_avg_order_value,
        SUM(dfm.daily_active_customers) AS unique_customers_served,
        SUM(dfm.daily_gmv) / NULLIF(SUM(dfm.daily_active_customers), 0) AS revenue_per_customer,
        
        -- Customer metrics
        SUM(dfm.daily_new_customers) AS new_customers_acquired,
        SUM(dfm.daily_new_customers) / NULLIF(SUM(dfm.daily_active_customers), 0) * 100 AS new_customer_acquisition_rate,
        
        -- Payment insights
        AVG(dfm.credit_card_mix_pct) AS credit_card_penetration,
        AVG(dfm.avg_payment_installments) AS avg_installment_terms,
        
        -- Platform health  
        AVG(dfm.high_risk_volume_pct) AS monthly_risk_exposure,
        SUM(dfm.daily_critical_anomalies) / NULLIF(SUM(dfm.daily_orders), 0) * 100 AS anomaly_rate_pct,
        
        -- Take rate estimation (assuming 5% commission + payment processing)
        SUM(dfm.daily_gmv) * 0.05 AS estimated_gross_commission_revenue,
        SUM(dfm.daily_shipping_revenue) * 0.15 AS estimated_logistics_revenue,
        (SUM(dfm.daily_gmv) * 0.05 + SUM(dfm.daily_shipping_revenue) * 0.15) AS estimated_total_platform_revenue
        
    FROM daily_financial_metrics dfm
    WHERE dfm.report_date >= CURRENT_DATE - INTERVAL '24 months'
    GROUP BY DATE_TRUNC('month', dfm.report_date)
),

-- Financial forecasting and projections
financial_forecasting AS (
    SELECT 
        mfp.report_date,
        'Monthly' AS reporting_period,
        'Financial Forecasting' AS financial_category,
        
        -- Historical data
        mfp.total_gmv,
        mfp.gmv_mom_change_pct,
        mfp.gmv_yoy_change_pct,
        
        -- Simple moving average forecasts
        AVG(mfp.total_gmv) OVER (
            ORDER BY mfp.report_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS gmv_3month_moving_avg,
        
        AVG(mfp.total_gmv) OVER (
            ORDER BY mfp.report_date 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS gmv_6month_moving_avg,
        
        -- Seasonal indexing (same month prior year)
        LAG(mfp.total_gmv, 12) OVER (ORDER BY mfp.report_date) AS same_month_prior_year,
        mfp.total_gmv / NULLIF(LAG(mfp.total_gmv, 12) OVER (ORDER BY mfp.report_date), 0) AS seasonal_index,
        
        -- Platform economics  
        mfp.estimated_total_platform_revenue,
        mfp.estimated_total_platform_revenue / NULLIF(mfp.total_gmv, 0) * 100 AS effective_take_rate_pct,
        
        -- Revenue quality indicators
        CASE 
            WHEN mfp.gmv_mom_change_pct > 15 THEN 'High Growth'
            WHEN mfp.gmv_mom_change_pct > 5 THEN 'Moderate Growth'
            WHEN mfp.gmv_mom_change_pct > -5 THEN 'Stable'
            WHEN mfp.gmv_mom_change_pct > -15 THEN 'Declining'
            ELSE 'Concerning Decline'
        END AS revenue_trend_category,
        
        CASE 
            WHEN mfp.monthly_risk_exposure < 2 THEN 'Low Risk'
            WHEN mfp.monthly_risk_exposure < 5 THEN 'Medium Risk' 
            ELSE 'High Risk'
        END AS financial_risk_level
        
    FROM monthly_financial_performance mfp
    WHERE mfp.report_date >= CURRENT_DATE - INTERVAL '18 months'
),

-- Combine all financial metrics
consolidated_financial_metrics AS (
    -- Weekly performance
    SELECT 
        report_date,
        reporting_period,
        financial_category,
        'Revenue Performance' AS metric_subcategory,
        total_gmv AS metric_value,
        'GMV' AS metric_name,
        'Weekly GMV performance tracking' AS metric_description,
        gmv_wow_change AS period_over_period_change,
        gmv_wow_change_pct AS period_over_period_change_pct,
        NULL AS forecast_value,
        total_orders AS supporting_volume,
        unique_customers_served AS supporting_count
    FROM weekly_financial_performance
    
    UNION ALL
    
    -- Monthly performance  
    SELECT 
        report_date,
        reporting_period,
        financial_category,
        'Revenue Performance' AS metric_subcategory,
        total_gmv AS metric_value,
        'GMV' AS metric_name,
        'Monthly GMV with MoM and YoY growth analysis' AS metric_description,
        gmv_mom_change AS period_over_period_change,
        gmv_mom_change_pct AS period_over_period_change_pct,
        NULL AS forecast_value,
        total_orders AS supporting_volume,
        unique_customers_served AS supporting_count
    FROM monthly_financial_performance
    
    UNION ALL
    
    -- Forecasting metrics
    SELECT 
        report_date,
        reporting_period,
        financial_category,
        'Revenue Forecasting' AS metric_subcategory,
        gmv_3month_moving_avg AS metric_value,
        'GMV Forecast (3-month MA)' AS metric_name,
        'Three-month moving average forecast for revenue planning' AS metric_description,
        NULL AS period_over_period_change,
        NULL AS period_over_period_change_pct,
        gmv_6month_moving_avg AS forecast_value,
        NULL AS supporting_volume,
        NULL AS supporting_count
    FROM financial_forecasting
),

final AS (
    SELECT 
        -- Time dimensions
        cfm.report_date,
        EXTRACT(year FROM cfm.report_date) AS report_year,
        EXTRACT(month FROM cfm.report_date) AS report_month,
        EXTRACT(quarter FROM cfm.report_date) AS report_quarter,
        CASE WHEN EXTRACT(dayofweek FROM cfm.report_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        
        -- Metric classification
        cfm.reporting_period,
        cfm.financial_category,
        cfm.metric_subcategory,
        cfm.metric_name,
        cfm.metric_description,
        
        -- Core financial metrics
        cfm.metric_value,
        cfm.period_over_period_change,
        cfm.period_over_period_change_pct,
        cfm.forecast_value,
        
        -- Supporting metrics
        cfm.supporting_volume,
        cfm.supporting_count,
        
        -- Business context flags
        CASE 
            WHEN cfm.period_over_period_change_pct > 20 THEN 'Exceptional Growth'
            WHEN cfm.period_over_period_change_pct > 10 THEN 'Strong Growth'
            WHEN cfm.period_over_period_change_pct > 5 THEN 'Moderate Growth'
            WHEN cfm.period_over_period_change_pct > -5 THEN 'Stable Performance'
            WHEN cfm.period_over_period_change_pct > -15 THEN 'Declining Performance'
            ELSE 'Critical Decline'
        END AS performance_category,
        
        -- Executive alerts
        CASE 
            WHEN cfm.metric_name = 'GMV' AND cfm.period_over_period_change_pct < -10 THEN 'REVENUE DECLINE ALERT'
            WHEN cfm.metric_name = 'GMV' AND cfm.metric_value = 0 THEN 'CRITICAL: NO REVENUE'
            WHEN cfm.reporting_period = 'Monthly' AND cfm.period_over_period_change_pct > 50 THEN 'INVESTIGATE: UNUSUAL GROWTH'
            ELSE 'NORMAL'
        END AS executive_alert,
        
        -- Financial health indicators
        CASE 
            WHEN cfm.metric_value > LAG(cfm.metric_value, 4) OVER (PARTITION BY cfm.metric_name ORDER BY cfm.report_date) * 1.1 
            THEN 'Accelerating'
            WHEN cfm.metric_value > LAG(cfm.metric_value, 1) OVER (PARTITION BY cfm.metric_name ORDER BY cfm.report_date) 
            THEN 'Growing'
            WHEN cfm.metric_value = LAG(cfm.metric_value, 1) OVER (PARTITION BY cfm.metric_name ORDER BY cfm.report_date) 
            THEN 'Flat'
            ELSE 'Declining'
        END AS trend_direction,
        
        -- Data quality and confidence
        CASE 
            WHEN cfm.supporting_volume >= 1000 THEN 'High Confidence'
            WHEN cfm.supporting_volume >= 100 THEN 'Medium Confidence'
            WHEN cfm.supporting_volume >= 10 THEN 'Low Confidence'
            ELSE 'Insufficient Data'
        END AS data_confidence_level,
        
        -- Metadata
        CURRENT_TIMESTAMP::timestamp_tz AS last_updated,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM consolidated_financial_metrics cfm
    WHERE cfm.report_date >= CURRENT_DATE - INTERVAL '12 months'
      OR (cfm.reporting_period = 'Monthly' AND cfm.report_date >= CURRENT_DATE - INTERVAL '24 months')
)

SELECT * FROM final
ORDER BY report_date DESC, reporting_period, metric_name
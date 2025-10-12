{{ config(
    materialized='table',
    cluster_by=['report_date', 'customer_segment'],
    schema='dbt_olist_marts',
    tags=['marts']
) }}

WITH unit_economics_base AS (
    SELECT
        c360.lifecycle_stage AS customer_segment,
        c360.region,
        clv.clv_segment,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS report_date,

        -- Order economics
        COUNT(DISTINCT o.order_id) AS orders,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        SUM(oi.total_item_value) AS gross_revenue,
        SUM(oi.freight_value) AS shipping_revenue,

        -- Unit metrics
        AVG(oi.total_item_value) AS avg_order_value,
        SUM(oi.total_item_value)
        / NULLIF(COUNT(DISTINCT o.customer_id), 0) AS revenue_per_customer,

        -- Customer economics
        AVG(clv.predicted_clv) AS avg_predicted_clv,
        AVG(c360.churn_probability) AS avg_churn_risk,

        -- Estimated platform economics (assumptions)
        SUM(oi.total_item_value) * 0.05 AS estimated_commission_revenue,
        SUM(oi.freight_value) * 0.15 AS estimated_shipping_margin,
        (SUM(oi.total_item_value) * 0.05 + SUM(oi.freight_value) * 0.15)
            AS estimated_gross_profit,

        -- Customer acquisition proxy (new customers)
        COUNT(DISTINCT CASE WHEN c360.total_orders = 1 THEN o.customer_id END)
            AS new_customers_acquired

    FROM {{ ref('stg_orders') }} AS o
    LEFT JOIN {{ ref('stg_order_items') }} AS oi ON o.order_id = oi.order_id
    LEFT JOIN
        {{ ref('int_customer_360') }} AS c360
        ON o.customer_id = c360.customer_id
    LEFT JOIN
        {{ ref('int_customer_lifetime_value') }} AS clv
        ON o.customer_id = clv.customer_id

    WHERE
        o.order_purchase_timestamp >= CURRENT_DATE - INTERVAL '12 months'
        AND o.order_status NOT IN ('canceled', 'unavailable')

    GROUP BY
        DATE_TRUNC('month', o.order_purchase_timestamp),
        c360.lifecycle_stage, c360.region, clv.clv_segment
),

profit AS (
    SELECT
        ueb.*,

        -- Unit economics calculations
        ueb.estimated_gross_profit / NULLIF(ueb.orders, 0) AS profit_per_order,
        ueb.estimated_gross_profit
        / NULLIF(ueb.unique_customers, 0) AS profit_per_customer,
        ueb.estimated_gross_profit
        / NULLIF(ueb.gross_revenue, 0)
        * 100 AS gross_margin_pct

    FROM unit_economics_base AS ueb
)

SELECT
    prf.*,
    -- Customer lifetime economics
    (prf.avg_predicted_clv * 0.05)
        AS estimated_lifetime_commission_per_customer,

    -- Payback period estimation (assuming $50 CAC)
    50
    / NULLIF(prf.estimated_gross_profit / NULLIF(prf.unique_customers, 0), 0)
        AS estimated_payback_months,

    -- Segment performance
    RANK()
        OVER (
            PARTITION BY prf.report_date ORDER BY prf.profit_per_customer DESC
        )
        AS profitability_rank,

    CASE
        WHEN prf.profit_per_customer > 100 THEN 'High Value'
        WHEN prf.profit_per_customer > 50 THEN 'Medium Value'
        WHEN prf.profit_per_customer > 20 THEN 'Low Value'
        ELSE 'Unprofitable'
    END AS profitability_segment,

    CURRENT_TIMESTAMP::timestamp_tz AS last_updated
FROM profit AS prf

ORDER BY prf.report_date DESC, prf.profit_per_customer DESC

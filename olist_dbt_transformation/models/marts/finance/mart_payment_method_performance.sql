{{ config(
    materialized='table',
    cluster_by=['report_date', 'payment_method'],
    schema='dbt_olist_marts',
    tags=['marts']
) }}

WITH payment_analysis AS (
    SELECT
        p.payment_type AS payment_method,
        p.payment_method_category,
        p.installment_category,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS report_date,

        -- Volume metrics
        COUNT(DISTINCT p.order_id) AS orders_count,
        SUM(p.payment_value) AS total_payment_volume,
        AVG(p.payment_value) AS avg_payment_value,

        -- Installment analysis
        AVG(p.payment_installments) AS avg_installments,
        SUM(
            CASE
                WHEN p.payment_installments > 12 THEN p.payment_value ELSE 0
            END
        ) AS long_term_financing_volume,

        -- Risk analysis  
        SUM(
            CASE
                WHEN
                    p.payment_risk_category = 'High Risk'
                    THEN p.payment_value
                ELSE 0
            END
        ) AS high_risk_volume,
        (
            SUM(
                CASE
                    WHEN
                        p.payment_risk_category = 'High Risk'
                        THEN p.payment_value
                    ELSE 0
                END
            )
            / NULLIF(SUM(p.payment_value), 0)
        ) * 100 AS risk_percentage,

        -- Success and failure rates (proxy through anomalies)
        COUNT(CASE WHEN oa.anomaly_severity = 'Critical' THEN 1 END)
            AS critical_anomaly_orders,
        (
            COUNT(CASE WHEN oa.anomaly_severity = 'Critical' THEN 1 END)::FLOAT
            / NULLIF(COUNT(DISTINCT p.order_id), 0)
        ) * 100 AS anomaly_rate_pct

    FROM {{ ref('stg_payments') }} AS p
    LEFT JOIN {{ ref('stg_orders') }} AS o ON p.order_id = o.order_id
    LEFT JOIN {{ ref('int_order_anomalies') }} AS oa ON o.order_sk = oa.order_sk

    WHERE
        o.order_purchase_timestamp >= CURRENT_DATE - INTERVAL '18 months'
        AND o.order_status NOT IN ('canceled', 'unavailable')

    GROUP BY
        DATE_TRUNC('month', o.order_purchase_timestamp),
        p.payment_type, p.payment_method_category, p.installment_category
)

SELECT
    pa.*,

    -- Performance benchmarking
    pa.total_payment_volume
    / SUM(pa.total_payment_volume) OVER (PARTITION BY pa.report_date)
    * 100 AS market_share_pct,

    -- Month-over-month growth
    LAG(pa.total_payment_volume, 1) OVER (
        PARTITION BY pa.payment_method
        ORDER BY pa.report_date
    ) AS prev_month_volume,

    (pa.total_payment_volume - LAG(pa.total_payment_volume, 1) OVER (
        PARTITION BY pa.payment_method ORDER BY pa.report_date
    )) / NULLIF(LAG(pa.total_payment_volume, 1) OVER (
        PARTITION BY pa.payment_method ORDER BY pa.report_date
    ), 0) * 100 AS mom_growth_pct,

    -- Payment method health scoring
    CASE
        WHEN pa.risk_percentage < 2 AND pa.anomaly_rate_pct < 1 THEN 'Excellent'
        WHEN pa.risk_percentage < 5 AND pa.anomaly_rate_pct < 3 THEN 'Good'
        WHEN pa.risk_percentage < 10 AND pa.anomaly_rate_pct < 5 THEN 'Average'
        ELSE 'Needs Attention'
    END AS payment_method_health,

    CURRENT_TIMESTAMP::timestamp_tz AS last_updated

FROM payment_analysis AS pa
ORDER BY pa.report_date DESC, pa.total_payment_volume DESC

models/
├── contracts/                    # Data contract definitions
│   ├── contract_definitions.sql
│   ├── schema_evolution_tracker.sql
│   └── contract_violations.sql
├── staging/                      # Source normalization + contracts
│   ├── stg_orders.sql
│   ├── stg_order_items.sql
│   ├── stg_customers.sql
│   ├── stg_products.sql
│   ├── stg_sellers.sql
│   ├── stg_payments.sql
│   ├── stg_reviews.sql
│   └── stg_geolocation.sql
├── intermediate/                 # Business logic + intelligence
│   ├── business_logic/
│   │   ├── int_order_lines.sql
│   │   ├── int_orders_enriched.sql
│   │   ├── int_payments_agg.sql
│   │   ├── int_customer_journey.sql
│   │   ├── int_product_catalog.sql
│   │   ├── int_seller_performance.sql
│   │   └── int_review_analytics.sql
│   ├── data_quality/
│   │   ├── int_anomaly_detection.sql
│   │   ├── int_data_quality_scores.sql
│   │   ├── int_volume_monitoring.sql
│   │   └── int_freshness_tracking.sql
│   ├── late_arriving/
│   │   ├── int_order_corrections.sql
│   │   ├── int_payment_adjustments.sql
│   │   └── int_review_updates.sql
│   └── performance/
│       ├── int_query_performance.sql
│       ├── int_model_benchmarks.sql
│       └── int_cost_attribution.sql
├── marts/
│   ├── finance/
│   │   ├── fct_daily_revenue.sql
│   │   ├── fct_orders.sql
│   │   ├── fct_order_profitability.sql
│   │   ├── dim_payment_methods.sql
│   │   └── fct_revenue_attribution.sql
│   ├── marketing/
│   │   ├── fct_customer_cohorts.sql
│   │   ├── dim_customer_segments.sql
│   │   ├── fct_clv_predictions.sql
│   │   ├── fct_rfm_analysis.sql
│   │   └── fct_campaign_performance.sql
│   ├── operations/
│   │   ├── fct_logistics_performance.sql
│   │   ├── fct_inventory_turnover.sql
│   │   ├── fct_seller_sla_tracking.sql
│   │   └── dim_fulfillment_centers.sql
│   ├── executive/
│   │   ├── fct_kpi_dashboard.sql
│   │   ├── fct_business_health.sql
│   │   ├── fct_data_quality_impact.sql
│   │   └── fct_cost_performance.sql
│   └── intelligence/
│       ├── fct_anomaly_alerts.sql
│       ├── fct_predictive_quality.sql
│       ├── fct_schema_changes.sql
│       └── fct_business_impact_correlation.sql
├── semantic/                     # Semantic layer definitions
│   ├── metrics/
│   │   ├── revenue_metrics.sql
│   │   ├── customer_metrics.sql
│   │   ├── operational_metrics.sql
│   │   └── quality_metrics.sql
│   ├── semantic_models/
│   │   ├── orders_semantic.sql
│   │   ├── customers_semantic.sql
│   │   ├── products_semantic.sql
│   │   └── performance_semantic.sql
│   └── saved_queries/
│       ├── executive_kpis.sql
│       ├── cohort_analysis.sql
│       └── quality_dashboard.sql
├── snapshots/                    # SCD2 + advanced versioning
│   ├── snap_dim_customer.sql
│   ├── snap_dim_product.sql
│   ├── snap_dim_seller.sql
│   └── snap_schema_versions.sql
└── utils/                        # Utility models
    ├── date_spine.sql
    ├── audit_runs.sql
    ├── cost_tracking.sql
    └── lineage_impact.sql

macros/
├── business_logic/
│   ├── generate_surrogate_key.sql
│   ├── normalize_currency.sql
│   ├── calculate_clv.sql
│   └── segment_customers.sql
├── data_contracts/
│   ├── validate_schema.sql
│   ├── check_data_types.sql
│   └── enforce_constraints.sql
├── incremental/
│   ├── late_arriving_merge.sql
│   ├── cdc_merge.sql
│   └── partition_refresh.sql
├── observability/
│   ├── log_run_metadata.sql
│   ├── detect_anomalies.sql
│   └── calculate_quality_score.sql
└── cost_optimization/
    ├── dynamic_clustering.sql
    ├── query_optimization.sql
    └── warehouse_sizing.sql

tests/
├── generic/
│   ├── cpf_validation.sql
│   ├── business_rule_validation.sql
│   └── cross_model_consistency.sql
├── singular/
│   ├── revenue_reconciliation.sql
│   ├── customer_lifecycle_logic.sql
│   └── payment_integrity.sql
└── advanced/
    ├── statistical_outliers.sql
    ├── time_series_anomalies.sql
    └── business_logic_validation.sql

seeds/
├── dim_date.csv
├── currency_rates.csv
├── business_rules.csv
└── sla_definitions.csv

exposures/
├── tableau_executive_dashboard.yml
├── looker_ops_dashboard.yml
├── ml_customer_scoring.yml
└── data_quality_monitoring.yml

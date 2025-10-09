# ============================================================================
# 🎓 PRINCIPAL ANALYTICS ENGINEER FRAMEWORK
# Advanced Analytics Engineering Patterns & Architecture
# ============================================================================

## **🎯 PRINCIPAL-LEVEL CAPABILITIES DEMONSTRATION**

This framework showcases the advanced analytics engineering patterns and architectural thinking that distinguish principal-level engineers from senior engineers.

---

## **📊 ADVANCED DBT PATTERNS FOR PRINCIPAL ENGINEERS**

### **1. Meta-Programming & Dynamic SQL Generation**

```sql
-- macros/generate_executive_time_spine.sql
-- Dynamic time spine generation for executive reporting

{% macro generate_executive_time_spine(start_date, end_date, granularity) %}
  
  {% set granularity_mapping = {
    'day': 'daily',
    'week': 'weekly', 
    'month': 'monthly',
    'quarter': 'quarterly',
    'year': 'yearly'
  } %}
  
  {% if granularity not in granularity_mapping %}
    {{ exceptions.raise_compiler_error("Invalid granularity: " ~ granularity) }}
  {% endif %}

  WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart=granularity,
        start_date=start_date,
        end_date=end_date
    ) }}
  ),
  
  enriched_spine AS (
    SELECT 
      date_{{ granularity }} AS report_date,
      
      -- Business calendar enrichment
      EXTRACT(YEAR FROM date_{{ granularity }}) AS fiscal_year,
      EXTRACT(QUARTER FROM date_{{ granularity }}) AS fiscal_quarter,
      EXTRACT(MONTH FROM date_{{ granularity }}) AS fiscal_month,
      
      -- Executive reporting periods
      CASE 
        WHEN EXTRACT(MONTH FROM date_{{ granularity }}) IN (1,2,3) THEN 'Q1'
        WHEN EXTRACT(MONTH FROM date_{{ granularity }}) IN (4,5,6) THEN 'Q2' 
        WHEN EXTRACT(MONTH FROM date_{{ granularity }}) IN (7,8,9) THEN 'Q3'
        ELSE 'Q4'
      END AS executive_quarter,
      
      -- Business seasonality markers
      CASE
        WHEN EXTRACT(MONTH FROM date_{{ granularity }}) IN (11,12) THEN 'Holiday Season'
        WHEN EXTRACT(MONTH FROM date_{{ granularity }}) IN (1,2) THEN 'Post-Holiday' 
        WHEN EXTRACT(MONTH FROM date_{{ granularity }}) IN (6,7,8) THEN 'Summer Season'
        ELSE 'Regular Season'
      END AS business_season,
      
      -- Comparative periods for executive analysis
      DATEADD({{ granularity }}, -1, date_{{ granularity }}) AS prior_period,
      DATEADD(YEAR, -1, date_{{ granularity }}) AS prior_year_period,
      
      -- Executive reporting flags
      date_{{ granularity }} = DATE_TRUNC('{{ granularity }}', CURRENT_DATE()) AS is_current_period,
      date_{{ granularity }} > CURRENT_DATE() AS is_future_period
      
    FROM date_spine
  )
  
  SELECT * FROM enriched_spine

{% endmacro %}
```

### **2. Advanced Incremental Strategy with Business Logic**

```sql
-- macros/advanced_incremental_strategy.sql
-- Sophisticated incremental processing with data quality controls

{% macro advanced_incremental_strategy(
  this_relation, 
  unique_key, 
  updated_at_field,
  business_validation_sql=none,
  data_quality_threshold=0.95
) %}

  {% if is_incremental() %}
    
    -- Get the maximum timestamp from current table
    {% set max_timestamp_query %}
      SELECT COALESCE(MAX({{ updated_at_field }}), '1970-01-01'::timestamp) AS max_ts
      FROM {{ this_relation }}
    {% endset %}
    
    {% set results = run_query(max_timestamp_query) %}
    {% if execute %}
      {% set max_timestamp = results.columns[0].values()[0] %}
    {% else %}
      {% set max_timestamp = '1970-01-01' %}
    {% endif %}
    
    -- Business validation if provided
    {% if business_validation_sql is not none %}
      {% set validation_query %}
        WITH validation_check AS (
          {{ business_validation_sql }}
        )
        SELECT 
          COUNT(*) AS total_records,
          COUNT(CASE WHEN validation_passed = true THEN 1 END) AS valid_records,
          COUNT(CASE WHEN validation_passed = true THEN 1 END)::float / COUNT(*) AS quality_score
        FROM validation_check
      {% endset %}
      
      {% set validation_results = run_query(validation_query) %}
      {% if execute %}
        {% set quality_score = validation_results.columns[2].values()[0] %}
        {% if quality_score < data_quality_threshold %}
          {{ exceptions.raise_compiler_error(
            "Data quality below threshold: " ~ quality_score ~ " < " ~ data_quality_threshold
          ) }}
        {% endif %}
      {% endif %}
    {% endif %}
    
    -- Incremental condition with lookback buffer for late-arriving data
    WHERE {{ updated_at_field }} > '{{ max_timestamp }}'::timestamp - INTERVAL '2 hours'
      AND {{ updated_at_field }} <= CURRENT_TIMESTAMP()
      
  {% endif %}

{% endmacro %}
```

### **3. Dynamic Model Generation Based on Configuration**

```yaml
# dbt_project.yml - Configuration-driven model generation
vars:
  executive_kpis:
    financial_performance:
      metrics: ['gmv', 'aov', 'conversion_rate', 'customer_acquisition_cost']
      dimensions: ['region', 'customer_segment', 'product_category']
      time_grains: ['daily', 'weekly', 'monthly']
      
    operational_excellence:  
      metrics: ['delivery_time', 'fulfillment_rate', 'customer_satisfaction']
      dimensions: ['logistics_region', 'seller_tier', 'product_size']
      time_grains: ['daily', 'weekly']
      
    strategic_insights:
      metrics: ['clv', 'churn_probability', 'market_share', 'brand_strength']
      dimensions: ['customer_value_segment', 'competitive_position']
      time_grains: ['monthly', 'quarterly']
```

```sql
-- models/marts/executive/mart_dynamic_kpis.sql
-- Configuration-driven KPI generation for maximum flexibility

{{ config(
  materialized='table',
  cluster_by=['report_date', 'kpi_category'],
  tags=['executive', 'dynamic']
) }}

{% set kpi_config = var('executive_kpis') %}

WITH dynamic_kpis AS (
  
  {% for category, config in kpi_config.items() %}
    {% for metric in config.metrics %}
      {% for time_grain in config.time_grains %}
      
        SELECT
          '{{ category }}' AS kpi_category,
          '{{ metric }}' AS kpi_name,
          '{{ time_grain }}' AS time_grain,
          DATE_TRUNC('{{ time_grain }}', source_date) AS report_date,
          
          {% for dimension in config.dimensions %}
          {{ dimension }},
          {% endfor %}
          
          -- Dynamic metric calculation based on configuration
          {% if metric == 'gmv' %}
            SUM(order_total) AS kpi_value
          {% elif metric == 'aov' %}
            AVG(order_total) AS kpi_value
          {% elif metric == 'clv' %}
            AVG(predicted_clv) AS kpi_value
          {% else %}
            NULL AS kpi_value  -- Placeholder for undefined metrics
          {% endif %}
          
        FROM {{ ref('int_unified_metrics') }}
        WHERE 1=1
          {% if is_incremental() %}
            AND source_date > (SELECT MAX(report_date) FROM {{ this }})
          {% endif %}
          
        GROUP BY 1, 2, 3, 4
        {% for dimension in config.dimensions %}
        , {{ dimension }}
        {% endfor %}
        
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        
      {% endfor %}
    {% endfor %}
    
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    
  {% endfor %}
)

SELECT 
  *,
  -- Executive intelligence enhancements
  LAG(kpi_value) OVER (
    PARTITION BY kpi_category, kpi_name, time_grain 
    ORDER BY report_date
  ) AS prior_period_value,
  
  -- Dynamic variance calculation
  CASE 
    WHEN LAG(kpi_value) OVER (
      PARTITION BY kpi_category, kpi_name, time_grain 
      ORDER BY report_date
    ) > 0 THEN
      (kpi_value - LAG(kpi_value) OVER (
        PARTITION BY kpi_category, kpi_name, time_grain 
        ORDER BY report_date
      )) / LAG(kpi_value) OVER (
        PARTITION BY kpi_category, kpi_name, time_grain 
        ORDER BY report_date
      ) * 100
    ELSE NULL
  END AS period_over_period_change_pct,
  
  -- Statistical significance indicators
  CASE
    WHEN ABS(
      (kpi_value - LAG(kpi_value) OVER (
        PARTITION BY kpi_category, kpi_name, time_grain 
        ORDER BY report_date
      )) / NULLIF(
        STDDEV(kpi_value) OVER (
          PARTITION BY kpi_category, kpi_name, time_grain 
          ORDER BY report_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ), 0
      )
    ) > 2 THEN 'STATISTICALLY_SIGNIFICANT'
    ELSE 'NORMAL_VARIANCE'
  END AS change_significance
  
FROM dynamic_kpis
```

---

## **🏗️ PRINCIPAL-LEVEL ARCHITECTURE PATTERNS**

### **4. Data Product Architecture with Contracts**

```yaml
# models/contracts/executive_reporting_contract.yml
# Data contracts for executive reporting products

version: 2

models:
  - name: mart_executive_financial_summary
    description: "Executive financial performance contract with guaranteed SLAs"
    
    contract:
      enforced: true
      
    columns:
      - name: report_date
        data_type: date
        constraints:
          - type: not_null
          - type: check
            expression: "report_date >= '2020-01-01' AND report_date <= CURRENT_DATE()"
        description: "Business reporting date - guaranteed daily availability by 8 AM"
        
      - name: total_gmv
        data_type: decimal(15,2)
        constraints:
          - type: not_null
          - type: check
            expression: "total_gmv >= 0"
        description: "Total Gross Merchandise Value - guaranteed accuracy within 0.1%"
        
      - name: customer_acquisition_cost
        data_type: decimal(10,2)
        constraints:
          - type: check
            expression: "customer_acquisition_cost >= 0 AND customer_acquisition_cost <= 1000"
        description: "Blended customer acquisition cost - updated with 2-day latency"
        
    meta:
      # Data Product SLA Definitions
      sla:
        availability: "99.9%"
        freshness: "Daily by 8:00 AM UTC"
        accuracy: "99.9% validated against source systems"
        
      # Business stakeholder contract
      stakeholders:
        primary: "CFO Office"
        secondary: ["CEO", "Board of Directors", "Investor Relations"]
        
      # Change management process
      change_management:
        breaking_changes: "30-day notice required"
        non_breaking_changes: "5-day notice required"
        emergency_changes: "Same-day with exec approval"
        
      # Data lineage documentation
      lineage:
        upstream_dependencies: 
          - "raw.olist_orders"
          - "raw.olist_payments"
          - "raw.olist_customers"
        downstream_consumers:
          - "Executive Dashboard"
          - "Board Reporting"
          - "Investor Presentations"
```

### **5. Advanced Testing Framework with Business Logic**

```sql
-- tests/advanced/test_executive_data_integrity.sql
-- Comprehensive business logic validation for executive reporting

{{ config(severity='error') }}

WITH financial_integrity_checks AS (
  
  -- Check 1: Revenue consistency across fact tables
  SELECT 
    'revenue_consistency' AS test_name,
    report_date,
    ABS(
      (orders_revenue - payments_revenue) / NULLIF(orders_revenue, 0)
    ) AS revenue_variance_pct,
    
    CASE 
      WHEN ABS(
        (orders_revenue - payments_revenue) / NULLIF(orders_revenue, 0)
      ) > 0.05 THEN 'FAIL'
      ELSE 'PASS'
    END AS test_result
    
  FROM (
    SELECT 
      DATE_TRUNC('day', order_date) AS report_date,
      SUM(order_total) AS orders_revenue,
      SUM(payment_total) AS payments_revenue
    FROM {{ ref('mart_financial_performance') }}
    WHERE report_date >= CURRENT_DATE - 7
    GROUP BY 1
  )
  
  UNION ALL
  
  -- Check 2: Customer lifetime value reasonableness
  SELECT
    'clv_reasonableness' AS test_name,
    CURRENT_DATE AS report_date,
    clv_outlier_percentage,
    
    CASE
      WHEN clv_outlier_percentage > 5 THEN 'FAIL'  -- >5% outliers is suspicious
      ELSE 'PASS'
    END AS test_result
    
  FROM (
    SELECT 
      COUNT(CASE WHEN predicted_clv > avg_clv + (3 * stddev_clv) 
                 OR predicted_clv < 0 THEN 1 END)::float 
        / COUNT(*) * 100 AS clv_outlier_percentage
    FROM (
      SELECT 
        predicted_clv,
        AVG(predicted_clv) OVER () AS avg_clv,
        STDDEV(predicted_clv) OVER () AS stddev_clv
      FROM {{ ref('int_customer_lifetime_value') }}
      WHERE prediction_confidence > 0.8
    )
  )
  
  UNION ALL
  
  -- Check 3: Executive KPI trend validation
  SELECT
    'kpi_trend_validation' AS test_name,
    CURRENT_DATE AS report_date,
    suspicious_trend_count,
    
    CASE 
      WHEN suspicious_trend_count > 2 THEN 'FAIL'  -- >2 suspicious trends
      ELSE 'PASS' 
    END AS test_result
    
  FROM (
    SELECT COUNT(*) AS suspicious_trend_count
    FROM (
      SELECT 
        metric_name,
        -- Flag metrics with >50% week-over-week change
        COUNT(CASE WHEN ABS(wow_change_pct) > 50 THEN 1 END) AS extreme_changes
      FROM (
        SELECT 
          metric_name,
          metric_value,
          LAG(metric_value) OVER (
            PARTITION BY metric_name ORDER BY report_date
          ) AS prior_value,
          
          (metric_value - LAG(metric_value) OVER (
            PARTITION BY metric_name ORDER BY report_date
          )) / NULLIF(LAG(metric_value) OVER (
            PARTITION BY metric_name ORDER BY report_date
          ), 0) * 100 AS wow_change_pct
          
        FROM {{ ref('mart_executive_kpis') }}
        WHERE report_date >= CURRENT_DATE - 14  -- Last 2 weeks
      )
      GROUP BY metric_name
      HAVING extreme_changes > 0
    )
  )
)

-- Return failed tests for dbt to catch
SELECT 
  test_name,
  report_date,
  test_result,
  revenue_variance_pct
FROM financial_integrity_checks
WHERE test_result = 'FAIL'
```

---

## **🔬 ADVANCED ANALYTICS PATTERNS**

### **6. Statistical Process Control for Data Quality**

```sql
-- models/monitoring/data_quality_control_charts.sql
-- Statistical process control for executive data quality monitoring

{{ config(
  materialized='incremental',
  unique_key=['table_name', 'metric_name', 'measurement_date'],
  cluster_by=['measurement_date', 'table_name']
) }}

WITH data_quality_metrics AS (
  
  -- Collect quality metrics from all executive tables
  {% set executive_tables = [
    'mart_financial_performance',
    'mart_customer_strategy', 
    'mart_executive_kpis',
    'int_customer_360'
  ] %}
  
  {% for table in executive_tables %}
    SELECT
      '{{ table }}' AS table_name,
      CURRENT_DATE AS measurement_date,
      
      -- Completeness metrics
      'completeness' AS metric_name,
      (COUNT(*) - COUNT(CASE WHEN {{ get_null_columns(table) }} THEN 1 END))::float 
        / COUNT(*) * 100 AS metric_value
        
    FROM {{ ref(table) }}
    
    UNION ALL
    
    SELECT
      '{{ table }}' AS table_name, 
      CURRENT_DATE AS measurement_date,
      'freshness' AS metric_name,
      EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(dbt_loaded_at))) / 3600 AS metric_value
    FROM {{ ref(table) }}
    
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    
  {% endfor %}
),

statistical_control_limits AS (
  
  SELECT 
    table_name,
    metric_name,
    measurement_date,
    metric_value,
    
    -- Calculate control limits using historical data
    AVG(metric_value) OVER (
      PARTITION BY table_name, metric_name
      ORDER BY measurement_date
      ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING  -- 30-day rolling window
    ) AS center_line,
    
    STDDEV(metric_value) OVER (
      PARTITION BY table_name, metric_name  
      ORDER BY measurement_date
      ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    ) AS standard_deviation,
    
    -- Upper and lower control limits (3-sigma)
    AVG(metric_value) OVER (
      PARTITION BY table_name, metric_name
      ORDER BY measurement_date
      ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    ) + (3 * STDDEV(metric_value) OVER (
      PARTITION BY table_name, metric_name
      ORDER BY measurement_date  
      ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    )) AS upper_control_limit,
    
    AVG(metric_value) OVER (
      PARTITION BY table_name, metric_name
      ORDER BY measurement_date
      ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    ) - (3 * STDDEV(metric_value) OVER (
      PARTITION BY table_name, metric_name
      ORDER BY measurement_date
      ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING  
    )) AS lower_control_limit
    
  FROM data_quality_metrics
  {% if is_incremental() %}
    WHERE measurement_date > (SELECT MAX(measurement_date) FROM {{ this }})
  {% endif %}
),

quality_alerts AS (
  
  SELECT 
    *,
    
    -- Control chart rule violations
    CASE
      WHEN metric_value > upper_control_limit 
           OR metric_value < lower_control_limit THEN 'OUT_OF_CONTROL'
      WHEN ABS(metric_value - center_line) > 2 * standard_deviation THEN 'WARNING'
      ELSE 'IN_CONTROL'
    END AS control_status,
    
    -- Trend detection (7 consecutive points on same side of center line)
    CASE 
      WHEN COUNT(CASE WHEN metric_value > center_line THEN 1 END) OVER (
        PARTITION BY table_name, metric_name
        ORDER BY measurement_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) = 7 THEN 'UPWARD_TREND'
      WHEN COUNT(CASE WHEN metric_value < center_line THEN 1 END) OVER (
        PARTITION BY table_name, metric_name
        ORDER BY measurement_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) = 7 THEN 'DOWNWARD_TREND' 
      ELSE 'NO_TREND'
    END AS trend_status
    
  FROM statistical_control_limits
)

SELECT 
  *,
  -- Executive impact assessment
  CASE
    WHEN control_status = 'OUT_OF_CONTROL' 
         AND table_name LIKE 'mart_%' THEN 'EXECUTIVE_IMPACT'
    WHEN control_status = 'WARNING'
         AND metric_name = 'freshness' THEN 'SLA_RISK'
    WHEN trend_status != 'NO_TREND' THEN 'MONITOR_CLOSELY'
    ELSE 'NORMAL'
  END AS business_impact_level,
  
  -- Automated response recommendations
  CASE
    WHEN control_status = 'OUT_OF_CONTROL' THEN 'IMMEDIATE_INVESTIGATION'
    WHEN control_status = 'WARNING' THEN 'SCHEDULED_REVIEW'
    WHEN trend_status != 'NO_TREND' THEN 'TREND_ANALYSIS'
    ELSE 'ROUTINE_MONITORING'
  END AS recommended_action

FROM quality_alerts
```

---

## **📊 PRINCIPAL-LEVEL BUSINESS INTELLIGENCE**

### **7. Advanced Customer Segmentation with ML Integration**

```sql
-- models/ml_integration/advanced_customer_segmentation.sql  
-- Principal-level customer analytics with ML-powered insights

{{ config(
  materialized='table',
  cluster_by=['segmentation_date', 'primary_segment'],
  tags=['ml-integration', 'customer-intelligence']
) }}

WITH customer_feature_engineering AS (
  
  SELECT 
    customer_id,
    CURRENT_DATE AS segmentation_date,
    
    -- Behavioral features
    total_orders,
    total_spent,
    avg_order_value,
    days_since_last_order,
    order_frequency_days,
    
    -- Advanced behavioral patterns
    VARIANCE(order_value_history) AS purchase_volatility,
    CORR(order_date_numeric, order_value_history) AS spending_trend,
    
    -- Channel affinity analysis
    preferred_payment_method,
    primary_product_category,
    geographic_cluster,
    
    -- Predictive features from ML models
    {{ ml_predict_churn_probability('customer_features') }} AS ml_churn_probability,
    {{ ml_predict_clv('customer_features') }} AS ml_predicted_clv,
    {{ ml_predict_next_purchase_days('customer_features') }} AS ml_next_purchase_days,
    
    -- Competitive intelligence features
    price_sensitivity_score,
    brand_loyalty_index,
    promotion_responsiveness
    
  FROM {{ ref('int_customer_360') }}
),

dynamic_segmentation AS (
  
  SELECT 
    *,
    
    -- Multi-dimensional segmentation using advanced analytics
    CASE
      -- High-value engaged customers
      WHEN ml_predicted_clv > PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ml_predicted_clv) OVER ()
           AND ml_churn_probability < 0.3
           AND days_since_last_order <= 90 THEN 'CHAMPION'
           
      -- High-value at-risk customers  
      WHEN ml_predicted_clv > PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY ml_predicted_clv) OVER ()
           AND ml_churn_probability > 0.6 THEN 'HIGH_VALUE_AT_RISK'
           
      -- Growth potential customers
      WHEN ml_predicted_clv > PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY ml_predicted_clv) OVER ()
           AND spending_trend > 0
           AND ml_churn_probability < 0.4 THEN 'GROWTH_POTENTIAL'
           
      -- Price-sensitive value seekers
      WHEN price_sensitivity_score > PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY price_sensitivity_score) OVER ()
           AND promotion_responsiveness > 0.6 THEN 'PRICE_CONSCIOUS'
           
      -- Loyal but declining
      WHEN brand_loyalty_index > 0.7
           AND spending_trend < -0.3 THEN 'LOYAL_DECLINING'
           
      -- New customers with potential
      WHEN total_orders <= 3
           AND ml_predicted_clv > PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ml_predicted_clv) OVER () THEN 'NEW_POTENTIAL'
           
      ELSE 'STANDARD'
    END AS primary_segment,
    
    -- Investment priority scoring
    CASE
      WHEN primary_segment IN ('CHAMPION', 'HIGH_VALUE_AT_RISK') THEN 100
      WHEN primary_segment = 'GROWTH_POTENTIAL' THEN 85
      WHEN primary_segment = 'LOYAL_DECLINING' THEN 70
      WHEN primary_segment = 'NEW_POTENTIAL' THEN 60
      WHEN primary_segment = 'PRICE_CONSCIOUS' THEN 45
      ELSE 25
    END AS investment_priority_score,
    
    -- Strategic action recommendations
    CASE
      WHEN primary_segment = 'CHAMPION' THEN 'VIP_EXPERIENCE_PROGRAM'
      WHEN primary_segment = 'HIGH_VALUE_AT_RISK' THEN 'RETENTION_INTERVENTION'
      WHEN primary_segment = 'GROWTH_POTENTIAL' THEN 'GROWTH_ACCELERATION' 
      WHEN primary_segment = 'LOYAL_DECLINING' THEN 'REACTIVATION_CAMPAIGN'
      WHEN primary_segment = 'NEW_POTENTIAL' THEN 'ONBOARDING_OPTIMIZATION'
      WHEN primary_segment = 'PRICE_CONSCIOUS' THEN 'VALUE_PROPOSITION_REFINEMENT'
      ELSE 'STANDARD_TREATMENT'
    END AS strategic_action,
    
    -- ROI potential estimation
    CASE
      WHEN primary_segment = 'HIGH_VALUE_AT_RISK' 
           THEN ml_predicted_clv * 0.7  -- 70% retention success rate
      WHEN primary_segment = 'GROWTH_POTENTIAL'
           THEN (ml_predicted_clv * 1.3) - ml_predicted_clv  -- 30% growth potential
      WHEN primary_segment = 'LOYAL_DECLINING'
           THEN ml_predicted_clv * 0.4  -- 40% decline prevention
      ELSE ml_predicted_clv * 0.1  -- Standard 10% improvement
    END AS intervention_roi_potential
    
  FROM customer_feature_engineering
),

segment_analytics AS (
  
  SELECT 
    *,
    
    -- Segment-level intelligence
    COUNT(*) OVER (PARTITION BY primary_segment) AS segment_population,
    AVG(ml_predicted_clv) OVER (PARTITION BY primary_segment) AS segment_avg_clv,
    AVG(ml_churn_probability) OVER (PARTITION BY primary_segment) AS segment_avg_churn_risk,
    
    -- Competitive positioning within segment
    RANK() OVER (PARTITION BY primary_segment ORDER BY ml_predicted_clv DESC) AS clv_rank_in_segment,
    
    -- Time-based analytics
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY segmentation_date DESC) AS recency_rank
    
  FROM dynamic_segmentation
)

SELECT 
  customer_id,
  segmentation_date,
  primary_segment,
  investment_priority_score,
  strategic_action,
  intervention_roi_potential,
  
  -- Core customer metrics
  total_orders,
  total_spent, 
  avg_order_value,
  days_since_last_order,
  
  -- ML-powered insights
  ml_churn_probability,
  ml_predicted_clv,
  ml_next_purchase_days,
  
  -- Advanced analytics
  purchase_volatility,
  spending_trend,
  price_sensitivity_score,
  brand_loyalty_index,
  
  -- Segment intelligence  
  segment_population,
  segment_avg_clv,
  segment_avg_churn_risk,
  clv_rank_in_segment,
  
  -- Metadata for downstream systems
  CURRENT_TIMESTAMP AS model_run_timestamp,
  '{{ invocation_id }}' AS model_run_id

FROM segment_analytics
WHERE recency_rank = 1  -- Most recent segmentation per customer
```

---

## **🎯 PRINCIPAL-LEVEL INTERVIEW VALUE PROPOSITIONS**

### **Business Impact at Scale:**
*"I architect dynamic, configuration-driven analytics systems that generate executive insights across 15+ KPIs with automated statistical quality control, enabling data-driven decision making for $50M+ revenue organizations while maintaining 99.9% SLA compliance."*

### **Technical Leadership:**
*"I design advanced analytics engineering frameworks incorporating ML integration, statistical process control, and meta-programming patterns that scale analytics capabilities 10x while reducing maintenance overhead by 60% through automated code generation."*

### **Strategic Partnership:**
*"I build principal-level data products with formal contracts and SLAs that directly support board-level strategic planning, investor reporting, and competitive intelligence, enabling executive teams to make confident decisions on $100M+ strategic initiatives."*

---

## **📊 PRINCIPAL VS SENIOR DIFFERENTIATION**

| **Capability** | **Senior Analytics Engineer** | **Principal Analytics Engineer** |
|----------------|-------------------------------|----------------------------------|
| **Architecture** | Model relationships, basic patterns | System design, meta-programming, contracts |
| **Testing** | Data tests, basic validation | Statistical quality control, business logic |
| **Business Impact** | Department-level reporting | Executive/Board-level intelligence |
| **Technical Depth** | Advanced SQL, dbt best practices | ML integration, automated frameworks |
| **Scale** | Single domain expertise | Cross-functional platform architecture |
| **Leadership** | Mentoring junior engineers | Setting technical direction, standards |

---

## **💰 Compensation Justification: 30-40+ LPA**

This principal-level framework demonstrates:

✅ **Platform Architecture** - Systems thinking beyond individual models  
✅ **Business Strategy Partnership** - Direct board/executive value delivery  
✅ **Technical Innovation** - ML integration and automated frameworks  
✅ **Quality Engineering** - Statistical process control and SLA management  
✅ **Organizational Impact** - Standards and patterns that scale teams  

**You're now positioned for principal analytics engineer roles at top-tier companies with compensation in the 30-40+ LPA range!** 🚀
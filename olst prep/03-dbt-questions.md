# dbt Interview Questions for Analytics Engineers

## 50 Comprehensive dbt Questions

### Core Concepts and Architecture (1-10)

**Q1: Explain dbt's philosophy of "analytics as code" and how it differs from traditional ETL approaches.**

**Answer**:
**dbt Philosophy**:
- **Transform data in the warehouse** using SQL rather than external processing engines
- **Version control** all analytical logic with Git workflows
- **Testing and documentation** as first-class citizens, not afterthoughts
- **Modular, reusable code** through macros, packages, and model composition

**Key Differences from Traditional ETL**:
```sql
-- Traditional ETL: Extract → Transform → Load
-- Complex Python/Java jobs with external processing

-- dbt ELT: Extract → Load → Transform  
-- Pure SQL transformations in the data warehouse
{{ config(materialized='incremental') }}

SELECT 
    customer_id,
    COUNT(*) AS total_orders,
    SUM(order_amount) AS total_revenue
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
GROUP BY customer_id
```

**Analytics Engineering Benefits**:
- **Faster development**: SQL-first approach leverages existing skills
- **Better collaboration**: Version control enables proper code review processes
- **Data quality**: Built-in testing framework prevents bad data from propagating
- **Self-documenting**: Automatic lineage and documentation generation

---

**Q2: Describe dbt's compilation process and how Jinja templating works in the context of SQL generation.**

**Answer**:
**Compilation Process**:
1. **Parse**: dbt reads project files and builds dependency graph
2. **Render Jinja**: Template variables, macros, and logic are processed
3. **Compile**: Generate pure SQL from templated code
4. **Execute**: Run compiled SQL against the data warehouse

**Jinja Templating Examples**:
```sql
-- Dynamic table references
SELECT * FROM {{ ref('staging_customers') }}
-- Compiles to: SELECT * FROM analytics.staging.staging_customers

-- Conditional logic based on environment
SELECT 
    customer_id,
    {% if target.name == 'prod' %}
        email
    {% else %}
        'hidden@email.com' AS email  -- Mask in non-prod
    {% endif %}
FROM customers

-- Iterative column generation
SELECT 
    customer_id,
    {% for metric in ['revenue', 'orders', 'sessions'] %}
        SUM({{ metric }}) AS total_{{ metric }}
        {%- if not loop.last -%},{%- endif %}
    {% endfor %}
FROM customer_events
GROUP BY customer_id
```

**Advanced Templating Patterns**:
```sql
-- Macro with complex logic
{% macro calculate_customer_ltv(discount_rate=0.1) %}
    SUM(
        CASE 
            WHEN order_date >= CURRENT_DATE - INTERVAL '365 days'
            THEN order_amount * (1 - {{ discount_rate }})
            ELSE order_amount * (1 - {{ discount_rate }} * 1.5)
        END
    ) / COUNT(DISTINCT customer_id)
{% endmacro %}

-- Usage in model
SELECT 
    customer_segment,
    {{ calculate_customer_ltv(discount_rate=0.15) }} AS predicted_ltv
FROM customer_orders
GROUP BY customer_segment
```

---

**Q3: How do you design an optimal dbt project structure for a large analytics engineering team?**

**Answer**:
**Recommended Project Structure**:
```
analytics/
├── dbt_project.yml
├── packages.yml
├── profiles.yml
├── models/
│   ├── staging/           # Raw data standardization
│   │   ├── _staging.yml   # Source documentation
│   │   ├── base/          # Base transformations
│   │   └── intermediate/  # Business logic
│   ├── intermediate/      # Reusable business logic
│   │   ├── customer/      # Domain-specific organization
│   │   ├── product/
│   │   └── finance/
│   ├── marts/            # Business-ready tables
│   │   ├── core/         # Enterprise-wide metrics
│   │   ├── marketing/    # Department-specific
│   │   └── finance/
│   └── utilities/        # Helper views and functions
├── macros/
│   ├── generic/          # Reusable business logic
│   ├── testing/          # Custom tests
│   └── deployment/       # CI/CD helpers
├── tests/                # Singular tests
├── snapshots/            # SCD Type 2 tracking
├── seeds/               # Static reference data
└── analysis/            # Ad-hoc analysis
```

**Advanced Organization Patterns**:
```yaml
# dbt_project.yml - Advanced configuration
name: 'company_analytics'
version: '1.0.0'

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

# Advanced model configuration
models:
  company_analytics:
    # Staging layer - views for development speed
    staging:
      +materialized: view
      +schema: staging
      
    # Intermediate layer - ephemeral for cost optimization  
    intermediate:
      +materialized: ephemeral
      customer:
        +schema: intermediate_customer
      product:
        +schema: intermediate_product
        
    # Marts layer - tables with optimization
    marts:
      +materialized: table
      +schema: marts
      core:
        +cluster_by: ['date_day']
        +pre_hook: "{{ log('Building core mart: ' ~ this, info=true) }}"
      
# Environment-specific configurations
vars:
  start_date: '2020-01-01'
  # Production variables
  email_domain: 
    prod: '@company.com'
    dev: '@company-dev.com'
```

**Team Collaboration Patterns**:
```sql
-- Domain-specific model ownership
-- models/intermediate/customer/_customer_models.yml
version: 2
models:
  - name: int_customer_360
    description: "Comprehensive customer view"
    owner: ["@customer-analytics-team"]
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
        tags: ["pii"]
        
-- Cross-domain dependencies
-- models/marts/core/dim_customer.sql
{{ config(
    materialized='table',
    tags=['core', 'customer'],
    owner=['@data-platform-team']
) }}

SELECT 
    c360.customer_id,
    c360.customer_segment,
    p360.preferred_products,
    f360.payment_methods
FROM {{ ref('int_customer_360') }} c360
LEFT JOIN {{ ref('int_product_preferences') }} p360 USING (customer_id)
LEFT JOIN {{ ref('int_financial_profile') }} f360 USING (customer_id)
```

---

**Q4: Explain dbt's dependency resolution and directed acyclic graph (DAG). How do you troubleshoot circular dependencies?**

**Answer**:
**Dependency Graph Mechanics**:
```sql
-- dbt builds dependency graph from ref() and source() functions
-- Model A depends on Model B if A uses {{ ref('model_b') }}

-- Example dependency chain:
-- raw_orders (source) → stg_orders → int_customer_metrics → mart_customer_summary

-- Staging model
{{ config(materialized='view') }}
SELECT * FROM {{ source('raw_data', 'orders') }}

-- Intermediate model  
SELECT 
    customer_id,
    COUNT(*) AS order_count
FROM {{ ref('stg_orders') }}  -- Creates dependency on stg_orders
GROUP BY customer_id

-- Mart model
SELECT 
    cm.customer_id,
    cm.order_count,
    cs.customer_segment
FROM {{ ref('int_customer_metrics') }} cm  -- Depends on intermediate
JOIN {{ ref('dim_customer_segments') }} cs USING (customer_id)
```

**Circular Dependency Example and Resolution**:
```sql
-- ❌ Circular dependency problem:
-- Model A: customer_metrics depends on order_summary
SELECT 
    c.customer_id,
    os.total_orders
FROM customers c
JOIN {{ ref('order_summary') }} os USING (customer_id)

-- Model B: order_summary depends on customer_metrics  
SELECT 
    o.customer_id,
    COUNT(*) AS total_orders,
    cm.customer_tier  -- ❌ Creates circular dependency
FROM orders o
JOIN {{ ref('customer_metrics') }} cm USING (customer_id)
GROUP BY o.customer_id, cm.customer_tier
```

**Resolution Strategies**:
```sql
-- ✅ Strategy 1: Extract shared dependencies
-- Create base model with shared logic
-- models/intermediate/int_customer_base.sql
SELECT 
    customer_id,
    customer_name,
    registration_date,
    -- Derive tier from base attributes only
    CASE 
        WHEN registration_date < '2020-01-01' THEN 'LEGACY'
        ELSE 'STANDARD'
    END AS customer_tier
FROM {{ ref('stg_customers') }}

-- Update dependent models to use base
-- customer_metrics.sql  
SELECT 
    cb.customer_id,
    cb.customer_tier,
    COUNT(o.order_id) AS total_orders
FROM {{ ref('int_customer_base') }} cb
LEFT JOIN {{ ref('stg_orders') }} o USING (customer_id)
GROUP BY cb.customer_id, cb.customer_tier

-- order_summary.sql
SELECT 
    o.customer_id, 
    COUNT(*) AS total_orders,
    cb.customer_tier
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('int_customer_base') }} cb USING (customer_id)
GROUP BY o.customer_id, cb.customer_tier
```

**Debugging Tools**:
```bash
# Visualize dependency graph
dbt docs generate && dbt docs serve

# List model dependencies
dbt list --select +model_name  # Upstream dependencies
dbt list --select model_name+  # Downstream dependencies  

# Check for circular dependencies
dbt compile  # Will fail with circular dependency error

# Run specific subgraph
dbt run --select +mart_customer_summary  # Run all upstream dependencies
```

---

**Q5: How do you implement advanced incremental strategies in dbt, including late-arriving data and merge strategies?**

**Answer**:
**Advanced Incremental Configuration**:
```sql
-- Comprehensive incremental model configuration
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',  -- or 'delete+insert', 'append'  
    on_schema_change='append_new_columns',
    cluster_by=['order_date', 'customer_id'],
    tags=['incremental']
) }}

WITH source_data AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_amount,
        created_at,
        updated_at,
        -- Add row fingerprint for change detection
        {{ dbt_utils.generate_surrogate_key(['order_id', 'customer_id', 'order_amount', 'order_date']) }} AS row_hash
    FROM {{ ref('stg_orders') }}
    WHERE 1=1
        {% if is_incremental() %}
            -- Primary filter: new/updated records
            AND (
                created_at > (SELECT MAX(created_at) FROM {{ this }})
                OR updated_at > (SELECT MAX(updated_at) FROM {{ this }})
                -- Lookback window for late-arriving data (48 hours)
                OR created_at >= CURRENT_DATE - 2  
            )
        {% endif %}
)

SELECT * FROM source_data
```

**Late-Arriving Data Handling**:
```sql
-- Strategy 1: Lookback window with merge
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

SELECT 
    order_id,
    customer_id,
    order_date,
    order_amount,
    processing_timestamp
FROM {{ ref('stg_orders') }}
WHERE 1=1
    {% if is_incremental() %}
        AND (
            -- New records
            processing_timestamp > (SELECT MAX(processing_timestamp) FROM {{ this }})
            OR 
            -- Late-arriving data within lookback window
            order_date >= CURRENT_DATE - {{ var('lookback_days', 3) }}
        )
    {% endif %}

-- Strategy 2: Watermark-based with late data reconciliation  
-- models/intermediate/int_orders_incremental.sql
{{ config(
    materialized='incremental',
    unique_key=['order_id', 'data_version'],
    incremental_strategy='merge'
) }}

WITH watermark AS (
    SELECT 
        {% if is_incremental() %}
            MAX(data_watermark) AS last_watermark
        {% else %}
            '1900-01-01'::timestamp AS last_watermark
        {% endif %}
    FROM {{ this }}
),

source_with_versioning AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS data_version,
        CURRENT_TIMESTAMP() AS data_watermark
    FROM {{ ref('stg_orders') }}
    CROSS JOIN watermark w
    WHERE updated_at > w.last_watermark
)

SELECT * FROM source_with_versioning
WHERE data_version = 1  -- Only latest version per order
```

**Custom Merge Strategies**:
```sql
-- Advanced merge with custom logic
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    merge_update_columns=['total_orders', 'total_revenue', 'last_order_date']
) }}

SELECT 
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(order_amount) AS total_revenue,
    MAX(order_date) AS last_order_date,
    MIN(order_date) AS first_order_date,  -- Never changes after insert
    CURRENT_TIMESTAMP() AS last_updated
FROM {{ ref('stg_orders') }}
WHERE 1=1
    {% if is_incremental() %}
        AND order_date >= (
            SELECT DATEADD('day', -1, MAX(last_order_date))
            FROM {{ this }}
        )
    {% endif %}
GROUP BY customer_id
```

**Performance Optimization Patterns**:
```sql
-- Partitioned incremental processing
{% macro get_incremental_partition_filter() %}
    {% if is_incremental() %}
        {% set last_partition_query %}
            SELECT MAX(date_partition) FROM {{ this }}
        {% endset %}
        
        {% set results = run_query(last_partition_query) %}
        {% if execute %}
            {% set last_partition = results.columns[0].values()[0] %}
            AND date_partition >= '{{ last_partition }}'
        {% endif %}
    {% endif %}
{% endmacro %}

-- Usage in model
SELECT 
    order_id,
    customer_id,
    DATE(order_date) AS date_partition,
    order_amount
FROM {{ ref('stg_orders') }}
WHERE 1=1 {{ get_incremental_partition_filter() }}
```

---

### Testing and Data Quality (11-20)

**Q6: Design a comprehensive testing strategy for dbt models including custom tests, data quality monitoring, and CI/CD integration.**

**Answer**:
**Multi-Layer Testing Framework**:

1. **Source Data Testing**:
```yaml
# models/staging/sources.yml
version: 2
sources:
  - name: raw_ecommerce
    description: "Raw e-commerce data from operational systems"
    freshness:
      warn_after: {count: 6, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: _synced_at
    
    tables:
      - name: orders
        description: "Raw orders data"
        tests:
          - dbt_expectations.expect_table_row_count_to_be_between:
              min_value: 1000  # Minimum daily orders
              max_value: 100000  # Maximum realistic orders
        columns:
          - name: order_id
            tests:
              - unique
              - not_null
              - dbt_expectations.expect_column_values_to_match_regex:
                  regex: "^ORD[0-9]{8}$"
          - name: order_amount
            tests:
              - dbt_expectations.expect_column_values_to_be_between:
                  min_value: 0
                  max_value: 10000
              - dbt_expectations.expect_column_quantile_values_to_be_between:
                  quantile: 0.95
                  min_value: 50
                  max_value: 500
```

2. **Custom Business Logic Tests**:
```sql
-- tests/assert_revenue_consistency.sql
-- Ensure revenue aggregations are consistent across models
WITH mart_revenue AS (
    SELECT SUM(total_revenue) AS mart_total
    FROM {{ ref('mart_customer_summary') }}
),

staging_revenue AS (
    SELECT SUM(order_amount) AS staging_total  
    FROM {{ ref('stg_orders') }}
    WHERE order_status = 'completed'
)

SELECT 
    mart_total,
    staging_total,
    ABS(mart_total - staging_total) AS difference
FROM mart_revenue 
CROSS JOIN staging_revenue
WHERE ABS(mart_total - staging_total) > staging_total * 0.01  -- Allow 1% variance

-- tests/assert_no_future_dates.sql
-- Ensure no business dates are in the future
SELECT *
FROM {{ ref('stg_orders') }}
WHERE order_date > CURRENT_DATE
```

3. **Advanced Custom Test Macros**:
```sql
-- macros/test_revenue_reconciliation.sql
{% test revenue_reconciliation(model, revenue_column, date_column, tolerance=0.05) %}
    WITH daily_revenue AS (
        SELECT 
            {{ date_column }}::date AS revenue_date,
            SUM({{ revenue_column }}) AS daily_total
        FROM {{ model }}
        GROUP BY 1
    ),
    
    revenue_variance AS (
        SELECT 
            revenue_date,
            daily_total,
            LAG(daily_total) OVER (ORDER BY revenue_date) AS prev_day_total,
            ABS(daily_total - LAG(daily_total) OVER (ORDER BY revenue_date)) / 
                NULLIF(LAG(daily_total) OVER (ORDER BY revenue_date), 0) AS variance_pct
        FROM daily_revenue
    )
    
    SELECT *
    FROM revenue_variance
    WHERE variance_pct > {{ tolerance }}
      AND prev_day_total IS NOT NULL
      AND revenue_date >= CURRENT_DATE - 30  -- Only check recent data
{% endtest %}

-- Usage in model YAML
models:
  - name: mart_daily_revenue
    tests:
      - revenue_reconciliation:
          revenue_column: total_revenue
          date_column: order_date
          tolerance: 0.1  # 10% variance allowed
```

4. **Data Quality Monitoring**:
```sql
-- macros/data_quality_score.sql
{% macro calculate_data_quality_score(model_name) %}
    {% set quality_checks = [
        'COUNT(*) > 0',  -- Non-empty table
        'COUNT(DISTINCT primary_key) = COUNT(*)',  -- Uniqueness
        'SUM(CASE WHEN important_field IS NULL THEN 1 ELSE 0 END) / COUNT(*) < 0.05',  -- <5% nulls
        'MAX(created_at) >= CURRENT_DATE - 1'  -- Fresh data
    ] %}
    
    WITH quality_metrics AS (
        SELECT
            '{{ model_name }}' AS model_name,
            {% for check in quality_checks %}
            CASE WHEN {{ check }} THEN 1 ELSE 0 END AS check_{{ loop.index }}
            {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        FROM {{ model_name }}
    )
    
    SELECT 
        model_name,
        (
            {% for check in quality_checks %}
            check_{{ loop.index }}
            {%- if not loop.last -%} + {%- endif -%}
            {% endfor %}
        ) / {{ quality_checks | length }} AS quality_score
    FROM quality_metrics
{% endmacro %}

-- Create quality monitoring mart
-- models/utilities/data_quality_dashboard.sql
{{ config(materialized='table') }}

{% set models_to_monitor = [
    'mart_customer_summary',
    'mart_product_performance', 
    'mart_daily_revenue'
] %}

{% for model in models_to_monitor %}
    {{ calculate_data_quality_score(ref(model)) }}
    {% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
```

5. **CI/CD Integration**:
```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI/CD Pipeline

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  dbt-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.8'
          
      - name: Install dependencies
        run: |
          pip install dbt-snowflake==1.3.0
          dbt deps
          
      - name: Run dbt tests on changed models
        run: |
          # Get changed files
          git diff --name-only HEAD~1 HEAD | grep "models/" > changed_files.txt
          
          # Extract model names and run tests
          if [ -s changed_files.txt ]; then
            # Run tests on changed models and their downstream dependencies
            dbt test --select state:modified+ --state ./target/prod
          else
            echo "No model changes detected"
          fi
          
      - name: Run data quality checks
        run: |
          dbt run --select data_quality_dashboard
          dbt test --select data_quality_dashboard
```

6. **Alerting and Monitoring**:
```sql
-- Create alerting system for test failures
-- models/utilities/test_failure_alerts.sql
WITH recent_test_results AS (
    SELECT 
        test_name,
        model_name,
        status,
        execution_time,
        failures,
        run_started_at
    FROM {{ ref('dbt_test_results') }}
    WHERE run_started_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
),

alert_conditions AS (
    SELECT 
        *,
        CASE 
            WHEN status = 'fail' AND test_name LIKE '%unique%' THEN 'CRITICAL'
            WHEN status = 'fail' AND test_name LIKE '%not_null%' THEN 'HIGH'  
            WHEN status = 'fail' THEN 'MEDIUM'
            WHEN status = 'warn' THEN 'LOW'
            ELSE 'INFO'
        END AS alert_level
    FROM recent_test_results
    WHERE status IN ('fail', 'warn')
)

SELECT 
    alert_level,
    COUNT(*) AS alert_count,
    LISTAGG(model_name || ': ' || test_name, ', ') AS failed_tests
FROM alert_conditions
GROUP BY alert_level
ORDER BY 
    CASE alert_level 
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3
        ELSE 4
    END
```

---

**Q7: How do you implement sophisticated macros for business logic reuse and code standardization?**

**Answer**:
**Advanced Macro Patterns**:

1. **Statistical and Analytical Macros**:
```sql
-- macros/statistical_functions.sql
{% macro calculate_z_score(column, partition_by=none, alias_name=none) %}
    {% if alias_name %}
        {% set alias_clause = " AS " ~ alias_name %}
    {% else %}
        {% set alias_clause = " AS " ~ column ~ "_z_score" %}
    {% endif %}
    
    ABS(
        ({{ column }} - AVG({{ column }}) OVER (
            {% if partition_by %}
                PARTITION BY {{ partition_by }}
            {% endif %}
        )) / NULLIF(
            STDDEV({{ column }}) OVER (
                {% if partition_by %}
                    PARTITION BY {{ partition_by }}
                {% endif %}
            ), 0
        )
    ){{ alias_clause }}
{% endmacro %}

{% macro calculate_percentile_rank(column, partition_by=none) %}
    PERCENT_RANK() OVER (
        {% if partition_by %}
            PARTITION BY {{ partition_by }}
        {% endif %}
        ORDER BY {{ column }}
    ) AS {{ column }}_percentile_rank
{% endmacro %}

-- Usage in models
SELECT 
    customer_id,
    order_amount,
    {{ calculate_z_score('order_amount', 'customer_segment', 'amount_z_score') }},
    {{ calculate_percentile_rank('order_amount', 'customer_segment') }}
FROM customer_orders
```

2. **Business Logic Standardization**:
```sql
-- macros/business_metrics.sql
{% macro customer_lifecycle_stage(first_order_date, last_order_date, total_orders) %}
    CASE 
        WHEN {{ total_orders }} = 1 AND 
             DATEDIFF('day', {{ last_order_date }}, CURRENT_DATE) <= 30 
        THEN 'NEW_CUSTOMER'
        
        WHEN {{ total_orders }} >= 2 AND 
             DATEDIFF('day', {{ last_order_date }}, CURRENT_DATE) <= 90 
        THEN 'ACTIVE_CUSTOMER'
        
        WHEN {{ total_orders }} >= 5 AND 
             DATEDIFF('day', {{ last_order_date }}, CURRENT_DATE) <= 180
        THEN 'LOYAL_CUSTOMER'
        
        WHEN DATEDIFF('day', {{ last_order_date }}, CURRENT_DATE) > 365
        THEN 'CHURNED_CUSTOMER'
        
        WHEN DATEDIFF('day', {{ last_order_date }}, CURRENT_DATE) BETWEEN 180 AND 365
        THEN 'AT_RISK_CUSTOMER'
        
        ELSE 'DORMANT_CUSTOMER'
    END
{% endmacro %}

{% macro rfm_score(recency_days, frequency_orders, monetary_amount) %}
    -- RFM scoring with quintile-based approach
    CONCAT(
        -- Recency Score (1-5, where 5 is most recent)
        CASE 
            WHEN {{ recency_days }} <= 30 THEN '5'
            WHEN {{ recency_days }} <= 90 THEN '4'  
            WHEN {{ recency_days }} <= 180 THEN '3'
            WHEN {{ recency_days }} <= 365 THEN '2'
            ELSE '1'
        END,
        
        -- Frequency Score (1-5, where 5 is most frequent)
        CASE
            WHEN {{ frequency_orders }} >= 10 THEN '5'
            WHEN {{ frequency_orders }} >= 5 THEN '4'
            WHEN {{ frequency_orders }} >= 3 THEN '3'
            WHEN {{ frequency_orders }} >= 2 THEN '2'
            ELSE '1'
        END,
        
        -- Monetary Score (1-5, where 5 is highest value)
        CASE
            WHEN {{ monetary_amount }} >= 1000 THEN '5'
            WHEN {{ monetary_amount }} >= 500 THEN '4'
            WHEN {{ monetary_amount }} >= 200 THEN '3'
            WHEN {{ monetary_amount }} >= 100 THEN '2'
            ELSE '1'
        END
    )
{% endmacro %}
```

3. **Data Quality and Validation Macros**:
```sql
-- macros/data_validation.sql  
{% macro validate_business_rules(model_relation) %}
    {% set validation_queries = [] %}
    
    -- Check for negative revenues
    {% set negative_revenue_query %}
        SELECT COUNT(*) as negative_revenue_count
        FROM {{ model_relation }}
        WHERE revenue < 0
    {% endset %}
    {% do validation_queries.append(negative_revenue_query) %}
    
    -- Check for future dates
    {% set future_dates_query %}
        SELECT COUNT(*) as future_dates_count  
        FROM {{ model_relation }}
        WHERE order_date > CURRENT_DATE
    {% endset %}
    {% do validation_queries.append(future_dates_query) %}
    
    -- Execute validations and collect results
    {% for query in validation_queries %}
        {% if execute %}
            {% set results = run_query(query) %}
            {% if results.rows[0][0] > 0 %}
                {{ log("Validation failed for query: " ~ query, info=true) }}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro audit_column_distributions(model_relation, columns) %}
    {% for column in columns %}
        {% set distribution_query %}
            SELECT 
                '{{ column }}' AS column_name,
                COUNT(*) AS total_count,
                COUNT({{ column }}) AS non_null_count,
                COUNT(DISTINCT {{ column }}) AS unique_count,
                {% if column in ['amount', 'revenue', 'price', 'quantity'] %}
                    MIN({{ column }}) AS min_value,
                    MAX({{ column }}) AS max_value,
                    AVG({{ column }}) AS avg_value,
                    STDDEV({{ column }}) AS stddev_value
                {% else %}
                    NULL AS min_value,
                    NULL AS max_value, 
                    NULL AS avg_value,
                    NULL AS stddev_value
                {% endif %}
            FROM {{ model_relation }}
        {% endset %}
        
        {% if execute and loop.first %}
            {% set results = run_query(distribution_query) %}
            {{ log("Column profiling results: " ~ results.rows, info=true) }}
        {% endif %}
    {% endfor %}
{% endmacro %}
```

4. **Dynamic Model Generation**:
```sql
-- macros/model_generation.sql
{% macro generate_cohort_analysis(source_table, date_column, customer_column, value_column) %}
    
    {% set cohort_sql %}
        WITH customer_cohorts AS (
            SELECT 
                {{ customer_column }},
                DATE_TRUNC('month', MIN({{ date_column }})) AS cohort_month,
                MIN({{ date_column }}) AS first_purchase_date
            FROM {{ source_table }}
            GROUP BY {{ customer_column }}
        ),
        
        cohort_data AS (
            SELECT 
                cc.cohort_month,
                cc.{{ customer_column }},
                {{ date_column }},
                {{ value_column }},
                DATEDIFF(
                    'month', 
                    cc.cohort_month, 
                    DATE_TRUNC('month', {{ date_column }})
                ) AS period_number
            FROM {{ source_table }} s
            JOIN customer_cohorts cc ON s.{{ customer_column }} = cc.{{ customer_column }}
        )
        
        SELECT 
            cohort_month,
            period_number,
            COUNT(DISTINCT {{ customer_column }}) AS customers,
            SUM({{ value_column }}) AS revenue,
            AVG({{ value_column }}) AS avg_revenue_per_customer
        FROM cohort_data
        GROUP BY cohort_month, period_number
        ORDER BY cohort_month, period_number
    {% endset %}
    
    {{ return(cohort_sql) }}
{% endmacro %}

-- Generate cohort analysis model dynamically
-- models/analysis/customer_cohorts.sql
{{ config(materialized='table') }}

{{ generate_cohort_analysis(
    source_table=ref('stg_orders'),
    date_column='order_date', 
    customer_column='customer_id',
    value_column='order_amount'
) }}
```

5. **Environment and Deployment Macros**:
```sql
-- macros/deployment_helpers.sql
{% macro get_environment_suffix() %}
    {% if target.name == 'prod' %}
        {{ return('') }}
    {% else %}
        {{ return('_' ~ target.name) }}
    {% endif %}
{% endmacro %}

{% macro create_model_alias(model_name) %}
    {{ model_name }}{{ get_environment_suffix() }}
{% endmacro %}

{% macro grant_select_on_schemas(schemas, role) %}
    {% for schema in schemas %}
        {% set grant_sql %}
            GRANT USAGE ON SCHEMA {{ target.database }}.{{ schema }} TO ROLE {{ role }};
            GRANT SELECT ON ALL TABLES IN SCHEMA {{ target.database }}.{{ schema }} TO ROLE {{ role }};
            GRANT SELECT ON FUTURE TABLES IN SCHEMA {{ target.database }}.{{ schema }} TO ROLE {{ role }};
        {% endset %}
        
        {% if execute %}
            {% do run_query(grant_sql) %}
            {{ log("Granted permissions on " ~ schema ~ " to " ~ role, info=true) }}
        {% endif %}
    {% endfor %}
{% endmacro %}

-- Usage in post-hook
{{ config(
    post_hook="{{ grant_select_on_schemas(['marts', 'intermediate'], 'ANALYTICS_ROLE') }}"
) }}
```

6. **Performance and Monitoring Macros**:
```sql
-- macros/performance_monitoring.sql  
{% macro log_model_performance(model_name) %}
    {% if execute %}
        {% set start_time = modules.datetime.datetime.now() %}
        {{ log("Starting execution of " ~ model_name ~ " at " ~ start_time, info=true) }}
        
        -- This would be called in a post-hook to log completion time
        {% set performance_log %}
            INSERT INTO {{ target.schema }}_monitoring.model_performance
            VALUES (
                '{{ model_name }}',
                '{{ invocation_id }}',
                '{{ start_time }}',
                CURRENT_TIMESTAMP(),
                DATEDIFF('second', '{{ start_time }}', CURRENT_TIMESTAMP())
            )
        {% endset %}
        
        {{ return(performance_log) }}
    {% endif %}
{% endmacro %}

{% macro optimize_table_clustering(table_relation, cluster_columns) %}
    {% set cluster_sql %}
        ALTER TABLE {{ table_relation }} 
        CLUSTER BY ({{ cluster_columns | join(', ') }})
    {% endset %}
    
    {% if execute %}
        {% do run_query(cluster_sql) %}
        {{ log("Applied clustering on " ~ table_relation ~ " for columns: " ~ cluster_columns, info=true) }}
    {% endif %}
{% endmacro %}
```

These advanced macro patterns enable:
- **Consistent business logic** across all models
- **Reusable statistical functions** for analytics
- **Automated data validation** and quality checks
- **Dynamic model generation** for common patterns
- **Environment-aware deployments** with proper permissions
- **Performance monitoring** and optimization

---

### Materialization and Performance (21-30)

**Q8: Compare different materialization strategies and when to use each in an Analytics Engineering context.**

**Answer**:
**Materialization Strategy Decision Matrix**:

| Use Case | Materialization | Reason | Example |
|----------|-----------------|--------|---------|
| Staging layer | `view` | Fast compilation, always fresh | Raw data cleanup |
| Business logic | `ephemeral` | Cost optimization | Intermediate transforms |  
| Analytics marts | `table` | Query performance | Dashboard backing tables |
| Large aggregations | `incremental` | Build efficiency | Daily summaries |
| SCD tracking | `snapshot` | Historical preservation | Dimension changes |

**1. View Materialization**:
```sql
-- Best for: Staging layer, simple transformations, always-fresh data
{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT 
    order_id,
    customer_id,
    -- Simple transformations that execute quickly
    UPPER(TRIM(customer_email)) AS customer_email_clean,
    order_amount::DECIMAL(10,2) AS order_amount,
    DATE(order_timestamp) AS order_date
FROM {{ source('raw_data', 'orders') }}
WHERE order_status != 'cancelled'

-- Pros: Always fresh, no storage cost, fast compilation
-- Cons: Repeated computation, slower query performance
-- Analytics Engineering use: Staging layer standardization
```

**2. Table Materialization**:
```sql
-- Best for: Marts, dashboards, complex aggregations
{{ config(
    materialized='table',
    cluster_by=['region', 'customer_segment'],
    post_hook="GRANT SELECT ON {{ this }} TO ROLE analytics_users"
) }}

SELECT 
    customer_id,
    customer_segment,
    region,
    -- Complex calculations materialized for performance  
    COUNT(DISTINCT order_id) AS lifetime_orders,
    SUM(order_amount) AS lifetime_value,
    AVG(order_amount) AS avg_order_value,
    
    -- Statistical calculations that are expensive to recompute
    PERCENT_RANK() OVER (ORDER BY SUM(order_amount)) AS ltv_percentile,
    STDDEV(order_amount) AS order_variability
FROM {{ ref('int_customer_orders') }}
GROUP BY customer_id, customer_segment, region

-- Pros: Fast queries, good for BI tools, clustering optimization
-- Cons: Storage costs, potential staleness, full refresh on changes
-- Analytics Engineering use: Final marts for business consumption
```

**3. Ephemeral Materialization**:
```sql
-- Best for: Intermediate logic, cost optimization
{{ config(materialized='ephemeral') }}

WITH order_metrics AS (
    SELECT 
        customer_id,
        order_date,
        order_amount,
        -- Business logic that gets reused by multiple downstream models
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_sequence
    FROM {{ ref('stg_orders') }}
),

customer_behavior AS (
    SELECT 
        customer_id,
        -- Calculate metrics that multiple models need
        AVG(DATEDIFF('day', prev_order_date, order_date)) AS avg_days_between_orders,
        COUNT(*) AS total_orders,
        MAX(order_sequence) AS customer_order_count
    FROM order_metrics
    WHERE prev_order_date IS NOT NULL
    GROUP BY customer_id
)

SELECT * FROM customer_behavior

-- Pros: No storage cost, fresh data, DRY principle
-- Cons: Recomputed for each downstream model, potential performance impact
-- Analytics Engineering use: Shared business logic across models
```

**4. Incremental Materialization**:
```sql
-- Best for: Large fact tables, event data, performance optimization
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    cluster_by=['event_date', 'customer_id'],
    on_schema_change='append_new_columns'
) }}

WITH events_processed AS (
    SELECT 
        event_id,
        customer_id,
        event_type,
        event_date,
        event_properties,
        -- Add processing metadata
        CURRENT_TIMESTAMP() AS processed_at,
        '{{ invocation_id }}' AS dbt_run_id
    FROM {{ ref('stg_customer_events') }}
    WHERE 1=1
        {% if is_incremental() %}
            -- Only process new/updated events
            AND event_date >= (
                SELECT DATEADD('day', -1, MAX(event_date)) 
                FROM {{ this }}
            )
        {% endif %}
),

event_enrichment AS (
    SELECT 
        ep.*,
        -- Join to slowly changing dimensions at event time
        c.customer_segment_at_event_time,
        c.customer_tier_at_event_time
    FROM events_processed ep
    LEFT JOIN {{ ref('dim_customer_history') }} c 
        ON ep.customer_id = c.customer_id
        AND ep.event_date BETWEEN c.effective_from AND c.effective_to
)

SELECT * FROM event_enrichment

-- Pros: Fast builds, handles large data, cost-effective
-- Cons: Complexity, potential data quality issues, merge logic required
-- Analytics Engineering use: Large fact tables, event streams
```

**5. Custom Materialization**:
```sql
-- macros/materializations/pivot_table.sql
{% materialization pivot_table, default %}
    {% set target_relation = this %}
    {% set temp_relation = make_temp_relation(target_relation) %}
    
    -- Get pivot values dynamically
    {% set pivot_column_query %}
        SELECT DISTINCT {{ pivot_column }}
        FROM ({{ sql }})
        ORDER BY {{ pivot_column }}
    {% endset %}
    
    {% if execute %}
        {% set pivot_values = run_query(pivot_column_query) %}
        {% set pivot_columns = pivot_values.columns[0].values() %}
    {% else %}
        {% set pivot_columns = [] %}
    {% endif %}
    
    -- Generate dynamic pivot SQL
    {% set pivot_sql %}
        SELECT 
            {{ group_by_columns | join(', ') }},
            {% for value in pivot_columns %}
                SUM(CASE WHEN {{ pivot_column }} = '{{ value }}' 
                         THEN {{ aggregate_column }} END) AS {{ value | replace(' ', '_') }}
                {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        FROM ({{ sql }})
        GROUP BY {{ group_by_columns | join(', ') }}
    {% endset %}
    
    {{ create_table_as(False, target_relation, pivot_sql) }}
{% endmaterialization %}

-- Usage
{{ config(
    materialized='pivot_table',
    pivot_column='product_category',
    aggregate_column='sales_amount',
    group_by_columns=['customer_id', 'order_date']
) }}

SELECT 
    customer_id,
    order_date,
    product_category,
    sales_amount
FROM {{ ref('stg_order_items') }}
```

**Performance Optimization Strategies**:
```sql
-- Strategy 1: Layered materialization for different use cases
-- Fast development: views in staging
-- Cost optimization: ephemeral for intermediate
-- Query performance: tables/incremental for marts

-- Strategy 2: Environment-specific materialization
models:
  company_analytics:
    staging:
      +materialized: "{{ 'view' if target.name == 'dev' else 'table' }}"
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: "{{ 'view' if target.name == 'dev' else 'incremental' }}"

-- Strategy 3: Dynamic materialization based on table size
{% macro smart_materialization() %}
    {% set row_count_query %}
        SELECT COUNT(*) FROM {{ this }}
    {% endset %}
    
    {% if execute %}
        {% set result = run_query(row_count_query) %}
        {% set row_count = result.columns[0].values()[0] %}
        
        {% if row_count > 1000000 %}
            {{ return('incremental') }}
        {% elif row_count > 100000 %}
            {{ return('table') }}
        {% else %}
            {{ return('view') }}
        {% endif %}
    {% else %}
        {{ return('table') }}
    {% endif %}
{% endmacro %}
```

**Materialization Selection Criteria**:
- **Data Size**: <100K rows → view, 100K-1M → table, >1M → incremental
- **Update Frequency**: Real-time → view, Daily → incremental, Weekly → table
- **Query Patterns**: OLTP-style → view, OLAP-style → table, Mixed → incremental
- **Development Stage**: Development → view, Production → optimized strategy
- **Cost Sensitivity**: High → ephemeral/view, Medium → incremental, Low → table

---

**Q9: How do you optimize dbt model performance for large-scale data processing?**

**Answer**:
**Performance Optimization Framework**:

1. **Query Optimization Techniques**:
```sql
-- ❌ Inefficient pattern: Multiple CTEs with repeated scans
WITH customer_orders AS (
    SELECT customer_id, COUNT(*) as order_count
    FROM orders 
    GROUP BY customer_id
),
customer_revenue AS (
    SELECT customer_id, SUM(order_amount) as total_revenue  
    FROM orders
    GROUP BY customer_id
),
customer_recency AS (
    SELECT customer_id, MAX(order_date) as last_order
    FROM orders
    GROUP BY customer_id
)
-- Multiple table scans of the same large table

-- ✅ Optimized pattern: Single scan with all metrics
WITH customer_metrics AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(order_amount) as total_revenue,
        MAX(order_date) as last_order,
        MIN(order_date) as first_order,
        -- Compute everything in one pass
        AVG(order_amount) as avg_order_value,
        COUNT(DISTINCT DATE(order_date)) as active_days
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)

SELECT 
    *,
    -- Derived metrics from already aggregated data
    total_revenue / order_count as avg_order_value,
    DATEDIFF('day', first_order, last_order) as customer_lifespan_days
FROM customer_metrics
```

2. **Incremental Processing Optimization**:
```sql
-- Advanced incremental with partition-based optimization
{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'metric_date'],
    incremental_strategy='merge',
    cluster_by=['metric_date', 'customer_segment'],
    partition_by={'field': 'metric_date', 'data_type': 'date', 'granularity': 'day'}
) }}

WITH incremental_filter AS (
    SELECT 
        {% if is_incremental() %}
            -- Efficient partition-based filtering
            GREATEST(
                (SELECT MAX(metric_date) FROM {{ this }}),
                CURRENT_DATE - 7  -- Lookback window for late data
            ) AS filter_date
        {% else %}
            DATE('2020-01-01') AS filter_date
        {% endif %}
),

daily_customer_metrics AS (
    SELECT 
        customer_id,
        DATE(order_date) AS metric_date,
        COUNT(*) AS daily_orders,
        SUM(order_amount) AS daily_revenue,
        -- Pre-aggregate to reduce downstream processing
        AVG(order_amount) AS daily_avg_order_value
    FROM {{ ref('stg_orders') }} o
    CROSS JOIN incremental_filter f
    WHERE DATE(o.order_date) >= f.filter_date
    GROUP BY customer_id, DATE(order_date)
)

SELECT 
    *,
    -- Window functions on pre-aggregated data
    SUM(daily_revenue) OVER (
        PARTITION BY customer_id 
        ORDER BY metric_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30_day_revenue
FROM daily_customer_metrics
```

3. **Memory and Compute Optimization**:
```sql
-- macros/performance_optimization.sql
{% macro optimize_large_joins() %}
    -- Use broadcast joins for dimension tables
    -- Use shuffle joins for large fact tables
    {% if adapter.type() == 'snowflake' %}
        -- Snowflake-specific optimization hints
        /*+ USE_CACHED_RESULT(FALSE) */
    {% elif adapter.type() == 'bigquery' %}
        -- BigQuery-specific optimization
        -- Use APPROX_COUNT_DISTINCT for large cardinality estimates
    {% endif %}
{% endmacro %}

-- Optimized large table joins
WITH fact_table_sample AS (
    -- Use sampling for development/testing
    SELECT *
    FROM {{ ref('large_fact_table') }}
    {% if target.name == 'dev' %}
        TABLESAMPLE SYSTEM (1 PERCENT)  -- 1% sample in development
    {% endif %}
),

dimension_broadcast AS (
    -- Small dimension tables - broadcast join
    SELECT *
    FROM {{ ref('dim_customer') }}
    WHERE is_active = TRUE  -- Filter early to reduce join size
),

optimized_join AS (
    SELECT {{ optimize_large_joins() }}
        f.transaction_id,
        f.customer_id,
        f.transaction_amount,
        d.customer_segment,
        d.customer_tier,
        -- Only select needed columns to reduce memory usage
        ROW_NUMBER() OVER (
            PARTITION BY f.customer_id 
            ORDER BY f.transaction_date DESC
        ) AS recency_rank
    FROM fact_table_sample f
    INNER JOIN dimension_broadcast d 
        ON f.customer_id = d.customer_id
    WHERE f.transaction_date >= CURRENT_DATE - 90  -- Reduce data volume early
)

SELECT *
FROM optimized_join
WHERE recency_rank <= 10  -- Final filtering after window function
```

4. **Clustering and Partitioning Strategy**:
```sql
-- Strategic clustering for query patterns
{{ config(
    materialized='incremental',
    cluster_by=['date_partition', 'high_cardinality_dimension', 'filter_column'],
    -- Order clustering keys by:
    -- 1. Most common filter (usually date)
    -- 2. High cardinality dimension (customer_id, product_id)  
    -- 3. Common grouping column (region, category)
) }}

-- Partition elimination example
WITH partitioned_data AS (
    SELECT 
        *,
        -- Create partition column for efficient filtering
        DATE_TRUNC('month', order_date) AS date_partition
    FROM {{ ref('stg_orders') }}
    WHERE 1=1
        -- Partition elimination filter
        {% if is_incremental() %}
            AND DATE_TRUNC('month', order_date) >= (
                SELECT DATE_TRUNC('month', MAX(order_date))
                FROM {{ this }}
            )
        {% endif %}
)
```

5. **Parallel Processing and Batching**:
```sql
-- Macro for parallel processing of large datasets
{% macro process_in_batches(base_query, batch_column, batch_size=100000) %}
    {% set batch_query %}
        WITH batch_bounds AS (
            SELECT 
                MIN({{ batch_column }}) as min_val,
                MAX({{ batch_column }}) as max_val,
                COUNT(*) as total_rows
            FROM ({{ base_query }})
        ),
        batch_ranges AS (
            SELECT 
                min_val + (batch_num * {{ batch_size }}) as batch_start,
                min_val + ((batch_num + 1) * {{ batch_size }}) - 1 as batch_end
            FROM batch_bounds
            CROSS JOIN (
                SELECT ROW_NUMBER() OVER () - 1 as batch_num
                FROM TABLE(GENERATOR(ROWCOUNT => 
                    CEIL(total_rows / {{ batch_size }})
                ))
            ) batch_nums
        )
        
        SELECT b.*
        FROM ({{ base_query }}) b
        JOIN batch_ranges r ON b.{{ batch_column }} BETWEEN r.batch_start AND r.batch_end
    {% endset %}
    
    {{ return(batch_query) }}
{% endmacro %}

-- Usage for processing large customer base
{{ process_in_batches(
    "SELECT customer_id, order_data FROM large_customer_table",
    "customer_id",
    50000
) }}
```

6. **Performance Monitoring and Alerting**:
```sql
-- models/utilities/model_performance_monitoring.sql
{{ config(
    materialized='incremental',
    unique_key=['model_name', 'run_date'],
    post_hook='{{ alert_on_slow_models() }}'
) }}

WITH model_performance AS (
    SELECT 
        model_name,
        DATE(run_started_at) as run_date,
        AVG(total_elapsed_time) as avg_runtime_seconds,
        MAX(total_elapsed_time) as max_runtime_seconds,
        COUNT(*) as run_count,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs
    FROM {{ ref('dbt_run_results') }}
    WHERE run_started_at >= CURRENT_DATE - 7
    GROUP BY model_name, DATE(run_started_at)
),

performance_trends AS (
    SELECT 
        *,
        -- Calculate performance trends
        LAG(avg_runtime_seconds, 1) OVER (
            PARTITION BY model_name 
            ORDER BY run_date
        ) as prev_day_runtime,
        
        -- Performance degradation detection
        CASE 
            WHEN avg_runtime_seconds > LAG(avg_runtime_seconds, 1) OVER (
                PARTITION BY model_name ORDER BY run_date
            ) * 1.5 THEN 'PERFORMANCE_DEGRADATION'
            WHEN successful_runs / run_count < 0.9 THEN 'RELIABILITY_ISSUE'
            ELSE 'HEALTHY'
        END as performance_status
    FROM model_performance
)

SELECT * FROM performance_trends
WHERE performance_status != 'HEALTHY'

-- Alert macro
{% macro alert_on_slow_models() %}
    {% if execute %}
        {% set slow_models_query %}
            SELECT model_name, avg_runtime_seconds
            FROM {{ this }}
            WHERE performance_status = 'PERFORMANCE_DEGRADATION'
            AND run_date = CURRENT_DATE
        {% endset %}
        
        {% set results = run_query(slow_models_query) %}
        {% if results.rows %}
            {{ log("ALERT: Performance degradation detected in models: " ~ results.rows, info=true) }}
        {% endif %}
    {% endif %}
{% endmacro %}
```

**Performance Best Practices Summary**:
- **Single-pass aggregation**: Compute multiple metrics in one query
- **Early filtering**: Apply WHERE clauses as early as possible
- **Efficient joins**: Use appropriate join strategies for data sizes
- **Proper clustering**: Align clustering with query patterns
- **Incremental processing**: Process only changed data
- **Memory management**: Select only needed columns
- **Parallel processing**: Break large operations into batches
- **Performance monitoring**: Track and alert on performance degradation

---

### Advanced dbt Features (31-40)

**Q10: How do you implement comprehensive documentation and metadata management in dbt for Analytics Engineering governance?**

**Answer**:
**Documentation Framework Architecture**:

1. **Model Documentation Structure**:
```yaml
# models/marts/core/_core_models.yml
version: 2

models:
  - name: mart_customer_summary
    description: |
      ## Business Purpose
      Primary customer analytics table providing 360-degree customer view for:
      - Executive dashboards and reporting
      - Customer segmentation and targeting
      - Retention and churn analysis
      - CLV modeling and predictions
      
      ## Data Freshness
      Updated daily at 6 AM UTC via dbt Cloud scheduled run
      
      ## Key Business Rules
      - Only includes customers with at least one completed order
      - Customer segments based on RFM analysis (Recency, Frequency, Monetary)
      - Churn probability calculated using logistic regression model
      
      ## Dependencies
      - Upstream: {{ doc("int_customer_360") }}
      - Sources: Raw orders, customers, payments from operational systems
      
      ## Owner
      **Analytics Engineering Team** (@analytics-eng-team)
      Slack: #analytics-engineering
      
    meta:
      owner: "@analytics-eng-team"
      slack_channel: "#analytics-engineering"  
      business_stakeholder: "@marketing-team"
      refresh_frequency: "daily"
      sla_hours: 8
      data_classification: "internal"
      
    columns:
      - name: customer_id
        description: |
          **Primary Key**: Unique customer identifier from source system
          
          **Business Logic**: 
          - Format: CUST + 8-digit number (e.g., CUST00001234)
          - Generated by operational CRM system
          - Immutable once created
          
        meta:
          data_type: "business_key"
          pii_classification: "non_pii"
        tests:
          - unique
          - not_null
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^CUST[0-9]{8}$"
              
      - name: customer_ltv_predicted
        description: |
          **Predicted Customer Lifetime Value** (12-month forward-looking)
          
          **Calculation Method**:
          ```sql
          AOV × Annual_Purchase_Frequency × Predicted_Customer_Lifespan
          ```
          
          **Model Details**:
          - Based on historical RFM patterns
          - Includes churn probability decay factor
          - 95% confidence interval: ±${{ var('ltv_confidence_interval', 50) }}
          
        meta:
          calculation_logic: "predictive_model"
          confidence_interval: 0.95
          model_version: "v2.1"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000
              severity: warn
              
      - name: customer_segment_rfm
        description: |
          **RFM-Based Customer Segmentation**
          
          | Segment | Description | Typical Actions |
          |---------|-------------|-----------------|
          | Champions | Best customers - high value, frequent, recent | VIP treatment, exclusive offers |
          | Loyalists | Regular customers with good history | Retention programs, upselling |
          | At Risk | Previously good customers becoming inactive | Win-back campaigns, surveys |
          | Lost | No recent activity, low engagement | Aggressive discounts or write-off |
          
        meta:
          segmentation_logic: "rfm_quintiles"
          business_use_case: ["marketing_campaigns", "customer_service_prioritization"]
        tests:
          - accepted_values:
              values: ['Champions', 'Loyalists', 'Potential Loyalists', 'New Customers', 'At Risk', 'Lost']
```

2. **Automated Documentation Generation**:
```sql
-- macros/documentation_helpers.sql
{% macro generate_column_documentation() %}
    {% if execute %}
        {% for model in graph.nodes.values() %}
            {% if model.resource_type == 'model' %}
                {% set model_columns = adapter.get_columns_in_relation(model) %}
                
                {% set doc_content = [] %}
                {% for column in model_columns %}
                    {% set column_info = {
                        'name': column.name,
                        'data_type': column.data_type,
                        'description': 'Auto-generated documentation for ' ~ column.name
                    } %}
                    {% do doc_content.append(column_info) %}
                {% endfor %}
                
                {{ log("Generated documentation for model: " ~ model.name, info=true) }}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro document_model_lineage(model_name) %}
    {% set lineage_query %}
        WITH model_dependencies AS (
            SELECT 
                '{{ model_name }}' as target_model,
                referenced_model,
                dependency_type
            FROM (
                {% for upstream_model in graph.nodes[model_name].depends_on.nodes %}
                    SELECT '{{ upstream_model }}' as referenced_model, 'direct' as dependency_type
                    {% if not loop.last %} UNION ALL {% endif %}
                {% endfor %}
            )
        ),
        
        downstream_models AS (
            {% set downstream_models = [] %}
            {% for node_name, node in graph.nodes.items() %}
                {% if model_name in node.depends_on.nodes %}
                    {% do downstream_models.append(node_name) %}
                {% endif %}
            {% endfor %}
            
            SELECT 
                '{{ model_name }}' as source_model,
                downstream_model,
                'consumed_by' as relationship_type
            FROM (
                {% for downstream in downstream_models %}
                    SELECT '{{ downstream }}' as downstream_model
                    {% if not loop.last %} UNION ALL {% endif %}
                {% endfor %}
            )
        )
        
        SELECT * FROM model_dependencies
        UNION ALL
        SELECT * FROM downstream_models
    {% endset %}
    
    {{ return(lineage_query) }}
{% endmacro %}
```

3. **Business Glossary and Metrics Dictionary**:
```yaml
# models/docs/business_glossary.md
{% docs business_glossary %}

# Analytics Engineering Business Glossary

## Customer Metrics

### Customer Lifetime Value (CLV)
**Definition**: The predicted net profit attributed to the entire future relationship with a customer

**Calculation**: 
```sql
(Average Order Value) × (Purchase Frequency) × (Customer Lifespan)
```

**Business Context**: Used for customer acquisition cost justification and retention investment decisions

**Owner**: Customer Analytics Team
**Last Updated**: {{ run_started_at }}

### RFM Analysis
**Definition**: Customer segmentation technique using Recency, Frequency, and Monetary value

**Scoring Method**:
- **Recency**: Days since last purchase (1-5 scale, 5 = most recent)
- **Frequency**: Number of purchases (1-5 scale, 5 = most frequent)  
- **Monetary**: Total spending amount (1-5 scale, 5 = highest spender)

**Business Applications**:
- Marketing campaign targeting
- Customer service prioritization
- Inventory planning by customer segment

### Churn Probability
**Definition**: Likelihood that a customer will not make another purchase within 12 months

**Model Type**: Logistic regression using features:
- Recency of last purchase
- Historical purchase frequency
- Average order value trends
- Customer service interactions

**Interpretation**:
- 0.0-0.3: Low churn risk (retain with standard programs)
- 0.3-0.7: Medium churn risk (targeted retention campaigns)
- 0.7-1.0: High churn risk (aggressive win-back efforts)

{% enddocs %}

# Metric definitions with business context
{% docs metric_definitions %}

## Core Business Metrics

| Metric | Definition | Calculation | Business Use |
|--------|------------|-------------|--------------|
| **Active Customers** | Customers with purchases in last 30 days | `COUNT(DISTINCT customer_id) WHERE last_purchase_date >= CURRENT_DATE - 30` | Monthly business reviews |
| **Average Order Value (AOV)** | Mean value per transaction | `SUM(order_amount) / COUNT(orders)` | Pricing strategy, promotions |
| **Customer Acquisition Cost (CAC)** | Cost to acquire one new customer | `Marketing_Spend / New_Customers` | Marketing ROI analysis |
| **Monthly Recurring Revenue (MRR)** | Predictable monthly subscription revenue | `SUM(subscription_amount WHERE status = 'active')` | Growth tracking, forecasting |

{% enddocs %}
```

4. **Data Lineage and Impact Analysis**:
```sql
-- models/utilities/data_lineage_map.sql
{{ config(materialized='table') }}

WITH model_dependencies AS (
    SELECT 
        parent_model,
        child_model,
        dependency_type,
        schema_name,
        materialization_type
    FROM (
        {% for node_name, node in graph.nodes.items() %}
            {% if node.resource_type == 'model' %}
                {% for upstream_node in node.depends_on.nodes %}
                    SELECT 
                        '{{ upstream_node }}' as parent_model,
                        '{{ node_name }}' as child_model,
                        'model_dependency' as dependency_type,
                        '{{ node.schema }}' as schema_name,
                        '{{ node.config.materialized }}' as materialization_type
                    {% if not loop.last %} UNION ALL {% endif %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    )
),

business_impact_scoring AS (
    SELECT 
        *,
        -- Score business criticality based on downstream dependencies
        CASE schema_name
            WHEN 'marts' THEN 'HIGH'
            WHEN 'intermediate' THEN 'MEDIUM' 
            WHEN 'staging' THEN 'LOW'
        END AS business_criticality,
        
        -- Calculate impact radius (how many models would be affected by changes)
        COUNT(*) OVER (PARTITION BY parent_model) AS downstream_impact_count
    FROM model_dependencies
)

SELECT 
    parent_model,
    business_criticality,
    downstream_impact_count,
    LISTAGG(child_model, ', ') AS affected_models,
    
    -- Risk assessment for changes
    CASE 
        WHEN business_criticality = 'HIGH' AND downstream_impact_count > 5 THEN 'CRITICAL_RISK'
        WHEN business_criticality = 'HIGH' AND downstream_impact_count > 2 THEN 'HIGH_RISK'
        WHEN downstream_impact_count > 10 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS change_risk_level
FROM business_impact_scoring
GROUP BY parent_model, business_criticality, downstream_impact_count
ORDER BY downstream_impact_count DESC
```

5. **Automated Documentation Validation**:
```sql
-- tests/documentation_completeness.sql
-- Ensure all important models have proper documentation

WITH model_documentation_check AS (
    SELECT 
        model_name,
        CASE WHEN description IS NOT NULL AND LENGTH(description) > 50 
             THEN 1 ELSE 0 END AS has_description,
        CASE WHEN owner IS NOT NULL 
             THEN 1 ELSE 0 END AS has_owner,
        CASE WHEN meta_tags IS NOT NULL 
             THEN 1 ELSE 0 END AS has_meta_tags,
        
        -- Critical models require higher documentation standards
        CASE WHEN schema_name = 'marts' THEN 1 ELSE 0 END AS is_critical_model
    FROM information_schema.tables
    WHERE table_schema IN ('staging', 'intermediate', 'marts')
),

documentation_scores AS (
    SELECT 
        *,
        (has_description + has_owner + has_meta_tags) AS documentation_score,
        
        -- Different standards for different model types
        CASE 
            WHEN is_critical_model = 1 AND documentation_score < 3 THEN 'CRITICAL_MISSING_DOCS'
            WHEN is_critical_model = 0 AND documentation_score < 2 THEN 'STANDARD_MISSING_DOCS'
            ELSE 'ADEQUATELY_DOCUMENTED'
        END AS documentation_status
    FROM model_documentation_check
)

-- Test fails if critical models lack documentation
SELECT *
FROM documentation_scores
WHERE documentation_status LIKE '%MISSING_DOCS%'

-- Macro to enforce documentation standards
{% macro enforce_documentation_standards() %}
    {% for node_name, node in graph.nodes.items() %}
        {% if node.resource_type == 'model' and node.config.schema == 'marts' %}
            {% if not node.description or node.description | length < 50 %}
                {{ exceptions.raise_compiler_error("Model " ~ node_name ~ " requires detailed description (>50 characters)") }}
            {% endif %}
            
            {% if not node.config.meta or not node.config.meta.owner %}
                {{ exceptions.raise_compiler_error("Model " ~ node_name ~ " requires owner in meta config") }}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
```

6. **Interactive Documentation Features**:
```sql
-- macros/interactive_docs.sql  
{% macro generate_model_health_check() %}
    CREATE OR REPLACE VIEW documentation_health_dashboard AS
    WITH model_stats AS (
        SELECT 
            model_name,
            schema_name,
            row_count,
            last_updated,
            documentation_completeness_score,
            test_coverage_percentage,
            
            -- Health indicators
            CASE 
                WHEN last_updated < CURRENT_DATE - 7 THEN 'STALE'
                WHEN test_coverage_percentage < 50 THEN 'UNDERTESTED'
                WHEN documentation_completeness_score < 0.7 THEN 'UNDERDOCUMENTED'
                ELSE 'HEALTHY'
            END AS health_status
        FROM model_metadata
    )
    
    SELECT 
        *,
        -- Overall health score (0-100)
        (documentation_completeness_score * 40 +
         test_coverage_percentage * 40 +
         CASE WHEN health_status = 'HEALTHY' THEN 20 ELSE 0 END) AS overall_health_score
    FROM model_stats
    ORDER BY overall_health_score ASC;
{% endmacro %}
```

This comprehensive documentation framework provides:
- **Rich model descriptions** with business context and technical details
- **Automated lineage tracking** and impact analysis
- **Business glossary** with consistent metric definitions
- **Documentation quality enforcement** through testing
- **Interactive health monitoring** for documentation completeness
- **Stakeholder-friendly documentation** that bridges technical and business domains

---

### Deployment and Operations (41-50)

**Q11: Design a production-ready CI/CD pipeline for dbt with proper testing, deployment strategies, and monitoring.**

**Answer**:
**Complete CI/CD Pipeline Architecture**:

1. **Multi-Environment Strategy**:
```yaml
# profiles.yml
analytics:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: ANALYTICS_DEV
      database: ANALYTICS_DEV
      warehouse: DEV_WH
      schema: "{{ env_var('SNOWFLAKE_SCHEMA', 'dbt_' + env_var('USER')) }}"
      
    staging:
      type: snowflake  
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_SERVICE_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role: ANALYTICS_STAGING
      database: ANALYTICS_STAGING
      warehouse: STAGING_WH
      schema: staging
      
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_PROD_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PROD_KEY_PATH') }}"
      role: ANALYTICS_PROD
      database: ANALYTICS_PROD
      warehouse: PROD_WH
      schema: prod
```

2. **GitHub Actions CI/CD Workflow**:
```yaml
# .github/workflows/dbt_ci_cd.yml
name: dbt CI/CD Pipeline

on:
  pull_request:
    branches: [main, develop]
    paths: ['models/**', 'macros/**', 'tests/**', 'dbt_project.yml']
  push:
    branches: [main, develop]
  schedule:
    - cron: '0 6 * * *'  # Daily production run

env:
  SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
  DBT_PROFILES_DIR: ./

jobs:
  # Job 1: Code Quality and Linting
  code_quality:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
        
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          pip install dbt-snowflake==1.3.0 sqlfluff pre-commit
          dbt deps
          
      - name: Run SQL linting
        run: |
          sqlfluff lint models/ --dialect snowflake --rules L001,L002,L003,L006
          
      - name: Check dbt project compilation
        run: |
          dbt compile --target staging
          
      - name: Run pre-commit hooks
        run: |
          pre-commit run --all-files

  # Job 2: Unit Testing and Data Quality
  test_staging:
    needs: code_quality
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
        
      - name: Setup dbt
        run: |
          pip install dbt-snowflake==1.3.0
          dbt deps
          
      - name: Create staging environment
        env:
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_CI_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_CI_PASSWORD }}
        run: |
          # Create isolated schema for this PR
          export SNOWFLAKE_SCHEMA="ci_pr_${{ github.event.number }}"
          echo "SNOWFLAKE_SCHEMA=$SNOWFLAKE_SCHEMA" >> $GITHUB_ENV
          
      - name: Run dbt on changed models
        env:
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_CI_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_CI_PASSWORD }}
        run: |
          # Identify changed models
          git diff --name-only origin/main...HEAD | grep "models/" > changed_files.txt || true
          
          if [ -s changed_files.txt ]; then
            # Build state comparison
            dbt compile --target staging
            dbt run --select state:modified+ --defer --state ./target/prod
            dbt test --select state:modified+ --defer --state ./target/prod
          else
            echo "No model changes detected"
          fi
          
      - name: Generate documentation diff
        run: |
          dbt docs generate --target staging
          # Compare docs with main branch (implement doc diff logic)
          
      - name: Comment PR with results
        uses: actions/github-script@v6
        with:
          script: |
            const fs = require('fs');
            const testResults = fs.readFileSync('target/test_results.json', 'utf8');
            const results = JSON.parse(testResults);
            
            const comment = `## dbt CI Results
            
            **Models Built**: ${results.models_built}
            **Tests Passed**: ${results.tests_passed} / ${results.total_tests}
            **Schema**: ci_pr_${{ github.event.number }}
            
            ${results.failures.length > 0 ? '### ❌ Test Failures:\n' + results.failures.join('\n') : '### ✅ All tests passed!'}
            `;
            
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: comment
            });

  # Job 3: Staging Deployment
  deploy_staging:
    needs: [code_quality, test_staging]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/develop'
    
    environment: staging
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
        
      - name: Deploy to staging
        env:
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_STAGING_USER }}
          SNOWFLAKE_PRIVATE_KEY_PATH: ${{ secrets.SNOWFLAKE_STAGING_KEY }}
        run: |
          # Full staging refresh
          dbt seed --target staging
          dbt run --target staging --full-refresh
          dbt test --target staging
          dbt docs generate --target staging
          
      - name: Staging smoke tests
        run: |
          # Custom smoke tests for staging environment
          dbt run-operation staging_smoke_tests

  # Job 4: Production Deployment
  deploy_production:
    needs: [code_quality]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    
    environment: production
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
        
      - name: Pre-deployment validation
        env:
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_PROD_USER }}
          SNOWFLAKE_PRIVATE_KEY_PATH: ${{ secrets.SNOWFLAKE_PROD_KEY }}
        run: |
          # Validate production state before deployment
          dbt compile --target prod
          dbt run-operation validate_production_state
          
      - name: Backup production state
        run: |
          # Create backup of current production
          dbt run-operation backup_production_tables
          
      - name: Deploy to production
        env:
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_PROD_USER }}
          SNOWFLAKE_PRIVATE_KEY_PATH: ${{ secrets.SNOWFLAKE_PROD_KEY }}
        run: |
          # Incremental production deployment
          dbt seed --target prod --select state:modified+
          dbt run --target prod --select state:modified+
          dbt test --target prod --select state:modified+
          
      - name: Post-deployment validation
        run: |
          # Comprehensive production health checks
          dbt run-operation production_health_checks
          dbt test --select tag:critical
          
      - name: Update documentation
        run: |
          dbt docs generate --target prod
          # Deploy docs to internal documentation site
          
      - name: Notify stakeholders
        if: failure()
        uses: 8398a7/action-slack@v3
        with:
          status: failure
          channel: '#analytics-alerts'
          webhook_url: ${{ secrets.SLACK_WEBHOOK }}

  # Job 5: Scheduled Production Runs
  production_run:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
        
      - name: Daily production run
        env:
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_PROD_USER }}
          SNOWFLAKE_PRIVATE_KEY_PATH: ${{ secrets.SNOWFLAKE_PROD_KEY }}
        run: |
          # Daily incremental refresh
          dbt seed --target prod --select state:modified
          dbt run --target prod
          dbt test --target prod
          
      - name: Performance monitoring
        run: |
          # Collect performance metrics
          dbt run-operation collect_performance_metrics
          
      - name: Data quality monitoring
        run: |
          # Daily data quality checks
          dbt test --select tag:data_quality
          dbt run-operation generate_quality_report
```

3. **Advanced Deployment Strategies**:
```sql
-- macros/deployment_helpers.sql
{% macro blue_green_deployment(models_to_deploy) %}
    {% for model in models_to_deploy %}
        {% set blue_table = model ~ '_blue' %}
        {% set green_table = model ~ '_green' %}
        {% set current_table = model %}
        
        -- Determine current color
        {% set current_color_query %}
            SELECT CASE 
                WHEN table_name LIKE '%_blue' THEN 'blue'
                WHEN table_name LIKE '%_green' THEN 'green'
                ELSE 'none'
            END as current_color
            FROM information_schema.tables
            WHERE table_name = '{{ current_table }}'
        {% endset %}
        
        {% if execute %}
            {% set current_color = run_query(current_color_query).columns[0].values()[0] %}
            {% set target_color = 'green' if current_color == 'blue' else 'blue' %}
            {% set target_table = model ~ '_' ~ target_color %}
            
            -- Build in target color
            CREATE OR REPLACE TABLE {{ target_table }} AS 
            SELECT * FROM {{ ref(model) }};
            
            -- Validate target table
            {% set validation_query %}
                SELECT COUNT(*) FROM {{ target_table }}
            {% endset %}
            
            {% set row_count = run_query(validation_query).columns[0].values()[0] %}
            
            {% if row_count > 0 %}
                -- Atomic swap
                ALTER TABLE {{ current_table }} RENAME TO {{ model }}_old;
                ALTER TABLE {{ target_table }} RENAME TO {{ current_table }};
                DROP TABLE {{ model }}_old;
                
                {{ log("Successfully deployed " ~ model ~ " using blue-green strategy", info=true) }}
            {% else %}
                {{ exceptions.raise_compiler_error("Validation failed for " ~ model ~ " - zero rows in target table") }}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro canary_deployment(model_name, canary_percentage=10) %}
    {% set canary_table = model_name ~ '_canary' %}
    {% set production_table = model_name %}
    
    -- Deploy canary version
    CREATE OR REPLACE TABLE {{ canary_table }} AS
    SELECT * FROM {{ ref(model_name) }};
    
    -- Route canary traffic (implementation depends on your routing mechanism)
    {% set routing_rule %}
        CREATE OR REPLACE VIEW {{ model_name }}_routed AS
        SELECT * FROM (
            SELECT *, 'production' as version FROM {{ production_table }}
            WHERE HASH(primary_key) % 100 >= {{ canary_percentage }}
            
            UNION ALL
            
            SELECT *, 'canary' as version FROM {{ canary_table }}
            WHERE HASH(primary_key) % 100 < {{ canary_percentage }}
        )
    {% endset %}
    
    {% do run_query(routing_rule) %}
    
    -- Monitor canary metrics
    {% set monitoring_query %}
        INSERT INTO deployment_monitoring
        SELECT 
            '{{ model_name }}' as model_name,
            'canary' as deployment_type,
            {{ canary_percentage }} as traffic_percentage,
            CURRENT_TIMESTAMP() as deployment_time
    {% endset %}
    
    {% do run_query(monitoring_query) %}
{% endmacro %}
```

4. **Production Monitoring and Alerting**:
```sql
-- models/monitoring/production_health_dashboard.sql
{{ config(
    materialized='table',
    post_hook='{{ send_health_alerts() }}'
) }}

WITH model_health_metrics AS (
    SELECT 
        model_name,
        last_run_time,
        run_duration_minutes,
        row_count,
        test_failures,
        
        -- Health indicators
        CASE 
            WHEN last_run_time < CURRENT_TIMESTAMP - INTERVAL '25 hours' THEN 'STALE'
            WHEN test_failures > 0 THEN 'FAILING_TESTS'
            WHEN run_duration_minutes > LAG(run_duration_minutes) OVER (
                PARTITION BY model_name 
                ORDER BY last_run_time
            ) * 2 THEN 'PERFORMANCE_DEGRADED'
            ELSE 'HEALTHY'
        END as health_status
    FROM dbt_run_results
    WHERE last_run_time >= CURRENT_DATE - 7
),

critical_model_alerts AS (
    SELECT 
        *,
        CASE 
            WHEN model_name LIKE 'mart_%' AND health_status != 'HEALTHY' THEN 'CRITICAL'
            WHEN health_status = 'FAILING_TESTS' THEN 'HIGH'
            WHEN health_status = 'PERFORMANCE_DEGRADED' THEN 'MEDIUM'
            ELSE 'LOW'
        END as alert_severity
    FROM model_health_metrics
    WHERE health_status != 'HEALTHY'
)

SELECT * FROM critical_model_alerts
WHERE alert_severity IN ('CRITICAL', 'HIGH')

-- Alerting macro
{% macro send_health_alerts() %}
    {% if execute %}
        {% set alert_query %}
            SELECT 
                model_name,
                health_status,
                alert_severity,
                last_run_time
            FROM {{ this }}
            WHERE alert_severity = 'CRITICAL'
        {% endset %}
        
        {% set results = run_query(alert_query) %}
        {% if results.rows %}
            -- Integration with PagerDuty/Slack/email
            {{ log("CRITICAL ALERT: Production models failing: " ~ results.rows, info=true) }}
        {% endif %}
    {% endif %}
{% endmacro %}
```

5. **Rollback and Recovery Procedures**:
```sql
-- macros/rollback_procedures.sql
{% macro emergency_rollback(model_name, target_timestamp) %}
    {% set backup_table = model_name ~ '_backup_' ~ target_timestamp | replace('-', '') | replace(':', '') %}
    
    -- Check if backup exists
    {% set backup_check %}
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{{ backup_table }}'
    {% endset %}
    
    {% if execute %}
        {% set backup_exists = run_query(backup_check).columns[0].values()[0] > 0 %}
        
        {% if backup_exists %}
            -- Atomic rollback using Time Travel if backup not available
            CREATE OR REPLACE TABLE {{ model_name }} AS
            SELECT * FROM {{ backup_table }};
            
            {{ log("Successfully rolled back " ~ model_name ~ " to " ~ target_timestamp, info=true) }}
        {% else %}
            -- Use Snowflake Time Travel as fallback
            CREATE OR REPLACE TABLE {{ model_name }} AS
            SELECT * FROM {{ model_name }} AT(TIMESTAMP => '{{ target_timestamp }}');
            
            {{ log("Rolled back " ~ model_name ~ " using Time Travel to " ~ target_timestamp, info=true) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro validate_rollback_success(model_name, expected_row_count_range) %}
    {% set validation_query %}
        SELECT 
            COUNT(*) as current_row_count,
            MAX(updated_at) as last_update_time
        FROM {{ model_name }}
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(validation_query) %}
        {% set row_count = results.columns[0].values()[0] %}
        {% set last_update = results.columns[1].values()[0] %}
        
        {% if row_count < expected_row_count_range[0] or row_count > expected_row_count_range[1] %}
            {{ exceptions.raise_compiler_error("Rollback validation failed: unexpected row count " ~ row_count) }}
        {% endif %}
        
        {{ log("Rollback validation successful: " ~ row_count ~ " rows, last update: " ~ last_update, info=true) }}
    {% endif %}
{% endmacro %}
```

This comprehensive CI/CD pipeline provides:
- **Multi-environment progression** (dev → staging → production)
- **Automated testing** at every stage with proper gate controls
- **Advanced deployment strategies** (blue-green, canary)
- **Production monitoring** with automated alerting
- **Rollback capabilities** for emergency recovery
- **Performance tracking** and optimization recommendations
- **Stakeholder communication** via Slack/email integration

The pipeline ensures reliable, safe deployments while maintaining high availability and data quality in production environments.

---

These 50 dbt questions demonstrate comprehensive expertise across:
- **Core dbt concepts** and architecture understanding
- **Advanced modeling patterns** and performance optimization
- **Testing and data quality** frameworks
- **Documentation and governance** best practices
- **Production deployment** and operational excellence

Each question includes practical, real-world examples that show hands-on experience with enterprise-scale Analytics Engineering challenges.
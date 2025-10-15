# DBT Questions for Analytics Engineers

## 1. Core Concepts & Architecture

**Q1: Explain the difference between DBT models, sources, seeds, and snapshots.**

**Answer:**
- **Models**: SQL transformations that create tables/views (staging, intermediate, marts)
- **Sources**: External data sources defined for lineage and testing
- **Seeds**: CSV files for reference data (small, static datasets)
- **Snapshots**: Type 2 slowly changing dimensions that track historical changes

```yaml
# Example source definition
sources:
  - name: raw_ecommerce
    tables:
      - name: orders
        columns:
          - name: order_id
            tests:
              - unique
              - not_null
```

**Q2: What are the different materialization strategies in DBT and when would you use each?**

**Answer:**
- **Table**: Physical table, best for frequently queried models, slower builds
- **View**: Virtual view, fast builds, slower queries, no historical data
- **Incremental**: Append/merge only new/changed records, large datasets
- **Ephemeral**: CTE in dependent models, intermediate transformations
- **Materialized View**: Database-managed optimization (Snowflake)

```sql
-- Incremental model example
{{ config(materialized='incremental', unique_key='order_id') }}

SELECT * FROM {{ source('raw', 'orders') }}
{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

**Q3: How does DBT's compilation process work?**

**Answer:**
1. **Parse**: Read project files and build dependency graph
2. **Compile**: Convert Jinja to SQL, resolve refs and sources
3. **Execute**: Run compiled SQL against warehouse
4. **Test**: Run data quality tests
5. **Document**: Generate and serve documentation

## 2. Advanced Modeling Patterns

**Q4: Implement a slowly changing dimension (SCD) Type 2 using DBT snapshots.**

**Answer:**
```sql
-- snapshots/customer_snapshot.sql
{% snapshot customer_snapshot %}
  {{
    config(
      target_database='analytics',
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
  }}
  SELECT * FROM {{ source('raw', 'customers') }}
{% endsnapshot %}
```

**Q5: How do you implement incremental models with different update strategies?**

**Answer:**
```sql
-- Append-only incremental
{{ config(
  materialized='incremental',
  on_schema_change='append_new_columns'
) }}

-- Merge strategy with delete
{{ config(
  materialized='incremental',
  unique_key='transaction_id',
  merge_exclude_columns=['created_at']
) }}

SELECT * FROM {{ source('raw', 'transactions') }}
{% if is_incremental() %}
  WHERE transaction_date > (
    SELECT COALESCE(MAX(transaction_date), '1900-01-01') 
    FROM {{ this }}
  )
{% endif %}
```

**Q6: Create a macro for dynamic pivot operations.**

**Answer:**
```sql
-- macros/pivot.sql
{% macro pivot(group_by, pivot_column, agg_column, agg_func='sum') %}
  {% set pivot_values_query %}
    SELECT DISTINCT {{ pivot_column }}
    FROM {{ this }}
    ORDER BY {{ pivot_column }}
  {% endset %}
  
  {% set results = run_query(pivot_values_query) %}
  {% if execute %}
    {% set pivot_values = results.columns[0].values() %}
  {% else %}
    {% set pivot_values = [] %}
  {% endif %}
  
  SELECT
    {{ group_by }},
    {% for value in pivot_values %}
    {{ agg_func }}(
      CASE WHEN {{ pivot_column }} = '{{ value }}' 
           THEN {{ agg_column }} 
           ELSE 0 END
    ) AS {{ value | replace(' ', '_') | lower }}
    {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
  FROM {{ this }}
  GROUP BY {{ group_by }}
{% endmacro %}
```

## 3. Testing & Quality Assurance

**Q7: How do you implement custom data quality tests in DBT?**

**Answer:**
```sql
-- tests/assert_valid_email_format.sql
SELECT *
FROM {{ ref('dim_customers') }}
WHERE email IS NOT NULL
  AND NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
```

```yaml
# dbt_project.yml
tests:
  - name: assert_valid_email_format
    config:
      severity: warn
      tags: ["data_quality"]
```

**Q8: Implement comprehensive testing strategy for a fact table.**

**Answer:**
```yaml
# models/marts/fact_sales.yml
version: 2
models:
  - name: fact_sales
    description: "Sales fact table with comprehensive testing"
    tests:
      - dbt_utils.expression_is_true:
          expression: "total_amount >= 0"
      - dbt_utils.recency:
          datepart: day
          field: sale_date
          interval: 1
    columns:
      - name: sale_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: product_id
        tests:
          - relationships:
              to: ref('dim_products')
              field: product_id
      - name: sale_date
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: "'2020-01-01'"
              max_value: "current_date()"
```

**Q9: How do you implement data freshness monitoring?**

**Answer:**
```yaml
# models/sources.yml
sources:
  - name: raw_data
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: loaded_at
    tables:
      - name: orders
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
```

## 4. Advanced Jinja & Macros

**Q10: Create a macro that generates different SQL for different databases.**

**Answer:**
```sql
-- macros/date_spine.sql
{% macro date_spine(start_date, end_date) %}
  {% if target.type == 'snowflake' %}
    SELECT 
      DATEADD(day, row_number() over (order by 1) - 1, '{{ start_date }}') as date_column
    FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF(day, '{{ start_date }}', '{{ end_date }}') + 1))
  {% elif target.type == 'bigquery' %}
    SELECT date_column
    FROM UNNEST(
      GENERATE_DATE_ARRAY('{{ start_date }}', '{{ end_date }}', INTERVAL 1 DAY)
    ) AS date_column
  {% elif target.type == 'postgres' %}
    SELECT '{{ start_date }}'::date + generate_series(0, '{{ end_date }}'::date - '{{ start_date }}'::date) as date_column
  {% endif %}
{% endmacro %}
```

**Q11: Implement a macro for dynamic schema generation based on environment.**

**Answer:**
```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {%- if custom_schema_name is none -%}
    {{ default_schema }}
  {%- elif target.name == 'prod' -%}
    {{ custom_schema_name | trim }}
  {%- else -%}
    {{ default_schema }}_{{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
```

**Q12: Create a macro for logging model execution metrics.**

**Answer:**
```sql
-- macros/log_model_metrics.sql
{% macro log_model_metrics() %}
  {% if execute %}
    {% set query %}
      INSERT INTO {{ target.database }}.logs.model_execution_log (
        model_name,
        execution_time,
        rows_affected,
        execution_timestamp
      ) VALUES (
        '{{ this.name }}',
        {{ (modules.datetime.datetime.now() - invocation_started_at).total_seconds() }},
        (SELECT COUNT(*) FROM {{ this }}),
        '{{ modules.datetime.datetime.now().isoformat() }}'
      )
    {% endset %}
    {{ run_query(query) }}
  {% endif %}
{% endmacro %}
```

## 5. Documentation & Lineage

**Q13: How do you implement comprehensive documentation strategy?**

**Answer:**
```yaml
# models/marts/customers.yml
version: 2
models:
  - name: dim_customers
    description: |
      Customer dimension table containing all unique customers.
      
      **Business Logic:**
      - Combines data from multiple source systems
      - Includes derived fields for segmentation
      - Updated daily via incremental refresh
      
      **Data Quality:**
      - Customer IDs are unique and not null
      - Email addresses follow standard format validation
      - Phone numbers are standardized to E.164 format
      
    meta:
      owner: "analytics-team@company.com"
      tags: ["daily", "customer", "dimension"]
      
    columns:
      - name: customer_id
        description: "Unique identifier for customer"
        meta:
          dimension_type: "primary_key"
        tests:
          - unique
          - not_null
          
      - name: customer_ltv
        description: |
          Customer lifetime value calculated as sum of all historical purchases
          plus predicted future value based on cohort analysis.
        meta:
          calculation_method: "Sum of historical purchases + ML prediction"
          update_frequency: "Daily"
```

**Q14: How do you track data lineage across complex transformations?**

**Answer:**
```yaml
# Use exposures to track downstream usage
exposures:
  - name: executive_dashboard
    type: dashboard
    url: https://company.looker.com/dashboards/executive
    description: "C-level executive dashboard"
    depends_on:
      - ref('fact_sales')
      - ref('dim_customers')
    owner:
      name: "Executive Team"
      email: "executives@company.com"
      
  - name: customer_churn_model
    type: ml
    description: "Machine learning model for predicting customer churn"
    depends_on:
      - ref('customer_features')
    meta:
      model_type: "Random Forest"
      accuracy: 0.89
```

## 6. Performance & Optimization

**Q15: How do you optimize DBT model performance?**

**Answer:**
1. **Model Materialization Strategy**:
```sql
-- Use appropriate materializations
{{ config(
  materialized='incremental',
  cluster_by=['date_column'],
  pre_hook="ALTER WAREHOUSE SET warehouse_size = 'LARGE'",
  post_hook="ALTER WAREHOUSE SET warehouse_size = 'MEDIUM'"
) }}
```

2. **Efficient Incremental Logic**:
```sql
-- Avoid expensive operations in incremental filters
{% if is_incremental() %}
  WHERE _loaded_at > (
    SELECT MAX(_loaded_at) FROM {{ this }}
  )
{% endif %}
```

3. **Model Modularity**:
```sql
-- Break complex models into stages
-- staging/stg_orders.sql → intermediate/int_orders_enriched.sql → marts/fact_orders.sql
```

**Q16: Implement efficient testing strategies for large datasets.**

**Answer:**
```yaml
# Limit test scope for large tables
tests:
  +limit: 1000
  +where: "created_at >= current_date() - 7"

# Use sampling for data quality tests
models:
  my_project:
    large_table:
      tests:
        - dbt_utils.expression_is_true:
            expression: "amount > 0"
            config:
              where: "SAMPLE(1000 ROWS)"
```

## 7. CI/CD & Deployment

**Q17: How do you implement DBT in a CI/CD pipeline?**

**Answer:**
```yaml
# .github/workflows/dbt_ci.yml
name: DBT CI
on:
  pull_request:
    branches: [main]

jobs:
  dbt_ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          pip install dbt-snowflake
          dbt deps
          
      - name: Run DBT tests
        run: |
          dbt seed --target ci
          dbt run --models state:modified+ --target ci
          dbt test --models state:modified+ --target ci
          
      - name: Generate documentation
        run: dbt docs generate --target ci
        
      - name: Upload artifacts
        uses: actions/upload-artifact@v2
        with:
          name: dbt-docs
          path: target/
```

**Q18: How do you handle environment-specific configurations?**

**Answer:**
```yaml
# profiles.yml
dbt_project:
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      role: DEV_ROLE
      database: DEV_DB
      warehouse: DEV_WH
      schema: "{{ env_var('DBT_USER') }}_dev"
      
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      role: DBT_PROD_ROLE
      database: PROD_DB
      warehouse: PROD_WH
      schema: analytics
      
  target: dev
```

## 8. Advanced Configurations

**Q19: How do you implement complex model configurations?**

**Answer:**
```yaml
# dbt_project.yml
models:
  my_project:
    +materialized: table
    staging:
      +materialized: view
      +tags: ["staging"]
    intermediate:
      +materialized: ephemeral
      +tags: ["intermediate"]
    marts:
      +materialized: table
      +tags: ["marts"]
      finance:
        +schema: finance
        +post-hook: "GRANT SELECT ON {{ this }} TO ROLE FINANCE_ROLE"
      marketing:
        +schema: marketing
        +cluster_by: ["customer_id", "campaign_date"]
```

**Q20: Implement custom hooks for data quality monitoring.**

**Answer:**
```sql
-- macros/data_quality_hooks.sql
{% macro check_row_count_threshold(min_rows=1000) %}
  {% set row_count_query %}
    SELECT COUNT(*) as row_count FROM {{ this }}
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(row_count_query) %}
    {% set row_count = results.columns[0].values()[0] %}
    
    {% if row_count < min_rows %}
      {{ exceptions.raise_compiler_error(
        "Row count " ~ row_count ~ " is below minimum threshold of " ~ min_rows
      ) }}
    {% endif %}
  {% endif %}
{% endmacro %}
```

## 9. Package Development & Management

**Q21: How do you create and maintain custom DBT packages?**

**Answer:**
```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.9.2
  - git: "https://github.com/company/dbt-company-utils.git"
    revision: v1.2.0
  - local: ../local-packages/custom-macros

# In custom package - dbt_project.yml
name: 'company_utils'
version: '1.0.0'
config-version: 2

macro-paths: ["macros"]
model-paths: ["models"]

vars:
  company_utils:
    timezone: 'America/New_York'
    fiscal_year_start_month: 4
```

**Q22: Implement version management for DBT projects.**

**Answer:**
```yaml
# Use version constraints
packages:
  - package: dbt-labs/dbt_utils
    version: [">=0.8.0", "<0.10.0"]
    
# Lock file management
# Run: dbt deps --lock
# Creates package-lock.yml with exact versions
```

## 10. Error Handling & Debugging

**Q23: How do you implement robust error handling in DBT models?**

**Answer:**
```sql
-- Use try-catch patterns with macros
{% macro safe_divide(numerator, denominator) %}
  CASE 
    WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL 
    THEN NULL
    ELSE {{ numerator }} / {{ denominator }}
  END
{% endmacro %}

-- Implement data validation
{{ config(pre_hook="{{ validate_required_vars(['start_date', 'end_date']) }}") }}

-- Use on_run_start/end hooks for logging
on-run-start:
  - "{{ log_run_start() }}"
  
on-run-end:
  - "{{ log_run_results() }}"
```

**Q24: How do you debug complex DBT model issues?**

**Answer:**
1. **Compilation debugging**:
```bash
dbt compile --models my_model
# Check target/compiled/my_project/models/my_model.sql
```

2. **Use DBT log levels**:
```bash
dbt run --log-level debug
```

3. **Model introspection**:
```sql
-- Use DBT context variables
SELECT 
  '{{ this }}' as current_model,
  '{{ target.name }}' as target_environment,
  '{{ run_started_at }}' as run_timestamp
```

## 11-50. Additional Advanced Questions

**Q25: Implementing data contracts and API-first modeling**
**Q26: Advanced incremental strategies (merge vs. insert-overwrite)**
**Q27: Cross-database modeling patterns**
**Q28: Advanced testing with great_expectations integration**
**Q29: Model governance and access controls**
**Q30: Advanced macro development patterns**
**Q31: Custom materialization development**
**Q32: DBT mesh architecture implementation**
**Q33: Advanced Jinja templating techniques**
**Q34: Performance monitoring and optimization**
**Q35: Custom adapter development**
**Q36: Advanced documentation automation**
**Q37: Data lineage and impact analysis**
**Q38: Multi-tenant modeling patterns**
**Q39: Advanced seed management**
**Q40: Custom test development**
**Q41: Advanced snapshot strategies**
**Q42: Model versioning and backwards compatibility**
**Q43: Advanced variable and configuration management**
**Q44: Custom compilation and parsing logic**
**Q45: Advanced project structure patterns**
**Q46: Integration with external orchestration tools**
**Q47: Advanced source control strategies**
**Q48: Model performance profiling**
**Q49: Advanced data freshness monitoring**
**Q50: DBT Cloud vs. Core architectural decisions**

---

## DBT Best Practices for Analytics Engineers:

1. **Model Organization**: Follow staging → intermediate → marts pattern
2. **Naming Conventions**: Consistent prefixes (stg_, int_, dim_, fact_)
3. **Documentation**: Document business logic, not just technical details
4. **Testing Strategy**: Test early and often, focus on business rules
5. **Performance**: Choose appropriate materializations, use incremental wisely
6. **Version Control**: Treat DBT code like application code
7. **Environment Management**: Separate dev/staging/prod environments
8. **Code Quality**: Use linters, formatters, and peer reviews

## Key Differentiators for Senior AE Roles:
- Deep understanding of DBT internals and compilation process
- Custom macro and package development experience
- Advanced testing and data quality frameworks
- Performance optimization expertise
- CI/CD pipeline implementation
- Mentoring and code review capabilities
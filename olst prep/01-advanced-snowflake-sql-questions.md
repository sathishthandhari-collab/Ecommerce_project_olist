# Advanced Snowflake SQL Interview Questions for Analytics Engineers

## 30 Most Advanced SQL Questions with Snowflake-Specific Solutions

### 1. Complex Window Functions and Analytics

**Q1: Write a query to calculate running totals, rank, and percentiles in a single query for sales data partitioned by region and ordered by date.**

```sql
-- Advanced window functions with multiple analytics
WITH sales_analytics AS (
  SELECT 
    region,
    sale_date,
    sales_amount,
    -- Running total partitioned by region
    SUM(sales_amount) OVER (
      PARTITION BY region 
      ORDER BY sale_date 
      ROWS UNBOUNDED PRECEDING
    ) AS running_total,
    
    -- Dense rank within region by amount
    DENSE_RANK() OVER (
      PARTITION BY region 
      ORDER BY sales_amount DESC
    ) AS sales_rank,
    
    -- Percentile rank for sales amount within region
    PERCENT_RANK() OVER (
      PARTITION BY region 
      ORDER BY sales_amount
    ) AS percentile_rank,
    
    -- Lead and lag for trend analysis
    LAG(sales_amount, 1, 0) OVER (
      PARTITION BY region 
      ORDER BY sale_date
    ) AS prev_sale,
    
    LEAD(sales_amount, 1, 0) OVER (
      PARTITION BY region 
      ORDER BY sale_date
    ) AS next_sale,
    
    -- Moving average (3-period)
    AVG(sales_amount) OVER (
      PARTITION BY region 
      ORDER BY sale_date 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3
  FROM sales_table
)
SELECT *,
  CASE 
    WHEN sales_amount > prev_sale THEN 'INCREASING'
    WHEN sales_amount < prev_sale THEN 'DECREASING'
    ELSE 'STABLE'
  END AS trend_direction
FROM sales_analytics;
```

**Key Points**: Demonstrates mastery of window functions, partitioning, frame specifications, and analytical functions in one query.

---

### 2. Advanced Time Series Analysis with MATCH_RECOGNIZE

**Q2: Use MATCH_RECOGNIZE to identify consecutive months of declining sales for each product.**

```sql
-- Pattern matching for consecutive declining sales
SELECT *
FROM sales_monthly
MATCH_RECOGNIZE(
  PARTITION BY product_id
  ORDER BY month_year
  MEASURES
    FIRST(month_year) AS decline_start,
    LAST(month_year) AS decline_end,
    COUNT(*) AS consecutive_months,
    FIRST(sales_amount) AS start_amount,
    LAST(sales_amount) AS end_amount
  ONE ROW PER MATCH
  PATTERN (DECLINE{3,})  -- At least 3 consecutive declining months
  DEFINE DECLINE AS sales_amount < LAG(sales_amount)
);
```

**Key Points**: Shows advanced pattern recognition capabilities, essential for analytics engineering use cases like anomaly detection and trend analysis.

---

### 3. Recursive CTEs for Hierarchical Data

**Q3: Write a recursive CTE to calculate organizational hierarchy levels and cumulative budget rollups.**

```sql
-- Recursive CTE for organizational hierarchy with budget aggregation
WITH RECURSIVE org_hierarchy AS (
  -- Base case: top-level managers
  SELECT 
    employee_id,
    manager_id,
    employee_name,
    department,
    budget,
    1 as hierarchy_level,
    employee_name AS hierarchy_path,
    budget AS cumulative_budget
  FROM employees 
  WHERE manager_id IS NULL
  
  UNION ALL
  
  -- Recursive case: subordinates
  SELECT 
    e.employee_id,
    e.manager_id,
    e.employee_name,
    e.department,
    e.budget,
    oh.hierarchy_level + 1,
    oh.hierarchy_path || ' -> ' || e.employee_name,
    oh.cumulative_budget + e.budget
  FROM employees e
  INNER JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
  WHERE oh.hierarchy_level < 10  -- Prevent infinite recursion
)
SELECT 
  *,
  -- Calculate span of control
  COUNT(*) OVER (PARTITION BY manager_id) - 1 AS span_of_control,
  -- Calculate budget as percentage of total
  budget / SUM(budget) OVER () * 100 AS budget_percentage
FROM org_hierarchy
ORDER BY hierarchy_level, hierarchy_path;
```

**Key Points**: Demonstrates recursive queries for complex hierarchical analysis, crucial for org charts and multi-level aggregations.

---

### 4. Advanced JSON Processing and Flattening

**Q4: Parse and flatten complex nested JSON data with arrays and objects, calculating metrics at each level.**

```sql
-- Advanced JSON processing with nested structures
WITH flattened_events AS (
  SELECT 
    event_id,
    event_timestamp,
    -- Extract top-level properties
    event_data:user_id::varchar AS user_id,
    event_data:session_id::varchar AS session_id,
    
    -- Flatten nested array of products
    products.value:product_id::varchar AS product_id,
    products.value:product_name::varchar AS product_name,
    products.value:price::decimal(10,2) AS price,
    products.value:quantity::int AS quantity,
    
    -- Extract nested properties from each product
    properties.value:name::varchar AS property_name,
    properties.value:value::varchar AS property_value,
    
    -- Calculate line total
    (products.value:price::decimal(10,2) * products.value:quantity::int) AS line_total
  FROM 
    events,
    LATERAL FLATTEN(input => event_data:products) AS products,
    LATERAL FLATTEN(input => products.value:properties, OUTER => TRUE) AS properties
  WHERE event_data:event_type::varchar = 'purchase'
),

aggregated_metrics AS (
  SELECT 
    user_id,
    session_id,
    COUNT(DISTINCT product_id) AS unique_products,
    SUM(quantity) AS total_quantity,
    SUM(line_total) AS total_amount,
    
    -- Create property arrays for analysis
    ARRAY_AGG(DISTINCT property_name) AS all_properties,
    
    -- Statistical measures
    AVG(price) AS avg_price,
    STDDEV(price) AS price_stddev,
    
    -- JSON aggregation back to structured format
    OBJECT_CONSTRUCT(
      'total_amount', total_amount,
      'product_count', unique_products,
      'avg_price', avg_price
    ) AS summary_stats
  FROM flattened_events
  GROUP BY user_id, session_id
)
SELECT * FROM aggregated_metrics;
```

**Key Points**: Shows expertise in complex JSON processing, essential for modern analytics on semi-structured data.

---

### 5. Advanced Statistical Functions and Outlier Detection

**Q5: Calculate statistical measures and identify outliers using Z-scores and IQR methods.**

```sql
-- Advanced statistical analysis with outlier detection
WITH statistical_base AS (
  SELECT 
    product_category,
    product_id,
    sales_amount,
    
    -- Basic statistical measures
    AVG(sales_amount) OVER (PARTITION BY product_category) AS category_mean,
    STDDEV(sales_amount) OVER (PARTITION BY product_category) AS category_stddev,
    
    -- Percentile calculations
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY sales_amount) OVER (PARTITION BY product_category) AS q1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sales_amount) OVER (PARTITION BY product_category) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sales_amount) OVER (PARTITION BY product_category) AS q3,
    
    -- Count for statistical significance
    COUNT(*) OVER (PARTITION BY product_category) AS sample_size
  FROM sales_data
),

outlier_analysis AS (
  SELECT *,
    -- Z-score calculation
    ABS(sales_amount - category_mean) / NULLIF(category_stddev, 0) AS z_score,
    
    -- IQR calculation
    (q3 - q1) AS iqr,
    
    -- IQR-based outlier bounds
    q1 - (1.5 * (q3 - q1)) AS lower_bound,
    q3 + (1.5 * (q3 - q1)) AS upper_bound,
    
    -- Outlier flags
    CASE 
      WHEN ABS(sales_amount - category_mean) / NULLIF(category_stddev, 0) > 3 THEN 'Z_SCORE_OUTLIER'
      WHEN sales_amount < (q1 - (1.5 * (q3 - q1))) OR sales_amount > (q3 + (1.5 * (q3 - q1))) THEN 'IQR_OUTLIER'
      ELSE 'NORMAL'
    END AS outlier_type,
    
    -- Statistical significance flag
    CASE WHEN sample_size >= 30 THEN TRUE ELSE FALSE END AS statistically_significant
  FROM statistical_base
)

SELECT 
  product_category,
  COUNT(*) AS total_products,
  COUNT(CASE WHEN outlier_type != 'NORMAL' THEN 1 END) AS outlier_count,
  COUNT(CASE WHEN outlier_type != 'NORMAL' THEN 1 END) / COUNT(*) * 100 AS outlier_percentage,
  AVG(z_score) AS avg_z_score,
  MAX(z_score) AS max_z_score,
  AVG(CASE WHEN statistically_significant THEN category_mean END) AS reliable_mean
FROM outlier_analysis
GROUP BY product_category
ORDER BY outlier_percentage DESC;
```

**Key Points**: Demonstrates statistical expertise crucial for data quality and anomaly detection in analytics engineering.

---

### 6. Complex Data Quality and Validation

**Q6: Create a comprehensive data quality framework that checks completeness, validity, consistency, and accuracy.**

```sql
-- Comprehensive data quality assessment framework
WITH data_quality_checks AS (
  SELECT 
    table_name,
    column_name,
    total_rows,
    
    -- Completeness checks
    COUNT(CASE WHEN column_value IS NOT NULL THEN 1 END) AS non_null_count,
    COUNT(CASE WHEN column_value IS NULL THEN 1 END) AS null_count,
    (non_null_count::decimal / total_rows) * 100 AS completeness_percentage,
    
    -- Validity checks (example for email)
    COUNT(CASE 
      WHEN column_name = 'email' AND column_value RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' 
      THEN 1 
    END) AS valid_email_count,
    
    -- Uniqueness checks
    COUNT(DISTINCT column_value) AS unique_values,
    COUNT(column_value) - COUNT(DISTINCT column_value) AS duplicate_count,
    
    -- Consistency checks (format patterns)
    COUNT(CASE 
      WHEN column_name = 'phone' AND column_value RLIKE '^\\+?[1-9]\\d{1,14}$' 
      THEN 1 
    END) AS consistent_phone_format,
    
    -- Range validity for numeric fields
    COUNT(CASE 
      WHEN column_name = 'age' AND TRY_CAST(column_value AS INTEGER) BETWEEN 0 AND 120 
      THEN 1 
    END) AS valid_age_range,
    
    -- Calculate quality scores
    CASE 
      WHEN completeness_percentage >= 95 THEN 5
      WHEN completeness_percentage >= 85 THEN 4
      WHEN completeness_percentage >= 70 THEN 3
      WHEN completeness_percentage >= 50 THEN 2
      ELSE 1
    END AS completeness_score
  FROM (
    -- Unpivot table structure for generic quality checks
    SELECT 
      'customers' AS table_name,
      'email' AS column_name,
      email AS column_value,
      COUNT(*) OVER () AS total_rows
    FROM customers
    UNION ALL
    SELECT 'customers', 'phone', phone, COUNT(*) OVER () FROM customers
    UNION ALL
    SELECT 'customers', 'age', age::varchar, COUNT(*) OVER () FROM customers
  ) unpivoted_data
  GROUP BY table_name, column_name, total_rows
),

quality_summary AS (
  SELECT 
    table_name,
    COUNT(*) AS columns_checked,
    AVG(completeness_score) AS avg_completeness_score,
    MIN(completeness_percentage) AS worst_completeness,
    SUM(duplicate_count) AS total_duplicates,
    
    -- Overall quality score calculation
    (AVG(completeness_score) + 
     CASE WHEN SUM(duplicate_count) = 0 THEN 5 ELSE 3 END +
     CASE WHEN MIN(completeness_percentage) > 90 THEN 5 ELSE 2 END
    ) / 3 AS overall_quality_score
  FROM data_quality_checks
  GROUP BY table_name
)

SELECT 
  *,
  CASE 
    WHEN overall_quality_score >= 4.5 THEN 'EXCELLENT'
    WHEN overall_quality_score >= 3.5 THEN 'GOOD' 
    WHEN overall_quality_score >= 2.5 THEN 'FAIR'
    ELSE 'POOR'
  END AS quality_grade
FROM quality_summary;
```

**Key Points**: Shows systematic approach to data quality, essential for analytics engineering and data governance.

---

### 7. Advanced Incremental Processing Logic

**Q7: Design an intelligent incremental processing system that handles late-arriving data and maintains data consistency.**

```sql
-- Advanced incremental processing with late-arriving data handling
CREATE OR REPLACE TABLE incremental_metadata AS (
  SELECT 
    'orders' AS table_name,
    '2024-01-01'::timestamp AS last_successful_run,
    '2024-01-01'::timestamp AS data_watermark,
    'DAILY' AS frequency
);

-- Incremental processing logic with lookback window
WITH processing_window AS (
  SELECT 
    table_name,
    last_successful_run,
    -- Lookback window to catch late-arriving data (72 hours)
    DATEADD('hour', -72, last_successful_run) AS lookback_start,
    CURRENT_TIMESTAMP() AS current_run_time
  FROM incremental_metadata 
  WHERE table_name = 'orders'
),

source_data AS (
  SELECT 
    order_id,
    customer_id,
    order_date,
    order_amount,
    created_at,
    modified_at,
    -- Add processing metadata
    CURRENT_TIMESTAMP() AS processed_at,
    HASH(order_id, customer_id, order_date, order_amount) AS row_hash
  FROM raw_orders r
  CROSS JOIN processing_window p
  WHERE 
    -- New records since last run
    (r.created_at > p.last_successful_run)
    OR 
    -- Modified records within lookback window (catch late updates)
    (r.modified_at > p.lookback_start AND r.modified_at <= p.current_run_time)
),

incremental_logic AS (
  SELECT 
    s.*,
    -- Identify record type for merge logic
    CASE 
      WHEN t.order_id IS NULL THEN 'INSERT'
      WHEN t.row_hash != s.row_hash THEN 'UPDATE'
      ELSE 'NO_CHANGE'
    END AS operation_type,
    
    -- Track version for SCD Type 2 if needed
    COALESCE(t.version, 0) + 1 AS new_version
  FROM source_data s
  LEFT JOIN target_orders t ON s.order_id = t.order_id
  WHERE operation_type != 'NO_CHANGE'  -- Only process changes
)

-- MERGE statement for incremental processing
MERGE INTO target_orders t
USING incremental_logic s ON t.order_id = s.order_id
WHEN MATCHED AND s.operation_type = 'UPDATE' THEN
  UPDATE SET 
    customer_id = s.customer_id,
    order_date = s.order_date,
    order_amount = s.order_amount,
    modified_at = s.modified_at,
    processed_at = s.processed_at,
    row_hash = s.row_hash,
    version = s.new_version
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, order_date, order_amount, 
          created_at, modified_at, processed_at, row_hash, version)
  VALUES (s.order_id, s.customer_id, s.order_date, s.order_amount,
          s.created_at, s.modified_at, s.processed_at, s.row_hash, 1);

-- Update processing metadata
UPDATE incremental_metadata 
SET 
  last_successful_run = (SELECT current_run_time FROM processing_window),
  data_watermark = (SELECT MAX(created_at) FROM incremental_logic)
WHERE table_name = 'orders';
```

**Key Points**: Demonstrates sophisticated incremental processing patterns essential for high-volume analytics engineering.

---

### 8. Advanced Pivoting and Unpivoting with Dynamic Columns

**Q8: Create a dynamic pivot that handles unknown column values and generates summary statistics.**

```sql
-- Dynamic pivot with statistical aggregations
SET (pivot_columns) = (
  SELECT LISTAGG(DISTINCT '''' || product_category || '''', ', ') 
  FROM sales_data 
  WHERE product_category IS NOT NULL
);

-- Dynamic pivot query construction
EXECUTE IMMEDIATE $$
WITH pivot_base AS (
  SELECT 
    customer_id,
    order_date,
    $$ || $pivot_columns || $$
  FROM (
    SELECT 
      customer_id,
      order_date,
      product_category,
      sales_amount
    FROM sales_data
  ) 
  PIVOT (
    SUM(sales_amount) FOR product_category IN ($$ || $pivot_columns || $$)
  ) AS pivoted_data
),

statistical_summary AS (
  SELECT 
    customer_id,
    COUNT(*) AS order_count,
    
    -- Calculate statistics across all category columns
    $$ || (
      SELECT LISTAGG(
        'AVG(' || column_name || ') AS avg_' || column_name ||
        ', STDDEV(' || column_name || ') AS stddev_' || column_name, 
        ', '
      )
      FROM (
        SELECT REPLACE(column_value, '''', '') AS column_name
        FROM TABLE(SPLIT_TO_TABLE($pivot_columns, ', '))
      )
    ) || $$,
    
    -- Calculate total across all categories
    $$ || (
      SELECT LISTAGG('COALESCE(' || REPLACE(column_value, '''', '') || ', 0)', ' + ')
      FROM TABLE(SPLIT_TO_TABLE($pivot_columns, ', '))
    ) || $$ AS total_sales,
    
    -- Calculate diversity index (number of categories purchased)
    $$ || (
      SELECT LISTAGG('CASE WHEN ' || REPLACE(column_value, '''', '') || ' > 0 THEN 1 ELSE 0 END', ' + ')
      FROM TABLE(SPLIT_TO_TABLE($pivot_columns, ', '))
    ) || $$ AS category_diversity
  FROM pivot_base
  GROUP BY customer_id
)
SELECT * FROM statistical_summary
ORDER BY total_sales DESC;
$$;
```

**Key Points**: Shows dynamic SQL generation and advanced pivoting techniques for flexible analytics.

---

### 9. Complex Cohort Analysis with Multiple Dimensions

**Q9: Perform multi-dimensional cohort analysis tracking customer behavior across acquisition channel and geographic segments.**

```sql
-- Multi-dimensional cohort analysis
WITH customer_cohorts AS (
  SELECT 
    customer_id,
    acquisition_channel,
    customer_region,
    -- Cohort month based on first purchase
    DATE_TRUNC('month', MIN(order_date)) AS cohort_month,
    MIN(order_date) AS first_purchase_date,
    COUNT(DISTINCT order_id) AS lifetime_orders,
    SUM(order_amount) AS lifetime_value
  FROM orders o
  JOIN customers c ON o.customer_id = c.customer_id
  GROUP BY customer_id, acquisition_channel, customer_region
),

customer_activity AS (
  SELECT 
    c.customer_id,
    c.cohort_month,
    c.acquisition_channel,
    c.customer_region,
    o.order_date,
    o.order_amount,
    -- Period number (0 = cohort month, 1 = month 1, etc.)
    DATEDIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS period_number
  FROM customer_cohorts c
  JOIN orders o ON c.customer_id = o.customer_id
),

cohort_metrics AS (
  SELECT 
    cohort_month,
    acquisition_channel,
    customer_region,
    period_number,
    
    -- Customer retention metrics
    COUNT(DISTINCT customer_id) AS active_customers,
    FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
      PARTITION BY cohort_month, acquisition_channel, customer_region 
      ORDER BY period_number
    ) AS cohort_size,
    
    -- Revenue metrics
    SUM(order_amount) AS period_revenue,
    AVG(order_amount) AS avg_order_value,
    COUNT(DISTINCT order_id) AS total_orders,
    
    -- Calculated percentages
    active_customers::decimal / cohort_size * 100 AS retention_rate,
    period_revenue / cohort_size AS revenue_per_customer,
    
    -- Cumulative metrics
    SUM(SUM(order_amount)) OVER (
      PARTITION BY cohort_month, acquisition_channel, customer_region 
      ORDER BY period_number
    ) AS cumulative_revenue
  FROM customer_activity
  GROUP BY cohort_month, acquisition_channel, customer_region, period_number
),

cohort_summary AS (
  SELECT 
    cohort_month,
    acquisition_channel,
    customer_region,
    cohort_size,
    
    -- Retention rates by period
    MAX(CASE WHEN period_number = 0 THEN retention_rate END) AS month_0_retention,
    MAX(CASE WHEN period_number = 1 THEN retention_rate END) AS month_1_retention,
    MAX(CASE WHEN period_number = 3 THEN retention_rate END) AS month_3_retention,
    MAX(CASE WHEN period_number = 6 THEN retention_rate END) AS month_6_retention,
    MAX(CASE WHEN period_number = 12 THEN retention_rate END) AS month_12_retention,
    
    -- LTV calculations
    MAX(cumulative_revenue) / cohort_size AS ltv_current,
    
    -- Predict LTV using retention curve
    (MAX(cumulative_revenue) / cohort_size) / 
    NULLIF(MAX(CASE WHEN period_number = 6 THEN retention_rate END) / 100, 0) AS predicted_ltv
  FROM cohort_metrics
  GROUP BY cohort_month, acquisition_channel, customer_region, cohort_size
)

SELECT *,
  -- Cohort quality classification
  CASE 
    WHEN month_6_retention >= 40 AND ltv_current >= 500 THEN 'HIGH_VALUE'
    WHEN month_6_retention >= 25 AND ltv_current >= 200 THEN 'MEDIUM_VALUE'
    ELSE 'LOW_VALUE'
  END AS cohort_quality
FROM cohort_summary
ORDER BY cohort_month DESC, ltv_current DESC;
```

**Key Points**: Demonstrates complex analytical thinking and multi-dimensional analysis essential for customer analytics.

---

### 10. Advanced Data Profiling and Schema Discovery

**Q10: Create a comprehensive data profiling system that automatically discovers schema patterns, data types, and relationships.**

```sql
-- Automated data profiling and schema discovery
CREATE OR REPLACE PROCEDURE profile_table(TABLE_NAME STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
DECLARE
  profiling_results STRING DEFAULT '';
  column_cursor CURSOR FOR
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = TABLE_NAME;
BEGIN
  FOR record IN column_cursor DO
    -- Build dynamic profiling query for each column
    LET profile_query STRING := '
    SELECT 
      ''' || record.column_name || ''' AS column_name,
      ''' || record.data_type || ''' AS data_type,
      COUNT(*) AS total_rows,
      COUNT(' || record.column_name || ') AS non_null_count,
      COUNT(DISTINCT ' || record.column_name || ') AS unique_values,
      
      -- Statistical measures for numeric columns
      ' || CASE 
        WHEN record.data_type IN ('NUMBER', 'INTEGER', 'FLOAT', 'DECIMAL') THEN
          'MIN(' || record.column_name || ') AS min_value,
           MAX(' || record.column_name || ') AS max_value,
           AVG(' || record.column_name || ') AS avg_value,
           STDDEV(' || record.column_name || ') AS stddev_value,'
        ELSE 'NULL AS min_value, NULL AS max_value, NULL AS avg_value, NULL AS stddev_value,'
      END || '
      
      -- String pattern analysis
      ' || CASE 
        WHEN record.data_type IN ('STRING', 'TEXT', 'VARCHAR') THEN
          'MIN(LENGTH(' || record.column_name || ')) AS min_length,
           MAX(LENGTH(' || record.column_name || ')) AS max_length,
           AVG(LENGTH(' || record.column_name || ')) AS avg_length,
           
           -- Common patterns
           COUNT(CASE WHEN ' || record.column_name || ' RLIKE ''^[0-9]+$'' THEN 1 END) AS numeric_pattern,
           COUNT(CASE WHEN ' || record.column_name || ' RLIKE ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'' THEN 1 END) AS email_pattern,
           COUNT(CASE WHEN ' || record.column_name || ' RLIKE ''^\\+?[1-9]\\d{1,14}$'' THEN 1 END) AS phone_pattern,'
        ELSE 'NULL AS min_length, NULL AS max_length, NULL AS avg_length, 
              NULL AS numeric_pattern, NULL AS email_pattern, NULL AS phone_pattern,'
      END || '
      
      -- Data quality indicators
      COUNT(*) - COUNT(' || record.column_name || ') AS null_count,
      (COUNT(*) - COUNT(' || record.column_name || ')) / COUNT(*) * 100 AS null_percentage,
      COUNT(DISTINCT ' || record.column_name || ') / COUNT(' || record.column_name || ') * 100 AS uniqueness_ratio,
      
      -- Sample values
      LISTAGG(DISTINCT ' || record.column_name || ', '', '') WITHIN GROUP (ORDER BY ' || record.column_name || ') AS sample_values
    FROM ' || TABLE_NAME || ' 
    WHERE ROWNUM <= 1000';  -- Sample for large tables
    
    -- Execute profiling query and store results
    EXECUTE IMMEDIATE profile_query;
  END FOR;
  
  RETURN 'Profiling completed for ' || TABLE_NAME;
END;
$$;

-- Create comprehensive data dictionary
WITH table_profiling AS (
  SELECT 
    t.table_name,
    t.row_count,
    COUNT(c.column_name) AS column_count,
    
    -- Column type distribution
    COUNT(CASE WHEN c.data_type IN ('NUMBER', 'INTEGER', 'FLOAT', 'DECIMAL') THEN 1 END) AS numeric_columns,
    COUNT(CASE WHEN c.data_type IN ('STRING', 'TEXT', 'VARCHAR') THEN 1 END) AS string_columns,
    COUNT(CASE WHEN c.data_type IN ('DATE', 'TIMESTAMP', 'TIME') THEN 1 END) AS date_columns,
    COUNT(CASE WHEN c.data_type = 'BOOLEAN' THEN 1 END) AS boolean_columns,
    
    -- Identify potential keys
    LISTAGG(
      CASE WHEN c.is_nullable = 'NO' AND 'ID' IN UPPER(c.column_name) THEN c.column_name END,
      ', '
    ) AS potential_primary_keys,
    
    -- Identify foreign key patterns
    LISTAGG(
      CASE WHEN UPPER(c.column_name) LIKE '%_ID' AND UPPER(c.column_name) != 'ID' THEN c.column_name END,
      ', '
    ) AS potential_foreign_keys
  FROM information_schema.tables t
  JOIN information_schema.columns c ON t.table_name = c.table_name
  WHERE t.table_schema = CURRENT_SCHEMA()
  GROUP BY t.table_name, t.row_count
),

relationship_analysis AS (
  SELECT 
    t1.table_name AS parent_table,
    t2.table_name AS child_table,
    t1.column_name AS parent_column,
    t2.column_name AS child_column,
    -- Calculate relationship strength
    (SELECT COUNT(DISTINCT t2_data.column_value) FROM table_data t2_data WHERE t2_data.table_name = t2.table_name) /
    (SELECT COUNT(DISTINCT t1_data.column_value) FROM table_data t1_data WHERE t1_data.table_name = t1.table_name) AS relationship_ratio
  FROM 
    (SELECT table_name, column_name FROM information_schema.columns WHERE UPPER(column_name) NOT LIKE '%_ID') t1
  CROSS JOIN
    (SELECT table_name, column_name FROM information_schema.columns WHERE UPPER(column_name) LIKE '%_ID') t2
  WHERE t1.table_name != t2.table_name
    AND REPLACE(UPPER(t2.column_name), '_ID', '') = UPPER(t1.table_name)
)

SELECT 
  tp.*,
  ra.child_table AS related_tables,
  ra.relationship_ratio AS relationship_strength
FROM table_profiling tp
LEFT JOIN relationship_analysis ra ON tp.table_name = ra.parent_table
ORDER BY tp.row_count DESC;
```

**Key Points**: Shows ability to build automated data discovery tools, essential for analytics engineering in large organizations.

---

## Additional Advanced Questions (11-30)

### 11. Complex Streaming Data Processing

**Q11: Design a system to process streaming data with sessionization and real-time anomaly detection.**

```sql
-- Streaming sessionization with anomaly detection
CREATE OR REPLACE STREAM user_events_stream ON TABLE raw_user_events;

-- Real-time sessionization
CREATE OR REPLACE TASK sessionize_events
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 minute'
AS
WITH streaming_events AS (
  SELECT *
  FROM user_events_stream
  WHERE METADATA$ACTION = 'INSERT'
),

sessionized_data AS (
  SELECT 
    user_id,
    event_timestamp,
    event_type,
    -- Session boundary detection (30 minutes of inactivity)
    CASE 
      WHEN DATEDIFF('minute', 
        LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp), 
        event_timestamp
      ) > 30 OR LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) IS NULL
      THEN ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp)
      ELSE NULL
    END AS session_start_flag
  FROM streaming_events
),

sessions AS (
  SELECT 
    user_id,
    event_timestamp,
    event_type,
    LAST_VALUE(session_start_flag IGNORE NULLS) OVER (
      PARTITION BY user_id ORDER BY event_timestamp 
      ROWS UNBOUNDED PRECEDING
    ) AS session_id
  FROM sessionized_data
),

session_metrics AS (
  SELECT 
    user_id,
    session_id,
    MIN(event_timestamp) AS session_start,
    MAX(event_timestamp) AS session_end,
    COUNT(*) AS event_count,
    DATEDIFF('minute', MIN(event_timestamp), MAX(event_timestamp)) AS session_duration,
    COUNT(DISTINCT event_type) AS event_type_diversity,
    
    -- Anomaly detection flags
    CASE WHEN COUNT(*) > (
      SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY COUNT(*))
      FROM sessions 
      WHERE user_id = sessions.user_id
    ) THEN TRUE ELSE FALSE END AS high_activity_anomaly,
    
    CASE WHEN DATEDIFF('minute', MIN(event_timestamp), MAX(event_timestamp)) > 180 
    THEN TRUE ELSE FALSE END AS long_session_anomaly
  FROM sessions
  GROUP BY user_id, session_id
)

INSERT INTO processed_sessions
SELECT 
  *,
  CURRENT_TIMESTAMP() AS processed_at
FROM session_metrics
WHERE high_activity_anomaly OR long_session_anomaly;
```

---

### 12. Advanced Cost Optimization Analysis

**Q12: Create a comprehensive cost analysis system that identifies optimization opportunities.**

```sql
-- Warehouse cost optimization analysis
WITH query_performance AS (
  SELECT 
    query_id,
    query_text,
    warehouse_name,
    warehouse_size,
    execution_time_ms,
    credits_used_cloud_services,
    COALESCE(credits_used_compute_wh, 0) AS credits_used_compute,
    bytes_scanned,
    rows_produced,
    
    -- Cost efficiency metrics
    CASE WHEN rows_produced > 0 
         THEN (credits_used_cloud_services + COALESCE(credits_used_compute_wh, 0)) / rows_produced 
         ELSE NULL END AS cost_per_row,
    
    CASE WHEN bytes_scanned > 0 
         THEN execution_time_ms / (bytes_scanned / 1024 / 1024) -- ms per MB
         ELSE NULL END AS scan_efficiency,
    
    -- Query classification
    CASE 
      WHEN UPPER(query_text) LIKE '%SELECT%COUNT(*)%' THEN 'AGGREGATION'
      WHEN UPPER(query_text) LIKE '%JOIN%' THEN 'JOIN_HEAVY'
      WHEN UPPER(query_text) LIKE '%WINDOW%' OR UPPER(query_text) LIKE '%OVER%' THEN 'ANALYTICAL'
      WHEN UPPER(query_text) LIKE '%INSERT%' OR UPPER(query_text) LIKE '%UPDATE%' THEN 'DML'
      ELSE 'OTHER'
    END AS query_type
  FROM snowflake.account_usage.query_history
  WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    AND warehouse_name IS NOT NULL
),

warehouse_analysis AS (
  SELECT 
    warehouse_name,
    warehouse_size,
    query_type,
    
    -- Performance metrics
    COUNT(*) AS query_count,
    AVG(execution_time_ms) AS avg_execution_time,
    SUM(credits_used_cloud_services + credits_used_compute) AS total_credits,
    AVG(cost_per_row) AS avg_cost_per_row,
    AVG(scan_efficiency) AS avg_scan_efficiency,
    
    -- Utilization analysis
    SUM(execution_time_ms) / (30 * 24 * 60 * 60 * 1000) * 100 AS utilization_percentage,
    
    -- Optimization opportunities
    COUNT(CASE WHEN execution_time_ms > 300000 THEN 1 END) AS long_running_queries, -- > 5 minutes
    COUNT(CASE WHEN cost_per_row > 0.001 THEN 1 END) AS expensive_queries,
    
    -- Size optimization suggestions
    CASE 
      WHEN AVG(execution_time_ms) < 10000 AND warehouse_size IN ('LARGE', 'X-LARGE') THEN 'DOWNSIZE'
      WHEN AVG(execution_time_ms) > 300000 AND warehouse_size IN ('X-SMALL', 'SMALL') THEN 'UPSIZE'
      WHEN SUM(execution_time_ms) / (30 * 24 * 60 * 60 * 1000) * 100 < 10 THEN 'UNDERUTILIZED'
      ELSE 'OPTIMAL'
    END AS size_recommendation
  FROM query_performance
  GROUP BY warehouse_name, warehouse_size, query_type
),

cost_optimization_recommendations AS (
  SELECT 
    warehouse_name,
    warehouse_size,
    total_credits,
    utilization_percentage,
    size_recommendation,
    
    -- Calculate potential savings
    CASE size_recommendation
      WHEN 'DOWNSIZE' THEN total_credits * 0.5  -- 50% savings by downsizing
      WHEN 'UNDERUTILIZED' THEN total_credits * 0.3  -- 30% savings by right-sizing schedule
      ELSE 0
    END AS potential_monthly_savings,
    
    -- Specific recommendations
    CASE size_recommendation
      WHEN 'DOWNSIZE' THEN 'Reduce warehouse size - queries complete quickly with current size'
      WHEN 'UPSIZE' THEN 'Increase warehouse size - reduce long-running query times'
      WHEN 'UNDERUTILIZED' THEN 'Implement auto-suspend or reduce uptime - low utilization detected'
      ELSE 'Warehouse size is optimal for current workload'
    END AS recommendation_details
  FROM warehouse_analysis
  GROUP BY warehouse_name, warehouse_size, total_credits, utilization_percentage, size_recommendation
)

SELECT 
  *,
  SUM(potential_monthly_savings) OVER () AS total_potential_savings,
  RANK() OVER (ORDER BY potential_monthly_savings DESC) AS optimization_priority
FROM cost_optimization_recommendations
WHERE potential_monthly_savings > 0
ORDER BY potential_monthly_savings DESC;
```

---

[Continue with questions 13-30 covering topics like:]

### 13. Advanced Data Masking and Privacy
### 14. Time Travel and Historical Analysis  
### 15. Multi-cluster Warehouse Optimization
### 16. Advanced Security and Access Control
### 17. Cross-Database Analytics
### 18. Advanced Clustering Strategies
### 19. Complex Data Sharing Scenarios
### 20. Advanced Stored Procedure Logic
### 21. Performance Tuning and Query Optimization
### 22. Advanced Error Handling and Logging
### 23. Complex Data Migration Patterns
### 24. Advanced Monitoring and Alerting
### 25. Integration with External Systems
### 26. Advanced Backup and Recovery
### 27. Complex Metadata Management
### 28. Advanced Data Governance
### 29. Performance Benchmarking
### 30. Advanced Troubleshooting Techniques

**Key Interview Tips for SQL Questions:**
1. Always explain your thinking process
2. Discuss performance implications
3. Consider edge cases and error handling
4. Explain how each technique applies to analytics engineering
5. Demonstrate understanding of Snowflake-specific features
6. Show awareness of cost implications
7. Discuss data governance and security considerations
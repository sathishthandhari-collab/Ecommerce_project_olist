# 📚 **50 ADVANCED SQL INTERVIEW QUESTIONS & ANSWERS**
## **Snowflake Dialect - Analytics Engineering Focus**

***

## **SECTION 1: WINDOW FUNCTIONS & ANALYTICS (10 Questions)**

### **Q1: Explain the difference between ROW_NUMBER(), RANK(), and DENSE_RANK() with an example.**

**Answer:**
```sql
-- ROW_NUMBER(): Assigns unique sequential integers, no ties
-- RANK(): Assigns same rank to ties, skips next rank
-- DENSE_RANK(): Assigns same rank to ties, no gaps

SELECT 
    seller_id,
    total_sales,
    ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS row_num,
    RANK() OVER (ORDER BY total_sales DESC) AS rank,
    DENSE_RANK() OVER (ORDER BY total_sales DESC) AS dense_rank
FROM sellers;

-- Example output:
-- seller_id | total_sales | row_num | rank | dense_rank
-- 101       | 10000      | 1       | 1    | 1
-- 102       | 10000      | 2       | 1    | 1
-- 103       | 9000       | 3       | 3    | 2
-- 104       | 9000       | 4       | 3    | 2
-- 105       | 8000       | 5       | 5    | 3
```

**Key Difference:** ROW_NUMBER() always unique, RANK() skips ranks after ties, DENSE_RANK() doesn't skip.

---

### **Q2: How do you use QUALIFY in Snowflake to filter window function results?**

**Answer:**
```sql
-- QUALIFY filters window function results without subquery
-- Get top 3 sellers per region by sales

SELECT 
    region,
    seller_id,
    total_sales,
    RANK() OVER (PARTITION BY region ORDER BY total_sales DESC) AS sales_rank
FROM sellers
QUALIFY sales_rank <= 3;

-- Without QUALIFY (traditional approach):
SELECT * FROM (
    SELECT 
        region,
        seller_id,
        total_sales,
        RANK() OVER (PARTITION BY region ORDER BY total_sales DESC) AS sales_rank
    FROM sellers
) WHERE sales_rank <= 3;
```

**Use Case:** QUALIFY is Snowflake-specific and cleaner for filtering window functions.

***

### **Q3: Calculate running totals and moving averages in a single query.**

**Answer:**
```sql
SELECT 
    order_date,
    daily_revenue,
    -- Running total from start
    SUM(daily_revenue) OVER (
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    
    -- 7-day moving average
    AVG(daily_revenue) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7day,
    
    -- Month-to-date total
    SUM(daily_revenue) OVER (
        PARTITION BY DATE_TRUNC('month', order_date)
        ORDER BY order_date
    ) AS mtd_revenue
FROM daily_sales
ORDER BY order_date;
```

***

### **Q4: How do you calculate percentage contribution using window functions?**

**Answer:**
```sql
SELECT 
    product_category,
    product_id,
    sales_amount,
    -- Percentage of category total
    sales_amount / SUM(sales_amount) OVER (
        PARTITION BY product_category
    ) * 100 AS pct_of_category,
    
    -- Percentage of grand total
    sales_amount / SUM(sales_amount) OVER () * 100 AS pct_of_total,
    
    -- Cumulative percentage within category
    SUM(sales_amount) OVER (
        PARTITION BY product_category 
        ORDER BY sales_amount DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) / SUM(sales_amount) OVER (
        PARTITION BY product_category
    ) * 100 AS cumulative_pct
FROM product_sales;
```

***

### **Q5: Explain LEAD() and LAG() functions with practical use case.**

**Answer:**
```sql
-- Calculate period-over-period growth
SELECT 
    report_month,
    monthly_revenue,
    
    -- Previous month revenue
    LAG(monthly_revenue, 1) OVER (ORDER BY report_month) AS prev_month_revenue,
    
    -- Next month revenue (for forecasting comparison)
    LEAD(monthly_revenue, 1) OVER (ORDER BY report_month) AS next_month_revenue,
    
    -- Month-over-month growth %
    (monthly_revenue - LAG(monthly_revenue, 1) OVER (ORDER BY report_month)) 
    / NULLIF(LAG(monthly_revenue, 1) OVER (ORDER BY report_month), 0) * 100 
    AS mom_growth_pct,
    
    -- Year-over-year comparison
    LAG(monthly_revenue, 12) OVER (ORDER BY report_month) AS same_month_last_year,
    
    (monthly_revenue - LAG(monthly_revenue, 12) OVER (ORDER BY report_month))
    / NULLIF(LAG(monthly_revenue, 12) OVER (ORDER BY report_month), 0) * 100
    AS yoy_growth_pct
FROM monthly_financials
ORDER BY report_month;
```

**Use Case:** Period-over-period analysis, time series comparisons.

***

### **Q6: How do you identify gaps and islands in sequential data?**

**Answer:**
```sql
-- Find consecutive days of activity (islands)
WITH activity_with_groups AS (
    SELECT 
        user_id,
        activity_date,
        -- Calculate group identifier for consecutive dates
        activity_date - ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY activity_date
        )::DATE AS activity_group
    FROM user_activity
)
SELECT 
    user_id,
    MIN(activity_date) AS streak_start,
    MAX(activity_date) AS streak_end,
    COUNT(*) AS consecutive_days
FROM activity_with_groups
GROUP BY user_id, activity_group
HAVING COUNT(*) >= 3  -- Streaks of 3+ days
ORDER BY user_id, streak_start;
```

***

### **Q7: Calculate percentiles using window functions in Snowflake.**

**Answer:**
```sql
SELECT 
    customer_id,
    total_spent,
    
    -- Percentile rank (0 to 1)
    PERCENT_RANK() OVER (ORDER BY total_spent) AS percentile_rank,
    
    -- Percentile values using PERCENTILE_CONT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_spent) 
        OVER () AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_spent) 
        OVER () AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_spent) 
        OVER () AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_spent) 
        OVER () AS p90,
    
    -- Assign quartile buckets
    NTILE(4) OVER (ORDER BY total_spent) AS quartile,
    
    -- Customer segment based on percentile
    CASE 
        WHEN PERCENT_RANK() OVER (ORDER BY total_spent) >= 0.95 THEN 'Top 5%'
        WHEN PERCENT_RANK() OVER (ORDER BY total_spent) >= 0.80 THEN 'Top 20%'
        WHEN PERCENT_RANK() OVER (ORDER BY total_spent) >= 0.50 THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END AS customer_segment
FROM customer_purchases;
```

***

### **Q8: How do you calculate first and last values in a window?**

**Answer:**
```sql
SELECT 
    customer_id,
    order_date,
    order_amount,
    
    -- First order amount
    FIRST_VALUE(order_amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_order_amount,
    
    -- Last order amount
    LAST_VALUE(order_amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_order_amount,
    
    -- Compare current to first order
    order_amount - FIRST_VALUE(order_amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS change_from_first,
    
    -- Days since first order
    DATEDIFF('day', 
        FIRST_VALUE(order_date) OVER (
            PARTITION BY customer_id 
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        order_date
    ) AS days_since_first_order
FROM orders;
```

**Important:** Always use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` with LAST_VALUE to get actual last value.

***

### **Q9: Write a query to detect outliers using statistical methods.**

**Answer:**
```sql
WITH stats AS (
    SELECT 
        product_id,
        AVG(sales_amount) AS mean_sales,
        STDDEV(sales_amount) AS stddev_sales,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY sales_amount) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sales_amount) AS q3
    FROM product_sales
    GROUP BY product_id
),
outlier_detection AS (
    SELECT 
        ps.sale_id,
        ps.product_id,
        ps.sales_amount,
        s.mean_sales,
        s.stddev_sales,
        
        -- Z-score method (values beyond 3 std deviations)
        ABS(ps.sales_amount - s.mean_sales) / NULLIF(s.stddev_sales, 0) AS z_score,
        
        -- IQR method
        s.q3 - s.q1 AS iqr,
        s.q1 - 1.5 * (s.q3 - s.q1) AS lower_bound,
        s.q3 + 1.5 * (s.q3 - s.q1) AS upper_bound,
        
        -- Outlier flags
        CASE WHEN ABS(ps.sales_amount - s.mean_sales) / NULLIF(s.stddev_sales, 0) > 3 
             THEN TRUE ELSE FALSE END AS is_outlier_zscore,
        
        CASE WHEN ps.sales_amount < (s.q1 - 1.5 * (s.q3 - s.q1)) 
                  OR ps.sales_amount > (s.q3 + 1.5 * (s.q3 - s.q1))
             THEN TRUE ELSE FALSE END AS is_outlier_iqr
    FROM product_sales ps
    JOIN stats s ON ps.product_id = s.product_id
)
SELECT *
FROM outlier_detection
WHERE is_outlier_zscore OR is_outlier_iqr;
```

***

### **Q10: Implement a complex cohort analysis using window functions.**

**Answer:**
```sql
WITH first_purchase AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM orders
    GROUP BY customer_id
),
cohort_data AS (
    SELECT 
        fp.cohort_month,
        DATE_TRUNC('month', o.order_date) AS order_month,
        DATEDIFF('month', fp.cohort_month, o.order_date) AS months_since_first,
        COUNT(DISTINCT o.customer_id) AS active_customers,
        SUM(o.order_amount) AS cohort_revenue
    FROM first_purchase fp
    JOIN orders o ON fp.customer_id = o.customer_id
    GROUP BY fp.cohort_month, DATE_TRUNC('month', o.order_date)
)
SELECT 
    cohort_month,
    months_since_first,
    active_customers,
    cohort_revenue,
    
    -- Retention rate
    active_customers * 100.0 / FIRST_VALUE(active_customers) OVER (
        PARTITION BY cohort_month 
        ORDER BY months_since_first
    ) AS retention_rate,
    
    -- Cumulative revenue per cohort
    SUM(cohort_revenue) OVER (
        PARTITION BY cohort_month 
        ORDER BY months_since_first
    ) AS cumulative_revenue,
    
    -- Average revenue per cohort customer
    SUM(cohort_revenue) OVER (
        PARTITION BY cohort_month 
        ORDER BY months_since_first
    ) / FIRST_VALUE(active_customers) OVER (
        PARTITION BY cohort_month 
        ORDER BY months_since_first
    ) AS avg_revenue_per_user
FROM cohort_data
ORDER BY cohort_month, months_since_first;
```

***

## **SECTION 2: SNOWFLAKE-SPECIFIC FEATURES (10 Questions)**

### **Q11: How do you work with VARIANT data type and parse JSON in Snowflake?**

**Answer:**
```sql
-- Create table with semi-structured data
CREATE TABLE events (
    event_id NUMBER,
    event_data VARIANT,
    event_timestamp TIMESTAMP
);

-- Query JSON data
SELECT 
    event_id,
    event_data:user_id::STRING AS user_id,
    event_data:action::STRING AS action,
    event_data:metadata.device::STRING AS device,
    event_data:metadata.location.city::STRING AS city,
    
    -- Extract array elements
    event_data:items[0].product_id::STRING AS first_product,
    
    -- Check if key exists
    IFF(event_data:promotion_code IS NOT NULL, TRUE, FALSE) AS has_promotion,
    
    -- Parse nested arrays
    PARSE_JSON(event_data:items) AS items_array
FROM events;

-- Flatten nested arrays
SELECT 
    event_id,
    event_data:user_id::STRING AS user_id,
    f.value:product_id::STRING AS product_id,
    f.value:quantity::NUMBER AS quantity,
    f.value:price::FLOAT AS price
FROM events,
LATERAL FLATTEN(input => event_data:items) f;
```

***

### **Q12: Explain FLATTEN function and use it to unnest hierarchical data.**

**Answer:**
```sql
-- Flatten multi-level nested JSON
WITH nested_data AS (
    SELECT PARSE_JSON('{
        "order_id": "12345",
        "customer": {
            "id": "C001",
            "addresses": [
                {"type": "billing", "city": "Mumbai"},
                {"type": "shipping", "city": "Delhi"}
            ]
        },
        "items": [
            {"product": "A", "qty": 2, "addons": ["warranty", "gift_wrap"]},
            {"product": "B", "qty": 1, "addons": ["express_shipping"]}
        ]
    }') AS order_data
)
-- Flatten items
SELECT 
    order_data:order_id::STRING AS order_id,
    order_data:customer.id::STRING AS customer_id,
    item.value:product::STRING AS product,
    item.value:qty::NUMBER AS quantity,
    addon.value::STRING AS addon_service
FROM nested_data,
LATERAL FLATTEN(input => order_data:items) item,
LATERAL FLATTEN(input => item.value:addons, OUTER => TRUE) addon;

-- Result shows each item-addon combination
```

**Key Points:**
- FLATTEN converts array/object to rows
- OUTER => TRUE keeps records even if array is empty
- Can chain multiple FLATTEN for nested arrays

***

### **Q13: What are Snowflake Time Travel and how do you use it?**

**Answer:**
```sql
-- Query historical data (up to 90 days with Enterprise)
-- Query table as it was 1 hour ago
SELECT * 
FROM orders 
AT(OFFSET => -3600);  -- 3600 seconds = 1 hour

-- Query at specific timestamp
SELECT * 
FROM orders 
AT(TIMESTAMP => '2025-10-15 10:00:00'::TIMESTAMP);

-- Query before a statement was executed
SELECT * 
FROM orders 
BEFORE(STATEMENT => '01a9f2b3-0000-4c5d-0000-0001234567890');

-- Restore accidentally deleted data
CREATE OR REPLACE TABLE orders_restored AS
SELECT * FROM orders AT(OFFSET => -7200);

-- Undrop a table
UNDROP TABLE orders;

-- Check data retention period
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE orders;

-- Clone table with Time Travel
CREATE TABLE orders_backup 
CLONE orders 
AT(TIMESTAMP => '2025-10-15 00:00:00'::TIMESTAMP);
```

**Use Cases:** Data recovery, auditing, debugging, testing.

***

### **Q14: Explain TRANSIENT and TEMPORARY tables in Snowflake.**

**Answer:**
```sql
-- TRANSIENT TABLE: No Fail-safe, lower storage cost
CREATE TRANSIENT TABLE staging_orders (
    order_id NUMBER,
    order_date DATE,
    amount FLOAT
);
-- Time Travel: 0-1 days (configurable)
-- Fail-safe: NO (data not recoverable after Time Travel)
-- Use case: Staging tables, ETL intermediates

-- TEMPORARY TABLE: Session-scoped, automatically dropped
CREATE TEMPORARY TABLE temp_calculations (
    customer_id NUMBER,
    total_spent FLOAT
);
-- Exists only in current session
-- No Time Travel, No Fail-safe
-- Use case: Session-specific processing, CTEs alternative

-- PERMANENT TABLE (default): Full protection
CREATE TABLE prod_orders (
    order_id NUMBER,
    order_date DATE,
    amount FLOAT
);
-- Time Travel: 0-90 days (Enterprise)
-- Fail-safe: 7 days (not queryable, Snowflake recovery only)
-- Use case: Production data

-- Comparison
/*
| Feature        | PERMANENT | TRANSIENT | TEMPORARY |
|----------------|-----------|-----------|-----------|
| Time Travel    | 0-90 days | 0-1 days  | 0-1 days  |
| Fail-safe      | 7 days    | None      | None      |
| Scope          | Account   | Account   | Session   |
| Storage Cost   | Highest   | Lower     | Lowest    |
*/
```

***

### **Q15: How do you use Snowflake COPY INTO for data loading?**

**Answer:**
```sql
-- Load from S3 with error handling
COPY INTO orders
FROM @my_s3_stage/orders/
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    COMPRESSION = 'GZIP'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE'  -- Options: CONTINUE, SKIP_FILE, ABORT_STATEMENT
VALIDATION_MODE = 'RETURN_ERRORS';  -- Test without loading

-- Load with transformation
COPY INTO customers
FROM (
    SELECT 
        $1::NUMBER AS customer_id,
        $2::STRING AS customer_name,
        $3::STRING AS email,
        UPPER($4)::STRING AS country,
        TO_DATE($5, 'YYYY-MM-DD') AS registration_date,
        CURRENT_TIMESTAMP AS loaded_at
    FROM @my_stage/customers/
)
FILE_FORMAT = (TYPE = 'CSV')
PATTERN = '.*customer.*[.]csv';

-- Load JSON files
COPY INTO events
FROM @json_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';

-- Check load history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
));
```

***

### **Q16: Explain MERGE statement in Snowflake for SCD Type 2.**

**Answer:**
```sql
-- Slowly Changing Dimension Type 2 implementation
MERGE INTO dim_customer target
USING (
    SELECT 
        customer_id,
        customer_name,
        email,
        address,
        CURRENT_TIMESTAMP AS effective_date
    FROM staging_customers
) source
ON target.customer_id = source.customer_id 
   AND target.is_current = TRUE

-- When dimensions changed: close old record, insert new
WHEN MATCHED 
    AND (target.customer_name != source.customer_name 
         OR target.email != source.email 
         OR target.address != source.address)
THEN UPDATE SET
    target.is_current = FALSE,
    target.end_date = source.effective_date

-- Insert new customers
WHEN NOT MATCHED THEN INSERT (
    customer_id,
    customer_name,
    email,
    address,
    start_date,
    end_date,
    is_current
) VALUES (
    source.customer_id,
    source.customer_name,
    source.email,
    source.address,
    source.effective_date,
    '9999-12-31'::DATE,
    TRUE
);

-- Insert new versions for changed records
INSERT INTO dim_customer (
    customer_id, customer_name, email, address,
    start_date, end_date, is_current
)
SELECT 
    customer_id, customer_name, email, address,
    CURRENT_TIMESTAMP,
    '9999-12-31'::DATE,
    TRUE
FROM staging_customers s
WHERE EXISTS (
    SELECT 1 FROM dim_customer d
    WHERE d.customer_id = s.customer_id
    AND d.is_current = TRUE
    AND (d.customer_name != s.customer_name 
         OR d.email != s.email 
         OR d.address != s.address)
);
```

***

### **Q17: How do you implement incremental loading pattern?**

**Answer:**
```sql
-- Create table with metadata for incremental tracking
CREATE TABLE IF NOT EXISTS orders (
    order_id NUMBER PRIMARY KEY,
    order_date TIMESTAMP,
    customer_id NUMBER,
    amount FLOAT,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Incremental load query
MERGE INTO orders target
USING (
    SELECT 
        order_id,
        order_date,
        customer_id,
        amount,
        updated_at
    FROM source_orders
    WHERE updated_at > (
        SELECT COALESCE(MAX(updated_at), '1900-01-01'::TIMESTAMP)
        FROM orders
    )
) source
ON target.order_id = source.order_id

WHEN MATCHED AND source.updated_at > target.updated_at THEN
    UPDATE SET
        target.order_date = source.order_date,
        target.customer_id = source.customer_id,
        target.amount = source.amount,
        target.updated_at = source.updated_at,
        target._loaded_at = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN
    INSERT (order_id, order_date, customer_id, amount, updated_at, _loaded_at)
    VALUES (source.order_id, source.order_date, source.customer_id, 
            source.amount, source.updated_at, CURRENT_TIMESTAMP);

-- Audit table for tracking loads
CREATE TABLE IF NOT EXISTS load_audit (
    table_name STRING,
    load_start TIMESTAMP,
    load_end TIMESTAMP,
    records_processed NUMBER,
    max_timestamp TIMESTAMP
);

-- Insert audit record
INSERT INTO load_audit
SELECT 
    'orders' AS table_name,
    load_start,
    CURRENT_TIMESTAMP AS load_end,
    COUNT(*) AS records_processed,
    MAX(updated_at) AS max_timestamp
FROM orders
WHERE _loaded_at >= (SELECT load_start FROM load_tracking);
```

***

### **Q18: Explain Snowflake Clustering Keys and when to use them.**

**Answer:**
```sql
-- Create table with clustering key
CREATE TABLE large_orders (
    order_id NUMBER,
    order_date DATE,
    customer_id NUMBER,
    region STRING,
    amount FLOAT
)
CLUSTER BY (order_date, region);

-- Check clustering quality
SELECT SYSTEM$CLUSTERING_INFORMATION('large_orders');

-- Result shows clustering depth (lower is better, 0-4 ideal)
-- Add clustering to existing table
ALTER TABLE large_orders CLUSTER BY (order_date, region);

-- When to use clustering:
/*
1. Table size > 1TB
2. Queries filter on specific columns consistently
3. Columns have high cardinality
4. Query performance is slow despite partition pruning

Best practices:
- Use 1-4 columns (order matters)
- Put most filtered column first
- High cardinality columns work best
- Date columns are common candidates
- Monitor clustering depth

Example query benefiting from clustering:
*/
SELECT customer_id, SUM(amount)
FROM large_orders
WHERE order_date BETWEEN '2025-01-01' AND '2025-01-31'
  AND region = 'Southeast'
GROUP BY customer_id;
-- This query scans fewer micro-partitions due to clustering

-- Check clustering ratio
SELECT 
    SYSTEM$CLUSTERING_INFORMATION('large_orders', 
    '(order_date, region)') AS clustering_info;
```

***

### **Q19: How do you use Snowflake STREAMS for CDC (Change Data Capture)?**

**Answer:**
```sql
-- Create stream on source table
CREATE STREAM orders_stream ON TABLE orders;

-- Stream captures INSERT, UPDATE, DELETE operations
-- Query stream to see changes
SELECT 
    order_id,
    customer_id,
    amount,
    METADATA$ACTION AS dml_type,  -- INSERT, DELETE
    METADATA$ISUPDATE AS is_update,  -- TRUE for UPDATEs
    METADATA$ROW_ID AS row_id
FROM orders_stream;

-- Process changes into target table
BEGIN TRANSACTION;

-- Handle updates (appears as DELETE + INSERT)
MERGE INTO orders_summary target
USING (
    SELECT 
        customer_id,
        SUM(CASE WHEN METADATA$ACTION = 'INSERT' 
                 AND METADATA$ISUPDATE = FALSE 
                 THEN amount ELSE 0 END) AS new_orders,
        SUM(CASE WHEN METADATA$ACTION = 'DELETE' 
                 AND METADATA$ISUPDATE = FALSE 
                 THEN -amount ELSE 0 END) AS cancelled_orders,
        COUNT(DISTINCT order_id) AS order_count
    FROM orders_stream
    GROUP BY customer_id
) source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        target.total_amount = target.total_amount + source.new_orders + source.cancelled_orders,
        target.order_count = target.order_count + source.order_count,
        target.updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (customer_id, total_amount, order_count, updated_at)
    VALUES (source.customer_id, source.new_orders + source.cancelled_orders, 
            source.order_count, CURRENT_TIMESTAMP);

COMMIT;

-- Stream is automatically consumed after successful commit
-- Check stream offset
SHOW STREAMS LIKE 'orders_stream';
```

***

### **Q20: Implement a Type 4 (History Table) slowly changing dimension.**

**Answer:**
```sql
-- Current dimension table
CREATE TABLE dim_product_current (
    product_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    product_id STRING,
    product_name STRING,
    category STRING,
    price FLOAT,
    effective_date TIMESTAMP,
    current_flag BOOLEAN DEFAULT TRUE,
    UNIQUE (product_id)
);

-- History table
CREATE TABLE dim_product_history (
    history_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    product_sk NUMBER,
    product_id STRING,
    product_name STRING,
    category STRING,
    price FLOAT,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    change_type STRING
);

-- Merge process
MERGE INTO dim_product_current target
USING staging_products source
ON target.product_id = source.product_id

-- When changed: archive to history, update current
WHEN MATCHED AND (
    target.product_name != source.product_name OR
    target.category != source.category OR
    target.price != source.price
) THEN UPDATE SET
    target.product_name = source.product_name,
    target.category = source.category,
    target.price = source.price,
    target.effective_date = CURRENT_TIMESTAMP

-- New products
WHEN NOT MATCHED THEN INSERT (
    product_id, product_name, category, price, effective_date
) VALUES (
    source.product_id, source.product_name, source.category, 
    source.price, CURRENT_TIMESTAMP
);

-- Archive changed records to history
INSERT INTO dim_product_history (
    product_sk, product_id, product_name, category, price,
    effective_date, end_date, change_type
)
SELECT 
    target.product_sk,
    target.product_id,
    target.product_name,
    target.category,
    target.price,
    target.effective_date,
    CURRENT_TIMESTAMP AS end_date,
    'UPDATE' AS change_type
FROM dim_product_current target
JOIN staging_products source ON target.product_id = source.product_id
WHERE target.product_name != source.product_name 
   OR target.category != source.category 
   OR target.price != source.price;
```

***

## **SECTION 3: COMPLEX JOINS & SET OPERATIONS (10 Questions)**

### **Q21: Explain the difference between INNER, LEFT, RIGHT, FULL OUTER, and CROSS JOIN.**

**Answer:**
```sql
-- Sample data
-- customers: id(1,2,3), name
-- orders: order_id, customer_id(1,1,2), amount

-- INNER JOIN: Only matching records
SELECT c.id, c.name, o.order_id, o.amount
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id;
-- Result: customers 1,2 (3 excluded - no orders)

-- LEFT JOIN: All left table + matching right
SELECT c.id, c.name, o.order_id, o.amount
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id;
-- Result: customers 1,2,3 (3 has NULL order values)

-- RIGHT JOIN: All right table + matching left
SELECT c.id, c.name, o.order_id, o.amount
FROM customers c
RIGHT JOIN orders o ON c.id = o.customer_id;
-- Result: All orders with customer details

-- FULL OUTER JOIN: All records from both tables
SELECT c.id, c.name, o.order_id, o.amount
FROM customers c
FULL OUTER JOIN orders o ON c.id = o.customer_id;
-- Result: All customers + all orders (NULLs where no match)

-- CROSS JOIN: Cartesian product
SELECT c.id, c.name, o.order_id, o.amount
FROM customers c
CROSS JOIN orders o;
-- Result: 3 customers × 3 orders = 9 rows (every combination)

-- Find customers with NO orders (anti-join pattern)
SELECT c.id, c.name
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.order_id IS NULL;

-- Find customers WITH orders (semi-join pattern)
SELECT DISTINCT c.id, c.name
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id;
```

***

### **Q22: How do you implement a self-join for hierarchical data?**

**Answer:**
```sql
-- Employee hierarchy
CREATE TABLE employees (
    emp_id NUMBER,
    emp_name STRING,
    manager_id NUMBER,
    salary FLOAT
);

-- Find employee with their manager
SELECT 
    e.emp_id,
    e.emp_name AS employee,
    e.salary AS emp_salary,
    m.emp_id AS manager_id,
    m.emp_name AS manager,
    m.salary AS manager_salary
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.emp_id;

-- Find hierarchy levels (recursive CTE)
WITH RECURSIVE org_hierarchy AS (
    -- Anchor: Top-level (CEO - no manager)
    SELECT 
        emp_id,
        emp_name,
        manager_id,
        salary,
        1 AS level,
        emp_name AS path
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive: Subordinates
    SELECT 
        e.emp_id,
        e.emp_name,
        e.manager_id,
        e.salary,
        oh.level + 1,
        oh.path || ' -> ' || e.emp_name AS path
    FROM employees e
    INNER JOIN org_hierarchy oh ON e.manager_id = oh.emp_id
)
SELECT * FROM org_hierarchy
ORDER BY level, emp_name;

-- Count direct reports per manager
SELECT 
    m.emp_id AS manager_id,
    m.emp_name AS manager,
    COUNT(e.emp_id) AS direct_reports,
    AVG(e.salary) AS avg_team_salary
FROM employees m
LEFT JOIN employees e ON m.emp_id = e.manager_id
GROUP BY m.emp_id, m.emp_name
HAVING COUNT(e.emp_id) > 0;
```

***

### **Q23: Write a query to find gaps in sequential data.**

**Answer:**
```sql
-- Find missing order IDs
WITH order_sequence AS (
    SELECT order_id
    FROM orders
    WHERE order_date >= '2025-01-01'
),
expected_sequence AS (
    SELECT SEQ4() + 1000 AS expected_id  -- Start from 1000
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)
SELECT e.expected_id AS missing_order_id
FROM expected_sequence e
LEFT JOIN order_sequence o ON e.expected_id = o.order_id
WHERE o.order_id IS NULL
  AND e.expected_id <= (SELECT MAX(order_id) FROM orders)
ORDER BY e.expected_id
LIMIT 100;

-- Find date gaps (missing days)
WITH date_range AS (
    SELECT DATEADD(day, SEQ4(), '2025-01-01'::DATE) AS expected_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
),
actual_dates AS (
    SELECT DISTINCT order_date
    FROM orders
    WHERE order_date >= '2025-01-01'
)
SELECT dr.expected_date AS missing_date
FROM date_range dr
LEFT JOIN actual_dates ad ON dr.expected_date = ad.order_date
WHERE ad.order_date IS NULL
  AND dr.expected_date <= CURRENT_DATE;

-- Find gaps in employee IDs
SELECT 
    prev_id + 1 AS gap_start,
    curr_id - 1 AS gap_end,
    curr_id - prev_id - 1 AS gap_size
FROM (
    SELECT 
        emp_id AS curr_id,
        LAG(emp_id) OVER (ORDER BY emp_id) AS prev_id
    FROM employees
) 
WHERE curr_id - prev_id > 1
ORDER BY gap_start;
```

***

### **Q24: Implement UNION vs UNION ALL vs INTERSECT vs EXCEPT.**

**Answer:**
```sql
-- UNION: Combines results, removes duplicates (slower)
SELECT customer_id, 'Online' AS channel
FROM online_orders
UNION
SELECT customer_id, 'Retail' AS channel
FROM retail_orders;
-- Deduplicates, sorts automatically

-- UNION ALL: Combines results, keeps duplicates (faster)
SELECT customer_id, order_date, amount
FROM orders_2024
UNION ALL
SELECT customer_id, order_date, amount
FROM orders_2025;
-- No deduplication, faster for large datasets

-- INTERSECT: Only records in BOTH queries
SELECT customer_id
FROM online_orders
INTERSECT
SELECT customer_id
FROM retail_orders;
-- Customers who bought both online AND retail

-- EXCEPT: Records in first query NOT in second
SELECT customer_id
FROM online_orders
EXCEPT
SELECT customer_id
FROM retail_orders;
-- Customers who bought online but NEVER retail

-- Practical use case: Customer segmentation
WITH online_customers AS (
    SELECT DISTINCT customer_id FROM online_orders
),
retail_customers AS (
    SELECT DISTINCT customer_id FROM retail_orders
)
SELECT 
    'Online Only' AS segment,
    COUNT(*) AS customer_count
FROM (
    SELECT customer_id FROM online_customers
    EXCEPT
    SELECT customer_id FROM retail_customers
)
UNION ALL
SELECT 
    'Retail Only' AS segment,
    COUNT(*) AS customer_count
FROM (
    SELECT customer_id FROM retail_customers
    EXCEPT
    SELECT customer_id FROM online_customers
)
UNION ALL
SELECT 
    'Omnichannel' AS segment,
    COUNT(*) AS customer_count
FROM (
    SELECT customer_id FROM online_customers
    INTERSECT
    SELECT customer_id FROM retail_customers
);
```

***

### **Q25: How do you handle duplicate records in joins?**

**Answer:**
```sql
-- Problem: Multiple addresses per customer causing duplicates
-- customers: 1 row per customer
-- addresses: Multiple rows per customer

-- WRONG: Cartesian explosion
SELECT 
    c.customer_id,
    c.total_spent,
    a.address_type,
    a.city
FROM customers c
JOIN addresses a ON c.customer_id = a.customer_id;
-- If customer has 3 addresses, total_spent counted 3 times!

-- Solution 1: Use DISTINCT with aggregate
SELECT 
    c.customer_id,
    MAX(c.total_spent) AS total_spent,  -- Use MAX to avoid sum multiplication
    COUNT(DISTINCT a.address_id) AS address_count
FROM customers c
LEFT JOIN addresses a ON c.customer_id = a.customer_id
GROUP BY c.customer_id;

-- Solution 2: Filter join to specific address type
SELECT 
    c.customer_id,
    c.total_spent,
    a.city AS primary_city
FROM customers c
LEFT JOIN (
    SELECT * FROM addresses 
    WHERE address_type = 'Primary'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY address_id) = 1
) a ON c.customer_id = a.customer_id;

-- Solution 3: Aggregate before joining
SELECT 
    c.customer_id,
    c.total_spent,
    a.address_count,
    a.cities
FROM customers c
LEFT JOIN (
    SELECT 
        customer_id,
        COUNT(*) AS address_count,
        LISTAGG(city, ', ') AS cities
    FROM addresses
    GROUP BY customer_id
) a ON c.customer_id = a.customer_id;

-- Solution 4: Use QUALIFY to get specific row
SELECT 
    c.customer_id,
    c.total_spent,
    a.city
FROM customers c
LEFT JOIN addresses a ON c.customer_id = a.customer_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY a.is_primary DESC, a.address_id) = 1;
```

***

### **Q26: Implement complex multi-table joins with filtering.**

**Answer:**
```sql
-- Business requirement: High-value orders analysis
-- Join 5 tables with complex conditions

SELECT 
    -- Customer details
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    
    -- Order summary
    o.order_id,
    o.order_date,
    o.order_status,
    
    -- Product details
    p.product_category,
    p.product_name,
    
    -- Financial metrics
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS line_total,
    
    -- Payment details
    pay.payment_method,
    pay.payment_status,
    
    -- Seller information
    s.seller_name,
    s.seller_rating,
    
    -- Delivery performance
    DATEDIFF('day', o.order_date, o.delivered_date) AS delivery_days,
    CASE 
        WHEN o.delivered_date <= o.expected_delivery_date THEN 'On Time'
        ELSE 'Delayed'
    END AS delivery_status

FROM customers c

-- Orders with high value filter
INNER JOIN orders o 
    ON c.customer_id = o.customer_id
    AND o.order_date >= DATEADD('month', -3, CURRENT_DATE)
    AND o.order_status != 'cancelled'

-- Order items with quantity check
INNER JOIN order_items oi 
    ON o.order_id = oi.order_id
    AND oi.quantity > 0

-- Products with category filter
INNER JOIN products p 
    ON oi.product_id = p.product_id
    AND p.product_category IN ('Electronics', 'Furniture')

-- Payment information
LEFT JOIN payments pay 
    ON o.order_id = pay.order_id

-- Seller details with rating threshold
INNER JOIN sellers s 
    ON oi.seller_id = s.seller_id
    AND s.seller_rating >= 4.0
    AND s.is_active = TRUE

WHERE 
    -- Additional filters
    oi.quantity * oi.unit_price > 1000  -- High-value items
    AND c.customer_segment = 'Premium'
    
QUALIFY 
    -- Get latest order per customer-product combo
    ROW_NUMBER() OVER (
        PARTITION BY c.customer_id, p.product_id 
        ORDER BY o.order_date DESC
    ) = 1

ORDER BY 
    o.order_date DESC,
    line_total DESC;
```

***

### **Q27: Write lateral joins for complex array processing.**

**Answer:**
```sql
-- LATERAL join allows correlated subqueries in FROM clause

-- Example: Top 3 products per category
SELECT 
    c.category_id,
    c.category_name,
    tp.product_id,
    tp.product_name,
    tp.sales_amount,
    tp.sales_rank
FROM categories c,
LATERAL (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(oi.quantity * oi.unit_price) AS sales_amount,
        RANK() OVER (ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS sales_rank
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    WHERE p.category_id = c.category_id
    GROUP BY p.product_id, p.product_name
    ORDER BY sales_amount DESC
    LIMIT 3
) tp;

-- Customer purchase frequency bands
SELECT 
    c.customer_id,
    c.customer_name,
    pf.total_orders,
    pf.frequency_band,
    pf.days_between_orders
FROM customers c,
LATERAL (
    SELECT 
        COUNT(*) AS total_orders,
        AVG(DATEDIFF('day', 
            LAG(order_date) OVER (ORDER BY order_date),
            order_date
        )) AS days_between_orders,
        CASE 
            WHEN COUNT(*) >= 10 THEN 'High Frequency'
            WHEN COUNT(*) >= 5 THEN 'Medium Frequency'
            ELSE 'Low Frequency'
        END AS frequency_band
    FROM orders o
    WHERE o.customer_id = c.customer_id
) pf
WHERE pf.total_orders > 0;

-- Unnest JSON arrays with LATERAL
SELECT 
    o.order_id,
    o.customer_id,
    item.value:product_id::STRING AS product_id,
    item.value:quantity::NUMBER AS quantity,
    item.value:price::FLOAT AS price
FROM orders_json o,
LATERAL FLATTEN(input => o.order_items) item;
```

***

### **Q28: Implement anti-join and semi-join patterns.**

**Answer:**
```sql
-- ANTI-JOIN: Find records in A NOT in B

-- Method 1: LEFT JOIN with NULL check (traditional)
SELECT c.customer_id, c.customer_name
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;

-- Method 2: NOT EXISTS (optimal for large datasets)
SELECT c.customer_id, c.customer_name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 
    FROM orders o 
    WHERE o.customer_id = c.customer_id
);

-- Method 3: NOT IN (beware of NULLs!)
SELECT customer_id, customer_name
FROM customers
WHERE customer_id NOT IN (
    SELECT customer_id 
    FROM orders 
    WHERE customer_id IS NOT NULL  -- Important!
);

-- Method 4: EXCEPT (set operation)
SELECT customer_id FROM customers
EXCEPT
SELECT customer_id FROM orders;

-- SEMI-JOIN: Find records in A that have match in B (without duplicates)

-- Method 1: EXISTS (efficient)
SELECT c.customer_id, c.customer_name, c.total_spent
FROM customers c
WHERE EXISTS (
    SELECT 1 
    FROM high_value_orders h 
    WHERE h.customer_id = c.customer_id
);

-- Method 2: IN (less efficient with large subqueries)
SELECT customer_id, customer_name, total_spent
FROM customers
WHERE customer_id IN (
    SELECT DISTINCT customer_id 
    FROM high_value_orders
);

-- Method 3: INNER JOIN with DISTINCT (less efficient)
SELECT DISTINCT c.customer_id, c.customer_name, c.total_spent
FROM customers c
INNER JOIN high_value_orders h ON c.customer_id = h.customer_id;

-- Performance comparison for Analytics Engineering:
/*
Best to Worst:
1. EXISTS / NOT EXISTS - Optimizer-friendly, stops at first match
2. SEMI/ANTI JOIN (if supported directly)
3. LEFT JOIN + WHERE NULL (for anti-join)
4. IN / NOT IN - Watch for NULLs
5. DISTINCT after INNER JOIN - Creates unnecessary rows
*/
```

***

### **Q29: Handle circular joins and recursive relationships.**

**Answer:**
```sql
-- Recursive CTE for Bill of Materials (parts explosion)
WITH RECURSIVE parts_explosion AS (
    -- Base case: Top-level products
    SELECT 
        product_id,
        component_id,
        quantity,
        1 AS level,
        CAST(product_id AS STRING) AS path,
        quantity AS total_quantity
    FROM bill_of_materials
    WHERE product_id IN (SELECT product_id FROM finished_products)
    
    UNION ALL
    
    -- Recursive case: Sub-components
    SELECT 
        pe.product_id,
        bom.component_id,
        bom.quantity,
        pe.level + 1,
        pe.path || '->' || bom.component_id,
        pe.total_quantity * bom.quantity  -- Multiply quantities down the tree
    FROM parts_explosion pe
    JOIN bill_of_materials bom 
        ON pe.component_id = bom.product_id
    WHERE pe.level < 10  -- Prevent infinite loops
      AND pe.path NOT LIKE '%' || bom.component_id || '%'  -- Detect cycles
)
SELECT 
    product_id AS finished_product,
    component_id AS raw_component,
    level AS depth,
    path AS component_path,
    total_quantity,
    CASE 
        WHEN component_id NOT IN (SELECT DISTINCT product_id FROM bill_of_materials) 
        THEN 'Leaf Component'
        ELSE 'Sub-assembly'
    END AS component_type
FROM parts_explosion
ORDER BY product_id, level, component_id;

-- Network path analysis (e.g., social connections)
WITH RECURSIVE connection_network AS (
    -- Direct connections
    SELECT 
        user_id AS start_user,
        friend_id AS end_user,
        1 AS degree,
        user_id || '->' || friend_id AS path
    FROM friendships
    WHERE user_id = 12345  -- Starting user
    
    UNION ALL
    
    -- Friend of friend, up to 3 degrees
    SELECT 
        cn.start_user,
        f.friend_id,
        cn.degree + 1,
        cn.path || '->' || f.friend_id
    FROM connection_network cn
    JOIN friendships f ON cn.end_user = f.user_id
    WHERE cn.degree < 3
      AND cn.path NOT LIKE '%' || f.friend_id || '%'  -- Avoid revisiting
)
SELECT 
    degree,
    end_user,
    path,
    COUNT(*) OVER (PARTITION BY degree) AS connections_at_degree
FROM connection_network
ORDER BY degree, end_user;
```

***

### **Q30: Complex join with conditional logic.**

**Answer:**
```sql
-- Conditional join based on business rules
SELECT 
    o.order_id,
    o.order_date,
    o.customer_id,
    o.order_total,
    
    -- Conditional joins based on order characteristics
    CASE 
        WHEN o.order_total >= 1000 THEN vip.discount_rate
        WHEN o.order_total >= 500 THEN reg.discount_rate
        ELSE new.discount_rate
    END AS applicable_discount,
    
    -- Join to different pricing tables based on customer type
    COALESCE(
        CASE WHEN c.customer_type = 'Enterprise' THEN ep.unit_price END,
        CASE WHEN c.customer_type = 'Business' THEN bp.unit_price END,
        sp.unit_price
    ) AS final_price,
    
    -- Get appropriate shipping based on location and size
    CASE 
        WHEN o.order_weight > 50 AND o.shipping_country = 'USA' 
            THEN freight.shipping_cost
        WHEN o.shipping_country IN ('USA', 'Canada') 
            THEN standard.shipping_cost
        ELSE international.shipping_cost
    END AS shipping_cost

FROM orders o

-- Customer information
JOIN customers c ON o.customer_id = c.customer_id

-- Conditional discount tiers
LEFT JOIN vip_discounts vip 
    ON o.order_total >= 1000
    
LEFT JOIN regular_discounts reg 
    ON o.order_total >= 500 AND o.order_total < 1000
    
LEFT JOIN new_customer_discounts new 
    ON o.order_total < 500

-- Conditional pricing
LEFT JOIN enterprise_pricing ep 
    ON o.product_id = ep.product_id 
    AND c.customer_type = 'Enterprise'
    
LEFT JOIN business_pricing bp 
    ON o.product_id = bp.product_id 
    AND c.customer_type = 'Business'
    
LEFT JOIN standard_pricing sp 
    ON o.product_id = sp.product_id

-- Conditional shipping
LEFT JOIN freight_shipping freight 
    ON o.order_weight > 50 
    AND o.shipping_country = 'USA'
    
LEFT JOIN standard_shipping standard 
    ON o.shipping_country IN ('USA', 'Canada') 
    AND o.order_weight <= 50
    
LEFT JOIN international_shipping international 
    ON o.shipping_country NOT IN ('USA', 'Canada')

WHERE o.order_date >= CURRENT_DATE - 30;
```

***

## **SECTION 4: AGGREGATIONS & GROUPING (10 Questions)**

### **Q31: Explain GROUP BY, ROLLUP, CUBE, and GROUPING SETS.**

**Answer:**
```sql
-- Sample data: sales by region, product, month

-- Basic GROUP BY
SELECT region, product, SUM(sales) AS total_sales
FROM sales
GROUP BY region, product;
-- Result: One row per region-product combination

-- ROLLUP: Hierarchical subtotals (right to left)
SELECT 
    region, 
    product, 
    SUM(sales) AS total_sales,
    GROUPING(region) AS is_region_total,
    GROUPING(product) AS is_product_total
FROM sales
GROUP BY ROLLUP(region, product);
-- Results:
-- 1. region, product (detail)
-- 2. region, NULL (region subtotal)
-- 3. NULL, NULL (grand total)

-- CUBE: All possible combinations
SELECT region, product, SUM(sales) AS total_sales
FROM sales
GROUP BY CUBE(region, product);
-- Results:
-- 1. region, product (detail)
-- 2. region, NULL (region total)
-- 3. NULL, product (product total)
-- 4. NULL, NULL (grand total)

-- GROUPING SETS: Custom aggregation levels
SELECT 
    region, 
    product, 
    month,
    SUM(sales) AS total_sales
FROM sales
GROUP BY GROUPING SETS (
    (region, product),      -- Region-Product level
    (region, month),        -- Region-Month level
    (product),              -- Product level only
    ()                      -- Grand total
);

-- Practical example: Executive dashboard
SELECT 
    COALESCE(region, 'All Regions') AS region,
    COALESCE(product, 'All Products') AS product,
    COALESCE(TO_CHAR(month, 'YYYY-MM'), 'All Months') AS month,
    SUM(sales) AS total_sales,
    COUNT(DISTINCT customer_id) AS unique_customers,
    AVG(sales) AS avg_sale,
    -- Identify aggregation level
    CASE 
        WHEN GROUPING(region) = 0 AND GROUPING(product) = 0 AND GROUPING(month) = 0 
            THEN 'Detail'
        WHEN GROUPING(region) = 0 AND GROUPING(product) = 0 
            THEN 'Region-Product Total'
        WHEN GROUPING(region) = 0 
            THEN 'Region Total'
        ELSE 'Grand Total'
    END AS aggregation_level
FROM sales
GROUP BY CUBE(region, product, month)
ORDER BY 
    GROUPING(region),
    GROUPING(product),
    GROUPING(month),
    region, product, month;
```

***

### **Q32: How do you handle NULL values in aggregations?**

**Answer:**
```sql
-- Problem: NULLs affect different aggregates differently

SELECT 
    category,
    
    -- COUNT(*): Counts all rows including NULLs
    COUNT(*) AS total_rows,
    
    -- COUNT(column): Counts non-NULL values only
    COUNT(discount) AS non_null_discounts,
    COUNT(DISTINCT discount) AS unique_discounts,
    
    -- SUM: Ignores NULLs (SUM of 5, NULL, 10 = 15)
    SUM(sales) AS total_sales,
    
    -- AVG: Average of non-NULL values only
    AVG(discount) AS avg_discount,
    
    -- Replace NULL with 0 for calculation
    AVG(COALESCE(discount, 0)) AS avg_discount_with_zero,
    
    -- Calculate percentage of NULLs
    (COUNT(*) - COUNT(discount)) * 100.0 / COUNT(*) AS pct_missing,
    
    -- Conditional aggregation with NULLs
    SUM(CASE WHEN discount IS NOT NULL THEN sales ELSE 0 END) AS sales_with_discount,
    SUM(CASE WHEN discount IS NULL THEN sales ELSE 0 END) AS sales_without_discount,
    
    -- Handle NULLs in conditional logic
    COUNT(CASE WHEN discount > 0.1 THEN 1 END) AS high_discount_count

FROM products
GROUP BY category;

-- COALESCE for default values in aggregation
SELECT 
    customer_segment,
    AVG(COALESCE(lifetime_value, 0)) AS avg_ltv,
    SUM(COALESCE(total_purchases, 0)) AS total_purchases,
    -- Multiple fallbacks
    AVG(COALESCE(estimated_value, predicted_value, 100)) AS avg_value
FROM customers
GROUP BY customer_segment;

-- NULLIF to handle division by zero
SELECT 
    product_id,
    total_revenue,
    total_units,
    total_revenue / NULLIF(total_units, 0) AS revenue_per_unit,
    -- Safe division with default
    COALESCE(total_revenue / NULLIF(total_units, 0), 0) AS safe_revenue_per_unit
FROM product_sales;
```

***

### **Q33: Implement pivot and unpivot operations.**

**Answer:**
```sql
-- PIVOT: Convert rows to columns
-- Transform monthly sales from rows to columns

SELECT * 
FROM (
    SELECT 
        product_name,
        TO_CHAR(order_month, 'YYYY-MM') AS month,
        sales_amount
    FROM monthly_sales
)
PIVOT (
    SUM(sales_amount)
    FOR month IN ('2025-01', '2025-02', '2025-03', '2025-04')
) AS pivoted_data
ORDER BY product_name;

-- Dynamic PIVOT using conditional aggregation
SELECT 
    product_name,
    SUM(CASE WHEN order_month = '2025-01' THEN sales_amount END) AS jan_2025,
    SUM(CASE WHEN order_month = '2025-02' THEN sales_amount END) AS feb_2025,
    SUM(CASE WHEN order_month = '2025-03' THEN sales_amount END) AS mar_2025,
    SUM(CASE WHEN order_month = '2025-04' THEN sales_amount END) AS apr_2025,
    SUM(sales_amount) AS total
FROM monthly_sales
GROUP BY product_name;

-- UNPIVOT: Convert columns to rows
-- Transform quarter columns back to rows

SELECT * 
FROM quarterly_sales
UNPIVOT (
    sales_amount
    FOR quarter IN (q1, q2, q3, q4)
) AS unpivoted_data;

-- Manual UNPIVOT using UNION ALL
SELECT product_id, 'Q1' AS quarter, q1 AS sales_amount FROM quarterly_sales
UNION ALL
SELECT product_id, 'Q2' AS quarter, q2 AS sales_amount FROM quarterly_sales
UNION ALL
SELECT product_id, 'Q3' AS quarter, q3 AS sales_amount FROM quarterly_sales
UNION ALL
SELECT product_id, 'Q4' AS quarter, q4 AS sales_amount FROM quarterly_sales;

-- Complex pivot: Multiple aggregations
SELECT * 
FROM (
    SELECT region, product_category, sales, units
    FROM sales_data
)
PIVOT (
    SUM(sales) AS total_sales,
    SUM(units) AS total_units,
    AVG(sales/NULLIF(units,0)) AS avg_price
    FOR region IN ('East', 'West', 'North', 'South')
);
```

***

### **Q34: Write complex conditional aggregations.**

**Answer:**
```sql
-- Multiple conditional aggregations in single query
SELECT 
    customer_segment,
    report_month,
    
    -- Basic aggregations
    COUNT(*) AS total_orders,
    SUM(order_amount) AS total_revenue,
    
    -- Conditional counts
    COUNT(CASE WHEN order_amount > 1000 THEN 1 END) AS high_value_orders,
    COUNT(CASE WHEN order_status = 'returned' THEN 1 END) AS returned_orders,
    COUNT(CASE WHEN delivery_days <= 2 THEN 1 END) AS fast_deliveries,
    
    -- Conditional sums
    SUM(CASE WHEN payment_method = 'credit_card' THEN order_amount ELSE 0 END) AS cc_revenue,
    SUM(CASE WHEN is_first_purchase THEN order_amount ELSE 0 END) AS new_customer_revenue,
    SUM(CASE WHEN has_discount THEN discount_amount ELSE 0 END) AS total_discounts,
    
    -- Conditional averages
    AVG(CASE WHEN order_amount > 500 THEN delivery_days END) AS avg_delivery_high_value,
    AVG(CASE WHEN customer_tier = 'Premium' THEN satisfaction_score END) AS premium_satisfaction,
    
    -- Percentage calculations
    COUNT(CASE WHEN order_status = 'returned' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0) AS return_rate,
    
    SUM(CASE WHEN has_discount THEN order_amount ELSE 0 END) * 100.0 / 
        NULLIF(SUM(order_amount), 0) AS pct_revenue_with_discount,
    
    -- Complex conditions
    COUNT(CASE 
        WHEN order_amount > 1000 
        AND customer_tier = 'Premium' 
        AND delivery_days <= 2 
        THEN 1 
    END) AS perfect_orders,
    
    -- Nested CASE in aggregation
    SUM(CASE 
        WHEN order_amount > 2000 THEN order_amount * 0.10
        WHEN order_amount > 1000 THEN order_amount * 0.05
        WHEN order_amount > 500 THEN order_amount * 0.02
        ELSE 0 
    END) AS tiered_commission

FROM orders
GROUP BY customer_segment, report_month;
```

***

### **Q35: Implement HAVING clause with complex conditions.**

**Answer:**
```sql
-- HAVING filters after GROUP BY (WHERE filters before)

-- Basic HAVING
SELECT 
    customer_id,
    COUNT(*) AS order_count,
    SUM(order_amount) AS total_spent
FROM orders
GROUP BY customer_id
HAVING COUNT(*) >= 5  -- Only customers with 5+ orders
   AND SUM(order_amount) > 10000;  -- And spent > $10k

-- HAVING with aggregation conditions
SELECT 
    product_category,
    AVG(price) AS avg_price,
    STDDEV(price) AS price_volatility,
    MAX(price) - MIN(price) AS price_range
FROM products
GROUP BY product_category
HAVING COUNT(*) >= 10  -- Categories with 10+ products
   AND STDDEV(price) > 50  -- High price variation
   AND AVG(price) BETWEEN 100 AND 1000;  -- Moderate avg price

-- HAVING with subqueries
SELECT 
    seller_id,
    COUNT(*) AS total_sales,
    SUM(sales_amount) AS total_revenue
FROM sales
GROUP BY seller_id
HAVING SUM(sales_amount) > (
    SELECT AVG(total_revenue) 
    FROM (
        SELECT seller_id, SUM(sales_amount) AS total_revenue
        FROM sales
        GROUP BY seller_id
    )
);  -- Sellers above average revenue

-- Complex HAVING with multiple conditions
SELECT 
    region,
    product_category,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(order_amount) AS total_revenue,
    AVG(order_amount) AS avg_order_value,
    COUNT(*) AS order_count
FROM orders
WHERE order_date >= DATEADD('month', -3, CURRENT_DATE)
GROUP BY region, product_category
HAVING 
    COUNT(DISTINCT customer_id) >= 100  -- Significant customer base
    AND SUM(order_amount) > 50000  -- Meaningful revenue
    AND AVG(order_amount) > 200  -- Higher value orders
    AND COUNT(*) > 200  -- Sufficient volume
    AND COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () > 1;  -- At least 1% of total orders

-- HAVING with window functions
SELECT 
    category,
    subcategory,
    COUNT(*) AS product_count,
    AVG(price) AS avg_price
FROM products
GROUP BY category, subcategory
HAVING AVG(price) > AVG(AVG(price)) OVER (PARTITION BY category)
-- Subcategories with above-category-average price
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY COUNT(*) DESC) <= 3;
-- Top 3 subcategories by product count per category
```

***

### **Q36: Calculate running aggregations across partitions.**

**Answer:**
```sql
-- Running totals within and across groups
SELECT 
    category,
    subcategory,
    order_date,
    daily_sales,
    
    -- Running total within subcategory
    SUM(daily_sales) OVER (
        PARTITION BY category, subcategory 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS subcategory_running_total,
    
    -- Running total within category (all subcategories)
    SUM(daily_sales) OVER (
        PARTITION BY category 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS category_running_total,
    
    -- Overall running total
    SUM(daily_sales) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS grand_running_total,
    
    -- Running average (30-day)
    AVG(daily_sales) OVER (
        PARTITION BY category
        ORDER BY order_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS category_30day_avg,
    
    -- Running max and min
    MAX(daily_sales) OVER (
        PARTITION BY category
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS category_max_to_date,
    
    -- Percentage of category total (at each point in time)
    daily_sales * 100.0 / SUM(daily_sales) OVER (
        PARTITION BY category
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS pct_of_running_total

FROM sales_by_day
ORDER BY category, subcategory, order_date;

-- Reset running total on condition (e.g., new month)
SELECT 
    order_date,
    daily_sales,
    DATE_TRUNC('month', order_date) AS month,
    
    -- Running total resets each month
    SUM(daily_sales) OVER (
        PARTITION BY DATE_TRUNC('month', order_date)
        ORDER BY order_date
    ) AS mtd_sales,
    
    -- Cumulative for the year
    SUM(daily_sales) OVER (
        PARTITION BY DATE_TRUNC('year', order_date)
        ORDER BY order_date
    ) AS ytd_sales

FROM daily_sales;
```

***

### **Q37: Implement complex string aggregations.**

**Answer:**
```sql
-- LISTAGG for string concatenation
SELECT 
    customer_id,
    LISTAGG(product_name, ', ') 
        WITHIN GROUP (ORDER BY order_date) AS products_purchased,
    
    -- Limit concatenated string length
    LISTAGG(DISTINCT category, ', ') 
        WITHIN GROUP (ORDER BY category) AS categories,
    
    -- With separator and overflow handling (Snowflake specific)
    LISTAGG(product_name, '; ') 
        WITHIN GROUP (ORDER BY order_date)
        OVER (PARTITION BY customer_id) AS all_products
        
FROM orders
GROUP BY customer_id;

-- ARRAY_AGG for array aggregation
SELECT 
    customer_id,
    ARRAY_AGG(order_id) WITHIN GROUP (ORDER BY order_date) AS order_ids,
    ARRAY_AGG(DISTINCT product_category) AS unique_categories,
    
    -- Nested array aggregation
    ARRAY_AGG(OBJECT_CONSTRUCT(
        'order_id', order_id,
        'amount', order_amount,
        'date', order_date
    )) AS order_details

FROM orders
GROUP BY customer_id;

-- String aggregation with conditions
SELECT 
    region,
    
    -- Only high-value products
    LISTAGG(
        CASE WHEN price > 1000 
        THEN product_name 
        END, ', '
    ) WITHIN GROUP (ORDER BY price DESC) AS premium_products,
    
    -- With prefix for each item
    LISTAGG(
        'Order #' || order_id || ': $' || order_amount,
        ' | '
    ) WITHIN GROUP (ORDER BY order_date DESC) AS order_summary

FROM sales
GROUP BY region;

-- Advanced: Create JSON from aggregation
SELECT 
    category,
    OBJECT_CONSTRUCT(
        'total_products', COUNT(*),
        'avg_price', ROUND(AVG(price), 2),
        'product_list', ARRAY_AGG(product_name),
        'price_range', OBJECT_CONSTRUCT(
            'min', MIN(price),
            'max', MAX(price)
        )
    ) AS category_summary
FROM products
GROUP BY category;
```

***

### **Q38: Handle hierarchical aggregations (parent-child).**

**Answer:**
```sql
-- Aggregate with hierarchy levels
WITH category_hierarchy AS (
    SELECT 
        category_id,
        category_name,
        parent_category_id,
        1 AS level
    FROM categories
    WHERE parent_category_id IS NULL
    
    UNION ALL
    
    SELECT 
        c.category_id,
        c.category_name,
        c.parent_category_id,
        ch.level + 1
    FROM categories c
    JOIN category_hierarchy ch ON c.parent_category_id = ch.category_id
),
sales_by_category AS (
    SELECT 
        ch.category_id,
        ch.category_name,
        ch.parent_category_id,
        ch.level,
        SUM(s.sales_amount) AS direct_sales
    FROM category_hierarchy ch
    LEFT JOIN sales s ON ch.category_id = s.category_id
    GROUP BY ch.category_id, ch.category_name, ch.parent_category_id, ch.level
)
SELECT 
    sbc.category_id,
    sbc.category_name,
    sbc.level,
    sbc.direct_sales,
    
    -- Include sales from all child categories
    (
        SELECT SUM(s2.sales_amount)
        FROM sales s2
        JOIN category_hierarchy ch2 ON s2.category_id = ch2.category_id
        WHERE ch2.level >= sbc.level
        START WITH ch2.category_id = sbc.category_id
        CONNECT BY PRIOR ch2.category_id = ch2.parent_category_id
    ) AS total_sales_with_children,
    
    -- Parent category sales
    parent.direct_sales AS parent_sales,
    
    -- Percentage of parent
    sbc.direct_sales * 100.0 / NULLIF(parent.direct_sales, 0) AS pct_of_parent

FROM sales_by_category sbc
LEFT JOIN sales_by_category parent 
    ON sbc.parent_category_id = parent.category_id
ORDER BY sbc.level, sbc.category_name;
```

***

### **Q39: Calculate percentile-based aggregations.**

**Answer:**
```sql
-- Percentiles and quantiles
SELECT 
    product_category,
    
    -- Exact percentiles using PERCENTILE_CONT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY price) AS p90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price) AS p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY price) AS p99,
    
    -- Discrete percentile (actual value from data)
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY price) AS median_discrete,
    
    -- IQR for outlier detection
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) - 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) AS iqr,
    
    -- Standard aggregations for comparison
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS mean_price,
    STDDEV(price) AS stddev_price,
    
    -- Count outliers (beyond 1.5*IQR)
    COUNT(CASE 
        WHEN price < PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) 
                   - 1.5 * (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) 
                          - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price))
          OR price > PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) 
                   + 1.5 * (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) 
                          - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price))
        THEN 1 
    END) AS outlier_count

FROM products
GROUP BY product_category;

-- Window function approach for percentiles
SELECT 
    customer_id,
    total_spent,
    NTILE(100) OVER (ORDER BY total_spent) AS percentile,
    NTILE(10) OVER (ORDER BY total_spent) AS decile,
    NTILE(4) OVER (ORDER BY total_spent) AS quartile,
    
    -- Rank-based percentile
    PERCENT_RANK() OVER (ORDER BY total_spent) * 100 AS exact_percentile,
    
    -- Customer segment based on percentile
    CASE 
        WHEN PERCENT_RANK() OVER (ORDER BY total_spent) >= 0.95 THEN 'Top 5%'
        WHEN PERCENT_RANK() OVER (ORDER BY total_spent) >= 0.75 THEN 'Top 25%'
        WHEN PERCENT_RANK() OVER (ORDER BY total_spent) >= 0.50 THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END AS customer_segment

FROM customer_purchases;
```

***

### **Q40: Implement cross-tabulation with multiple metrics.**

**Answer:**
```sql
-- Complex cross-tab: Metrics by month and category
SELECT 
    product_category,
    
    -- Revenue by quarter
    SUM(CASE WHEN quarter = 1 THEN revenue END) AS q1_revenue,
    SUM(CASE WHEN quarter = 2 THEN revenue END) AS q2_revenue,
    SUM(CASE WHEN quarter = 3 THEN revenue END) AS q3_revenue,
    SUM(CASE WHEN quarter = 4 THEN revenue END) AS q4_revenue,
    
    -- Unit count by quarter
    SUM(CASE WHEN quarter = 1 THEN units_sold END) AS q1_units,
    SUM(CASE WHEN quarter = 2 THEN units_sold END) AS q2_units,
    SUM(CASE WHEN quarter = 3 THEN units_sold END) AS q3_units,
    SUM(CASE WHEN quarter = 4 THEN units_sold END) AS q4_units,
    
    -- Average price by quarter
    AVG(CASE WHEN quarter = 1 THEN avg_price END) AS q1_avg_price,
    AVG(CASE WHEN quarter = 2 THEN avg_price END) AS q2_avg_price,
    AVG(CASE WHEN quarter = 3 THEN avg_price END) AS q3_avg_price,
    AVG(CASE WHEN quarter = 4 THEN avg_price END) AS q4_avg_price,
    
    -- Totals
    SUM(revenue) AS total_revenue,
    SUM(units_sold) AS total_units,
    SUM(revenue) / NULLIF(SUM(units_sold), 0) AS overall_avg_price,
    
    -- Growth rates
    (SUM(CASE WHEN quarter = 4 THEN revenue END) - 
     SUM(CASE WHEN quarter = 1 THEN revenue END)) * 100.0 /
     NULLIF(SUM(CASE WHEN quarter = 1 THEN revenue END), 0) AS yoy_growth_rate

FROM quarterly_sales
GROUP BY product_category
ORDER BY total_revenue DESC;

-- Dynamic cross-tab with regions
SELECT 
    month,
    SUM(CASE WHEN region = 'North' THEN sales END) AS north_sales,
    SUM(CASE WHEN region = 'South' THEN sales END) AS south_sales,
    SUM(CASE WHEN region = 'East' THEN sales END) AS east_sales,
    SUM(CASE WHEN region = 'West' THEN sales END) AS west_sales,
    
    -- Percentage distribution
    SUM(CASE WHEN region = 'North' THEN sales END) * 100.0 / SUM(sales) AS north_pct,
    SUM(CASE WHEN region = 'South' THEN sales END) * 100.0 / SUM(sales) AS south_pct,
    SUM(CASE WHEN region = 'East' THEN sales END) * 100.0 / SUM(sales) AS east_pct,
    SUM(CASE WHEN region = 'West' THEN sales END) * 100.0 / SUM(sales) AS west_pct,
    
    SUM(sales) AS total_sales

FROM regional_sales
GROUP BY month
ORDER BY month;
```

***

## **SECTION 5: OPTIMIZATION & PERFORMANCE (10 Questions)**

### **Q41: How do you optimize query performance in Snowflake?**

**Answer:**
Key optimization strategies:

**1. Clustering:**
```sql
-- Cluster frequently filtered columns
ALTER TABLE large_table CLUSTER BY (date_column, region);
```

**2. Partition Pruning:**
```sql
-- Good: Filters on clustered column
SELECT * FROM orders 
WHERE order_date >= '2025-01-01'  -- Uses partition pruning
AND region = 'Northeast';

-- Bad: Function on filtered column prevents pruning
SELECT * FROM orders 
WHERE DATE_TRUNC('month', order_date) = '2025-01-01';  -- No pruning
```

**3. Minimize Data Scanned:**
```sql
-- Good: Select only needed columns
SELECT customer_id, order_total 
FROM orders WHERE order_date = CURRENT_DATE;

-- Bad: SELECT *
SELECT * FROM orders WHERE order_date = CURRENT_DATE;
```

**4. Use CTEs for Readability:**
```sql
-- CTEs are materialized once
WITH filtered_orders AS (
    SELECT * FROM orders WHERE order_date >= '2025-01-01'
)
SELECT * FROM filtered_orders WHERE amount > 1000
UNION ALL
SELECT * FROM filtered_orders WHERE customer_tier = 'Premium';
```

**5. Warehouse Sizing:**
```sql
-- Right-size warehouse for workload
-- Use XS-S for simple queries, L-XL for complex aggregations

-- Query profile shows:
EXPLAIN SELECT COUNT(*) FROM large_table;  -- Check execution plan
```

**Interview Answer Format:**
*"To optimize Snowflake queries, I focus on:
1. Clustering keys on frequently filtered columns
2. Avoiding functions on filter columns for partition pruning
3. Selecting only required columns
4. Using appropriate warehouse sizes
5. Materializing common subqueries with CTEs
6. Monitoring query profiles for bottlenecks"*

***

### **Q42: Explain the difference between VIEW, MATERIALIZED VIEW, and TABLE.**

**Answer:**
```sql
-- VIEW: Virtual table, no data storage
CREATE VIEW active_customers AS
SELECT customer_id, customer_name, total_orders
FROM customers
WHERE last_order_date >= DATEADD('month', -6, CURRENT_DATE);

-- Pros: Always current, no storage cost
-- Cons: Re-executed every query, can be slow

-- MATERIALIZED VIEW: Stored results, auto-refreshed
CREATE MATERIALIZED VIEW customer_summary AS
SELECT 
    customer_id,
    COUNT(*) AS order_count,
    SUM(order_amount) AS total_spent,
    MAX(order_date) AS last_order_date
FROM orders
GROUP BY customer_id;

-- Pros: Fast queries, automatic refresh
-- Cons: Storage cost, refresh lag, limited in Snowflake

-- TABLE: Physical storage, manual updates
CREATE TABLE customer_metrics AS
SELECT 
    customer_id,
    COUNT(*) AS order_count,
    SUM(order_amount) AS total_spent
FROM orders
GROUP BY customer_id;

-- Pros: Full control, predictable performance
-- Cons: Manual refresh required, data can be stale

/*
Comparison:
┌─────────────────┬──────┬───────────┬───────┐
│ Feature         │ VIEW │ MAT VIEW  │ TABLE │
├─────────────────┼──────┼───────────┼───────┤
│ Storage         │ No   │ Yes       │ Yes   │
│ Data Freshness  │ Real │ Periodic  │ Manual│
│ Query Speed     │ Slow │ Fast      │ Fast  │
│ Maintenance     │ None │ Auto      │ Manual│
│ Use Case        │ Logic│ Aggregates│ Marts │
└─────────────────┴──────┴───────────┴───────┘
*/
```

***

### **Q43: How do you handle slowly changing dimensions efficiently?**

**Answer:**
Refer to Q16 for SCD Type 2 implementation. Here's performance optimization:

```sql
-- Efficient SCD Type 2 with clustering
CREATE TABLE dim_customer (
    customer_key NUMBER AUTOINCREMENT,
    customer_id STRING,
    customer_name STRING,
    email STRING,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
) CLUSTER BY (customer_id, is_current);

-- Index on frequently queried columns
-- Snowflake doesn't have traditional indexes, but clustering helps

-- Efficient current record query
SELECT * FROM dim_customer 
WHERE customer_id = 'C12345' AND is_current = TRUE;
-- Clusters by customer_id ensure minimal scan

-- For analytics engineering: Use dbt snapshots
-- dbt automatically handles SCD Type 2
```

***

### **Q44: Write a query to identify and remove duplicates efficiently.**

**Answer:**
```sql
-- Method 1: Using QUALIFY
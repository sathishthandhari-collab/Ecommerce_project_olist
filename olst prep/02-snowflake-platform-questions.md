# Snowflake Platform Interview Questions for Analytics Engineers

## 50 Comprehensive Snowflake Platform Questions

### Architecture and Core Concepts (1-10)

**Q1: Explain Snowflake's unique multi-cluster, shared-data architecture and how it differs from traditional data warehouses.**

**Answer**: 
Snowflake's architecture consists of three layers:

1. **Storage Layer**: Centralized, auto-scaling cloud storage that's completely separated from compute
2. **Compute Layer**: Virtual warehouses that can scale independently without affecting storage
3. **Services Layer**: Manages metadata, query optimization, security, and transaction management

**Key Differentiators**:
- **Complete separation of compute and storage** - unlike traditional warehouses where they're tightly coupled
- **Automatic concurrency scaling** - multiple clusters can handle concurrent workloads
- **Zero-copy cloning** - instant database/table copies without additional storage costs
- **Time travel** - query historical data without backups

**Analytics Engineering Benefits**:
- Scale compute up/down based on dbt run requirements
- Multiple teams can run analytics workloads simultaneously without interference
- Cost optimization through independent scaling of storage and compute

---

**Q2: How does Snowflake's micro-partition architecture work, and how should an Analytics Engineer optimize for it?**

**Answer**:
**Micro-partitioning Mechanics**:
- Data automatically divided into 50-400MB compressed micro-partitions
- Each partition stores metadata (min/max values, bloom filters)
- Query optimizer uses metadata for partition pruning

**Optimization Strategies**:
```sql
-- 1. Natural clustering on frequently filtered columns
CREATE TABLE sales_optimized (
    order_date DATE,
    customer_id NUMBER,
    region VARCHAR(50),
    sales_amount DECIMAL(10,2)
) 
CLUSTER BY (order_date, region);

-- 2. Proper data loading order
INSERT INTO sales_optimized 
SELECT * FROM source_data 
ORDER BY order_date, region;  -- Load in clustered order

-- 3. Avoid small, frequent inserts
-- Instead, batch load data to maintain clustering
```

**Analytics Engineering Applications**:
- Design dbt models with clustering keys on primary filter columns
- Use `CLUSTER BY` in model configs for frequently queried dimensions
- Monitor clustering depth with `SYSTEM$CLUSTERING_INFORMATION()`

---

**Q3: Explain Snowflake's caching mechanisms and how they impact Analytics Engineering workflows.**

**Answer**:
**Three Caching Levels**:

1. **Result Cache (Global)**:
   - Caches query results for 24 hours
   - Shared across all users and warehouses
   - Invalidated when underlying data changes

2. **Local Disk Cache (per warehouse)**:
   - Caches raw micro-partition data on SSD
   - Persists while warehouse is running
   - Lost when warehouse suspends

3. **Remote Disk Cache (per account)**:
   - Caches frequently accessed data in cloud storage
   - Shared across warehouses
   - Improves cold start performance

**Analytics Engineering Optimizations**:
```sql
-- Leverage result cache for expensive transformations
{{ config(
    materialized='view',
    post_hook="SELECT COUNT(*) FROM {{ this }}"  -- Warm result cache
) }}

-- Design warehouse strategy for cache utilization
-- Use dedicated warehouses for dbt runs to maintain local cache
```

**Best Practices**:
- Keep warehouses running during development for local cache benefits
- Use consistent column ordering to improve cache hit rates
- Schedule related dbt models on same warehouse to maximize cache utilization

---

**Q4: How do you implement and manage Snowflake's Time Travel feature for Analytics Engineering use cases?**

**Answer**:
**Time Travel Configuration**:
```sql
-- Set retention period at different levels
ALTER ACCOUNT SET DATA_RETENTION_TIME_IN_DAYS = 7;  -- Account level
ALTER DATABASE analytics SET DATA_RETENTION_TIME_IN_DAYS = 30;  -- Database level
ALTER TABLE customer_facts SET DATA_RETENTION_TIME_IN_DAYS = 90;  -- Table level

-- Query historical data
SELECT * FROM customer_facts AT(TIMESTAMP => '2024-01-01 00:00:00'::timestamp);
SELECT * FROM customer_facts BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');
```

**Analytics Engineering Applications**:
1. **Data Quality Recovery**:
```sql
-- Recover from bad data loads
CREATE OR REPLACE TABLE customer_facts AS
SELECT * FROM customer_facts AT(TIMESTAMP => 'before_bad_load_timestamp');
```

2. **Historical Analysis**:
```sql
-- Compare data state across time periods
WITH current_state AS (
    SELECT customer_id, total_orders FROM customer_facts
),
past_state AS (
    SELECT customer_id, total_orders FROM customer_facts 
    AT(TIMESTAMP => DATEADD('day', -30, CURRENT_TIMESTAMP()))
)
SELECT 
    c.customer_id,
    c.total_orders AS current_orders,
    p.total_orders AS past_orders,
    c.total_orders - p.total_orders AS order_growth
FROM current_state c
JOIN past_state p ON c.customer_id = p.customer_id;
```

3. **dbt Model Rollback**:
```sql
-- Rollback dbt model to previous state
dbt run-operation rollback_model --args '{model: customer_facts, timestamp: "2024-01-01"}'
```

---

**Q5: Describe Snowflake's security model and how to implement proper access controls for Analytics Engineering teams.**

**Answer**:
**Security Architecture Components**:

1. **Role-Based Access Control (RBAC)**:
```sql
-- Create hierarchical role structure
CREATE ROLE analytics_engineer;
CREATE ROLE analytics_senior;
CREATE ROLE analytics_admin;

-- Grant role hierarchy
GRANT ROLE analytics_engineer TO ROLE analytics_senior;
GRANT ROLE analytics_senior TO ROLE analytics_admin;

-- Grant warehouse privileges
GRANT USAGE, OPERATE ON WAREHOUSE dbt_warehouse TO ROLE analytics_engineer;
GRANT MONITOR ON WAREHOUSE dbt_warehouse TO ROLE analytics_senior;
```

2. **Database and Schema Permissions**:
```sql
-- Production read access for analytics engineers
GRANT USAGE ON DATABASE production TO ROLE analytics_engineer;
GRANT USAGE ON SCHEMA production.marts TO ROLE analytics_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA production.marts TO ROLE analytics_engineer;
GRANT SELECT ON FUTURE TABLES IN SCHEMA production.marts TO ROLE analytics_engineer;

-- Development full access
GRANT ALL PRIVILEGES ON DATABASE dev_analytics TO ROLE analytics_engineer;
```

3. **Column-Level Security**:
```sql
-- Implement masking policies for PII
CREATE MASKING POLICY email_mask AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ANALYTICS_ADMIN') THEN val
    WHEN CURRENT_ROLE() IN ('ANALYTICS_SENIOR') THEN REGEXP_REPLACE(val, '.{1,}(@.{1,})', '***\\1')
    ELSE '***@***.com'
  END;

ALTER TABLE customers MODIFY COLUMN email SET MASKING POLICY email_mask;
```

**Analytics Engineering Best Practices**:
- Use service accounts for dbt Cloud/CI-CD with minimal required privileges
- Implement separate roles for development, staging, and production
- Use masking policies for sensitive data in development environments
- Audit access patterns with `SNOWFLAKE.ACCOUNT_USAGE` views

---

### Warehousing and Compute (11-20)

**Q6: How do you design an optimal warehouse strategy for different Analytics Engineering workloads?**

**Answer**:
**Warehouse Sizing Strategy**:

1. **Development Warehouses** (X-Small to Small):
```sql
CREATE WAREHOUSE dbt_dev WITH
    warehouse_size = 'X-SMALL'
    auto_suspend = 60  -- 1 minute
    auto_resume = TRUE
    comment = 'Development and testing workloads';
```

2. **Production ETL** (Medium to Large):
```sql
CREATE WAREHOUSE dbt_prod WITH 
    warehouse_size = 'LARGE'
    auto_suspend = 300  -- 5 minutes
    auto_resume = TRUE
    max_cluster_count = 3
    min_cluster_count = 1
    scaling_policy = 'STANDARD'
    comment = 'Production dbt runs';
```

3. **Ad-hoc Analytics** (Small with auto-scaling):
```sql
CREATE WAREHOUSE analytics_adhoc WITH
    warehouse_size = 'SMALL'
    auto_suspend = 60
    auto_resume = TRUE  
    max_cluster_count = 10
    min_cluster_count = 1
    scaling_policy = 'ECONOMY'
    comment = 'Analyst queries and dashboards';
```

**Workload-Specific Optimization**:
- **dbt Incremental Models**: Medium warehouse with longer auto-suspend
- **dbt Full Refresh**: Large warehouse with multi-cluster scaling  
- **Dashboard Queries**: Small warehouse with aggressive auto-suspend
- **Data Science**: X-Large single cluster for memory-intensive workloads

**Cost Optimization**:
```sql
-- Monitor warehouse usage
SELECT 
    warehouse_name,
    AVG(avg_running) AS avg_utilization,
    SUM(credits_used) AS total_credits,
    COUNT(*) AS query_count
FROM snowflake.account_usage.warehouse_load_history wlh
JOIN snowflake.account_usage.query_history qh 
    ON wlh.warehouse_name = qh.warehouse_name
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;
```

---

**Q7: Explain multi-cluster warehouses and when to use them in Analytics Engineering contexts.**

**Answer**:
**Multi-Cluster Architecture**:
```sql
CREATE WAREHOUSE analytics_scaling WITH
    warehouse_size = 'MEDIUM'
    max_cluster_count = 5      -- Maximum clusters
    min_cluster_count = 1      -- Always-on clusters  
    scaling_policy = 'STANDARD'  -- or 'ECONOMY'
    auto_suspend = 300
    auto_resume = TRUE;
```

**Scaling Policies**:
1. **Standard**: Immediately starts additional clusters when queued
2. **Economy**: Waits to see if running queries finish before scaling

**Analytics Engineering Use Cases**:

1. **Concurrent dbt Development**:
   - Multiple team members running dbt simultaneously
   - Each gets dedicated compute resources
   - Prevents job queuing during development

2. **Mixed Workload Support**:
   - dbt production runs + dashboard queries
   - BI tools accessing data during ETL processes
   - Ad-hoc analysis during scheduled data processing

3. **Peak Hour Scaling**:
```sql
-- Schedule-based scaling for business hours
ALTER WAREHOUSE analytics_scaling SET 
    min_cluster_count = 3     -- Scale up for business hours
    max_cluster_count = 8;

-- Off-hours scaling  
ALTER WAREHOUSE analytics_scaling SET
    min_cluster_count = 1     -- Scale down for off-hours
    max_cluster_count = 3;
```

**Cost vs. Performance Trade-offs**:
- **Economy Policy**: Lower costs, potential query queueing
- **Standard Policy**: Higher costs, better performance consistency
- **Min Cluster Count**: Guaranteed availability vs. cost optimization

**Monitoring Multi-Cluster Usage**:
```sql
SELECT 
    warehouse_name,
    cluster_number,
    AVG(avg_running) AS cluster_utilization,
    SUM(credits_used) AS cluster_credits
FROM snowflake.account_usage.warehouse_load_history
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name, cluster_number
ORDER BY warehouse_name, cluster_number;
```

---

**Q8: How do you implement effective resource monitoring and cost management for Snowflake warehouses?**

**Answer**:
**Comprehensive Monitoring Framework**:

1. **Real-time Resource Monitoring**:
```sql
-- Warehouse utilization dashboard
CREATE VIEW warehouse_monitoring AS
SELECT 
    wlh.warehouse_name,
    wlh.start_time,
    wlh.end_time,
    wlh.warehouse_size,
    wlh.cluster_number,
    wlh.avg_running,
    wlh.avg_queued_load,
    wlh.credits_used,
    
    -- Query context
    COUNT(qh.query_id) AS concurrent_queries,
    AVG(qh.execution_time_ms) AS avg_query_time,
    SUM(CASE WHEN qh.error_code IS NOT NULL THEN 1 ELSE 0 END) AS failed_queries
FROM snowflake.account_usage.warehouse_load_history wlh
LEFT JOIN snowflake.account_usage.query_history qh
    ON wlh.warehouse_name = qh.warehouse_name
    AND qh.start_time BETWEEN wlh.start_time AND wlh.end_time
WHERE wlh.start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY 1,2,3,4,5,6,7,8;
```

2. **Cost Analysis and Alerting**:
```sql
-- Daily cost analysis with alerts
WITH daily_costs AS (
    SELECT 
        DATE(start_time) AS usage_date,
        warehouse_name,
        warehouse_size,
        SUM(credits_used) AS daily_credits,
        
        -- Calculate cost trends
        LAG(SUM(credits_used), 1) OVER (
            PARTITION BY warehouse_name 
            ORDER BY DATE(start_time)
        ) AS prev_day_credits,
        
        -- Cost efficiency metrics
        SUM(credits_used) / COUNT(DISTINCT query_id) AS credits_per_query,
        AVG(execution_time_ms) / 1000 / 60 AS avg_query_minutes
    FROM snowflake.account_usage.warehouse_load_history wlh
    JOIN snowflake.account_usage.query_history qh USING (warehouse_name)
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY 1,2,3
)
SELECT 
    *,
    daily_credits - prev_day_credits AS credit_change,
    CASE 
        WHEN daily_credits > prev_day_credits * 1.5 THEN 'COST_SPIKE_ALERT'
        WHEN credits_per_query > 0.1 THEN 'INEFFICIENCY_ALERT'
        WHEN avg_query_minutes > 30 THEN 'PERFORMANCE_ALERT'
        ELSE 'NORMAL'
    END AS alert_status
FROM daily_costs
WHERE alert_status != 'NORMAL';
```

3. **Automated Resource Management**:
```sql
-- Stored procedure for dynamic warehouse sizing
CREATE OR REPLACE PROCEDURE optimize_warehouse_size(warehouse_name STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
DECLARE
    avg_utilization FLOAT;
    avg_queue_time FLOAT;
    current_size STRING;
    recommended_size STRING;
BEGIN
    -- Get current utilization metrics
    SELECT 
        AVG(avg_running),
        AVG(avg_queued_load),
        warehouse_size
    INTO avg_utilization, avg_queue_time, current_size
    FROM snowflake.account_usage.warehouse_load_history
    WHERE warehouse_name = :warehouse_name
      AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP());
    
    -- Determine optimal size
    IF avg_utilization < 30 AND current_size IN ('LARGE', 'X-LARGE') THEN
        recommended_size := CASE current_size 
            WHEN 'X-LARGE' THEN 'LARGE'
            WHEN 'LARGE' THEN 'MEDIUM'
        END;
    ELSIF avg_queue_time > 10 AND current_size IN ('X-SMALL', 'SMALL', 'MEDIUM') THEN
        recommended_size := CASE current_size
            WHEN 'X-SMALL' THEN 'SMALL'
            WHEN 'SMALL' THEN 'MEDIUM'
            WHEN 'MEDIUM' THEN 'LARGE'
        END;
    ELSE
        recommended_size := current_size;
    END IF;
    
    -- Apply recommendation if different
    IF recommended_size != current_size THEN
        EXECUTE IMMEDIATE 'ALTER WAREHOUSE ' || warehouse_name || 
                         ' SET warehouse_size = ''' || recommended_size || '''';
        RETURN 'Warehouse ' || warehouse_name || ' resized from ' || 
               current_size || ' to ' || recommended_size;
    ELSE
        RETURN 'Warehouse ' || warehouse_name || ' size is optimal';
    END IF;
END;
$$;

-- Schedule optimization checks
CREATE TASK optimize_warehouses
    warehouse = 'COMPUTE_WH'
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'  -- Daily at 6 AM
AS
    CALL optimize_warehouse_size('DBT_PROD');
    CALL optimize_warehouse_size('ANALYTICS_ADHOC');
```

4. **Cost Budgeting and Governance**:
```sql
-- Resource governor for cost control
CREATE RESOURCE MONITOR analytics_budget WITH
    credit_quota = 1000                    -- Monthly budget
    frequency = 'MONTHLY'
    start_timestamp = '2024-01-01 00:00 PST'
    triggers = [
        (80, 'NOTIFY'),                   -- 80% notification
        (95, 'SUSPEND_IMMEDIATE'),        -- 95% suspend immediately  
        (100, 'SUSPEND_IMMEDIATE')        -- 100% hard stop
    ];

-- Apply to warehouses
ALTER WAREHOUSE dbt_prod SET RESOURCE_MONITOR = analytics_budget;
ALTER WAREHOUSE analytics_adhoc SET RESOURCE_MONITOR = analytics_budget;
```

---

### Data Loading and Integration (21-30)

**Q9: Design a comprehensive data loading strategy using Snowflake's native features for Analytics Engineering pipelines.**

**Answer**:
**Multi-Stage Loading Architecture**:

1. **External Stage Setup**:
```sql
-- S3 integration for raw data
CREATE STORAGE INTEGRATION s3_analytics_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://analytics-raw-data/');

-- Create external stages for different data sources
CREATE STAGE raw_orders_stage
    STORAGE_INTEGRATION = s3_analytics_integration
    URL = 's3://analytics-raw-data/orders/'
    FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'GZIP');

CREATE STAGE raw_customers_stage  
    STORAGE_INTEGRATION = s3_analytics_integration
    URL = 's3://analytics-raw-data/customers/'
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
```

2. **Intelligent File Format Detection**:
```sql
-- Dynamic file format handling
CREATE OR REPLACE PROCEDURE load_dynamic_files(stage_name STRING, table_name STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
DECLARE
    file_list RESULTSET;
    file_name STRING;
    file_format STRING;
    load_command STRING;
BEGIN
    file_list := (SELECT file_name FROM TABLE(RESULT_SCAN(
        'LIST @' || stage_name
    )));
    
    FOR record IN file_list DO
        file_name := record.file_name;
        
        -- Determine file format
        IF ENDSWITH(file_name, '.json.gz') OR ENDSWITH(file_name, '.json') THEN
            file_format := '(TYPE = JSON COMPRESSION = AUTO)';
        ELSIF ENDSWITH(file_name, '.csv.gz') OR ENDSWITH(file_name, '.csv') THEN  
            file_format := '(TYPE = CSV FIELD_DELIMITER = \',\' SKIP_HEADER = 1 COMPRESSION = AUTO)';
        ELSIF ENDSWITH(file_name, '.parquet') THEN
            file_format := '(TYPE = PARQUET COMPRESSION = AUTO)';
        ELSE
            CONTINUE;  -- Skip unsupported formats
        END IF;
        
        -- Execute load
        load_command := 'COPY INTO ' || table_name || 
                       ' FROM @' || stage_name || '/' || file_name ||
                       ' FILE_FORMAT = ' || file_format ||
                       ' ON_ERROR = CONTINUE';
        EXECUTE IMMEDIATE load_command;
    END FOR;
    
    RETURN 'Loaded files from ' || stage_name || ' to ' || table_name;
END;
$$;
```

3. **Incremental Loading with Change Data Capture**:
```sql
-- Stream-based incremental loading
CREATE OR REPLACE STREAM orders_changes ON TABLE raw_orders;

-- Incremental load procedure
CREATE OR REPLACE PROCEDURE process_incremental_orders()
RETURNS STRING  
LANGUAGE SQL
AS $$
BEGIN
    -- Process stream changes
    MERGE INTO staging_orders tgt
    USING (
        SELECT 
            order_id,
            customer_id, 
            order_date,
            order_amount,
            METADATA$ACTION AS change_type,
            METADATA$ISUPDATE AS is_update
        FROM orders_changes
    ) src ON tgt.order_id = src.order_id
    
    WHEN MATCHED AND src.change_type = 'DELETE' THEN DELETE
    
    WHEN MATCHED AND src.change_type = 'INSERT' AND src.is_update THEN UPDATE SET
        customer_id = src.customer_id,
        order_date = src.order_date, 
        order_amount = src.order_amount,
        updated_at = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED AND src.change_type = 'INSERT' THEN INSERT
        (order_id, customer_id, order_date, order_amount, created_at)
        VALUES (src.order_id, src.customer_id, src.order_date, src.order_amount, CURRENT_TIMESTAMP());
        
    RETURN 'Processed ' || SQLROWCOUNT || ' order changes';
END;
$$;

-- Schedule incremental processing
CREATE TASK process_orders_incremental
    warehouse = 'LOAD_WH'
    SCHEDULE = '10 minute'
AS
    CALL process_incremental_orders();
```

4. **Data Quality and Validation During Load**:
```sql
-- Data quality validation during COPY
COPY INTO staging_orders
FROM (
    SELECT 
        $1:order_id::VARCHAR AS order_id,
        $1:customer_id::VARCHAR AS customer_id,
        $1:order_date::DATE AS order_date,
        $1:order_amount::DECIMAL(10,2) AS order_amount,
        
        -- Data quality flags
        CASE 
            WHEN $1:order_id IS NULL THEN 'MISSING_ORDER_ID'
            WHEN $1:order_amount <= 0 THEN 'INVALID_AMOUNT'
            WHEN $1:order_date > CURRENT_DATE() THEN 'FUTURE_DATE'
            ELSE NULL
        END AS quality_issue,
        
        -- Source metadata
        METADATA$FILENAME AS source_file,
        METADATA$FILE_ROW_NUMBER AS source_row,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM @raw_orders_stage
)
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = CONTINUE;

-- Separate good and bad records
CREATE VIEW clean_orders AS
SELECT * FROM staging_orders WHERE quality_issue IS NULL;

CREATE VIEW rejected_orders AS  
SELECT * FROM staging_orders WHERE quality_issue IS NOT NULL;
```

---

**Q10: How do you implement effective data sharing and collaboration patterns in Snowflake for Analytics Engineering teams?**

**Answer**:
**Data Sharing Architecture**:

1. **Secure Data Sharing Setup**:
```sql
-- Create share for external consumption
CREATE SHARE analytics_share COMMENT = 'Analytics data for business users';

-- Grant database access to share
GRANT USAGE ON DATABASE analytics_prod TO SHARE analytics_share;
GRANT USAGE ON SCHEMA analytics_prod.marts TO SHARE analytics_share;

-- Grant specific tables/views
GRANT SELECT ON analytics_prod.marts.customer_metrics TO SHARE analytics_share;
GRANT SELECT ON analytics_prod.marts.sales_summary TO SHARE analytics_share;

-- Create secure views for row-level security
CREATE SECURE VIEW customer_metrics_filtered AS
SELECT *
FROM analytics_prod.marts.customer_metrics
WHERE region = CURRENT_ROLE_CONTEXT('REGION');

GRANT SELECT ON customer_metrics_filtered TO SHARE analytics_share;

-- Add consumers to share
ALTER SHARE analytics_share ADD ACCOUNTS = ('ORG123.ACCOUNT456');
```

2. **Cross-Team Data Marketplace**:
```sql
-- Create data marketplace catalog
CREATE DATABASE data_marketplace;
CREATE SCHEMA data_marketplace.catalog;

-- Metadata table for discoverable datasets
CREATE TABLE data_marketplace.catalog.dataset_registry (
    dataset_name STRING,
    dataset_description STRING,
    owner_team STRING,
    contact_email STRING,
    schema_location STRING,
    update_frequency STRING,
    data_classification STRING,  -- PUBLIC, INTERNAL, CONFIDENTIAL
    last_updated TIMESTAMP,
    quality_score DECIMAL(3,2),
    sample_query STRING
);

-- Register datasets
INSERT INTO data_marketplace.catalog.dataset_registry VALUES
('customer_360', 'Complete customer analytics view', 'Analytics Engineering', 
 'analytics@company.com', 'ANALYTICS_PROD.MARTS.CUSTOMER_360', 'DAILY', 'INTERNAL',
 CURRENT_TIMESTAMP(), 0.95, 'SELECT * FROM customer_360 LIMIT 10'),
('product_performance', 'Product sales and performance metrics', 'Product Analytics',
 'product@company.com', 'ANALYTICS_PROD.MARTS.PRODUCT_METRICS', 'HOURLY', 'INTERNAL',
 CURRENT_TIMESTAMP(), 0.92, 'SELECT * FROM product_performance WHERE product_category = ''Electronics''');
```

3. **Automated Data Quality Monitoring for Shared Data**:
```sql
-- Data quality monitoring for shared datasets
CREATE OR REPLACE PROCEDURE monitor_shared_data_quality()
RETURNS STRING
LANGUAGE SQL  
AS $$
DECLARE
    dataset_cursor CURSOR FOR 
        SELECT dataset_name, schema_location 
        FROM data_marketplace.catalog.dataset_registry;
    quality_score DECIMAL(3,2);
    dataset_name STRING;
    schema_location STRING;
BEGIN
    FOR record IN dataset_cursor DO
        dataset_name := record.dataset_name;
        schema_location := record.schema_location;
        
        -- Calculate quality score (simplified example)
        EXECUTE IMMEDIATE 
            'SELECT 
                (COUNT(*) - COUNT(CASE WHEN key_column IS NULL THEN 1 END)) / 
                COUNT(*) * 100 
             FROM ' || schema_location
        INTO quality_score;
        
        -- Update registry
        UPDATE data_marketplace.catalog.dataset_registry 
        SET 
            quality_score = quality_score,
            last_updated = CURRENT_TIMESTAMP()
        WHERE dataset_name = dataset_name;
        
        -- Alert if quality drops
        IF quality_score < 0.90 THEN
            INSERT INTO data_marketplace.alerts.quality_alerts 
            VALUES (dataset_name, quality_score, CURRENT_TIMESTAMP());
        END IF;
    END FOR;
    
    RETURN 'Quality monitoring completed';
END;
$$;
```

4. **Cross-Database Analytics with Reader Accounts**:
```sql
-- Create reader account for external partners
CREATE MANAGED ACCOUNT partner_analytics
    ADMIN_NAME = 'partner_admin'
    ADMIN_PASSWORD = 'secure_password'
    TYPE = READER;

-- Set up data sharing with reader account
ALTER SHARE analytics_share 
ADD ACCOUNTS = ('PARTNER_ANALYTICS')
SHARE_RESTRICTIONS = FALSE;

-- Monitor reader account usage
SELECT 
    reader_account_name,
    database_name,
    schema_name,
    table_name,
    query_count,
    credits_used
FROM snowflake.reader_account_usage.query_history
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1,2,3,4
ORDER BY credits_used DESC;
```

---

### Advanced Features and Optimization (31-40)

**Q11: How do you implement and optimize Snowflake's Search Optimization Service for Analytics Engineering use cases?**

**Answer**:
**Search Optimization Implementation**:

1. **Enable and Configure Search Optimization**:
```sql
-- Enable at table level for frequently queried dimensions
ALTER TABLE customer_dimension 
ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id, email);

ALTER TABLE product_catalog
ADD SEARCH OPTIMIZATION ON EQUALITY(product_sku, product_name), 
    SUBSTRING(product_description);

-- Enable for fact tables with common filter patterns
ALTER TABLE sales_fact
ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id, product_id, sales_region);
```

2. **Analytics Engineering Query Patterns**:
```sql
-- Point lookups (optimized by equality search)
SELECT * FROM customer_dimension 
WHERE customer_id = 'CUST123456';  -- Fast lookup

SELECT * FROM product_catalog
WHERE product_sku IN ('SKU001', 'SKU002', 'SKU003');  -- Multi-value lookup

-- Substring searches (optimized by substring search)  
SELECT * FROM product_catalog
WHERE CONTAINS(product_description, 'wireless bluetooth');  -- Text search

-- Semi-structured data searches
SELECT * FROM events_table
WHERE event_data:user_id::STRING = 'USER789';  -- JSON path search
```

3. **Cost-Benefit Analysis and Monitoring**:
```sql
-- Monitor search optimization usage and costs
SELECT 
    table_name,
    search_optimization_bytes,
    search_optimization_credits_used,
    -- Cost per MB of search optimization
    search_optimization_credits_used / (search_optimization_bytes / 1024 / 1024) AS cost_per_mb
FROM snowflake.account_usage.search_optimization_history
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY search_optimization_credits_used DESC;

-- Query performance improvement analysis
WITH search_opt_queries AS (
    SELECT 
        query_text,
        execution_time_ms,
        rows_examined,
        CASE WHEN query_text ILIKE '%customer_id%=%' THEN 'POINT_LOOKUP'
             WHEN query_text ILIKE '%CONTAINS%' THEN 'TEXT_SEARCH' 
             ELSE 'OTHER' END AS query_type
    FROM snowflake.account_usage.query_history
    WHERE warehouse_name = 'ANALYTICS_WH'
      AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
)
SELECT 
    query_type,
    COUNT(*) AS query_count,
    AVG(execution_time_ms) AS avg_execution_time,
    AVG(rows_examined) AS avg_rows_examined,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time_ms) AS p95_execution_time
FROM search_opt_queries
GROUP BY query_type;
```

---

**Q12: Explain Snowflake's Result Set Caching and how to leverage it effectively in dbt workflows.**

**Answer**:
**Result Set Cache Mechanics**:

1. **Cache Behavior Understanding**:
```sql
-- Identical queries leverage result cache (24-hour TTL)
SELECT customer_id, total_orders, last_order_date
FROM customer_metrics 
WHERE region = 'NORTH_AMERICA';

-- Even slight differences bypass cache
SELECT customer_id, total_orders, last_order_date  
FROM customer_metrics 
WHERE region = 'NORTH_AMERICA'
ORDER BY customer_id;  -- Different query, no cache hit
```

2. **dbt Model Optimization for Caching**:
```sql
-- dbt model with cache-friendly design
{{ config(
    materialized='view',
    post_hook=[
        "SELECT COUNT(*) FROM {{ this }}",  -- Warm cache after creation
        "SELECT region, COUNT(*) FROM {{ this }} GROUP BY region"  -- Common aggregation
    ]
) }}

WITH standardized_customer_metrics AS (
    SELECT 
        customer_id,
        region,
        total_orders,
        total_revenue,
        last_order_date
    FROM {{ ref('int_customer_360') }}
    -- Consistent column ordering for cache efficiency
    ORDER BY customer_id  -- Standardized ordering
)
SELECT * FROM standardized_customer_metrics
```

3. **Cache Warming Strategies**:
```sql
-- Create cache warming procedure for common queries
CREATE OR REPLACE PROCEDURE warm_analytics_cache()
RETURNS STRING
LANGUAGE SQL
AS $$
BEGIN
    -- Common dashboard queries
    SELECT COUNT(*) FROM customer_metrics;
    SELECT region, COUNT(*) FROM customer_metrics GROUP BY region;
    SELECT DATE_TRUNC('month', last_order_date), COUNT(*) 
    FROM customer_metrics GROUP BY 1;
    
    -- Frequent drill-down patterns
    SELECT customer_segment, AVG(total_revenue) 
    FROM customer_metrics GROUP BY customer_segment;
    
    RETURN 'Cache warming completed';
END;
$$;

-- Schedule cache warming before business hours
CREATE TASK warm_cache_task
    warehouse = 'COMPUTE_WH'
    SCHEDULE = 'USING CRON 0 7 * * * America/New_York'  -- 7 AM daily
AS
    CALL warm_analytics_cache();
```

4. **Cache Invalidation Awareness in dbt**:
```sql
-- Monitor cache hit rates for dbt models
WITH cache_analysis AS (
    SELECT 
        query_text,
        execution_time_ms,
        bytes_scanned,
        CASE WHEN bytes_scanned = 0 THEN 'CACHE_HIT' ELSE 'CACHE_MISS' END AS cache_status
    FROM snowflake.account_usage.query_history  
    WHERE query_text ILIKE '%customer_metrics%'
      AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
)
SELECT 
    cache_status,
    COUNT(*) AS query_count,
    AVG(execution_time_ms) AS avg_execution_time,
    COUNT(*) / SUM(COUNT(*)) OVER () * 100 AS cache_hit_percentage
FROM cache_analysis
GROUP BY cache_status;

-- dbt macro to optimize for cache hits
{% macro generate_cache_friendly_query(base_query) %}
    {# Standardize query format for better cache utilization #}
    {% set standardized = base_query 
        | replace('\n', ' ') 
        | replace('\t', ' ')
        | regex_replace(r'\s+', ' ') %}
    {{ standardized }}
{% endmacro %}
```

---

### Data Governance and Compliance (41-50)

**Q13: How do you implement comprehensive data lineage and impact analysis in Snowflake for Analytics Engineering governance?**

**Answer**:
**Data Lineage Implementation**:

1. **Automated Lineage Tracking**:
```sql
-- Create lineage metadata tables
CREATE TABLE data_governance.lineage.table_dependencies (
    parent_table STRING,
    child_table STRING,
    dependency_type STRING,  -- 'SOURCE', 'TRANSFORM', 'AGGREGATE'
    created_by STRING,
    created_at TIMESTAMP,
    query_text STRING
);

-- Stored procedure to extract lineage from query history
CREATE OR REPLACE PROCEDURE extract_lineage_from_queries()
RETURNS STRING
LANGUAGE PYTHON  
RUNTIME_VERSION = '3.8'
PACKAGES = ('sqlparse')
HANDLER = 'extract_lineage'
AS $$
import sqlparse
import re
from typing import List, Tuple

def extract_tables_from_query(query: str) -> Tuple[List[str], List[str]]:
    """Extract source and target tables from SQL query"""
    parsed = sqlparse.parse(query)[0]
    
    # Extract FROM/JOIN tables (sources)
    from_pattern = r'FROM\s+([^\s,\(\)]+)|JOIN\s+([^\s,\(\)]+)'
    sources = re.findall(from_pattern, query, re.IGNORECASE)
    source_tables = [table for match in sources for table in match if table]
    
    # Extract INSERT INTO/CREATE TABLE (targets)  
    target_pattern = r'(?:INSERT\s+INTO\s+|CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+)([^\s,\(\)]+)'
    targets = re.findall(target_pattern, query, re.IGNORECASE)
    
    return source_tables, targets

def extract_lineage(session):
    # Get recent DDL/DML queries
    query_result = session.sql("""
        SELECT query_text, user_name, start_time
        FROM snowflake.account_usage.query_history  
        WHERE query_type IN ('INSERT', 'CREATE_TABLE', 'CREATE_VIEW')
          AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    """).collect()
    
    lineage_records = []
    for row in query_result:
        query_text = row['QUERY_TEXT']
        user_name = row['USER_NAME']  
        start_time = row['START_TIME']
        
        sources, targets = extract_tables_from_query(query_text)
        
        # Create lineage records
        for target in targets:
            for source in sources:
                lineage_records.append({
                    'parent_table': source,
                    'child_table': target, 
                    'created_by': user_name,
                    'created_at': start_time,
                    'query_text': query_text
                })
    
    # Insert lineage records
    if lineage_records:
        session.write_pandas(
            pd.DataFrame(lineage_records),
            'data_governance.lineage.table_dependencies',
            auto_create_table=True
        )
    
    return f"Extracted {len(lineage_records)} lineage relationships"
$$;
```

2. **Impact Analysis Framework**:
```sql
-- Recursive CTE for downstream impact analysis
WITH RECURSIVE downstream_impact AS (
    -- Base case: direct dependencies
    SELECT 
        parent_table,
        child_table,
        1 AS depth,
        child_table AS impact_path
    FROM data_governance.lineage.table_dependencies
    WHERE parent_table = 'ANALYTICS.STAGING.CUSTOMER_DATA'  -- Source of change
    
    UNION ALL
    
    -- Recursive case: transitive dependencies
    SELECT 
        di.parent_table,
        td.child_table,
        di.depth + 1,
        di.impact_path || ' -> ' || td.child_table
    FROM downstream_impact di
    JOIN data_governance.lineage.table_dependencies td 
        ON di.child_table = td.parent_table
    WHERE di.depth < 10  -- Prevent infinite recursion
),

impact_summary AS (
    SELECT 
        child_table AS impacted_table,
        MIN(depth) AS min_depth,
        COUNT(DISTINCT impact_path) AS impact_paths,
        
        -- Classify impact severity
        CASE 
            WHEN MIN(depth) = 1 THEN 'DIRECT_IMPACT'
            WHEN MIN(depth) <= 3 THEN 'HIGH_IMPACT'  
            WHEN MIN(depth) <= 5 THEN 'MEDIUM_IMPACT'
            ELSE 'LOW_IMPACT'
        END AS impact_severity,
        
        -- Get table metadata
        (SELECT row_count FROM information_schema.tables 
         WHERE table_name = child_table) AS affected_rows,
         
        -- Find downstream consumers
        (SELECT LISTAGG(DISTINCT role_name, ', ')
         FROM snowflake.account_usage.access_history ah
         WHERE ah.object_name = child_table
           AND query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        ) AS recent_users
    FROM downstream_impact
    GROUP BY child_table
)
SELECT * FROM impact_summary
ORDER BY 
    CASE impact_severity
        WHEN 'DIRECT_IMPACT' THEN 1
        WHEN 'HIGH_IMPACT' THEN 2
        WHEN 'MEDIUM_IMPACT' THEN 3 
        ELSE 4
    END,
    affected_rows DESC;
```

3. **dbt Integration for Lineage**:
```sql
-- dbt macro to register lineage metadata
{% macro register_model_lineage() %}
    {% if execute %}
        {% set model_name = this %}
        {% set depends_on = model.depends_on.nodes %}
        
        {% for dependency in depends_on %}
            INSERT INTO data_governance.lineage.dbt_lineage 
            VALUES (
                '{{ dependency }}',
                '{{ model_name }}',
                '{{ invocation_id }}',
                CURRENT_TIMESTAMP(),
                '{{ model.raw_sql | replace("'", "''") }}'
            );
        {% endfor %}
    {% endif %}
{% endmacro %}

-- Apply lineage tracking to models
{{ config(
    post_hook="{{ register_model_lineage() }}"
) }}
```

---

**Q14: Design a comprehensive data classification and masking strategy for Snowflake in an Analytics Engineering context.**

**Answer**:
**Data Classification Framework**:

1. **Automated Data Discovery and Classification**:
```sql
-- Data classification scanner
CREATE OR REPLACE PROCEDURE classify_sensitive_data()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('re')
HANDLER = 'classify_data'
AS $$
import re

def classify_data(session):
    # Pattern-based classification rules
    classification_rules = {
        'EMAIL': r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$',
        'PHONE': r'^\+?[1-9]\d{1,14}$',
        'SSN': r'^\d{3}-?\d{2}-?\d{4}$',
        'CREDIT_CARD': r'^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}$',
        'IP_ADDRESS': r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'
    }
    
    # Get all string/varchar columns
    columns_query = """
        SELECT 
            table_schema,
            table_name, 
            column_name,
            data_type
        FROM information_schema.columns
        WHERE data_type IN ('VARCHAR', 'STRING', 'TEXT')
          AND table_schema IN ('STAGING', 'INTERMEDIATE', 'MARTS')
    """
    
    columns = session.sql(columns_query).collect()
    classifications = []
    
    for column in columns:
        schema = column['TABLE_SCHEMA']
        table = column['TABLE_NAME'] 
        column_name = column['COLUMN_NAME']
        
        # Sample column data
        sample_query = f"""
            SELECT {column_name} 
            FROM {schema}.{table} 
            WHERE {column_name} IS NOT NULL 
            LIMIT 100
        """
        
        try:
            samples = session.sql(sample_query).collect()
            
            # Classify based on patterns
            for classification, pattern in classification_rules.items():
                match_count = sum(1 for row in samples 
                                if row[0] and re.match(pattern, str(row[0])))
                
                if match_count / len(samples) > 0.8:  # 80% pattern match
                    classifications.append({
                        'schema_name': schema,
                        'table_name': table,
                        'column_name': column_name,
                        'classification': classification,
                        'confidence': match_count / len(samples),
                        'sample_count': len(samples)
                    })
                    break
        except:
            continue  # Skip columns that can't be sampled
    
    # Store classifications
    if classifications:
        session.write_pandas(
            pd.DataFrame(classifications),
            'DATA_GOVERNANCE.CLASSIFICATION.COLUMN_CLASSIFICATIONS',
            auto_create_table=True
        )
    
    return f"Classified {len(classifications)} columns"
$$;
```

2. **Dynamic Masking Policy Creation**:
```sql
-- Email masking policy
CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ANALYTICS_ADMIN', 'DATA_GOVERNANCE') THEN val
        WHEN CURRENT_ROLE() IN ('ANALYTICS_ENGINEER', 'DATA_ANALYST') THEN 
            REGEXP_REPLACE(val, '(.{2}).+(@.+)', '\\1***\\2')
        ELSE '***@***.com'
    END;

-- Phone number masking  
CREATE OR REPLACE MASKING POLICY phone_mask AS (val STRING)
RETURNS STRING ->
    CASE  
        WHEN CURRENT_ROLE() IN ('ANALYTICS_ADMIN') THEN val
        WHEN CURRENT_ROLE() IN ('ANALYTICS_ENGINEER') THEN 
            REGEXP_REPLACE(val, '(.{3}).+(.{4})', '\\1-***-\\2')
        ELSE '***-***-****'
    END;

-- Credit card masking
CREATE OR REPLACE MASKING POLICY credit_card_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ANALYTICS_ADMIN', 'FINANCE_ADMIN') THEN val
        WHEN CURRENT_ROLE() IN ('ANALYTICS_ENGINEER') THEN 
            REGEXP_REPLACE(val, '(.{4}).+(.{4})', '\\1-****-****-\\2') 
        ELSE '****-****-****-****'
    END;
```

3. **Automated Policy Application**:
```sql
-- Procedure to apply masking policies based on classification
CREATE OR REPLACE PROCEDURE apply_masking_policies()
RETURNS STRING
LANGUAGE SQL
AS $$
DECLARE
    classification_cursor CURSOR FOR
        SELECT schema_name, table_name, column_name, classification
        FROM data_governance.classification.column_classifications;
    policy_name STRING;
    apply_command STRING;
BEGIN
    FOR record IN classification_cursor DO
        -- Map classification to policy
        CASE record.classification
            WHEN 'EMAIL' THEN policy_name := 'email_mask';
            WHEN 'PHONE' THEN policy_name := 'phone_mask'; 
            WHEN 'CREDIT_CARD' THEN policy_name := 'credit_card_mask';
            ELSE CONTINUE;  -- Skip unhandled classifications
        END CASE;
        
        -- Apply masking policy
        apply_command := 'ALTER TABLE ' || record.schema_name || '.' || 
                        record.table_name || ' MODIFY COLUMN ' || 
                        record.column_name || ' SET MASKING POLICY ' || policy_name;
        
        EXECUTE IMMEDIATE apply_command;
    END FOR;
    
    RETURN 'Applied masking policies to classified columns';
END;
$$;
```

4. **Compliance Monitoring and Auditing**:
```sql
-- Audit sensitive data access  
CREATE VIEW data_governance.audit.sensitive_data_access AS
SELECT 
    ah.query_start_time,
    ah.user_name,
    ah.role_name,
    ah.object_name,
    cc.classification,
    ah.query_text,
    
    -- Risk scoring
    CASE cc.classification
        WHEN 'CREDIT_CARD' THEN 'HIGH_RISK'
        WHEN 'SSN' THEN 'HIGH_RISK'
        WHEN 'EMAIL' THEN 'MEDIUM_RISK'
        WHEN 'PHONE' THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_level
FROM snowflake.account_usage.access_history ah
JOIN (
    SELECT DISTINCT 
        schema_name || '.' || table_name AS object_name,
        classification
    FROM data_governance.classification.column_classifications
) cc ON ah.object_name = cc.object_name
WHERE ah.query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND ah.query_text NOT ILIKE '%SYSTEM$%';  -- Exclude system queries

-- Compliance reporting
SELECT 
    classification,
    risk_level,
    COUNT(DISTINCT user_name) AS unique_users,
    COUNT(*) AS access_count,
    COUNT(DISTINCT object_name) AS tables_accessed
FROM data_governance.audit.sensitive_data_access
WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY classification, risk_level
ORDER BY 
    CASE risk_level WHEN 'HIGH_RISK' THEN 1 WHEN 'MEDIUM_RISK' THEN 2 ELSE 3 END,
    access_count DESC;
```

These comprehensive Snowflake platform questions demonstrate deep expertise in:
- Architecture and performance optimization
- Advanced warehousing strategies  
- Data loading and integration patterns
- Security and governance implementation
- Cost management and monitoring
- Native Snowflake features utilization

The questions are designed to show Analytics Engineering mastery specific to Snowflake's unique capabilities and how they support modern data stack workflows.
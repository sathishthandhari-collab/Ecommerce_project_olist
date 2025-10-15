# Snowflake Platform Questions for Analytics Engineers

## 1. Architecture & Warehousing

**Q1: Explain Snowflake's multi-cluster architecture and how it enables elastic scaling.**

**Answer:** Snowflake separates compute from storage through its unique architecture:
- **Storage Layer**: Compressed, columnar data stored in cloud storage (S3/Azure/GCS)
- **Compute Layer**: Virtual warehouses that can scale independently 
- **Services Layer**: Cloud services for metadata, authentication, optimization

Multi-cluster warehouses automatically add/remove clusters based on query queue depth and user-defined policies, enabling linear scaling without manual intervention.

**Q2: How do you determine the appropriate warehouse size for different workload types?**

**Answer:**
- **X-Small/Small**: Simple queries, small datasets, development
- **Medium/Large**: Complex analytics, medium datasets, production ETL
- **X-Large/XX-Large**: Heavy analytics, large datasets, complex transformations
- **3X-Large+**: Machine learning, very large datasets, intensive processing

Key factors: Query complexity, data volume, concurrency requirements, SLA requirements.

**Q3: Explain the difference between automatic clustering and manual clustering keys.**

**Answer:**
- **Manual Clustering**: Define clustering keys explicitly (ALTER TABLE SET CLUSTERING KEY)
- **Automatic Clustering**: Snowflake automatically maintains clustering based on usage patterns
- **When to use manual**: High-cardinality columns, predictable query patterns, cost-sensitive scenarios
- **When to use automatic**: Unpredictable patterns, multiple query types, hands-off management

## 2. Performance Optimization

**Q4: What are the key strategies for optimizing query performance in Snowflake?**

**Answer:**
1. **Clustering Keys**: For large tables with predictable filter patterns
2. **Warehouse Sizing**: Right-size based on workload complexity
3. **Result Caching**: Leverage 24-hour query result cache
4. **Metadata Elimination**: Use micro-partitioning effectively
5. **Query Profiling**: Use Query Profile to identify bottlenecks
6. **Projection Pushdown**: Select only needed columns
7. **Predicate Pushdown**: Filter early in query execution

**Q5: How does Snowflake's automatic query optimization work?**

**Answer:**
- **Cost-based optimizer**: Analyzes query patterns and data statistics
- **Automatic statistics**: Maintains column statistics and histograms
- **Join optimization**: Chooses optimal join algorithms (hash, merge, nested loop)
- **Pushdown optimization**: Pushes filters and projections to storage layer
- **Parallel processing**: Automatically parallelizes operations across clusters

**Q6: Explain Snowflake's caching mechanisms and their impact on performance.**

**Answer:**
1. **Result Cache**: 24-hour cache of exact query results (global across all users)
2. **Metadata Cache**: File-level metadata for micro-partitions
3. **Warehouse Cache**: Local SSD cache on compute nodes (persists during warehouse lifetime)

Cache hierarchy: Result Cache → Metadata Cache → Warehouse Cache → Remote Storage

## 3. Data Loading & Unloading

**Q7: Compare different data loading methods in Snowflake and their use cases.**

**Answer:**
- **COPY INTO**: Bulk loading from staged files (batch processing)
- **INSERT/MERGE**: Row-by-row operations (real-time updates)
- **Snowpipe**: Near real-time streaming ingestion
- **External Tables**: Query data without loading (data lake scenarios)
- **Streams**: Change data capture for incremental processing

**Q8: How do you implement incremental data loading patterns?**

**Answer:**
```sql
-- Using Streams for CDC
CREATE STREAM customer_stream ON TABLE customers;

-- Incremental merge pattern
MERGE INTO target_table t
USING (
  SELECT * FROM source_stream 
  WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
) s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET col1 = s.col1, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (id, col1, created_at) VALUES (s.id, s.col1, s.created_at);
```

**Q9: Explain Snowflake's file format options and their optimal use cases.**

**Answer:**
- **Parquet**: Columnar, compressed, optimal for analytics
- **ORC**: Hadoop ecosystem integration
- **Avro**: Schema evolution, streaming data
- **JSON**: Semi-structured data, flexible schemas
- **CSV**: Simple flat files, human-readable
- **XML**: Legacy systems, complex nested structures

## 4. Security & Governance

**Q10: How do you implement row-level security in Snowflake?**

**Answer:**
```sql
-- Create masking policy
CREATE MASKING POLICY ssn_mask AS (val STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ANALYST_ROLE') THEN val
    ELSE '***-**-' || RIGHT(val, 4)
  END;

-- Create row access policy
CREATE ROW ACCESS POLICY customer_policy AS (customer_region STRING) RETURNS BOOLEAN ->
  CURRENT_ROLE() = 'ADMIN_ROLE' 
  OR EXISTS (
    SELECT 1 FROM user_region_access 
    WHERE user_name = CURRENT_USER() AND region = customer_region
  );

-- Apply policies
ALTER TABLE customers SET ROW ACCESS POLICY customer_policy ON (region);
ALTER TABLE customers MODIFY ssn SET MASKING POLICY ssn_mask;
```

**Q11: Explain Snowflake's RBAC model and best practices.**

**Answer:**
- **Hierarchy**: Account → User → Role → Privilege → Object
- **Role Types**: 
  - System roles (ACCOUNTADMIN, SYSADMIN, SECURITYADMIN)
  - Custom roles (functional, departmental)
- **Best Practices**:
  - Principle of least privilege
  - Role hierarchy design
  - Regular access reviews
  - Separation of duties

**Q12: How do you implement data classification and privacy controls?**

**Answer:**
- **Object Tagging**: Classify data with custom tags
- **Dynamic Data Masking**: Protect sensitive data at query time
- **Column-level Security**: Fine-grained access control
- **Audit Logging**: Track data access patterns
- **Privacy Compliance**: GDPR, CCPA compliance features

## 5. Time Travel & Data Recovery

**Q13: Explain Snowflake's Time Travel feature and its practical applications.**

**Answer:**
- **Retention Period**: 1 day (Standard), up to 90 days (Enterprise)
- **Use Cases**:
  - Accidental data deletion recovery
  - Historical analysis and reporting
  - Regulatory compliance requirements
  - A/B testing with historical snapshots

```sql
-- Query historical data
SELECT * FROM table_name AT (TIMESTAMP => '2024-01-15 09:00:00');
SELECT * FROM table_name BEFORE (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');

-- Restore data
CREATE TABLE recovered_data AS 
SELECT * FROM original_table AT (TIMESTAMP => '2024-01-15 08:00:00');
```

**Q14: What are the cost implications of Time Travel and how do you optimize them?**

**Answer:**
- **Storage Costs**: Historical data stored separately, billed for storage
- **Optimization Strategies**:
  - Set appropriate retention periods per table
  - Use UNDROP vs. Time Travel appropriately  
  - Monitor storage usage with STORAGE_USAGE views
  - Consider Fail-safe vs. Time Travel trade-offs

## 6. Streams & Tasks

**Q15: How do you implement real-time data pipelines using Streams and Tasks?**

**Answer:**
```sql
-- Create stream on source table
CREATE STREAM order_stream ON TABLE orders;

-- Create task for processing
CREATE TASK process_orders
  WAREHOUSE = 'TRANSFORM_WH'
  SCHEDULE = '5 minute'
WHEN
  SYSTEM$STREAM_HAS_DATA('order_stream')
AS
  MERGE INTO order_summary os
  USING (
    SELECT customer_id, SUM(amount) as total_amount
    FROM order_stream 
    WHERE METADATA$ACTION = 'INSERT'
    GROUP BY customer_id
  ) s ON os.customer_id = s.customer_id
  WHEN MATCHED THEN UPDATE SET total_amount = os.total_amount + s.total_amount
  WHEN NOT MATCHED THEN INSERT VALUES (s.customer_id, s.total_amount);

-- Start task
ALTER TASK process_orders RESUME;
```

**Q16: Explain the difference between Standard and Append-only Streams.**

**Answer:**
- **Standard Streams**: Capture INSERT, UPDATE, DELETE operations with before/after images
- **Append-only Streams**: Only capture INSERT operations, more efficient for append-only sources
- **Use Cases**: 
  - Standard: Full CDC scenarios, complex transformations
  - Append-only: Log processing, event streaming, immutable data sources

## 7. Data Sharing & Marketplace

**Q17: How do you implement secure data sharing between Snowflake accounts?**

**Answer:**
```sql
-- Create share
CREATE SHARE customer_data_share;

-- Grant usage on database
GRANT USAGE ON DATABASE customer_db TO SHARE customer_data_share;
GRANT USAGE ON SCHEMA customer_db.public TO SHARE customer_data_share;
GRANT SELECT ON TABLE customer_db.public.customers TO SHARE customer_data_share;

-- Add accounts to share
ALTER SHARE customer_data_share ADD ACCOUNTS = ('consumer_account_1', 'consumer_account_2');

-- Consumer side
CREATE DATABASE shared_customer_data FROM SHARE provider_account.customer_data_share;
```

**Q18: What are the different types of data sharing and their use cases?**

**Answer:**
- **Direct Share**: One-to-one sharing between specific accounts
- **Data Exchange**: Private marketplace for invited members
- **Data Marketplace**: Public marketplace for monetizing data
- **Reader Accounts**: Provide access without requiring full Snowflake account

## 8. Semi-structured Data

**Q19: How do you optimize queries on JSON data in Snowflake?**

**Answer:**
```sql
-- Create materialized views for frequently accessed paths
CREATE MATERIALIZED VIEW customer_attributes AS
SELECT 
  id,
  profile_data:name::STRING as customer_name,
  profile_data:email::STRING as email,
  profile_data:preferences.marketing::BOOLEAN as marketing_opt_in
FROM customers;

-- Use FLATTEN for array processing
SELECT 
  c.id,
  f.value:product_id::STRING as product_id,
  f.value:rating::NUMBER as rating
FROM customers c,
LATERAL FLATTEN(input => c.purchase_history) f;

-- Automatic schema detection
SELECT * FROM TABLE(INFER_SCHEMA(
  LOCATION => '@my_stage/json_files/',
  FILE_FORMAT => 'my_json_format'
));
```

**Q20: Explain Snowflake's approach to schema evolution for semi-structured data.**

**Answer:**
- **Schema-on-Read**: No predefined schema required
- **Automatic Schema Detection**: INFER_SCHEMA function analyzes files
- **Column Evolution**: Add/modify columns without data migration
- **Type Coercion**: Automatic handling of type changes
- **Backward Compatibility**: Historical queries continue to work

## 9. Native Applications & Marketplace

**Q21: How do you build and deploy Native Apps in Snowflake?**

**Answer:**
- **Application Package**: Define app structure and permissions
- **Streamlit Integration**: Build interactive UI components
- **Secure Computation**: Process data without exposing it
- **Version Management**: Handle app updates and rollbacks
- **Distribution**: Private or public marketplace deployment

**Q22: What are the key considerations for Native App development?**

**Answer:**
- **Security Model**: Principle of least privilege
- **Performance**: Efficient resource utilization
- **User Experience**: Intuitive interface design
- **Data Privacy**: Compliance with regulations
- **Monetization**: Pricing and billing strategies

## 10. Monitoring & Observability

**Q23: How do you monitor Snowflake performance and costs?**

**Answer:**
```sql
-- Query performance monitoring
SELECT 
  query_id,
  query_text,
  total_elapsed_time,
  bytes_scanned,
  credits_used_cloud_services
FROM table(information_schema.query_history())
WHERE start_time >= CURRENT_DATE() - 7
ORDER BY total_elapsed_time DESC;

-- Warehouse utilization
SELECT 
  warehouse_name,
  SUM(credits_used) as total_credits,
  AVG(avg_running) as avg_concurrent_queries
FROM warehouse_metering_history
WHERE start_time >= CURRENT_DATE() - 30
GROUP BY warehouse_name;

-- Storage costs
SELECT 
  usage_date,
  storage_bytes / (1024*1024*1024) as storage_gb,
  stage_bytes / (1024*1024*1024) as stage_gb
FROM storage_usage;
```

## 11-50. Additional Platform Questions

**Q24: Disaster recovery and business continuity strategies**
**Q25: Multi-region deployment patterns**
**Q26: Integration with external systems (APIs, databases)**
**Q27: Snowflake on different cloud providers**
**Q28: Advanced security configurations**
**Q29: Cost optimization strategies**
**Q30: Data migration best practices**
**Q31: Snowflake SQL extensions and functions**
**Q32: Working with external stages**
**Q33: User-defined functions (UDFs) and stored procedures**
**Q34: Resource monitors and credit management**
**Q35: Network policies and IP whitelisting**
**Q36: Snowflake Connector ecosystem**
**Q37: Advanced analytics and machine learning features**
**Q38: Data pipeline orchestration patterns**
**Q39: Compliance and audit requirements**
**Q40: Troubleshooting common issues**
**Q41: Snowflake REST API usage**
**Q42: Partner tool integrations**
**Q43: Advanced data modeling techniques**
**Q44: Real-time analytics patterns**
**Q45: Snowflake ecosystem and third-party tools**
**Q46: Advanced warehouse management**
**Q47: Data governance frameworks**
**Q48: Performance tuning methodologies**
**Q49: Scaling strategies for enterprise workloads**
**Q50: Future roadmap and emerging features**

---

## Interview Success Tips:

1. **Hands-on Experience**: Demonstrate practical knowledge with specific examples
2. **Cost Awareness**: Always consider financial implications of design decisions
3. **Security First**: Show understanding of enterprise security requirements
4. **Scalability**: Think about how solutions work at enterprise scale
5. **Best Practices**: Reference Snowflake documentation and community best practices

## Key Differentiators for Premium Roles:
- Deep understanding of Snowflake's unique architecture
- Experience with advanced features (Native Apps, Data Sharing)
- Cost optimization expertise
- Security and compliance knowledge
- Integration patterns with modern data stack
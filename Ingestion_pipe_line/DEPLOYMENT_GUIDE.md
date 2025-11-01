
# Olist PySpark Ingestion Pipeline - Deployment Guide

## Overview
Complete guide to deploy the event-driven PySpark ingestion pipeline:
**AWS S3 → Lambda → EMR → Snowflake → DBT**

---

## Prerequisites

### 1. AWS Account Setup
- AWS Account with admin access
- AWS CLI configured with credentials
- Terraform installed (v1.0+)
- Python 3.11+ installed locally

### 2. Snowflake Account
- Snowflake account (AWS region recommended for same-region transfer)
- ACCOUNTADMIN or similar privileges

### 3. Required Tools
- Git
- Docker (optional, for local testing)
- AWS SAM CLI (optional, for Lambda testing)

---

## Deployment Steps

### Step 1: Clone Repository and Setup

```bash
# Create project directory
mkdir olist-pyspark-pipeline
cd olist-pyspark-pipeline

# Initialize git
git init

# Create directory structure
mkdir -p {src,infrastructure,snowflake,config,tests,docs}
```

### Step 2: Snowflake Setup

```bash
# Login to Snowflake and run DDL scripts
snowsql -a <account_name> -u <username>

# Execute DDL script
!source snowflake_ddl.sql

# Verify tables created
SHOW TABLES IN SCHEMA OLIST_DB.RAW;

# Create user for pipeline
CREATE USER pyspark_user 
  PASSWORD='<strong_password>'
  DEFAULT_ROLE=INGESTION_ROLE
  DEFAULT_WAREHOUSE=INGESTION_WH;

GRANT ROLE INGESTION_ROLE TO USER pyspark_user;
```

### Step 3: Download Snowflake Connector JARs

```bash
# Download Snowflake Spark Connector
wget https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/spark-snowflake_2.12-2.12.0-spark_3.3.jar

# Download Snowflake JDBC Driver
wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.30/snowflake-jdbc-3.13.30.jar
```

### Step 4: AWS Infrastructure Deployment

```bash
# Initialize Terraform
cd infrastructure
terraform init

# Create terraform.tfvars
cat > terraform.tfvars <<EOF
aws_region = "us-east-1"
project_name = "olist-ingestion"
environment = "prod"
emr_subnet_id = "subnet-xxxxx"  # Your subnet ID
emr_key_pair = "your-key-pair"   # Your EC2 key pair
EOF

# Plan infrastructure
terraform plan

# Deploy infrastructure
terraform apply -auto-approve

# Note the outputs
terraform output
```

### Step 5: Upload Files to S3

```bash
# Upload PySpark script
aws s3 cp pyspark_ingestion.py s3://<scripts-bucket>/

# Upload Snowflake JARs
aws s3 cp spark-snowflake_2.12-2.12.0-spark_3.3.jar s3://<jars-bucket>/
aws s3 cp snowflake-jdbc-3.13.30.jar s3://<jars-bucket>/
```

### Step 6: Package and Deploy Lambda Function

```bash
# Install Lambda dependencies
pip install boto3 -t lambda_package/
cp lambda_s3_trigger.py lambda_package/

# Create deployment package
cd lambda_package
zip -r ../lambda_deployment.zip .
cd ..

# Deploy Lambda (if not using Terraform)
aws lambda update-function-code \
  --function-name olist-ingestion-emr-trigger \
  --zip-file fileb://lambda_deployment.zip
```

### Step 7: Update Lambda Environment Variables

```bash
# Update Lambda with Snowflake credentials (use AWS Secrets Manager in production)
aws lambda update-function-configuration \
  --function-name olist-ingestion-emr-trigger \
  --environment Variables='{
    "SNOWFLAKE_ACCOUNT":"<account>",
    "SNOWFLAKE_USER":"pyspark_user",
    "SNOWFLAKE_PASSWORD":"<password>",
    "SNOWFLAKE_DATABASE":"OLIST_DB",
    "SNOWFLAKE_SCHEMA":"RAW",
    "SNOWFLAKE_WAREHOUSE":"INGESTION_WH"
  }'
```

### Step 8: Configure S3 Event Notification

```bash
# This is handled by Terraform, but to verify:
aws s3api get-bucket-notification-configuration \
  --bucket <data-bucket-name>
```

---

## Testing the Pipeline

### Test 1: Manual File Upload

```bash
# Download sample Olist data
wget https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/download

# Upload a small test file
aws s3 cp olist_orders_dataset.csv \
  s3://<data-bucket>/incoming/olist_orders_dataset.csv
```

### Test 2: Monitor Lambda Execution

```bash
# Check Lambda logs
aws logs tail /aws/lambda/olist-ingestion-emr-trigger --follow

# Check EMR cluster status
aws emr list-clusters --active
```

### Test 3: Verify Data in Snowflake

```sql
-- Connect to Snowflake
USE DATABASE OLIST_DB;
USE SCHEMA RAW;

-- Check ingestion
SELECT * FROM V_INGESTION_MONITORING;

-- Verify data
SELECT COUNT(*) FROM OLIST_ORDERS_DATASET;

-- Check data freshness
SELECT * FROM V_DATA_FRESHNESS;
```

---

## PySpark Optimizations Explained

### 1. Broadcast Joins
```python
# Small dimension tables are cached and broadcast to all executors
if table_name in DIMENSION_TABLES:
    df.cache()
    df.count()  # Materialize cache
```

**Why?** 
- Avoids shuffle for small lookup tables
- Significantly faster joins
- Reduces network I/O

### 2. Adaptive Query Execution (AQE)
```python
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Why?**
- Dynamically optimizes query plans at runtime
- Handles data skew automatically
- Combines small partitions to reduce overhead

### 3. Dynamic Partitioning
```python
# Fact tables: 20 partitions by date
df = df.repartition(20, "order_purchase_timestamp")

# Dimension tables: 4 coalesced partitions
df = df.coalesce(4)
```

**Why?**
- Optimizes parallelism for different data sizes
- Prevents small file problem
- Balances write performance

### 4. Kryo Serialization
```python
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

**Why?**
- 10x faster than Java serialization
- Significantly smaller serialized size
- Less network and disk I/O

---

## Event-Driven Architecture

### Components

1. **S3 Event Notification**
   - Triggers on CSV file upload
   - Filters: prefix="incoming/", suffix=".csv"

2. **Lambda Function**
   - Receives S3 event
   - Validates file type
   - Launches EMR cluster with job

3. **EMR Auto-Termination**
   - `KeepJobFlowAliveWhenNoSteps: False`
   - Automatically terminates after job completion
   - Cost optimization

4. **CloudWatch Monitoring**
   - Lambda execution logs
   - EMR cluster state changes
   - Alerts on failures

---

## Cost Optimization Tips

### 1. Use Spot Instances for EMR Core Nodes
```python
'Market': 'SPOT',
'BidPrice': 'OnDemandPrice'
```
**Savings:** Up to 70-90% on compute costs

### 2. Right-Size EMR Cluster
- Start small: 1 master + 2 core nodes
- Monitor and adjust based on data volume
- Use auto-scaling for variable workloads

### 3. Snowflake Auto-Suspend
```sql
ALTER WAREHOUSE INGESTION_WH SET 
  AUTO_SUSPEND = 60;  -- 1 minute
```

### 4. Use Internal Transfer Mode
- No additional S3 costs
- Faster than external transfer
- Temporary credentials auto-managed

---

## Monitoring and Alerts

### CloudWatch Alarms

```bash
# Create alarm for Lambda errors
aws cloudwatch put-metric-alarm \
  --alarm-name "Olist-Lambda-Errors" \
  --alarm-description "Alert on Lambda errors" \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --statistic Sum \
  --period 300 \
  --threshold 1 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 1
```

### Snowflake Monitoring Queries

```sql
-- Check ingestion health
SELECT 
    pipeline_run_id,
    ingestion_date,
    total_records,
    DATEDIFF('minute', first_record_ingested, last_record_ingested) as duration_minutes
FROM RAW.V_INGESTION_MONITORING
WHERE ingestion_date = CURRENT_DATE()
ORDER BY first_record_ingested DESC;

-- Check for data quality issues
SELECT 
    COUNT(*) as null_order_ids
FROM RAW.OLIST_ORDERS_DATASET
WHERE order_id IS NULL;
```

---

## Integration with DBT

### DBT Project Setup

```yaml
# profiles.yml
olist:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <account>
      user: <dbt_user>
      password: <password>
      role: AE_ROLE
      database: OLIST_DB
      warehouse: TRANSFORM_WH
      schema: STAGING
      threads: 4
```

### DBT Source Configuration

```yaml
# models/sources.yml
version: 2

sources:
  - name: raw
    database: OLIST_DB
    schema: RAW
    tables:
      - name: olist_orders_dataset
      - name: olist_order_items_dataset
      - name: olist_customers_dataset
```

### Incremental Model Using Stream

```sql
-- models/staging/stg_orders.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns'
    )
}}

SELECT 
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    ingestion_timestamp
FROM {{ source('raw', 'olist_orders_dataset') }}

{% if is_incremental() %}
WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}
```

---

## Troubleshooting

### Issue: Lambda Timeout
**Solution:** Increase timeout to 300 seconds (5 minutes)

### Issue: EMR Cluster Fails to Start
**Solution:** 
- Check IAM roles have proper permissions
- Verify subnet has internet access or NAT gateway
- Check security groups

### Issue: Snowflake Connection Failed
**Solution:**
- Verify Snowflake credentials in Lambda env vars
- Check network connectivity (VPC, security groups)
- Ensure Snowflake account is not suspended

### Issue: Files Not Triggering Lambda
**Solution:**
- Verify S3 event notification is configured
- Check file is in correct prefix path
- Ensure file has .csv extension
- Check Lambda permissions

---

## Production Best Practices

1. **Use AWS Secrets Manager** for credentials
2. **Enable CloudTrail** for audit logging
3. **Implement retry logic** in Lambda
4. **Use SNS** for failure notifications
5. **Enable versioning** on S3 buckets
6. **Implement data validation** in PySpark
7. **Use Snowflake time travel** for data recovery
8. **Set up cost alerts** in AWS Billing
9. **Regular EMR AMI updates** for security
10. **Document pipeline SLAs** and data freshness requirements

---

## Next Steps: Airflow Integration

For production orchestration, consider replacing Lambda with Airflow:

```python
# airflow/dags/olist_ingestion_dag.py
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator

# DAG definition for scheduled or sensor-based execution
```

---

## Appendix: Complete File Checklist

- [ ] lambda_s3_trigger.py
- [ ] pyspark_ingestion.py
- [ ] infrastructure.tf
- [ ] snowflake_ddl.sql
- [ ] terraform.tfvars
- [ ] requirements.txt
- [ ] README.md
- [ ] .gitignore

---

**Questions to Consider as an AE:**

1. Why use EMR instead of AWS Glue? 
   - More control, cheaper at scale, better Spark version control

2. Why RAW layer before DBT?
   - Separation of concerns, idempotency, data lineage

3. Why not use Snowflake's Snowpipe?
   - Learning PySpark, more transformation flexibility, cost at high volumes

4. What about schema evolution?
   - Monitor with dbt tests, use Snowflake streams, version control schemas

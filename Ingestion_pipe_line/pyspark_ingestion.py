
'''
PySpark Ingestion Pipeline for Olist Dataset
AWS S3 → EMR PySpark → Snowflake Ingestion Layer

Features:
- Broadcast joins for dimension tables
- Partition optimization
- Data quality checks
- Snowflake connector integration
- Error handling and logging
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import argparse
from datetime import datetime

# Initialize Spark Session with Snowflake connector
def create_spark_session(app_name="Olist-Ingestion"):
    '''
    Create Spark session with optimized configurations
    '''
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .config("spark.sql.broadcastTimeout", "600") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    return spark

# Snowflake connection parameters
SNOWFLAKE_OPTIONS = {
    "sfURL": "<YOUR_SNOWFLAKE_ACCOUNT>.snowflakecomputing.com",
    "sfUser": "<YOUR_USERNAME>",
    "sfPassword": "<YOUR_PASSWORD>",
    "sfDatabase": "OLIST_DB",
    "sfSchema": "RAW",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ANALYST_ROLE"
}

# Table schemas with data types
TABLE_SCHEMAS = {
    "olist_orders_dataset": {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
        "order_purchase_timestamp": "timestamp",
        "order_approved_at": "timestamp",
        "order_delivered_carrier_date": "timestamp",
        "order_delivered_customer_date": "timestamp",
        "order_estimated_delivery_date": "timestamp"
    },
    "olist_order_items_dataset": {
        "order_id": "string",
        "order_item_id": "integer",
        "product_id": "string",
        "seller_id": "string",
        "shipping_limit_date": "timestamp",
        "price": "double",
        "freight_value": "double"
    },
    "olist_order_payments_dataset": {
        "order_id": "string",
        "payment_sequential": "integer",
        "payment_type": "string",
        "payment_installments": "integer",
        "payment_value": "double"
    },
    "olist_order_reviews_dataset": {
        "review_id": "string",
        "order_id": "string",
        "review_score": "integer",
        "review_comment_title": "string",
        "review_comment_message": "string",
        "review_creation_date": "timestamp",
        "review_answer_timestamp": "timestamp"
    },
    "olist_customers_dataset": {
        "customer_id": "string",
        "customer_unique_id": "string",
        "customer_zip_code_prefix": "string",
        "customer_city": "string",
        "customer_state": "string"
    },
    "olist_sellers_dataset": {
        "seller_id": "string",
        "seller_zip_code_prefix": "string",
        "seller_city": "string",
        "seller_state": "string"
    },
    "olist_products_dataset": {
        "product_id": "string",
        "product_category_name": "string",
        "product_name_lenght": "integer",
        "product_description_lenght": "integer",
        "product_photos_qty": "integer",
        "product_weight_g": "integer",
        "product_length_cm": "integer",
        "product_height_cm": "integer",
        "product_width_cm": "integer"
    },
    "olist_geolocation_dataset": {
        "geolocation_zip_code_prefix": "string",
        "geolocation_lat": "double",
        "geolocation_lng": "double",
        "geolocation_city": "string",
        "geolocation_state": "string"
    },
    "product_category_name_translation": {
        "product_category_name": "string",
        "product_category_name_english": "string"
    }
}

# Dimension tables for broadcast joins
DIMENSION_TABLES = [
    "olist_customers_dataset",
    "olist_sellers_dataset", 
    "olist_products_dataset",
    "product_category_name_translation"
]

def read_csv_from_s3(spark, s3_path, table_name):
    '''
    Read CSV file from S3 with appropriate schema
    '''
    print(f"Reading data from: {s3_path}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .csv(s3_path)

    # Apply schema transformations
    df = apply_schema_transformations(df, table_name)

    return df

def apply_schema_transformations(df, table_name):
    '''
    Apply proper data types based on table schema
    '''
    if table_name not in TABLE_SCHEMAS:
        print(f"Warning: No schema defined for {table_name}, using inferred schema")
        return df

    schema = TABLE_SCHEMAS[table_name]

    for col_name, col_type in schema.items():
        if col_name in df.columns:
            if col_type == "timestamp":
                df = df.withColumn(col_name, to_timestamp(col(col_name)))
            elif col_type == "integer":
                df = df.withColumn(col_name, col(col_name).cast("int"))
            elif col_type == "double":
                df = df.withColumn(col_name, col(col_name).cast("double"))

    return df

def add_metadata_columns(df):
    '''
    Add metadata columns for tracking
    '''
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("ingestion_date", current_date()) \
           .withColumn("source_system", lit("S3")) \
           .withColumn("pipeline_run_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))

    return df

def perform_data_quality_checks(df, table_name):
    '''
    Perform basic data quality checks
    '''
    print(f"\nData Quality Checks for {table_name}:")
    print(f"Total Records: {df.count()}")
    print(f"Total Columns: {len(df.columns)}")

    # Check for null values in key columns
    if table_name == "olist_orders_dataset":
        null_orders = df.filter(col("order_id").isNull()).count()
        print(f"Null order_id count: {null_orders}")
        if null_orders > 0:
            print("WARNING: Found null values in order_id!")

    # Check for duplicates in primary key
    if table_name == "olist_orders_dataset":
        total_count = df.count()
        distinct_count = df.select("order_id").distinct().count()
        if total_count != distinct_count:
            print(f"WARNING: Duplicate order_ids found! Total: {total_count}, Distinct: {distinct_count}")

    return True

def optimize_partitioning(df, table_name):
    '''
    Optimize data partitioning based on table type
    '''
    # Fact tables: partition by date
    if table_name in ["olist_orders_dataset", "olist_order_items_dataset", 
                      "olist_order_payments_dataset", "olist_order_reviews_dataset"]:
        if "order_purchase_timestamp" in df.columns:
            df = df.repartition(20, "order_purchase_timestamp")
        elif "review_creation_date" in df.columns:
            df = df.repartition(20, "review_creation_date")
        else:
            df = df.repartition(20)

    # Dimension tables: smaller partitions
    elif table_name in DIMENSION_TABLES:
        df = df.coalesce(4)

    # Large tables: more partitions
    elif table_name == "olist_geolocation_dataset":
        df = df.repartition(30)

    return df

def write_to_snowflake(df, table_name, write_mode="append"):
    '''
    Write DataFrame to Snowflake using Spark connector
    '''
    print(f"\nWriting {table_name} to Snowflake...")

    snowflake_table = f"RAW.{table_name.upper()}"

    try:
        df.write \
            .format("snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("dbtable", snowflake_table) \
            .option("truncate_table", "off" if write_mode == "append" else "on") \
            .mode(write_mode) \
            .save()

        print(f"Successfully wrote to {snowflake_table}")
        return True

    except Exception as e:
        print(f"Error writing to Snowflake: {str(e)}")
        return False

def main():
    '''
    Main ingestion pipeline
    '''
    # Parse arguments
    parser = argparse.ArgumentParser(description='Olist PySpark Ingestion Pipeline')
    parser.add_argument('--source-bucket', required=True, help='S3 source bucket')
    parser.add_argument('--source-key', required=True, help='S3 source key/path')
    parser.add_argument('--table-name', required=True, help='Target table name')
    parser.add_argument('--write-mode', default='append', choices=['append', 'overwrite'], 
                        help='Write mode for Snowflake')

    args = parser.parse_args()

    # Build S3 path
    s3_path = f"s3a://{args.source_bucket}/{args.source_key}"

    print("="*80)
    print("OLIST PYSPARK INGESTION PIPELINE")
    print("="*80)
    print(f"Source: {s3_path}")
    print(f"Target Table: {args.table_name}")
    print(f"Write Mode: {args.write_mode}")
    print("="*80)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Step 1: Read data from S3
        df = read_csv_from_s3(spark, s3_path, args.table_name)
        print(f"\nSuccessfully read {df.count()} records")

        # Step 2: Add metadata columns
        df = add_metadata_columns(df)

        # Step 3: Data quality checks
        perform_data_quality_checks(df, args.table_name)

        # Step 4: Optimize partitioning
        df = optimize_partitioning(df, args.table_name)

        # Step 5: Cache dimension tables for broadcast joins
        if args.table_name in DIMENSION_TABLES:
            print(f"\nCaching dimension table {args.table_name} for broadcast joins")
            df.cache()
            df.count()  # Materialize cache

        # Step 6: Write to Snowflake
        success = write_to_snowflake(df, args.table_name, args.write_mode)

        if success:
            print("\n" + "="*80)
            print("INGESTION COMPLETED SUCCESSFULLY")
            print("="*80)
        else:
            print("\nIngestion failed!")
            sys.exit(1)

    except Exception as e:
        print(f"\nError in ingestion pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

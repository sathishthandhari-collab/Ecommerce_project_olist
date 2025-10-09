use database olist_dev;
use schema raw;

-- Create tables
CREATE OR REPLACE TABLE raw_olist_orders (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    -- Metadata columns for AE best practices
    _file_name STRING,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _batch_id STRING
);

create or replace table raw_olist_order_items(
    order_id string,
    order_item_id string,
    product_id string,
    seller_id string,
    shipping_limit_date timestamp,
    price float,
    freight_value float);

create or replace table raw_olist_sellers(
    seller_id string,
    seller_zip_code_prefix string,
    seller_city varchar,
    seller_state varchar
);

create or replace table raw_olist_customers(
    customer_id string,
    customer_unique_id string,
    customer_zip_code_prefix string,
    customer_city varchar,
    customer_state varchar
);


-- Create storage integration
CREATE or replace STORAGE INTEGRATION s3_olist_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::327903111009:role/s3fullaccess'
  STORAGE_ALLOWED_LOCATIONS = ('s3://olist-streaming-bucket/orders/streaming_data/');

-- Get IAM policy requirements
DESC STORAGE INTEGRATION s3_olist_integration;


-- Create external stage
CREATE OR REPLACE STAGE olist_streaming_stage
  STORAGE_INTEGRATION = s3_olist_integration
  URL = 's3://olist-streaming-bucket/orders/streaming_data/'
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Create pipe with auto-ingest
CREATE OR REPLACE PIPE olist_streaming_pipe
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sns:us-west-2:327903111009:snowpipe-olist-topic'
  AS
  COPY INTO raw_olist_orders (
    order_id, customer_id, order_status, order_purchase_timestamp,
    order_approved_at, order_delivered_carrier_date, 
    order_delivered_customer_date, order_estimated_delivery_date,
    _file_name, _batch_id
  )
  FROM (
    SELECT 
      $1, $2, $3, $4, $5, $6, $7, $8,
      METADATA$FILENAME,
      SPLIT_PART(METADATA$FILENAME, '_', 4) -- Extract batch ID
    FROM @olist_streaming_stage
  )
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('olist_streaming_pipe');




CREATE DATABASE OLIST_DW_PROD;
CREATE SCHEMA OLIST_DW_PROD.STAGING;
CREATE SCHEMA OLIST_DW_PROD.INTERMEDIATE;
CREATE SCHEMA OLIST_DW_PROD.MARTS;
CREATE SCHEMA OLIST_DW_PROD.ANALYTICS;
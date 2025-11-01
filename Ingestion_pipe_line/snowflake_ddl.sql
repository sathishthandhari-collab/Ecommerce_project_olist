
-- =====================================================
-- Snowflake DDL for Olist Ingestion Layer (RAW/BRONZE)
-- =====================================================

-- Create Database and Schema
CREATE DATABASE IF NOT EXISTS OLIST_DB;
USE DATABASE OLIST_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- Create Warehouse for ingestion
CREATE WAREHOUSE IF NOT EXISTS INGESTION_WH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Warehouse for data ingestion from EMR';

-- =====================================================
-- RAW TABLES (INGESTION LAYER)
-- =====================================================

-- Orders Table
CREATE TABLE IF NOT EXISTS RAW.OLIST_ORDERS_DATASET (
    order_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP_NTZ,
    order_approved_at TIMESTAMP_NTZ,
    order_delivered_carrier_date TIMESTAMP_NTZ,
    order_delivered_customer_date TIMESTAMP_NTZ,
    order_estimated_delivery_date TIMESTAMP_NTZ,
    -- Metadata columns
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE(),
    source_system VARCHAR(50),
    pipeline_run_id VARCHAR(50)
) COMMENT = 'Raw orders data from Olist';

-- Order Items Table
CREATE TABLE IF NOT EXISTS RAW.OLIST_ORDER_ITEMS_DATASET (
    order_id VARCHAR(100),
    order_item_id INTEGER,
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
    shipping_limit_date TIMESTAMP_NTZ,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    -- Metadata columns
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE(),
    source_system VARCHAR(50),
    pipeline_run_id VARCHAR(50),
    PRIMARY KEY (order_id, order_item_id)
) COMMENT = 'Raw order items data from Olist';

-- Order Payments Table
CREATE TABLE IF NOT EXISTS RAW.OLIST_ORDER_PAYMENTS_DATASET (
    order_id VARCHAR(100),
    payment_sequential INTEGER,
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    payment_value DECIMAL(10, 2),
    -- Metadata columns
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE(),
    source_system VARCHAR(50),
    pipeline_run_id VARCHAR(50),
    PRIMARY KEY (order_id, payment_sequential)
) COMMENT = 'Raw payment data from Olist';

-- Order Reviews Table
CREATE TABLE IF NOT EXISTS RAW.OLIST_ORDER_REVIEWS_DATASET (
    review_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100),
    review_score INTEGER,
    review_comment_title VARCHAR(500),
    review_comment_message VARCHAR(5000),
    review_creation_date TIMESTAMP_NTZ,
    review_answer_timestamp TIMESTAMP_NTZ,
    -- Metadata columns
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE(),
    source_system VARCHAR(50),
    pipeline_run_id VARCHAR(50)
) COMMENT = 'Raw reviews data from Olist';

-- Customers Table
CREATE TABLE IF NOT EXISTS RAW.OLIST_CUSTOMERS_DATASET (
    customer_id VARCHAR(100) PRIMARY KEY,
    customer_unique_id VARCHAR(100),
    customer_zip_code_prefix VARCHAR(20),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),
    -- Metadata columns
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE(),
    source_system VARCHAR(50),
    pipeline_run_id VARCHAR(50)
) COMMENT = 'Raw customers data from Olist';

-- Sellers Table
CREATE TABLE IF NOT EXISTS RAW.OLIST_SELLERS_DATASET (
    seller_id VARCHAR(100) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(20),
    seller_city VARCHAR(100),
    seller_state VARCHAR(10),
    -- Metadata columns
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE(),
    source_system VARCHAR(50),
    pipeline_run_id VARCHAR(50)
) COMMENT = 'Raw sellers data from Olist';

-- Products Table
CREATE TABLE IF NOT EXISTS RAW.OLIST_PRODUCTS_DATASET (
    product_id VARCHAR(100) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    -- Metadata columns
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE(),
    source_system VARCHAR(50),
    pipeline_run_id VARCHAR(50)
) COMMENT = 'Raw products data from Olist';

-- Geolocation Table
CREATE TABLE IF NOT EXISTS RAW.OLIST_GEOLOCATION_DATASET (
    geolocation_zip_code_prefix VARCHAR(20),
    geolocation_lat DECIMAL(10, 8),
    geolocation_lng DECIMAL(11, 8),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10),
    -- Metadata columns
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE(),
    source_system VARCHAR(50),
    pipeline_run_id VARCHAR(50)
) COMMENT = 'Raw geolocation data from Olist';

-- Product Category Translation Table
CREATE TABLE IF NOT EXISTS RAW.PRODUCT_CATEGORY_NAME_TRANSLATION (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100),
    -- Metadata columns
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE(),
    source_system VARCHAR(50),
    pipeline_run_id VARCHAR(50)
) COMMENT = 'Product category translation from Portuguese to English';

-- =====================================================
-- CLUSTERING KEYS FOR PERFORMANCE
-- =====================================================

-- Cluster fact tables by date
ALTER TABLE RAW.OLIST_ORDERS_DATASET 
  CLUSTER BY (ingestion_date, order_purchase_timestamp);

ALTER TABLE RAW.OLIST_ORDER_ITEMS_DATASET 
  CLUSTER BY (ingestion_date);

ALTER TABLE RAW.OLIST_ORDER_REVIEWS_DATASET 
  CLUSTER BY (review_creation_date);

-- =====================================================
-- STREAMS FOR CDC (Change Data Capture)
-- Optional: For DBT incremental models
-- =====================================================

CREATE STREAM IF NOT EXISTS RAW.ORDERS_STREAM 
  ON TABLE RAW.OLIST_ORDERS_DATASET
  COMMENT = 'Stream to capture changes in orders table';

CREATE STREAM IF NOT EXISTS RAW.ORDER_ITEMS_STREAM 
  ON TABLE RAW.OLIST_ORDER_ITEMS_DATASET
  COMMENT = 'Stream to capture changes in order items table';

-- =====================================================
-- MONITORING VIEWS
-- =====================================================

-- View to monitor ingestion pipeline runs
CREATE OR REPLACE VIEW RAW.V_INGESTION_MONITORING AS
SELECT 
    pipeline_run_id,
    source_system,
    ingestion_date,
    MIN(ingestion_timestamp) as first_record_ingested,
    MAX(ingestion_timestamp) as last_record_ingested,
    COUNT(*) as total_records
FROM RAW.OLIST_ORDERS_DATASET
GROUP BY pipeline_run_id, source_system, ingestion_date
ORDER BY ingestion_date DESC, pipeline_run_id DESC;

-- View to check data freshness
CREATE OR REPLACE VIEW RAW.V_DATA_FRESHNESS AS
SELECT 
    'ORDERS' as table_name,
    MAX(ingestion_timestamp) as last_ingestion,
    COUNT(*) as total_records
FROM RAW.OLIST_ORDERS_DATASET
UNION ALL
SELECT 
    'ORDER_ITEMS' as table_name,
    MAX(ingestion_timestamp) as last_ingestion,
    COUNT(*) as total_records
FROM RAW.OLIST_ORDER_ITEMS_DATASET
UNION ALL
SELECT 
    'CUSTOMERS' as table_name,
    MAX(ingestion_timestamp) as last_ingestion,
    COUNT(*) as total_records
FROM RAW.OLIST_CUSTOMERS_DATASET;

-- =====================================================
-- GRANTS AND PERMISSIONS
-- =====================================================

-- Create role for Analytics Engineers
CREATE ROLE IF NOT EXISTS AE_ROLE;

-- Grant usage on database and schema
GRANT USAGE ON DATABASE OLIST_DB TO ROLE AE_ROLE;
GRANT USAGE ON SCHEMA RAW TO ROLE AE_ROLE;

-- Grant select on all tables in RAW schema for DBT
GRANT SELECT ON ALL TABLES IN SCHEMA RAW TO ROLE AE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW TO ROLE AE_ROLE;

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE INGESTION_WH TO ROLE AE_ROLE;

-- Create role for ingestion pipeline
CREATE ROLE IF NOT EXISTS INGESTION_ROLE;

GRANT USAGE ON DATABASE OLIST_DB TO ROLE INGESTION_ROLE;
GRANT USAGE ON SCHEMA RAW TO ROLE INGESTION_ROLE;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA RAW TO ROLE INGESTION_ROLE;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA RAW TO ROLE INGESTION_ROLE;
GRANT USAGE ON WAREHOUSE INGESTION_WH TO ROLE INGESTION_ROLE;

-- =====================================================
-- RESOURCE MONITORS (Cost Control)
-- =====================================================

CREATE RESOURCE MONITOR IF NOT EXISTS INGESTION_MONITOR
  WITH CREDIT_QUOTA = 100
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS 
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE INGESTION_WH SET RESOURCE_MONITOR = INGESTION_MONITOR;

# Olist E-commerce Analytics Engineering Project

## 🎯 Project Overview

A comprehensive Analytics Engineering solution for Olist's Brazilian e-commerce marketplace data, implementing enterprise-grade data transformation patterns using **dbt Core**, **Snowflake**, and **GitHub Actions**. This project demonstrates advanced analytical capabilities including customer lifetime value prediction, fraud detection, and executive KPIs.

## 🏗️ Architecture & Data Lineage

### Data Flow Architecture
```
Raw Data Sources (Snowflake) 
    ↓
Staging Layer (8 models) - Data Quality & Standardization
    ↓  
Intermediate Layer (4 models) - Business Logic & Analytics
    ↓
Marts Layer (8 models) - Executive & Operational Reporting
    ↓
Analysis & BI Dashboards
```

### Detailed Data Lineage

**Staging Layer:**
- `stg_customers.sql` → Customer demographic data with location standardization
- `stg_orders.sql` → Order lifecycle and delivery tracking
- `stg_order_items.sql` → Product-level order details 
- `stg_payments.sql` → Payment method and installment analysis
- `stg_products.sql` → Product catalog with categorization
- `stg_sellers.sql` → Seller geographic and performance data
- `stg_reviews.sql` → Customer satisfaction and sentiment analysis
- `stg_geolocation.sql` → Brazilian geographic reference data

**Intermediate Layer:**
- `int_customer_360.sql` → Complete customer behavior profile (RFM analysis, lifecycle scoring)
- `int_customer_lifetime_value.sql` → CLV prediction with confidence intervals
- `int_order_anomalies.sql` → Statistical anomaly detection using Z-scores  
- `int_seller_health_score.sql` → Multi-dimensional seller performance scoring

**Marts Layer:**
- `mart_executive_kpis.sql` → C-suite dashboards with growth metrics
- `mart_financial_performance.sql` → Revenue analysis with forecasting
- `mart_customer_strategy.sql` → Customer segmentation and retention analytics
- `mart_seller_management.sql` → Seller performance and health monitoring
- `mart_logistics_performance.sql` → Delivery and fulfillment analytics
- `mart_fraud_monitoring.sql` → Real-time fraud alerts and risk scoring
- `mart_payment_method_performance.sql` → Payment channel optimization
- `mart_unit_economics.sql` → Cohort analysis and unit profitability

## 🚀 Key Features

### Advanced Analytics
- **Customer Lifetime Value (CLV)** prediction with 95% confidence intervals
- **Churn probability** modeling using logistic regression
- **Anomaly detection** with configurable Z-score thresholds  
- **Cohort analysis** for retention metrics
- **RFM segmentation** for customer targeting

### Data Quality & Governance
- **Automated data quality scoring** with configurable thresholds
- **Contract enforcement** for schema stability  
- **Advanced testing** with custom expectations
- **Cost monitoring** and warehouse optimization
- **Incremental processing** with smart refresh logic

### Performance Optimization
- **Incremental materialization** with merge strategies
- **Clustering** on high-cardinality dimensions
- **Development vs Production** configurations
- **Sampling** for faster development iterations
- **Query cost tracking** and optimization

## 🛠️ Technical Implementation

### dbt Configuration Highlights
- **Multi-environment setup**: Dev (sampled) vs Prod (full data)
- **Advanced materialization strategies**: Incremental, table, view
- **Custom schema organization**: Staging, intermediate, marts
- **Automated testing**: Data quality, business logic, referential integrity
- **Cost monitoring**: Credits usage and performance tracking

### Key Macros & Utilities
- `calculate_z_score()` - Statistical anomaly detection
- `data_quality_score()` - Automated quality scoring  
- `capture_run_costs()` - Snowflake cost monitoring
- `standardize_timestamp()` - Timezone standardization
- Incremental helpers for efficient processing

### CI/CD Pipeline
```yaml
GitHub Actions Workflow:
├── Code quality checks
├── dbt compilation (dev/prod)  
├── Automated testing
├── Production deployment
└── Cost monitoring alerts
```

## 📊 Business Impact & Use Cases

### Executive Reporting
- **Monthly/Weekly KPIs** with automated alerting
- **Revenue forecasting** using moving averages and seasonal indexing
- **Market penetration analysis** across Brazilian states
- **Platform health monitoring** with anomaly detection

### Operational Analytics
- **Real-time fraud detection** with severity scoring
- **Seller performance management** with health tiers
- **Logistics optimization** with delivery performance tracking  
- **Payment method analysis** with conversion optimization

### Strategic Insights
- **Customer segmentation** (Champions, Loyalists, At-Risk, Lost)
- **Product performance** across categories and regions
- **Market expansion** opportunities identification
- **Revenue optimization** through unit economics analysis

## 🏃‍♂️ Getting Started

### Prerequisites
```bash
# Install dbt and Snowflake adapter
pip install dbt-core==1.10.13 dbt-snowflake==1.10.2

# Clone repository
git clone <repository-url>
cd olist_dbt_transformation
```

### Environment Setup
```bash
# Install dependencies
dbt deps --profiles-dir . --project-dir .

# Test connection
dbt debug --profiles-dir . --project-dir .

# Development run (sampled data)
dbt run --target dev --profiles-dir . --project-dir .

# Production run (full data)
dbt run --target prod --profiles-dir . --project-dir .
```

### Key dbt Commands
```bash
# Full refresh with quality tests
dbt run --full-refresh && dbt test

# Specific model execution
dbt run --select mart_executive_kpis+

# Cost monitoring
dbt run --vars '{capture_costs: true}'

# Documentation generation
dbt docs generate && dbt docs serve
```

## 📈 Data Models Documentation

### Customer Analytics Models
- **int_customer_360**: 360-degree customer view with behavioral scoring
- **mart_customer_strategy**: Strategic customer insights for retention/acquisition
- **int_customer_lifetime_value**: Predictive CLV modeling with confidence bands

### Financial & Revenue Models  
- **mart_financial_performance**: Comprehensive revenue analysis with forecasting
- **mart_unit_economics**: Cohort-based profitability analysis
- **mart_payment_method_performance**: Payment channel optimization insights

### Operational Models
- **mart_fraud_monitoring**: Real-time fraud detection and alerting
- **mart_logistics_performance**: End-to-end delivery performance tracking
- **mart_seller_management**: Seller ecosystem health monitoring

## 🔧 Advanced Configuration

### Performance Tuning
- **Clustering strategies** optimized for query patterns
- **Incremental logic** with smart refresh capabilities  
- **Warehouse sizing** recommendations based on workload
- **Query optimization** with cost monitoring

### Data Quality Framework
- **Custom tests** for business logic validation
- **Automated quality scoring** with threshold-based alerts
- **Schema contracts** for downstream stability
- **Anomaly detection** with configurable sensitivity

### Cost Optimization
- **Development sampling** to reduce compute costs
- **Incremental processing** to minimize data scanning
- **Warehouse auto-suspend** configuration
- **Query cost tracking** and optimization recommendations

## 📋 Business Glossary

### Key Metrics
- **GMV (Gross Merchandise Value)**: Total value of orders placed
- **AOV (Average Order Value)**: Mean order value per transaction
- **CLV (Customer Lifetime Value)**: Predicted total customer value
- **CAC (Customer Acquisition Cost)**: Cost to acquire new customers
- **Churn Rate**: Percentage of customers who stop purchasing

### Customer Segments
- **Champions**: High-value, frequent, recent customers
- **Loyalists**: Consistent, satisfied customers  
- **Potential Loyalists**: Recent customers with growth potential
- **At-Risk**: Previously valuable customers showing decline
- **Lost Customers**: Inactive customers requiring win-back campaigns

## 🎯 Analytics Engineering Best Practices Demonstrated

### Data Modeling
- **Dimensional modeling** with proper fact/dimension separation
- **Slowly Changing Dimensions (SCD)** implementation
- **Surrogate key** management with dbt utilities
- **Referential integrity** maintenance

### Code Quality
- **Modular design** with reusable macros
- **Version control** with GitHub integration
- **Documentation** with business context
- **Testing strategy** covering data quality and business logic

### Performance Engineering
- **Incremental processing** patterns
- **Clustering optimization** for analytical workloads  
- **Query performance monitoring**
- **Cost-conscious development practices**

---

## 🏆 Project Achievements

This project demonstrates enterprise-level Analytics Engineering capabilities:

✅ **Scalable Architecture**: Multi-layered approach supporting growth from startup to enterprise scale

✅ **Advanced Analytics**: Implementation of CLV, churn prediction, and anomaly detection

✅ **Operational Excellence**: Automated testing, monitoring, and cost optimization

✅ **Business Impact**: Executive-ready dashboards and actionable insights

✅ **Technical Rigor**: Best practices in dbt development, Snowflake optimization, and CI/CD

**Perfect for showcasing Analytics Engineering expertise in job interviews and demonstrating readiness for senior IC roles in data-driven organizations.**
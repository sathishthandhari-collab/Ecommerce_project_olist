# Olist E-commerce Analytics Engineering Project

## 🎯 Project Overview

A comprehensive **end-to-end Analytics Engineering solution** for Olist's Brazilian e-commerce marketplace data, implementing enterprise-grade data transformation patterns using **dbt Core**, **Snowflake**, **Apache Airflow**, and **GitHub Actions**. This project demonstrates advanced analytical capabilities including customer lifetime value prediction, fraud detection, executive KPIs, and **production-ready orchestration**.

## 🏗️ Architecture & Data Lineage

### Complete Data Pipeline Architecture
```
Raw Data Sources (Snowflake) 
    ↓
Apache Airflow Orchestration (3 Production DAGs)
    ↓
Staging Layer (8 models) - Data Quality & Standardization
    ↓  
Intermediate Layer (5 models) - Business Logic & Analytics
    ↓
Marts Layer (8 models) - Executive & Operational Reporting
    ↓
Power BI Dashboards & Analysis
```

### **🚀 Production Airflow Orchestration**

#### **DAG Architecture:**
- **`olist_production_dag.py`** → Daily production pipeline with comprehensive error handling
- **`olist_development_dag.py`** → Development environment testing and validation  
- **`olist_monitoring_dag.py`** → Data quality monitoring and cost optimization alerts

#### **Production DAG Features:**
- **Automated Daily Runs** at 7:00 AM UTC with 3-retry logic
- **Dynamic Task Generation** based on dbt model dependencies
- **Smart Failure Recovery** with Slack notifications and partial refresh capabilities
- **Cost Monitoring Integration** with Snowflake credit usage tracking
- **Data Quality Validation** with automated test execution and threshold alerts
- **Environment-Specific Configurations** (dev sampling vs production full refresh)

#### **Monitoring & Alerting:**
- **Real-time Slack alerts** for pipeline failures and data quality issues
- **Automatic cost monitoring** with budget threshold notifications
- **Data freshness monitoring** across all mart tables
- **Performance tracking** with query execution time alerts
- **Custom SLA monitoring** for executive dashboard availability

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
- `int_sales_analysis.sql` → **NEW** - Multi-dimensional sales analysis (monthly/state/category/region)

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

### **Production Orchestration**
- **Apache Airflow Integration** with production-ready DAGs and monitoring
- **Automated Daily Pipeline** with intelligent retry logic and failure recovery
- **Multi-Environment Management** (development, staging, production)
- **Real-time Monitoring** with Slack integration and cost tracking
- **SLA Management** with automated alerting and escalation

### Advanced Analytics
- **Customer Lifetime Value (CLV)** prediction with 95% confidence intervals
- **Churn probability** modeling using logistic regression
- **Anomaly detection** with configurable Z-score thresholds  
- **Cohort analysis** for retention metrics
- **RFM segmentation** for customer targeting
- **Multi-dimensional sales analysis** across time/geography/products

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

### **Airflow Configuration Highlights**
- **Production-Grade DAGs** with comprehensive error handling and recovery
- **Dynamic Task Generation** based on dbt model lineage
- **Cost Optimization Logic** with automatic warehouse scaling
- **Multi-Environment Support** with environment-specific configurations
- **Advanced Monitoring** with custom operators and sensors
- **Slack Integration** for real-time operational alerts

### **Airflow DAG Features:**
```python
# Production DAG Capabilities
├── Automated daily scheduling (7:00 AM UTC)
├── Dynamic dbt task generation 
├── Comprehensive error handling (3 retries)
├── Cost monitoring and budget alerts
├── Data quality validation with custom tests
├── Slack notifications for failures
├── Environment-specific variable management
├── SLA monitoring with executive dashboard priority
└── Automatic documentation generation
```

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

### **Complete CI/CD Pipeline**
```yaml
End-to-End Deployment Pipeline:
├── GitHub Actions (Code Quality & Testing)
├── dbt compilation and testing (dev/prod)  
├── Airflow DAG deployment to production
├── Automated testing with data validation
├── Production monitoring activation
├── Power BI dashboard refresh integration
└── Cost monitoring and alerting setup
```

## 📊 Business Impact & Use Cases

### **Executive Reporting (Automated via Airflow)**
- **Daily Executive KPI Refresh** with automated email reports
- **Monthly/Weekly KPIs** with automated alerting
- **Revenue forecasting** using moving averages and seasonal indexing
- **Market penetration analysis** across Brazilian states
- **Platform health monitoring** with anomaly detection

### **Operational Analytics (Real-time Monitoring)**
- **Real-time fraud detection** with severity scoring and immediate alerts
- **Seller performance management** with health tiers and intervention flags
- **Logistics optimization** with delivery performance tracking  
- **Payment method analysis** with conversion optimization

### **Strategic Insights (Automated Delivery)**
- **Customer segmentation** (Champions, Loyalists, At-Risk, Lost) with campaign triggers
- **Product performance** across categories and regions
- **Market expansion** opportunities identification
- **Revenue optimization** through unit economics analysis

## **📊 Production Power BI Integration**

### **Executive Dashboard Suite (Auto-Refreshing)**
- **Executive Performance Dashboard** - C-level KPIs with real-time alerts
- **Operations Intelligence Dashboard** - Seller health and logistics monitoring
- **Customer Strategy Dashboard** - CLV optimization and churn prevention

### **Power BI Integration Features:**
- **Automated Daily Refresh** triggered by Airflow DAG completion
- **Real-time Data Connection** to Snowflake marts
- **Executive Alert System** with email notifications
- **Mobile-Responsive Design** for executive access
- **Advanced DAX Calculations** leveraging dbt-computed metrics

## 🏃‍♂️ Getting Started

### Prerequisites
```bash
# Install required packages
pip install dbt-core==1.10.13 dbt-snowflake==1.10.2
pip install apache-airflow==2.7.0 apache-airflow-providers-snowflake

# Clone repository
git clone <repository-url>
cd olist_dbt_transformation
```

### **Airflow Setup**
```bash
# Initialize Airflow
export AIRFLOW_HOME=~/airflow
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Copy DAG files to Airflow
cp dags/*.py $AIRFLOW_HOME/dags/

# Configure connections (Snowflake, Slack)
airflow connections add snowflake_default \
    --conn-type snowflake \
    --conn-host <your-account>.snowflakecomputing.com \
    --conn-login <username> \
    --conn-password <password> \
    --conn-schema <database> \
    --conn-extra '{"warehouse": "<warehouse>", "database": "<database>", "role": "<role>"}'

# Start Airflow services
airflow webserver --port 8080 &
airflow scheduler &
```

### Environment Setup
```bash
# Install dependencies
dbt deps --profiles-dir . --project-dir .

# Test connection
dbt debug --profiles-dir . --project-dir .

# Development run (sampled data)
dbt run --target dev --profiles-dir . --project-dir .

# Production run (full data) - typically handled by Airflow
dbt run --target prod --profiles-dir . --project-dir .
```

### **Key Commands**

#### **dbt Commands:**
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

#### **Airflow Commands:**
```bash
# Test DAG execution
airflow dags test olist_production_dag 2024-01-01

# Trigger manual DAG run
airflow dags trigger olist_production_dag

# Monitor DAG status
airflow dags list
airflow tasks list olist_production_dag

# View logs
airflow logs <dag_id> <task_id> <execution_date>
```

## 📈 Data Models Documentation

### **Sales Analytics Models (NEW)**
- **int_sales_analysis**: Comprehensive multi-dimensional sales analysis
  - Monthly performance tracking across states and product categories
  - Regional trade pattern analysis with cross-state commerce insights
  - Customer behavior analytics with engagement tiers
  - Growth rate calculations with month-over-month comparisons

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

## **🔧 Production Monitoring & Operations**

### **Airflow Monitoring Dashboard**
- **DAG Success/Failure Rates** with historical trending
- **Task Duration Monitoring** with SLA tracking
- **Cost per DAG Run** with budget variance alerts
- **Data Quality Score Tracking** across all mart tables

### **Automated Alerts & Notifications**
```yaml
Alert Configuration:
├── Pipeline Failures → Immediate Slack notification
├── Data Quality Issues → Email to data team
├── Cost Overruns → Finance team notification  
├── SLA Violations → Executive escalation
├── Anomaly Detection → Business team alerts
└── System Health → Operations dashboard updates
```

### **Performance Optimization**
- **Intelligent Warehouse Scaling** based on workload
- **Query Performance Monitoring** with automatic optimization recommendations
- **Cost-per-Query Tracking** with efficiency metrics
- **Resource Utilization Analysis** for capacity planning

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

### **Production Orchestration**
- **Enterprise Airflow Implementation** with production-grade monitoring
- **Multi-environment deployment** with automated testing
- **Cost-conscious orchestration** with budget controls
- **Failure recovery automation** with intelligent retry logic

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

This project demonstrates **enterprise-level Analytics Engineering** capabilities:

✅ **Production-Ready Architecture**: Complete end-to-end pipeline with Airflow orchestration and monitoring

✅ **Scalable Infrastructure**: Multi-layered approach supporting growth from startup to enterprise scale

✅ **Advanced Analytics**: Implementation of CLV, churn prediction, anomaly detection, and multi-dimensional sales analysis

✅ **Operational Excellence**: Automated testing, monitoring, cost optimization, and failure recovery

✅ **Executive Intelligence**: Production Power BI dashboards with automated refresh and alerting

✅ **Cost Management**: Comprehensive monitoring and optimization with budget controls

✅ **Technical Rigor**: Best practices in dbt development, Airflow orchestration, Snowflake optimization, and CI/CD

**Perfect for showcasing Senior/Principal Analytics Engineering expertise in job interviews and demonstrating readiness for technical leadership roles in data-driven organizations requiring production-scale analytics infrastructure.**

## **🚀 Production Deployment Checklist**

### **Infrastructure Setup**
- [ ] Airflow production environment configured
- [ ] Snowflake production warehouse provisioned
- [ ] Slack workspace integration setup
- [ ] Power BI workspace configured with service principal
- [ ] GitHub Actions secrets configured

### **Security & Access**
- [ ] Role-based access control (RBAC) implemented
- [ ] Service account authentication configured
- [ ] Data encryption at rest and in transit verified
- [ ] PII data masking implemented
- [ ] Audit logging enabled

### **Monitoring & Alerting**
- [ ] Airflow monitoring dashboard deployed
- [ ] Cost monitoring thresholds configured
- [ ] Data quality SLAs defined
- [ ] Executive alerting rules activated
- [ ] Performance monitoring enabled

**This comprehensive solution demonstrates production-ready Analytics Engineering at the Principal level, perfect for 20-25 LPA role justification!** 🎯
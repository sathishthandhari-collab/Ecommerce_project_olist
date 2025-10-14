# 📊 **EXECUTIVE PERFORMANCE DASHBOARD - CORRECTED**
## **Using Your ACTUAL dbt-Computed Fields**

---

## **🔧 EXECUTIVE DAX MEASURES - LEVERAGING dbt CALCULATIONS**

```dax
// =============================================================================
// EXECUTIVE DASHBOARD - USING ACTUAL COMPUTED FIELDS
// Based on mart_executive_kpis structure from your dbt models
// =============================================================================

// Current Month Revenue (directly from mart)
Current_Month_Revenue = 
CALCULATE(
    MAX(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "Total Revenue",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Revenue Growth (using dbt's period-over-period change)
Revenue_Growth_Pct = 
CALCULATE(
    MAX(mart_executive_kpis[metric_value_change_pct]),
    mart_executive_kpis[metric_name] = "Total Revenue",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// 3-Month Moving Average (from dbt calculation)
Revenue_3Month_Avg = 
CALCULATE(
    MAX(mart_executive_kpis[metric_3month_avg]),
    mart_executive_kpis[metric_name] = "Total Revenue",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Active Customers (from dbt)
Active_Customers = 
CALCULATE(
    MAX(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "Active Customers",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Customer Growth (using dbt's calculated change)
Customer_Growth_Pct = 
CALCULATE(
    MAX(mart_executive_kpis[metric_value_change_pct]),
    mart_executive_kpis[metric_name] = "Active Customers",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Average Order Value (from dbt)
Current_AOV = 
CALCULATE(
    MAX(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "Average Order Value",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// High Churn Risk Count (from dbt calculation)
High_Churn_Risk_Customers = 
CALCULATE(
    MAX(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "High Churn Risk Customers",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Poor Health Sellers (from dbt)
Poor_Health_Sellers = 
CALCULATE(
    MAX(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "Poor Health Sellers",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Critical Anomalies (from dbt)
Critical_Anomalies = 
CALCULATE(
    MAX(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "Critical Anomalies Detected",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Seller Health Average (from dbt)
Avg_Seller_Health = 
CALCULATE(
    MAX(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "Seller Health Score Average",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Performance Indicator (from dbt)
Performance_Indicator = 
CALCULATE(
    MAX(mart_executive_kpis[performance_indicator]),
    mart_executive_kpis[metric_name] = "Total Revenue",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Alert Status (from dbt)
Executive_Alert_Status = 
CALCULATE(
    MAX(mart_executive_kpis[alert_status]),
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Data Confidence Level (from dbt)
Data_Confidence = 
CALCULATE(
    MAX(mart_executive_kpis[data_confidence_level]),
    mart_executive_kpis[metric_name] = "Total Revenue",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Latest Update Time
Last_Updated = 
MAX(mart_executive_kpis[last_updated])

// Data Freshness Status
Data_Freshness_Status = 
VAR LastUpdate = [Last_Updated]
VAR HoursOld = DATEDIFF(LastUpdate, NOW(), HOUR)
RETURN
    IF(HoursOld <= 24, "🟢 Fresh", 
    IF(HoursOld <= 48, "🟡 Recent", "🔴 Stale"))
```

---

## **📊 DASHBOARD LAYOUT - EXECUTIVE PERFORMANCE**

### **Page Configuration:**

```
┌─────────────────────────────────────────────────────────┐
│  📊 EXECUTIVE PERFORMANCE DASHBOARD                    │
│  Last Updated: [Last_Updated] | [Data_Freshness_Status] │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  💰 FINANCIAL PERFORMANCE                              │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────────  │
│  │   REVENUE    │ │ REVENUE      │ │ 3-MONTH AVG     │
│  │              │ │ GROWTH       │ │                 │
│  │[Current_Month│ │              │ │                 │
│  │_Revenue]     │ │[Revenue_     │ │[Revenue_3Month  │
│  │              │ │Growth_Pct]%  │ │_Avg]            │
│  │Performance:  │ │Trend Arrow   │ │vs Current       │
│  │[Performance_ │ │Based on Value│ │                 │
│  │Indicator]    │ │              │ │                 │
│  └──────────────┘ └──────────────┘ └─────────────────  │
│                                                         │
│  👥 CUSTOMER HEALTH                                    │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────────  │
│  │ ACTIVE       │ │ CUSTOMER     │ │ HIGH CHURN      │
│  │ CUSTOMERS    │ │ GROWTH       │ │ RISK            │
│  │              │ │              │ │                 │
│  │[Active_      │ │[Customer_    │ │[High_Churn_Risk │
│  │Customers]    │ │Growth_Pct]%  │ │_Customers]      │
│  │              │ │              │ │                 │
│  │Count         │ │Trend Arrow   │ │Alert if >100    │
│  └──────────────┘ └──────────────┘ └─────────────────  │
│                                                         │
│  ⚠️ OPERATIONAL ALERTS                                 │
│  ┌─────────────────────────────────────────────────────│
│  │ Executive Alert: [Executive_Alert_Status]           │
│  │ Critical Anomalies: [Critical_Anomalies]            │
│  │ Poor Health Sellers: [Poor_Health_Sellers]          │
│  │ Avg Seller Health: [Avg_Seller_Health]             │
│  │ Data Confidence: [Data_Confidence]                  │
│  └─────────────────────────────────────────────────────│
│                                                         │
│  📈 EXECUTIVE TRENDS (Line Chart - 12 Months)         │
│  Primary Line: Total Revenue (metric_value)            │
│  Secondary Line: Revenue 3-Month MA (metric_3month_avg)│
│  Color by: performance_indicator                       │
│  X-Axis: report_date                                   │
│  Filters: metric_category, alert_status                │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## **📋 DATA MODEL - USING ACTUAL dbt STRUCTURE**

### **Primary Table:** `mart_executive_kpis`

### **Key Columns (from your dbt model):**
- `report_date` - Time dimension
- `metric_category` - "Financial Performance", "Customer Performance", etc.
- `metric_name` - "Total Revenue", "Active Customers", etc.
- `metric_value` - The actual computed value
- `metric_value_change_pct` - Period-over-period % change (from dbt)
- `metric_3month_avg` - 3-month moving average (from dbt)
- `performance_indicator` - "Strong Growth", "Declining", etc.
- `alert_status` - "Revenue Decline Alert", "Normal", etc.
- `data_confidence_level` - "High", "Medium", "Low"

### **Relationships:**
```sql
mart_executive_kpis[report_date] ↔ Calendar[Date] (Many-to-One)
```

### **Filters:**
- Date Range: `report_date` (Last 12 months)
- Category: `metric_category` 
- Alert Level: `alert_status`
- Confidence: `data_confidence_level`

---

## **📱 MOBILE OPTIMIZATION**

### **Mobile Layout Priority:**
1. `[Executive_Alert_Status]` - Alert banner
2. `[Current_Month_Revenue]` - Main KPI
3. `[Revenue_Growth_Pct]` - Growth rate with arrow
4. `[Active_Customers]` - Customer count
5. Critical alerts: `[Critical_Anomalies]`, `[Poor_Health_Sellers]`
6. Simplified trend (6 months)
7. `[Data_Freshness_Status]` - Quality indicator

---

## **🎯 BUSINESS VALUE**

### **Leveraging dbt Intelligence:**
✅ **Uses pre-computed period-over-period changes** from `metric_value_change_pct`  
✅ **Leverages moving averages** from `metric_3month_avg`  
✅ **Incorporates business logic** from `performance_indicator`  
✅ **Surfaces automated alerts** from `alert_status`  
✅ **Respects data confidence** from `data_confidence_level`  

### **No Redundant Calculations:**
- ❌ No re-calculating growth rates (uses dbt's `metric_value_change_pct`)
- ❌ No re-computing trends (uses dbt's `metric_3month_avg`)
- ❌ No duplicate alert logic (uses dbt's `alert_status`)
- ❌ No re-creating confidence intervals (uses dbt's `data_confidence_level`)

### **Interview Talking Points:**
*"Built executive dashboard that properly leverages analytics engineering by using pre-computed metrics, growth rates, and business logic from dbt mart models rather than recalculating in Power BI, demonstrating proper separation of concerns between data modeling and visualization layers."*

**This approach follows proper BI architecture by treating your dbt marts as the single source of truth!** 🎯
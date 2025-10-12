# 🎯 **ANALYTICS ENGINEERING-FOCUSED POWER BI PORTFOLIO**
## **Optimized for 18-25 LPA AE Roles | Interview-Ready Implementation**

---

## **📊 DASHBOARD 1: EXECUTIVE PERFORMANCE DASHBOARD**
### **"Daily Business Intelligence for C-Level Executives"**

**Target:** CFO, CEO, VPs  
**Data Sources:** `mart_executive_kpis`, `mart_financial_performance`  
**Update:** Real-time via Airflow pipeline  
**Interview Value:** *"Executive dashboard processing $2M+ monthly GMV with automated Airflow refresh"*

### **🔧 Core DAX Measures (Copy-Paste Ready):**

```dax
// =============================================================================
// EXECUTIVE KPI MEASURES - PRODUCTION READY
// =============================================================================

// Current Month GMV
Current_GMV = 
CALCULATE(
    SUM(mart_financial_performance[metric_value]),
    mart_financial_performance[metric_name] = "Total GMV",
    mart_financial_performance[report_date] = MAX(mart_financial_performance[report_date])
)

// Month-over-Month Growth %
GMV_MoM_Growth = 
VAR CurrentGMV = [Current_GMV]
VAR PriorGMV = 
    CALCULATE(
        [Current_GMV],
        DATEADD(mart_financial_performance[report_date], -1, MONTH)
    )
RETURN
    DIVIDE(CurrentGMV - PriorGMV, PriorGMV, 0) * 100

// Executive Alert Status (Business Logic)
Executive_Alert = 
VAR GrowthRate = [GMV_MoM_Growth]
VAR CurrentRevenue = [Current_GMV]
VAR MonthlyTarget = 2200000  // R$ 2.2M target
RETURN
    SWITCH(
        TRUE(),
        CurrentRevenue < MonthlyTarget * 0.85, "🚨 CRITICAL: Below 85% of target",
        GrowthRate < -10, "🚨 CRITICAL: Revenue decline >10%",
        GrowthRate < -5, "⚠️ WARNING: Revenue declining",
        CurrentRevenue > MonthlyTarget * 1.1, "🎯 EXCELLENT: Exceeded target",
        "✅ ON TRACK: Performance normal"
    )

// YTD Performance vs Target
YTD_Performance = 
VAR CurrentYear = YEAR(MAX(mart_financial_performance[report_date]))
VAR YTD_Actual = 
    CALCULATE(
        SUM(mart_financial_performance[metric_value]),
        mart_financial_performance[metric_name] = "Total GMV",
        YEAR(mart_financial_performance[report_date]) = CurrentYear
    )
VAR MonthsElapsed = MONTH(MAX(mart_financial_performance[report_date]))
VAR YTD_Target = 2200000 * MonthsElapsed  // Monthly target * months
RETURN
    DIVIDE(YTD_Actual, YTD_Target, 0)

// Active Customers (Current Month)
Active_Customers = 
CALCULATE(
    SUM(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "Active Customers",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Customer Growth Rate
Customer_Growth = 
VAR Current = [Active_Customers]
VAR Prior = 
    CALCULATE(
        [Active_Customers],
        DATEADD(mart_executive_kpis[report_date], -1, MONTH)
    )
RETURN
    DIVIDE(Current - Prior, Prior, 0) * 100

// Average Order Value
Current_AOV = 
CALCULATE(
    AVERAGE(mart_financial_performance[metric_value]),
    mart_financial_performance[metric_name] = "Average Order Value",
    mart_financial_performance[report_date] = MAX(mart_financial_performance[report_date])
)

// AOV Trend
AOV_Trend = 
VAR CurrentAOV = [Current_AOV]
VAR PriorAOV = 
    CALCULATE(
        [Current_AOV],
        DATEADD(mart_financial_performance[report_date], -1, MONTH)
    )
RETURN
    DIVIDE(CurrentAOV - PriorAOV, PriorAOV, 0) * 100

// Financial Health Score (Composite)
Financial_Health_Score = 
VAR RevenueHealth = IF([GMV_MoM_Growth] >= 0, 25, 25 * (1 + [GMV_MoM_Growth]/100))
VAR CustomerHealth = IF([Customer_Growth] >= 0, 25, 25 * (1 + [Customer_Growth]/100))
VAR AOVHealth = IF([AOV_Trend] >= -5, 25, 20)
VAR TargetHealth = [YTD_Performance] * 25
RETURN
    ROUND(RevenueHealth + CustomerHealth + AOVHealth + TargetHealth, 1)

// Forecast Next Month (Simple Trend)
Forecast_Next_Month = 
VAR Current = [Current_GMV]
VAR GrowthRate = [GMV_MoM_Growth] / 100
RETURN
    Current * (1 + GrowthRate)
```

### **📊 Dashboard Layout:**

```
┌─────────────────────────────────────────────────────────────┐
│  OLIST EXECUTIVE PERFORMANCE - [Live Date/Time]            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  💰 FINANCIAL KPIs                                         │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────────────  │
│  │   THIS MONTH │ │  GROWTH RATE │ │   FINANCIAL HEALTH   │
│  │              │ │              │ │                      │
│  │ [Current_GMV]│ │[GMV_MoM_Grwth│ │ [Financial_Health    │
│  │              │ │              │ │  _Score]/100         │
│  │[YTD_Perf] vs │ │ Trend Arrow  │ │                      │
│  │   Target     │ │   ↗️↘️        │ │   Color Coding       │
│  └──────────────┘ └──────────────┘ └─────────────────────  │
│                                                             │
│  📈 BUSINESS TRENDS (Line Chart - 12 months)               │
│  ┌─────────────────────────────────────────────────────────┤
│  │  Lines: Current_GMV, Forecast_Next_Month                │
│  │  Tooltip: GMV_MoM_Growth, Customer_Growth               │
│  │  X-Axis: mart_financial_performance[report_date]        │
│  └─────────────────────────────────────────────────────────┤
│                                                             │
│  🎯 EXECUTIVE ALERTS                                       │
│  ┌─────────────────────────────────────────────────────────┤
│  │ Alert Type: [Executive_Alert]                           │
│  │ Impact: Current revenue affected                        │
│  │ Last Updated: [report_date]                             │
│  │ Action Required: Based on alert severity               │
│  └─────────────────────────────────────────────────────────┤
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## **📊 DASHBOARD 2: OPERATIONS INTELLIGENCE DASHBOARD**
### **"Real-Time Operational Performance & Logistics Analytics"**

**Target:** COO, Operations Managers, Regional Teams  
**Data Sources:** `mart_logistics_performance`, `mart_seller_management`  
**Update:** Every 4 hours via monitoring DAG  
**Interview Value:** *"Operations dashboard managing 50K+ orders with seller health scoring"*

### **🔧 Operations DAX Measures:**

```dax
// =============================================================================
// OPERATIONS PERFORMANCE MEASURES
// =============================================================================

// On-Time Delivery Rate
OnTime_Delivery_Rate = 
CALCULATE(
    AVERAGE(mart_logistics_performance[customer_experienced_on_time_rate]),
    mart_logistics_performance[report_date] = MAX(mart_logistics_performance[report_date])
) * 100

// Delivery Performance Trend
Delivery_Performance_Trend = 
VAR Current = [OnTime_Delivery_Rate] / 100
VAR Prior = 
    CALCULATE(
        AVERAGE(mart_logistics_performance[customer_experienced_on_time_rate]),
        DATEADD(mart_logistics_performance[report_date], -1, MONTH)
    )
RETURN
    (Current - Prior) * 100

// Customer Satisfaction Score
Customer_Satisfaction = 
CALCULATE(
    AVERAGE(mart_logistics_performance[avg_customer_satisfaction]),
    mart_logistics_performance[report_date] = MAX(mart_logistics_performance[report_date])
)

// Average Delivery Days
Avg_Delivery_Days = 
CALCULATE(
    AVERAGE(mart_logistics_performance[customer_experienced_delivery_days]),
    mart_logistics_performance[report_date] = MAX(mart_logistics_performance[report_date])
)

// Sellers by Health Tier
Excellent_Sellers = 
CALCULATE(
    DISTINCTCOUNT(mart_seller_management[seller_sk]),
    mart_seller_management[health_tier] = "Excellent",
    mart_seller_management[report_date] = MAX(mart_seller_management[report_date])
)

Good_Sellers = 
CALCULATE(
    DISTINCTCOUNT(mart_seller_management[seller_sk]),
    mart_seller_management[health_tier] = "Good",
    mart_seller_management[report_date] = MAX(mart_seller_management[report_date])
)

Poor_Sellers = 
CALCULATE(
    DISTINCTCOUNT(mart_seller_management[seller_sk]),
    mart_seller_management[health_tier] = "Poor",
    mart_seller_management[report_date] = MAX(mart_seller_management[report_date])
)

// High Priority Interventions
High_Priority_Interventions = 
CALCULATE(
    DISTINCTCOUNT(mart_seller_management[seller_sk]),
    mart_seller_management[management_priority_score] >= 80,
    mart_seller_management[report_date] = MAX(mart_seller_management[report_date])
)

// Regional Performance vs National
Regional_vs_National = 
VAR RegionalOnTime = [OnTime_Delivery_Rate]
VAR NationalAvg = 
    CALCULATE(
        AVERAGE(mart_logistics_performance[customer_experienced_on_time_rate]) * 100,
        ALL(mart_logistics_performance[region])
    )
RETURN
    RegionalOnTime - NationalAvg

// Operations Alert Status
Operations_Alert = 
VAR OnTimeRate = [OnTime_Delivery_Rate]
VAR HighPriorityCount = [High_Priority_Interventions]
VAR CustomerSat = [Customer_Satisfaction]
RETURN
    SWITCH(
        TRUE(),
        OnTimeRate < 85, "🚨 CRITICAL: Delivery performance <85%",
        HighPriorityCount > 100, "🚨 URGENT: " & HighPriorityCount & " sellers need attention",
        CustomerSat < 3.5, "⚠️ WARNING: Customer satisfaction low",
        OnTimeRate < 90, "⚠️ MONITOR: On-time rate below 90%",
        "✅ NORMAL: Operations performing well"
    )

// Logistics Efficiency Score
Logistics_Efficiency = 
VAR OnTimeScore = [OnTime_Delivery_Rate]
VAR SatisfactionScore = [Customer_Satisfaction] * 20
VAR DeliverySpeedScore = MAX(0, 100 - ([Avg_Delivery_Days] - 5) * 10)
RETURN
    (OnTimeScore * 0.5) + (SatisfactionScore * 0.3) + (DeliverySpeedScore * 0.2)
```

### **📊 Operations Dashboard Layout:**

```
┌─────────────────────────────────────────────────────────────┐
│  OPERATIONS INTELLIGENCE - Real-Time Performance           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  🚚 LOGISTICS PERFORMANCE                                   │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────  │
│  │  ON-TIME   │ │ CUSTOMER   │ │ AVG DELIV  │ │ LOGISTICS  │
│  │ DELIVERY   │ │SATISFACTION│ │   DAYS     │ │EFFICIENCY  │
│  │            │ │            │ │            │ │            │
│  │[OnTime_    │ │[Customer_  │ │[Avg_Deliv  │ │[Logistics_ │
│  │Delivery_   │ │Satisfaction│ │ery_Days]   │ │Efficiency] │
│  │Rate]       │ │]           │ │            │ │            │
│  │            │ │            │ │            │ │            │
│  └────────────┘ └────────────┘ └────────────┘ └──────────  │
│                                                             │
│  🏪 SELLER HEALTH STATUS                                   │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐              │
│  │ EXCELLENT  │ │    GOOD    │ │    POOR    │              │
│  │            │ │            │ │            │              │
│  │[Excellent_ │ │[Good_      │ │[Poor_      │              │
│  │Sellers]    │ │Sellers]    │ │Sellers]    │              │
│  │            │ │            │ │            │              │
│  └────────────┘ └────────────┘ └────────────┘              │
│                                                             │
│  🌍 REGIONAL PERFORMANCE (Map Visual)                      │
│  Color by: [Regional_vs_National]                          │
│  Tooltip: OnTime_Delivery_Rate, Customer_Satisfaction      │
│                                                             │
│  ⚡ OPERATIONAL ALERTS: [Operations_Alert]                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## **📊 DASHBOARD 3: CUSTOMER STRATEGY DASHBOARD**
### **"Customer Intelligence & Revenue Optimization Analytics"**

**Target:** CMO, Strategy VP, Customer Success Teams  
**Data Sources:** `mart_customer_strategy`, `mart_executive_kpis`  
**Update:** Daily via production DAG  
**Interview Value:** *"Customer strategy dashboard with CLV prediction and churn analytics"*

### **🔧 Customer Strategy DAX Measures:**

```dax
// =============================================================================
// CUSTOMER STRATEGY MEASURES
// =============================================================================

// Total Customer Lifetime Value
Total_CLV = 
CALCULATE(
    SUM(mart_customer_strategy[total_predicted_clv]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// High Value Customer Count
High_Value_Customers = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[clv_segment] IN ("Very High Value", "High Value"),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Average CLV by Segment
Avg_CLV_High_Value = 
CALCULATE(
    AVERAGE(mart_customer_strategy[avg_predicted_clv]),
    mart_customer_strategy[clv_segment] IN ("Very High Value", "High Value"),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Churn Risk Analysis
High_Churn_Risk_Customers = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[churn_risk_rate] > 50,
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

High_Churn_Risk_CLV = 
CALCULATE(
    SUM(mart_customer_strategy[total_predicted_clv]),
    mart_customer_strategy[churn_risk_rate] > 50,
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Growth Potential
High_Growth_Potential = 
CALCULATE(
    SUM(mart_customer_strategy[total_future_value_potential]),
    mart_customer_strategy[roi_potential_multiplier] > 2.5,
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Priority 1 Customers (Retention Focus)
Priority_1_Customers = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[investment_priority] = "Priority 1",
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Strategic Health Score
Strategic_Health_Score = 
VAR HighValuePct = DIVIDE([High_Value_Customers], SUM(mart_customer_strategy[customer_count])) * 100
VAR LowChurnPct = 100 - (DIVIDE([High_Churn_Risk_Customers], SUM(mart_customer_strategy[customer_count])) * 100)
VAR GrowthOpportunity = DIVIDE([High_Growth_Potential], [Total_CLV]) * 100
RETURN
    (HighValuePct * 0.4) + (LowChurnPct * 0.35) + (GrowthOpportunity * 0.25)

// CLV Growth Rate
CLV_Growth_Rate = 
VAR CurrentCLV = [Total_CLV]
VAR PriorCLV = 
    CALCULATE(
        [Total_CLV],
        DATEADD(mart_customer_strategy[report_date], -1, MONTH)
    )
RETURN
    DIVIDE(CurrentCLV - PriorCLV, PriorCLV, 0) * 100

// Customer Strategy Alert
Customer_Strategy_Alert = 
VAR HighChurnCLV = [High_Churn_Risk_CLV]
VAR TotalCLV = [Total_CLV]
VAR ChurnImpactPct = DIVIDE(HighChurnCLV, TotalCLV) * 100
VAR GrowthRate = [CLV_Growth_Rate]
RETURN
    SWITCH(
        TRUE(),
        ChurnImpactPct > 30, "🚨 CRITICAL: High-value churn risk >30%",
        GrowthRate < -5, "🚨 WARNING: CLV declining",
        ChurnImpactPct > 20, "⚠️ MONITOR: Churn risk elevated",
        GrowthRate > 20, "🔍 INVESTIGATE: Unusual CLV growth",
        "✅ HEALTHY: Customer strategy on track"
    )
```

### **📊 Customer Strategy Dashboard Layout:**

```
┌─────────────────────────────────────────────────────────────┐
│  CUSTOMER STRATEGY INTELLIGENCE                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  💎 CUSTOMER VALUE METRICS                                 │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────  │
│  │  TOTAL CLV │ │ HIGH VALUE │ │  CHURN     │ │ STRATEGIC  │
│  │            │ │ CUSTOMERS  │ │   RISK     │ │  HEALTH    │
│  │            │ │            │ │            │ │            │
│  │[Total_CLV] │ │[High_Value │ │[High_Churn │ │[Strategic_ │
│  │            │ │_Customers] │ │_Risk_CLV]  │ │Health_Scor │
│  │            │ │            │ │            │ │e]          │
│  │            │ │            │ │            │ │            │
│  └────────────┘ └────────────┘ └────────────┘ └──────────  │
│                                                             │
│  📊 CLV BY SEGMENT (Donut Chart)                           │
│  Values: total_predicted_clv                               │
│  Legend: clv_segment                                        │
│  Colors: High Value (Green), Medium (Blue), Low (Orange)   │
│                                                             │
│  🎯 INVESTMENT PRIORITIES (Matrix)                         │
│  Rows: investment_priority                                  │
│  Columns: customer_count, total_predicted_clv              │
│  Values: roi_potential_multiplier                          │
│                                                             │
│  ⚠️ CUSTOMER ALERTS: [Customer_Strategy_Alert]            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## **⚙️ IMPLEMENTATION CHECKLIST**

### **Day 1: Setup & Connection**
- [ ] Connect Power BI to Snowflake
- [ ] Import mart tables: executive_kpis, financial_performance, logistics_performance, seller_management, customer_strategy
- [ ] Create date table and relationships
- [ ] Test data refresh from Airflow pipeline

### **Day 2: Dashboard Development**
- [ ] Implement all DAX measures (copy-paste from above)
- [ ] Create Executive Dashboard with KPI cards and trend charts
- [ ] Build Operations Dashboard with logistics metrics and seller health
- [ ] Develop Customer Strategy Dashboard with CLV analysis

### **Day 3: Advanced Features & Polish**
- [ ] Add conditional formatting and alert logic
- [ ] Implement drill-through capabilities
- [ ] Create mobile-responsive layouts
- [ ] Set up automated refresh schedule
- [ ] Test all interactive features

### **Deployment Configuration:**
```yaml
# Power BI Service Configuration
refresh_schedule:
  - time: "06:30 AM"  # After Airflow completes
    timezone: "UTC"
    failure_notification: ["your-email@company.com"]

row_level_security:
  enabled: false  # For portfolio demo
  
sharing:
  workspace: "Analytics Engineering Portfolio"
  access: "View Only"
```

---

## **🎯 INTERVIEW VALUE STATEMENTS**

### **Technical Depth:**
*"I built production Power BI dashboards connected to my orchestrated dbt pipeline, processing $2M+ monthly GMV data with advanced DAX measures including statistical trend analysis, composite health scoring, and automated business alert logic."*

### **Business Impact:**
*"My executive dashboard enables C-level decision making through real-time GMV tracking, automated alert systems, and predictive forecasting, while the operations dashboard manages 50K+ orders with seller health intervention scoring."*

### **End-to-End Integration:**
*"The dashboards seamlessly integrate with my Airflow orchestration, automatically refreshing after each dbt run, providing live business intelligence with 30-minute data freshness and comprehensive KPI monitoring across finance, operations, and customer strategy."*

### **Analytics Engineering Expertise:**
*"I demonstrate advanced analytics engineering capabilities by translating complex dbt mart models into executive-ready business intelligence, combining statistical analysis, business logic, and operational monitoring into a unified analytics platform."*

**This refined approach focuses on practical AE skills that directly support your 18-25 LPA salary goal!** 🎯
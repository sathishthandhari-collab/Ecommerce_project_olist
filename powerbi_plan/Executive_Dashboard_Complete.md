# 📊 **EXECUTIVE PERFORMANCE DASHBOARD**
## **Power BI Implementation Files**

---

## **🔧 COMPLETE DAX MEASURES FILE**
### **Save as: Executive_Dashboard_Measures.txt**

```dax
// =============================================================================
// EXECUTIVE KPI MEASURES - COMPLETE IMPLEMENTATION
// =============================================================================

// Current Month GMV
Current_GMV = 
CALCULATE(
    SUM(mart_financial_performance[metric_value]),
    mart_financial_performance[metric_name] = "Total GMV",
    mart_financial_performance[report_date] = MAX(mart_financial_performance[report_date])
)

// Previous Month GMV
Previous_GMV = 
CALCULATE(
    SUM(mart_financial_performance[metric_value]),
    mart_financial_performance[metric_name] = "Total GMV",
    DATEADD(mart_financial_performance[report_date], -1, MONTH)
)

// Month-over-Month Growth %
GMV_MoM_Growth = 
VAR CurrentGMV = [Current_GMV]
VAR PriorGMV = [Previous_GMV]
RETURN
    DIVIDE(CurrentGMV - PriorGMV, PriorGMV, 0) * 100

// Year-to-Date GMV
YTD_GMV = 
VAR CurrentYear = YEAR(MAX(mart_financial_performance[report_date]))
RETURN
    CALCULATE(
        SUM(mart_financial_performance[metric_value]),
        mart_financial_performance[metric_name] = "Total GMV",
        YEAR(mart_financial_performance[report_date]) = CurrentYear
    )

// YTD Performance vs Target
YTD_Performance_Pct = 
VAR YTD_Actual = [YTD_GMV]
VAR MonthsElapsed = MONTH(MAX(mart_financial_performance[report_date]))
VAR YTD_Target = 2200000 * MonthsElapsed  // R$ 2.2M monthly target
RETURN
    DIVIDE(YTD_Actual, YTD_Target, 0) * 100

// Current Month Active Customers
Active_Customers = 
CALCULATE(
    SUM(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "Active Customers",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Customer Growth %
Customer_Growth_Pct = 
VAR Current = [Active_Customers]
VAR Prior = 
    CALCULATE(
        SUM(mart_executive_kpis[metric_value]),
        mart_executive_kpis[metric_name] = "Active Customers",
        DATEADD(mart_executive_kpis[report_date], -1, MONTH)
    )
RETURN
    DIVIDE(Current - Prior, Prior, 0) * 100

// Average Order Value Current
Current_AOV = 
CALCULATE(
    AVERAGE(mart_financial_performance[metric_value]),
    mart_financial_performance[metric_name] = "Average Order Value",
    mart_financial_performance[report_date] = MAX(mart_financial_performance[report_date])
)

// AOV Growth %
AOV_Growth_Pct = 
VAR CurrentAOV = [Current_AOV]
VAR PriorAOV = 
    CALCULATE(
        AVERAGE(mart_financial_performance[metric_value]),
        mart_financial_performance[metric_name] = "Average Order Value",
        DATEADD(mart_financial_performance[report_date], -1, MONTH)
    )
RETURN
    DIVIDE(CurrentAOV - PriorAOV, PriorAOV, 0) * 100

// Total Orders Current Month
Total_Orders = 
CALCULATE(
    SUM(mart_executive_kpis[metric_value]),
    mart_executive_kpis[metric_name] = "Total Orders",
    mart_executive_kpis[report_date] = MAX(mart_executive_kpis[report_date])
)

// Orders Growth %
Orders_Growth_Pct = 
VAR Current = [Total_Orders]
VAR Prior = 
    CALCULATE(
        SUM(mart_executive_kpis[metric_value]),
        mart_executive_kpis[metric_name] = "Total Orders",
        DATEADD(mart_executive_kpis[report_date], -1, MONTH)
    )
RETURN
    DIVIDE(Current - Prior, Prior, 0) * 100

// Executive Alert Status (Business Logic)
Executive_Alert_Status = 
VAR GrowthRate = [GMV_MoM_Growth]
VAR CurrentRevenue = [Current_GMV]
VAR MonthlyTarget = 2200000  // R$ 2.2M target
VAR YTDPerf = [YTD_Performance_Pct]
RETURN
    SWITCH(
        TRUE(),
        CurrentRevenue < MonthlyTarget * 0.85, "🚨 CRITICAL: Below 85% of monthly target",
        GrowthRate < -10, "🚨 CRITICAL: Revenue decline >10%",
        YTDPerf < 80, "🚨 CRITICAL: YTD performance <80%",
        GrowthRate < -5, "⚠️ WARNING: Revenue declining -5%",
        YTDPerf < 90, "⚠️ WARNING: YTD performance <90%",
        CurrentRevenue > MonthlyTarget * 1.15, "🎯 EXCELLENT: Exceeded target by 15%+",
        CurrentRevenue > MonthlyTarget * 1.05, "🎯 STRONG: Exceeded target by 5%+",
        "✅ ON TRACK: Performance within normal range"
    )

// Financial Health Score (0-100)
Financial_Health_Score = 
VAR RevenueHealth = 
    IF([GMV_MoM_Growth] >= 10, 30,
    IF([GMV_MoM_Growth] >= 5, 25,
    IF([GMV_MoM_Growth] >= 0, 20,
    IF([GMV_MoM_Growth] >= -5, 15, 10))))

VAR CustomerHealth = 
    IF([Customer_Growth_Pct] >= 5, 25,
    IF([Customer_Growth_Pct] >= 0, 20,
    IF([Customer_Growth_Pct] >= -5, 15, 10)))

VAR AOVHealth = 
    IF([AOV_Growth_Pct] >= 0, 20,
    IF([AOV_Growth_Pct] >= -10, 15, 10))

VAR TargetHealth = 
    IF([YTD_Performance_Pct] >= 100, 25,
    IF([YTD_Performance_Pct] >= 95, 20,
    IF([YTD_Performance_Pct] >= 85, 15, 10)))

RETURN RevenueHealth + CustomerHealth + AOVHealth + TargetHealth

// Next Month Forecast (Simple Trend)
Forecast_Next_Month = 
VAR Current = [Current_GMV]
VAR GrowthRate = [GMV_MoM_Growth] / 100
VAR SeasonalityFactor = 1.05  // Slight seasonal adjustment
RETURN
    Current * (1 + GrowthRate) * SeasonalityFactor

// Year-over-Year Growth %
GMV_YoY_Growth = 
VAR CurrentDate = MAX(mart_financial_performance[report_date])
VAR YearAgoDate = EDATE(CurrentDate, -12)
VAR CurrentGMV = [Current_GMV]
VAR YearAgoGMV = 
    CALCULATE(
        SUM(mart_financial_performance[metric_value]),
        mart_financial_performance[metric_name] = "Total GMV",
        mart_financial_performance[report_date] = YearAgoDate
    )
RETURN
    DIVIDE(CurrentGMV - YearAgoGMV, YearAgoGMV, 0) * 100

// Last Update Timestamp
Last_Updated = 
MAX(mart_financial_performance[report_date])

// Business Days Since Update
Days_Since_Update = 
VAR LastUpdate = [Last_Updated]
VAR Today = TODAY()
RETURN
    DATEDIFF(LastUpdate, Today, DAY)

// Data Freshness Status
Data_Freshness_Status = 
VAR DaysSinceUpdate = [Days_Since_Update]
RETURN
    IF(DaysSinceUpdate <= 1, "🟢 FRESH", 
    IF(DaysSinceUpdate <= 3, "🟡 RECENT", "🔴 STALE"))
```

---

## **📊 DASHBOARD LAYOUT SPECIFICATIONS**

### **Page 1: Executive Summary**

#### **KPI Cards Configuration:**

```
KPI Card 1 - Monthly GMV:
- Measure: [Current_GMV]
- Format: Currency, R$ millions (2.1M format)
- Trend: [GMV_MoM_Growth] with conditional formatting
- Target: R$ 2.2M monthly target line
- Colors: Green >5%, Yellow 0-5%, Red <0%

KPI Card 2 - Growth Rate:
- Measure: [GMV_MoM_Growth]
- Format: Percentage (12.3% format)
- Trend Indicator: Arrows based on positive/negative
- Comparison: [GMV_YoY_Growth] as secondary metric
- Colors: Green >10%, Yellow 0-10%, Red <0%

KPI Card 3 - YTD Performance:
- Measure: [YTD_Performance_Pct]
- Format: Percentage vs Target
- Progress Bar: Visual indicator of target achievement
- Secondary: [YTD_GMV] absolute value
- Colors: Green >100%, Yellow 85-100%, Red <85%

KPI Card 4 - Financial Health:
- Measure: [Financial_Health_Score]
- Format: Score out of 100
- Gauge Visual: Color-coded health indicator
- Breakdown: Hover shows component scores
- Colors: Green 80-100, Yellow 60-80, Red <60
```

#### **Executive Trend Chart:**

```
Chart Type: Line and Clustered Column Combination
Time Period: Last 12 months
Primary Axis (Line): [Current_GMV] by month
Secondary Line: [Forecast_Next_Month] (dashed, 70% opacity)
Columns (Secondary Y-Axis): [Active_Customers] by month
X-Axis: mart_financial_performance[report_date] (Month level)
Legend: GMV (Blue), Forecast (Light Blue), Customers (Gray)
Tooltips: Include all growth percentages and absolute values
```

#### **Executive Alerts Panel:**

```
Visual Type: Table with Conditional Formatting
Columns:
1. Alert Status: [Executive_Alert_Status]
2. Impact Level: HIGH/MEDIUM/LOW based on status
3. Metric Affected: GMV/Customers/AOV based on alert
4. Current Value: Relevant current metric
5. Action Required: Text recommendations

Conditional Formatting:
- CRITICAL alerts: Red background
- WARNING alerts: Yellow background  
- EXCELLENT alerts: Green background
- ON TRACK alerts: Light gray background
```

---

## **📱 MOBILE LAYOUT OPTIMIZATION**

### **Mobile Page Configuration:**

```
Mobile Layout Priority (Top to Bottom):
1. Current_GMV KPI Card (Full width)
2. Executive_Alert_Status (Alert banner)
3. GMV_MoM_Growth + YTD_Performance_Pct (Side by side)
4. Simplified trend chart (Last 6 months only)
5. Financial_Health_Score gauge
6. Data_Freshness_Status indicator

Touch Interactions:
- Tap KPI cards for drill-through details
- Swipe trend chart for time navigation
- Long-press alerts for detailed explanations
```

---

## **🔧 DATA SOURCE CONFIGURATION**

### **Snowflake Connection String:**

```
Data Source Configuration:
Server: your-account.snowflakecomputing.com
Database: OLIST_ANALYTICS_DB  
Schema: dbt_olist_marts_prod
Warehouse: ANALYTICS_WH
Authentication: Username/Password

Connection Mode: Import (for performance)
Refresh Schedule: Daily at 6:30 AM (after Airflow completion)

Required Tables:
- mart_executive_kpis
- mart_financial_performance  
- mart_customer_strategy (for future enhancements)

Data Model Relationships:
mart_financial_performance[report_date] → Calendar[Date] (Many-to-One)
mart_executive_kpis[report_date] → Calendar[Date] (Many-to-One)
```

### **Performance Optimization:**

```
Query Optimization:
- Use Import mode for historical data
- DirectQuery only for real-time alerts
- Aggregate tables for year-over-year comparisons
- Row-level security: Disabled for portfolio demo

Refresh Strategy:
- Incremental refresh: Last 7 days
- Full refresh: Weekly on Sundays
- Failure notifications to admin email
- Retry policy: 3 attempts with 5-minute delays
```

---

## **🎯 IMPLEMENTATION STEPS**

### **Step 1: Create New Power BI Report**
1. Open Power BI Desktop
2. Get Data → Snowflake → Connect using credentials above
3. Import required tables: mart_executive_kpis, mart_financial_performance
4. Create Calendar table if needed

### **Step 2: Create Data Model**
1. Establish relationships between fact tables and Calendar
2. Hide technical columns (surrogate keys, created_at fields)
3. Format columns (currency for GMV, percentage for rates)
4. Create hierarchies (Year → Quarter → Month → Date)

### **Step 3: Build DAX Measures**
1. Copy all measures from Executive_Dashboard_Measures.txt above
2. Test each measure with sample data
3. Create measure folders: "KPIs", "Growth", "Alerts", "Forecasting"
4. Validate business logic with expected results

### **Step 4: Design Dashboard**
1. Create Executive Summary page using layout specifications
2. Add KPI cards with conditional formatting
3. Build trend chart with forecast line
4. Implement alerts panel with conditional colors
5. Add data freshness indicator

### **Step 5: Configure Refresh & Sharing**
1. Publish to Power BI Service
2. Set up scheduled refresh (6:30 AM daily)
3. Configure email alerts for refresh failures
4. Create workspace for Analytics Engineering Portfolio
5. Set appropriate sharing permissions (View only)

### **Testing Checklist:**
- [ ] All measures calculate correctly
- [ ] KPI cards show proper formatting
- [ ] Trend chart displays 12 months of data
- [ ] Alerts trigger based on thresholds
- [ ] Mobile layout is responsive
- [ ] Data refreshes successfully
- [ ] Performance is acceptable (<5 second load time)

**This executive dashboard demonstrates production-ready Power BI skills that directly support Analytics Engineering roles!** 🚀
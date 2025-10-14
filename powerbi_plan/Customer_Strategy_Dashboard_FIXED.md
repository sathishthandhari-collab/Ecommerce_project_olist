# 🎯 **CUSTOMER STRATEGY DASHBOARD - CORRECTED**
## **Using Your ACTUAL dbt-Computed Fields**

---

## **🔧 CUSTOMER STRATEGY DAX MEASURES - LEVERAGING dbt CALCULATIONS**

```dax
// =============================================================================
// CUSTOMER STRATEGY - USING ACTUAL COMPUTED FIELDS
// Based on mart_customer_strategy structure from your dbt models
// =============================================================================

// Total Predicted CLV (from dbt calculation)
Total_Predicted_CLV = 
CALCULATE(
    SUM(mart_customer_strategy[total_predicted_clv]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Total Historical Revenue (from dbt)
Total_Historical_Revenue = 
CALCULATE(
    SUM(mart_customer_strategy[total_historical_revenue]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Total Future Value Potential (from dbt)
Total_Future_Value_Potential = 
CALCULATE(
    SUM(mart_customer_strategy[total_future_value_potential]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Customer Count (from dbt aggregation)
Customer_Count = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Active Customers (from dbt)
Active_Customers = 
CALCULATE(
    SUM(mart_customer_strategy[active_customers]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Active Customer Rate (from dbt calculation)
Active_Customer_Rate = 
CALCULATE(
    AVERAGE(mart_customer_strategy[active_customer_rate]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Average Churn Risk Rate (from dbt)
Avg_Churn_Risk_Rate = 
CALCULATE(
    AVERAGE(mart_customer_strategy[avg_churn_probability]) * 100,
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Churn Risk Rate (from dbt calculation)
Churn_Risk_Rate = 
CALCULATE(
    AVERAGE(mart_customer_strategy[churn_risk_rate]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Engagement Rate (from dbt)
Engagement_Rate = 
CALCULATE(
    AVERAGE(mart_customer_strategy[engagement_rate]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Average Predicted CLV per Customer
Avg_Predicted_CLV = 
CALCULATE(
    AVERAGE(mart_customer_strategy[avg_predicted_clv]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// ROI Potential Multiplier (from dbt)
ROI_Potential_Multiplier = 
CALCULATE(
    AVERAGE(mart_customer_strategy[roi_potential_multiplier]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Investment Priority Score (from dbt)
Investment_Priority_Score = 
CALCULATE(
    AVERAGE(mart_customer_strategy[investment_priority_score]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Ready for Engagement Campaigns (from dbt flag)
Ready_for_Engagement = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[ready_for_engagement_campaigns] = TRUE,
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Needs Retention Campaigns (from dbt flag)  
Needs_Retention = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[needs_retention_campaigns] = TRUE,
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// High Prediction Confidence (from dbt)
High_Confidence_Predictions = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[high_prediction_confidence] = TRUE,
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Prediction Confidence Rate (from dbt calculation)
Prediction_Confidence_Rate = 
CALCULATE(
    AVERAGE(mart_customer_strategy[prediction_confidence_rate]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// CLV Segment Distribution (from dbt segments)
Very_High_Value_Customers = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[clv_segment] = "Very High Value",
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

High_Value_Customers = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[clv_segment] = "High Value",
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

Medium_Value_Customers = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[clv_segment] = "Medium Value",
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

Low_Value_Customers = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[clv_segment] IN ("Low Value", "Very Low Value"),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// High Value Customer Percentage
High_Value_Pct = 
DIVIDE([Very_High_Value_Customers] + [High_Value_Customers], [Customer_Count]) * 100

// Investment Priority Distribution (using dbt priorities)
Priority_1_Retention = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[investment_priority] = "Priority 1",
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

Priority_2_Growth = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[investment_priority] = "Priority 2",
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Strategic Recommendation Distribution (from dbt logic)
Retention_VIP = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[strategic_recommendation] = "Retention - VIP Treatment",
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

Urgent_Intervention = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[strategic_recommendation] = "Urgent Intervention - Save High Value",
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

Growth_Increase_Engagement = 
CALCULATE(
    SUM(mart_customer_strategy[customer_count]),
    mart_customer_strategy[strategic_recommendation] = "Growth - Increase Engagement",
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Regional Strategy (from dbt)
Regional_Strategy = 
CALCULATE(
    MAX(mart_customer_strategy[regional_strategy]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Regional Dominant Segment (from dbt)
Regional_Dominant_Segment = 
CALCULATE(
    MAX(mart_customer_strategy[regional_dominant_segment]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Executive Alert (from dbt business logic)
Executive_Alert = 
CALCULATE(
    MAX(mart_customer_strategy[executive_alert]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// CLV Rank in Region (from dbt calculation)
CLV_Rank_in_Region = 
CALCULATE(
    AVERAGE(mart_customer_strategy[clv_rank_in_region]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Volume Rank in Region (from dbt)
Volume_Rank_in_Region = 
CALCULATE(
    AVERAGE(mart_customer_strategy[volume_rank_in_region]),
    mart_customer_strategy[report_date] = MAX(mart_customer_strategy[report_date])
)

// Customer Strategy Health Score (composite using dbt calculations)
Customer_Strategy_Health_Score = 
VAR HighValuePct = [High_Value_Pct]
VAR LowChurnPct = 100 - [Churn_Risk_Rate]
VAR EngagementPct = [Engagement_Rate] 
VAR ConfidencePct = [Prediction_Confidence_Rate]
RETURN
    (HighValuePct * 0.3) + (LowChurnPct * 0.35) + (EngagementPct * 0.2) + (ConfidencePct * 0.15)

// Customer Strategy Alert Status
Customer_Strategy_Alert_Status = 
VAR ExecAlert = [Executive_Alert]
VAR ChurnRate = [Churn_Risk_Rate]
VAR HealthScore = [Customer_Strategy_Health_Score]
RETURN
    IF(ExecAlert <> "STABLE", "🚨 " & ExecAlert,
    IF(ChurnRate > 40, "🚨 WARNING: High churn risk >" & ROUND(ChurnRate,1) & "%",
    IF(HealthScore < 60, "⚠️ WARNING: Strategy health <60%",
    IF(HealthScore > 85, "🌟 EXCELLENT: Strong customer portfolio",
    "✅ HEALTHY: Strategy performing well"))))

// Data Freshness
Last_Updated = 
MAX(mart_customer_strategy[last_updated])
```

---

## **📊 DASHBOARD LAYOUT - CUSTOMER STRATEGY INTELLIGENCE**

### **Page Configuration:**

```
┌─────────────────────────────────────────────────────────┐
│  🎯 CUSTOMER STRATEGY INTELLIGENCE                     │
│  Regional Strategy: [Regional_Strategy]                │
│  Dominant Segment: [Regional_Dominant_Segment]         │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  💎 CUSTOMER VALUE METRICS (using dbt calculations)    │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────────  │
│  │  TOTAL CLV   │ │ HIGH VALUE   │ │ STRATEGY        │
│  │              │ │ CUSTOMERS    │ │ HEALTH          │
│  │              │ │              │ │                 │
│  │[Total_       │ │[High_Value_  │ │[Customer_       │
│  │Predicted_CLV]│ │Pct]%         │ │Strategy_Health  │
│  │              │ │              │ │_Score]          │
│  │vs Historical:│ │[Very_High_   │ │Executive Alert: │
│  │[Total_Future │ │Value_        │ │[Executive_Alert]│
│  │_Value_       │ │Customers] +  │ │                 │
│  │Potential]    │ │[High_Value_  │ │                 │
│  │              │ │Customers]    │ │                 │
│  └──────────────┘ └──────────────┘ └─────────────────  │
│                                                         │
│  📊 CLV SEGMENT DISTRIBUTION (using dbt segments)      │
│  Donut Chart:                                          │
│  - Very High Value: [Very_High_Value_Customers]        │
│  - High Value: [High_Value_Customers]                  │
│  - Medium Value: [Medium_Value_Customers]              │
│  - Low Value: [Low_Value_Customers]                    │
│  Center: [Customer_Count] total customers              │
│  Colors by CLV tier, tooltips show strategic_recommendation│
│                                                         │
│  🎯 INVESTMENT PRIORITIES (using dbt priorities)       │
│  Matrix Visual:                                        │
│  Rows: mart_customer_strategy[investment_priority]     │
│  Values: customer_count, total_predicted_clv           │
│  Color by: roi_potential_multiplier                    │
│  Tooltips: strategic_recommendation from dbt           │
│                                                         │
│  ⚠️ STRATEGIC ALERTS (using dbt business logic)        │
│  Alert Status: [Customer_Strategy_Alert_Status]        │
│  Ready for Engagement: [Ready_for_Engagement]          │
│  Needs Retention: [Needs_Retention]                    │
│  Confidence Rate: [Prediction_Confidence_Rate]%        │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### **Strategic Recommendations Breakdown:**
```
Visual: Stacked Bar Chart
Categories: mart_customer_strategy[strategic_recommendation]
Values: customer_count by recommendation type
Bars:
- "Retention - VIP Treatment": [Retention_VIP]
- "Urgent Intervention - Save High Value": [Urgent_Intervention]  
- "Growth - Increase Engagement": [Growth_Increase_Engagement]
- Others from strategic_recommendation field

Colors: Priority-based (Red for urgent, Green for growth, Blue for retention)
Tooltips: avg_predicted_clv, roi_potential_multiplier
```

### **Regional Strategy Heat Map:**
```
Visual: Filled Map (Brazil regions)
Location: mart_customer_strategy[region]
Color Saturation: [total_predicted_clv] (regional CLV)
Bubble Size: [customer_count]
Tooltips:
- regional_strategy (from dbt)
- regional_dominant_segment (from dbt)
- executive_alert (from dbt)
- clv_rank_in_region (from dbt)
- volume_rank_in_region (from dbt)
```

---

## **📋 DATA MODEL - USING ACTUAL dbt STRUCTURE**

### **Primary Table:** `mart_customer_strategy`

### **Key dbt-Computed Fields Used:**
- `customer_count` (aggregated customer volume)
- `total_predicted_clv` (CLV sum by segment/region)
- `total_future_value_potential` (growth opportunity)
- `avg_churn_probability` (risk assessment)
- `roi_potential_multiplier` (investment ROI)
- `strategic_recommendation` (business action logic)
- `investment_priority_score` (resource allocation scoring)
- `ready_for_engagement_campaigns` (campaign readiness flag)
- `needs_retention_campaigns` (retention campaign flag)
- `high_prediction_confidence` (data quality indicator)
- `regional_strategy` (geographic strategy)
- `executive_alert` (C-level alert status)

### **Relationships:**
```sql
mart_customer_strategy[report_date] ↔ Calendar[Date] (Many-to-One)
```

### **Filters:**
- Date: `report_date` (Current snapshot)
- Region: `region`
- CLV Segment: `clv_segment`
- Priority: `investment_priority`
- Lifecycle: `lifecycle_stage`

---

## **🎯 BUSINESS VALUE - LEVERAGING dbt STRATEGY INTELLIGENCE**

### **✅ Using Pre-Computed Strategic Logic:**
- **CLV Segmentation** from dbt statistical modeling
- **Strategic Recommendations** from dbt business rules  
- **Campaign Readiness Flags** from dbt engagement scoring
- **Investment Priority Scores** from dbt ROI calculations
- **Regional Strategies** from dbt geographic analysis
- **Executive Alerts** from dbt risk monitoring

### **✅ No Strategy Recalculation:**
- Uses dbt's `strategic_recommendation` for actions
- Uses dbt's `roi_potential_multiplier` for ROI analysis
- Uses dbt's `regional_strategy` for geographic planning
- Uses dbt's `executive_alert` for C-level escalation

### **Interview Value:**
*"Built customer strategy intelligence dashboard that leverages advanced analytics engineering including CLV segmentation models, ROI optimization algorithms, and strategic recommendation engines computed in dbt, enabling marketing and strategy teams to execute data-driven customer investments with confidence intervals and campaign readiness indicators."*

**This demonstrates senior Analytics Engineering by using your sophisticated dbt customer strategy intelligence rather than recreating strategic business logic!** 🎯
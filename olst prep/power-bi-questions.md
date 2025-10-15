# Power BI Questions for Analytics Engineers (Beginner-Intermediate)

## 1. Core Concepts & Data Modeling

**Q1: Explain the difference between Import, DirectQuery, and Live Connection modes in Power BI. When would you use each?**

**Answer:**
- **Import Mode**: Data is loaded into Power BI's in-memory engine (VertiPaq). Best for smaller datasets (<1GB), fastest query performance, supports all DAX functions, but requires scheduled refreshes.
- **DirectQuery**: Queries sent directly to source system in real-time. Best for large datasets, real-time data requirements, but limited DAX functionality and slower performance.
- **Live Connection**: Direct connection to Analysis Services or Power BI datasets. No data import, real-time updates, but limited modeling capabilities.

**Use Cases:**
- Import: Most common, good performance, full feature set
- DirectQuery: Large databases, real-time requirements, data security concerns
- Live Connection: Existing SSAS models, certified corporate datasets

**Q2: What is a star schema and why is it important in Power BI data modeling?**

**Answer:**
Star schema is a dimensional modeling approach with:
- **Fact tables**: Contain measures/metrics (sales, quantities, amounts)
- **Dimension tables**: Contain descriptive attributes (customers, products, dates)
- **Relationships**: One-to-many from dimensions to facts

**Benefits in Power BI:**
- Optimal query performance with VertiPaq engine
- Simplified DAX calculations
- Better compression ratios
- Easier to understand for business users
- Supports role-playing dimensions (order date, ship date)

```
Example Structure:
FactSales (Amount, Quantity, OrderID, CustomerID, ProductID, DateID)
  ← DimCustomer (CustomerID, Name, Region, Segment)
  ← DimProduct (ProductID, Category, SubCategory, Brand)
  ← DimDate (DateID, Year, Month, Quarter, DayName)
```

**Q3: How do you handle slowly changing dimensions (SCD) in Power BI?**

**Answer:**
Power BI handles SCDs differently based on type:

**Type 1 (Overwrite):**
- Simple refresh overwrites old values
- Use for corrections or when history isn't needed
- Automatic in Import mode

**Type 2 (Historical Tracking):**
```DAX
// Create calculated column for current record
Current Record = 
IF(
    [End Date] = BLANK() || [End Date] >= TODAY(),
    "Current",
    "Historical"
)

// Filter for current records only in relationships
```

**Best Practices:**
- Use surrogate keys for Type 2 SCDs
- Implement effective date ranges
- Create separate current/historical views
- Consider using incremental refresh for large historical datasets

## 2. DAX Fundamentals

**Q4: Explain the difference between calculated columns and measures in DAX.**

**Answer:**

**Calculated Columns:**
- Computed row-by-row during data refresh
- Stored in the model, increase model size
- Evaluated in row context
- Use for grouping, filtering, or creating new attributes

```DAX
// Calculated Column Example
Profit Margin = 
DIVIDE(
    Sales[Revenue] - Sales[Cost],
    Sales[Revenue],
    0
)
```

**Measures:**
- Computed at query time
- Not stored, don't increase model size
- Evaluated in filter context
- Use for aggregations and dynamic calculations

```DAX
// Measure Example
Total Sales = SUM(Sales[Amount])

Profit Margin % = 
DIVIDE(
    [Total Revenue] - [Total Cost],
    [Total Revenue],
    0
)
```

**Q5: What are filter context and row context in DAX? Provide examples.**

**Answer:**

**Filter Context:**
- Determines which rows are visible to a calculation
- Created by slicers, filters, rows/columns in visuals
- Affects measure calculations

**Row Context:**
- Iterates through table rows one by one
- Created by calculated columns, iterator functions
- Doesn't automatically propagate through relationships

```DAX
// Filter context example (measure)
Sales This Year = 
CALCULATE(
    SUM(Sales[Amount]),
    YEAR(Sales[Date]) = YEAR(TODAY())
)

// Row context example (calculated column)  
Sales Rank = 
RANKX(
    ALL(Sales),
    Sales[Amount],
    ,
    DESC
)

// Converting row context to filter context
Total Sales by Customer = 
CALCULATE(
    SUM(Sales[Amount]),
    FILTER(
        ALL(Sales),
        Sales[CustomerID] = EARLIER(Sales[CustomerID])
    )
)
```

**Q6: How do you create time intelligence calculations in DAX?**

**Answer:**

**Prerequisites:**
- Continuous date table with no gaps
- Mark as date table in model
- Create relationship with fact table

```DAX
// Year-to-Date
Sales YTD = 
TOTALYTD(
    SUM(Sales[Amount]),
    'Date'[Date]
)

// Previous Year Same Period
Sales PY = 
CALCULATE(
    SUM(Sales[Amount]),
    SAMEPERIODLASTYEAR('Date'[Date])
)

// Year-over-Year Growth
YoY Growth = 
DIVIDE(
    [Sales YTD] - [Sales YTD PY],
    [Sales YTD PY],
    0
)

// Moving Average (90 days)
Sales 90-Day MA = 
CALCULATE(
    AVERAGE(Sales[Amount]),
    DATESINPERIOD(
        'Date'[Date],
        MAX('Date'[Date]),
        -90,
        DAY
    )
)

// Custom Period Comparison
Sales vs Budget = 
VAR CurrentPeriodSales = SUM(Sales[Amount])
VAR BudgetAmount = SUM(Budget[Amount])
RETURN
DIVIDE(
    CurrentPeriodSales - BudgetAmount,
    BudgetAmount,
    0
)
```

## 3. Data Transformation & Power Query

**Q7: Explain the difference between Merge and Append operations in Power Query.**

**Answer:**

**Merge (JOIN):**
- Combines tables horizontally based on common columns
- Similar to SQL JOINs
- Creates new columns from the joined table

```M
// Left Join Example
= Table.NestedJoin(
    Sales, {"CustomerID"}, 
    Customers, {"ID"}, 
    "CustomerInfo", 
    JoinKind.LeftOuter
)
```

**Append (UNION):**
- Combines tables vertically
- Tables should have similar structure
- Stacks rows on top of each other

```M
// Append multiple tables
= Table.Combine({
    Sales2023,
    Sales2024,
    Sales2025
})
```

**Q8: How do you handle data quality issues in Power Query?**

**Answer:**

**Common Data Quality Patterns:**

```M
// Remove empty rows
= Table.SelectRows(Source, each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null})))

// Standardize text case
= Table.TransformColumns(Source, {{"CustomerName", Text.Proper}})

// Handle null values
= Table.ReplaceValue(Source, null, 0, Replacer.ReplaceValue, {"Amount"})

// Validate data types with error handling
= Table.TransformColumns(Source, {
    {"Amount", each try Number.FromText(_) otherwise 0, type number},
    {"Date", each try Date.FromText(_) otherwise #date(1900,1,1), type date}
})

// Remove duplicates based on key columns
= Table.Distinct(Source, {"CustomerID", "ProductID", "Date"})

// Create data quality flags
= Table.AddColumn(Source, "Quality Flag", each 
    if [Amount] < 0 then "Negative Amount"
    else if [CustomerID] = null then "Missing Customer"
    else if [Date] > DateTime.LocalNow() then "Future Date"
    else "Valid"
)
```

**Q9: How do you implement incremental refresh in Power BI?**

**Answer:**

**Setup Requirements:**
1. DateTime parameter for filtering
2. RangeStart and RangeEnd parameters
3. Configure incremental refresh policy

```M
// Create parameters
RangeStart = #datetime(2020, 1, 1, 0, 0, 0) meta [IsParameterQuery=true, Type="DateTime"]
RangeEnd = #datetime(2025, 12, 31, 23, 59, 59) meta [IsParameterQuery=true, Type="DateTime"]

// Apply filter in query
= Table.SelectRows(Source, each [ModifiedDate] >= RangeStart and [ModifiedDate] < RangeEnd)
```

**Configuration:**
- Archive data: Store years of historical data
- Incremental periods: Refresh recent months/days only
- Detect data changes: Use modifiedDate column
- Only refresh complete periods

**Benefits:**
- Faster refresh times for large datasets
- Reduced data gateway load
- Better performance for historical analysis
- Automatic partition management

## 4. Visualization Best Practices

**Q10: What are the best practices for creating effective Power BI dashboards?**

**Answer:**

**Design Principles:**
1. **5-Second Rule**: Key insights visible within 5 seconds
2. **Minimal Cognitive Load**: Avoid clutter, use white space
3. **Consistent Formatting**: Colors, fonts, sizing
4. **Mobile-First Design**: Consider phone/tablet views

**Layout Guidelines:**
```
Dashboard Structure:
┌─────────────────────────────────────┐
│  KPIs (Top Row - Most Important)    │
├─────────────────────────────────────┤
│  Main Chart    │  Supporting Chart  │
├─────────────────────────────────────┤
│  Detail Table / Secondary Analysis  │
└─────────────────────────────────────┘
```

**Visual Selection:**
- **Bar Charts**: Comparing categories
- **Line Charts**: Trends over time
- **Cards**: Single KPIs
- **Tables**: Detailed data
- **Maps**: Geographic data
- **Avoid**: 3D charts, pie charts with >5 categories

**Performance Optimization:**
- Limit visuals per page (15-20 maximum)
- Use bookmarks for drill-through navigation
- Implement proper data model relationships
- Use aggregated data when possible

**Q11: How do you implement row-level security (RLS) in Power BI?**

**Answer:**

**Implementation Steps:**

1. **Create Security Roles:**
```DAX
// Regional Manager Role
[Region] = USERNAME() || [Region] = "Global"

// Sales Rep Role  
[SalesRep] = USERPRINCIPALNAME()

// Department Role
[Department] IN {"Finance", "Sales", "Marketing"}
```

2. **Dynamic Security with Lookup Table:**
```DAX
// Create UserSecurity table with columns: Email, Region, Department
// Apply filter in Sales table:
[Region] IN 
CALCULATETABLE(
    VALUES(UserSecurity[Region]),
    FILTER(
        UserSecurity,
        UserSecurity[Email] = USERPRINCIPALNAME()
    )
)
```

3. **Testing RLS:**
- Use "View as Role" in Power BI Desktop
- Test with different user accounts
- Validate in Power BI Service

**Best Practices:**
- Always create security table separate from business data
- Use email addresses or UPNs for user identification
- Document security logic clearly
- Test thoroughly before deployment

## 5. Performance Optimization

**Q12: How do you optimize Power BI report performance?**

**Answer:**

**Data Model Optimization:**
```DAX
// Use SUMMARIZE for aggregated tables
Aggregated Sales = 
SUMMARIZE(
    Sales,
    Sales[Year],
    Sales[Month],
    Sales[ProductCategory],
    "Total Sales", SUM(Sales[Amount]),
    "Total Quantity", SUM(Sales[Quantity])
)

// Optimize measures with variables
Optimized Measure = 
VAR CurrentYearSales = 
    CALCULATE(
        SUM(Sales[Amount]),
        YEAR(Sales[Date]) = YEAR(TODAY())
    )
VAR PreviousYearSales = 
    CALCULATE(
        SUM(Sales[Amount]),
        YEAR(Sales[Date]) = YEAR(TODAY()) - 1
    )
RETURN
DIVIDE(CurrentYearSales - PreviousYearSales, PreviousYearSales, 0)
```

**Query Performance:**
- Reduce visual count per page
- Use appropriate visual types
- Implement proper filters and slicers
- Avoid complex calculated columns in large tables

**Refresh Performance:**
- Use incremental refresh for large datasets
- Optimize Power Query transformations
- Reduce data source connections
- Schedule refreshes during off-peak hours

## 6. Integration & Deployment

**Q13: How do you integrate Power BI with modern data stack (Snowflake, dbt, etc.)?**

**Answer:**

**Snowflake Integration:**
```SQL
-- Create Power BI service account in Snowflake
CREATE USER powerbi_service
PASSWORD = 'secure_password'
DEFAULT_ROLE = 'POWERBI_ROLE'
DEFAULT_WAREHOUSE = 'POWERBI_WH'
DEFAULT_NAMESPACE = 'ANALYTICS.MARTS';

-- Grant appropriate permissions
GRANT ROLE POWERBI_ROLE TO USER powerbi_service;
GRANT USAGE ON WAREHOUSE POWERBI_WH TO ROLE POWERBI_ROLE;
GRANT USAGE ON DATABASE ANALYTICS TO ROLE POWERBI_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS.MARTS TO ROLE POWERBI_ROLE;
```

**Connection Setup:**
- Use DirectQuery for real-time data
- Implement query folding optimization
- Create semantic layer views in Snowflake
- Use parameterized queries for flexibility

**dbt Integration:**
- Connect to dbt mart models
- Use dbt documentation for Power BI metadata
- Implement consistent naming conventions
- Leverage dbt tests for data quality validation

**Q14: How do you implement CI/CD for Power BI development?**

**Answer:**

**Development Workflow:**
```yaml
# Azure DevOps Pipeline
stages:
- stage: Build
  jobs:
  - job: ValidateReports
    steps:
    - task: PowerShell@2
      inputs:
        script: |
          # Validate PBIX files
          # Run data quality tests
          # Check DAX syntax
    
- stage: Deploy_Dev
  dependsOn: Build
  jobs:
  - deployment: DeployToDev
    environment: PowerBI-Dev
    strategy:
      runOnce:
        deploy:
          steps:
          - task: PowerBIActions@3
            inputs:
              action: 'Publish'
              workspaceName: 'Analytics-Dev'
              
- stage: Deploy_Prod
  dependsOn: Deploy_Dev
  condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
  jobs:
  - deployment: DeployToProd
    environment: PowerBI-Prod
```

**Version Control:**
- Use Power BI Desktop Developer Mode
- Store .pbix files in Git (with caution due to binary format)
- Version control DAX measures separately
- Document deployment procedures

## 7. Advanced Features

**Q15: How do you create dynamic measures that change based on user selections?**

**Answer:**

**Parameter-Driven Measures:**
```DAX
// Create parameter table
Metric Selection = {
    "Sales", 
    "Profit", 
    "Quantity", 
    "Margin %"
}

// Dynamic measure
Selected Metric = 
SWITCH(
    SELECTEDVALUE('Metric Selection'[Value]),
    "Sales", [Total Sales],
    "Profit", [Total Profit], 
    "Quantity", [Total Quantity],
    "Margin %", [Profit Margin %],
    BLANK()
)

// Field parameters (Power BI 2022+)
Dynamic Analysis = 
SWITCH(
    SELECTEDVALUE('Field Parameter'[Field Parameter Order]),
    0, [Total Sales],
    1, [Total Customers],
    2, [Average Order Value],
    BLANK()
)
```

**Advanced Scenarios:**
```DAX
// Time-based dynamic measures
Dynamic Period Comparison = 
VAR SelectedPeriod = SELECTEDVALUE(PeriodSelection[Period])
VAR CurrentValue = [Total Sales]
VAR ComparisonValue = 
    SWITCH(
        SelectedPeriod,
        "vs Previous Month", [Sales Previous Month],
        "vs Previous Year", [Sales Previous Year],
        "vs Budget", [Budget Amount],
        BLANK()
    )
RETURN
DIVIDE(CurrentValue - ComparisonValue, ComparisonValue, 0)
```

**Q16-30: Additional Intermediate Topics**

**Q16: Implementing custom tooltips and drill-through pages**
**Q17: Working with composite models and aggregations** 
**Q18: Power BI REST API basics and automation**
**Q19: Managing workspaces and content deployment**
**Q20: Advanced filtering techniques (KEEPFILTERS, REMOVEFILTERS)**
**Q21: Creating custom visuals and R/Python integration**
**Q22: Handling many-to-many relationships**
**Q23: Advanced date table creation and time intelligence**
**Q24: Performance analyzer usage and optimization**
**Q25: Mobile report optimization techniques**
**Q26: Implementing bookmarks and navigation**
**Q27: Advanced Power Query M functions**
**Q28: Working with streaming datasets and real-time dashboards**
**Q29: Power BI embedded analytics integration**
**Q30: Troubleshooting common Power BI issues and error handling**

---

## Key Power BI Concepts for Analytics Engineers:

**Data Architecture Integration:**
- Understanding how Power BI fits in modern data stack
- Semantic layer design principles
- Performance optimization for analytical workloads
- Integration with cloud data platforms

**Advanced Analytics:**
- Statistical functions and advanced DAX patterns
- Time series analysis and forecasting
- What-if analysis and scenario modeling
- Machine learning integration

**Enterprise Deployment:**
- Governance and security frameworks
- Scalable content distribution
- Monitoring and usage analytics
- Cost optimization strategies

**Best Practices:**
- Consistent development standards
- Documentation and metadata management
- Testing and validation procedures
- User training and adoption strategies
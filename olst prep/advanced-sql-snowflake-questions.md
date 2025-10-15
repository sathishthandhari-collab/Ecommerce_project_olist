# Advanced SQL Questions for Analytics Engineers - Snowflake Dialect

## 1. Window Functions & Analytics

**Q1: Write a query to calculate the percentage change in monthly revenue compared to the same month in the previous year, handling NULL values appropriately.**

```sql
WITH monthly_revenue AS (
  SELECT 
    DATE_TRUNC('month', order_date) as month_year,
    SUM(revenue) as total_revenue
  FROM orders 
  GROUP BY DATE_TRUNC('month', order_date)
)
SELECT 
  month_year,
  total_revenue,
  LAG(total_revenue, 12) OVER (ORDER BY month_year) as prev_year_revenue,
  CASE 
    WHEN LAG(total_revenue, 12) OVER (ORDER BY month_year) IS NULL 
    THEN NULL
    WHEN LAG(total_revenue, 12) OVER (ORDER BY month_year) = 0 
    THEN NULL
    ELSE ROUND(
      ((total_revenue - LAG(total_revenue, 12) OVER (ORDER BY month_year)) / 
       LAG(total_revenue, 12) OVER (ORDER BY month_year)) * 100, 2
    )
  END as yoy_percentage_change
FROM monthly_revenue
ORDER BY month_year;
```

**Q2: Create a query that identifies customers who had a purchase pattern break (no purchases for 90+ days) and then returned.**

```sql
WITH customer_purchases AS (
  SELECT 
    customer_id,
    purchase_date,
    LAG(purchase_date) OVER (PARTITION BY customer_id ORDER BY purchase_date) as prev_purchase_date,
    DATEDIFF(day, LAG(purchase_date) OVER (PARTITION BY customer_id ORDER BY purchase_date), purchase_date) as days_between_purchases
  FROM orders
),
breaks_identified AS (
  SELECT 
    customer_id,
    purchase_date,
    prev_purchase_date,
    days_between_purchases,
    CASE WHEN days_between_purchases >= 90 THEN 1 ELSE 0 END as had_break
  FROM customer_purchases
)
SELECT DISTINCT customer_id
FROM breaks_identified 
WHERE had_break = 1;
```

**Q3: Write a query using QUALIFY to find the top 2 products by revenue in each category for the current quarter.**

```sql
SELECT 
  category,
  product_name,
  quarterly_revenue,
  revenue_rank
FROM (
  SELECT 
    p.category,
    p.product_name,
    SUM(o.revenue) as quarterly_revenue,
    ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(o.revenue) DESC) as revenue_rank
  FROM products p
  JOIN orders o ON p.product_id = o.product_id
  WHERE DATE_TRUNC('quarter', o.order_date) = DATE_TRUNC('quarter', CURRENT_DATE())
  GROUP BY p.category, p.product_name
) 
QUALIFY revenue_rank <= 2
ORDER BY category, revenue_rank;
```

## 2. Advanced Aggregations & Pivoting

**Q4: Create a dynamic pivot query that shows monthly sales by product category without hardcoding category names.**

```sql
-- Note: Snowflake requires predefined columns for PIVOT, but here's a dynamic approach using conditional aggregation
SELECT 
  DATE_TRUNC('month', order_date) as month_year,
  SUM(CASE WHEN category = 'Electronics' THEN revenue END) as electronics_revenue,
  SUM(CASE WHEN category = 'Clothing' THEN revenue END) as clothing_revenue,
  SUM(CASE WHEN category = 'Books' THEN revenue END) as books_revenue,
  -- Use OBJECT_CONSTRUCT for truly dynamic approach
  OBJECT_CONSTRUCT(
    ARRAY_AGG(category),
    ARRAY_AGG(revenue)
  ) as category_revenue_map
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month_year;
```

**Q5: Write a query to calculate rolling 7-day, 30-day, and 90-day averages for daily active users.**

```sql
SELECT 
  date,
  daily_active_users,
  AVG(daily_active_users) OVER (
    ORDER BY date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as rolling_7day_avg,
  AVG(daily_active_users) OVER (
    ORDER BY date 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) as rolling_30day_avg,
  AVG(daily_active_users) OVER (
    ORDER BY date 
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
  ) as rolling_90day_avg
FROM daily_user_metrics
WHERE daily_active_users IS NOT NULL
ORDER BY date;
```

## 3. Complex CTEs and Recursive Queries

**Q6: Build a recursive CTE to calculate the cumulative impact of customer referrals up to 5 levels deep.**

```sql
WITH RECURSIVE referral_chain AS (
  -- Base case: direct customers (level 1)
  SELECT 
    customer_id,
    referred_by,
    customer_id as root_referrer,
    1 as referral_level,
    CAST(customer_id AS VARCHAR) as referral_path
  FROM customers 
  WHERE referred_by IS NULL
  
  UNION ALL
  
  -- Recursive case: referred customers
  SELECT 
    c.customer_id,
    c.referred_by,
    rc.root_referrer,
    rc.referral_level + 1,
    rc.referral_path || ' -> ' || c.customer_id
  FROM customers c
  INNER JOIN referral_chain rc ON c.referred_by = rc.customer_id
  WHERE rc.referral_level < 5
)
SELECT 
  root_referrer,
  referral_level,
  COUNT(*) as customers_at_level,
  SUM(COUNT(*)) OVER (PARTITION BY root_referrer ORDER BY referral_level) as cumulative_referrals
FROM referral_chain
GROUP BY root_referrer, referral_level
ORDER BY root_referrer, referral_level;
```

**Q7: Create a CTE that identifies seasonal patterns by comparing each month's performance to its historical average.**

```sql
WITH monthly_metrics AS (
  SELECT 
    EXTRACT(month FROM order_date) as month_num,
    EXTRACT(year FROM order_date) as year_num,
    SUM(revenue) as monthly_revenue,
    COUNT(DISTINCT customer_id) as unique_customers
  FROM orders
  GROUP BY EXTRACT(month FROM order_date), EXTRACT(year FROM order_date)
),
historical_averages AS (
  SELECT 
    month_num,
    AVG(monthly_revenue) as avg_historical_revenue,
    STDDEV(monthly_revenue) as stddev_revenue
  FROM monthly_metrics
  WHERE year_num < EXTRACT(year FROM CURRENT_DATE())
  GROUP BY month_num
)
SELECT 
  mm.year_num,
  mm.month_num,
  mm.monthly_revenue,
  ha.avg_historical_revenue,
  (mm.monthly_revenue - ha.avg_historical_revenue) / ha.stddev_revenue as z_score,
  CASE 
    WHEN ABS((mm.monthly_revenue - ha.avg_historical_revenue) / ha.stddev_revenue) > 2 
    THEN 'Anomaly'
    WHEN (mm.monthly_revenue - ha.avg_historical_revenue) / ha.stddev_revenue > 1 
    THEN 'Above Average'
    WHEN (mm.monthly_revenue - ha.avg_historical_revenue) / ha.stddev_revenue < -1 
    THEN 'Below Average'
    ELSE 'Normal'
  END as performance_category
FROM monthly_metrics mm
JOIN historical_averages ha ON mm.month_num = ha.month_num
WHERE mm.year_num = EXTRACT(year FROM CURRENT_DATE())
ORDER BY mm.month_num;
```

## 4. Advanced Joins and Set Operations

**Q8: Write a query to find customers who purchased from all product categories available in their registration year.**

```sql
WITH customer_categories AS (
  SELECT DISTINCT
    c.customer_id,
    c.registration_date,
    p.category
  FROM customers c
  JOIN orders o ON c.customer_id = o.customer_id
  JOIN products p ON o.product_id = p.product_id
  WHERE EXTRACT(year FROM o.order_date) = EXTRACT(year FROM c.registration_date)
),
available_categories_per_year AS (
  SELECT DISTINCT
    EXTRACT(year FROM registration_date) as reg_year,
    category
  FROM customers c
  CROSS JOIN (SELECT DISTINCT category FROM products) p
)
SELECT c.customer_id
FROM customers c
WHERE NOT EXISTS (
  SELECT category 
  FROM available_categories_per_year a
  WHERE a.reg_year = EXTRACT(year FROM c.registration_date)
  AND category NOT IN (
    SELECT category 
    FROM customer_categories cc 
    WHERE cc.customer_id = c.customer_id
  )
);
```

**Q9: Create a query to identify products that are frequently bought together using self-joins.**

```sql
WITH order_products AS (
  SELECT 
    o1.order_id,
    o1.product_id as product_a,
    o2.product_id as product_b
  FROM order_items o1
  JOIN order_items o2 ON o1.order_id = o2.order_id
  WHERE o1.product_id < o2.product_id  -- Avoid duplicate pairs and self-pairs
),
product_pairs AS (
  SELECT 
    product_a,
    product_b,
    COUNT(*) as times_bought_together
  FROM order_products
  GROUP BY product_a, product_b
),
individual_product_sales AS (
  SELECT 
    product_id,
    COUNT(DISTINCT order_id) as total_orders
  FROM order_items
  GROUP BY product_id
)
SELECT 
  pp.product_a,
  pa.product_name as product_a_name,
  pp.product_b,
  pb.product_name as product_b_name,
  pp.times_bought_together,
  ips1.total_orders as product_a_total_orders,
  ips2.total_orders as product_b_total_orders,
  pp.times_bought_together / LEAST(ips1.total_orders, ips2.total_orders) as association_strength
FROM product_pairs pp
JOIN products pa ON pp.product_a = pa.product_id
JOIN products pb ON pp.product_b = pb.product_id
JOIN individual_product_sales ips1 ON pp.product_a = ips1.product_id
JOIN individual_product_sales ips2 ON pp.product_b = ips2.product_id
WHERE pp.times_bought_together >= 10  -- Minimum threshold
ORDER BY association_strength DESC
LIMIT 20;
```

## 5. Performance Optimization & Query Tuning

**Q10: Optimize this query for better performance by restructuring the WHERE clause and using appropriate indexes.**

```sql
-- Poorly performing query
SELECT * FROM large_table 
WHERE UPPER(status) = 'ACTIVE' 
AND date_column + INTERVAL '1 DAY' > CURRENT_DATE();

-- Optimized version
SELECT * FROM large_table 
WHERE status = 'ACTIVE'  -- Remove function on column
AND date_column > DATEADD(day, -1, CURRENT_DATE())  -- Move computation to right side
AND date_column IS NOT NULL;  -- Explicit null handling

-- Additional optimization suggestions:
-- 1. Create index: CREATE INDEX idx_status_date ON large_table(status, date_column);
-- 2. Consider clustering key if table is large: ALTER TABLE large_table CLUSTER BY (date_column);
-- 3. Update statistics: ALTER TABLE large_table REBUILD;
```

**Q11: Write a query that efficiently handles large dataset joins by using appropriate join algorithms.**

```sql
-- Efficient large table join with broadcast hint for small dimension tables
SELECT /*+ USE_CACHED_RESULT(false) */
  f.transaction_id,
  f.amount,
  d1.customer_name,
  d2.product_name,
  d3.store_location
FROM fact_transactions f
  JOIN dim_customers d1 ON f.customer_id = d1.customer_id
  JOIN dim_products d2 ON f.product_id = d2.product_id  
  JOIN dim_stores d3 ON f.store_id = d3.store_id
WHERE f.transaction_date >= CURRENT_DATE() - 30
  AND f.amount > 100
ORDER BY f.transaction_date DESC;

-- Additional optimizations:
-- 1. Partition fact table by transaction_date
-- 2. Use appropriate warehouse size for join complexity
-- 3. Consider result caching for repeated queries
```

## 6. Data Quality & Validation

**Q12: Create a comprehensive data quality check query that identifies various anomalies.**

```sql
WITH data_quality_checks AS (
  SELECT 
    'duplicate_records' as check_type,
    COUNT(*) as issue_count,
    ARRAY_AGG(customer_id) as affected_records
  FROM (
    SELECT customer_id, email, COUNT(*)
    FROM customers
    GROUP BY customer_id, email
    HAVING COUNT(*) > 1
  )
  
  UNION ALL
  
  SELECT 
    'null_critical_fields',
    COUNT(*),
    ARRAY_AGG(customer_id)
  FROM customers 
  WHERE email IS NULL OR customer_name IS NULL
  
  UNION ALL
  
  SELECT 
    'invalid_email_format',
    COUNT(*),
    ARRAY_AGG(customer_id)
  FROM customers 
  WHERE email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
  
  UNION ALL
  
  SELECT 
    'future_dates',
    COUNT(*),
    ARRAY_AGG(customer_id)
  FROM customers 
  WHERE registration_date > CURRENT_DATE()
  
  UNION ALL
  
  SELECT 
    'negative_amounts',
    COUNT(*),
    ARRAY_AGG(order_id)
  FROM orders 
  WHERE amount < 0
)
SELECT 
  check_type,
  issue_count,
  CASE WHEN issue_count > 0 THEN 'FAIL' ELSE 'PASS' END as status,
  affected_records
FROM data_quality_checks
ORDER BY issue_count DESC;
```

## 7. Advanced Date/Time Handling

**Q13: Calculate business days between two dates excluding weekends and holidays.**

```sql
WITH RECURSIVE date_range AS (
  SELECT 
    order_id,
    order_date,
    ship_date,
    order_date as current_date
  FROM orders
  WHERE ship_date IS NOT NULL
  
  UNION ALL
  
  SELECT 
    order_id,
    order_date,
    ship_date,
    DATEADD(day, 1, current_date)
  FROM date_range
  WHERE current_date < ship_date
),
business_days AS (
  SELECT 
    order_id,
    order_date,
    ship_date,
    current_date,
    CASE 
      WHEN DAYOFWEEK(current_date) IN (1, 7) THEN 0  -- Weekend
      WHEN current_date IN (SELECT holiday_date FROM company_holidays) THEN 0  -- Holiday
      ELSE 1
    END as is_business_day
  FROM date_range
)
SELECT 
  order_id,
  order_date,
  ship_date,
  SUM(is_business_day) as business_days_to_ship
FROM business_days
GROUP BY order_id, order_date, ship_date
ORDER BY order_id;
```

**Q14: Create a fiscal calendar mapping with dynamic fiscal year start.**

```sql
CREATE OR REPLACE FUNCTION get_fiscal_quarter(input_date DATE, fiscal_year_start_month INT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
  WITH fiscal_mapping AS (
    SELECT 
      input_date,
      CASE 
        WHEN MONTH(input_date) >= fiscal_year_start_month 
        THEN YEAR(input_date) 
        ELSE YEAR(input_date) - 1 
      END as fiscal_year,
      CASE 
        WHEN MONTH(input_date) >= fiscal_year_start_month 
        THEN MONTH(input_date) - fiscal_year_start_month + 1
        ELSE MONTH(input_date) + 12 - fiscal_year_start_month + 1
      END as fiscal_month
  )
  SELECT 
    'FY' || fiscal_year || 'Q' || CEIL(fiscal_month / 3.0)
  FROM fiscal_mapping
$$;

-- Usage example:
SELECT 
  order_date,
  get_fiscal_quarter(order_date, 4) as fiscal_quarter,  -- April start
  SUM(amount) as quarterly_revenue
FROM orders
GROUP BY order_date, get_fiscal_quarter(order_date, 4)
ORDER BY order_date;
```

## 8. JSON and Semi-Structured Data

**Q15: Parse and flatten complex JSON data with nested arrays and objects.**

```sql
WITH parsed_events AS (
  SELECT 
    event_id,
    event_data:user_id::STRING as user_id,
    event_data:event_type::STRING as event_type,
    event_data:timestamp::TIMESTAMP as event_timestamp,
    event_data:properties as properties_json
  FROM raw_event_log
),
flattened_properties AS (
  SELECT 
    event_id,
    user_id,
    event_type,
    event_timestamp,
    f.key as property_name,
    f.value as property_value
  FROM parsed_events,
  LATERAL FLATTEN(input => properties_json) f
),
aggregated_user_properties AS (
  SELECT 
    user_id,
    OBJECT_AGG(property_name, property_value) as all_properties,
    COUNT(DISTINCT event_type) as unique_event_types,
    MAX(event_timestamp) as last_activity
  FROM flattened_properties
  GROUP BY user_id
)
SELECT 
  user_id,
  all_properties:device_type::STRING as device_type,
  all_properties:location::STRING as location,
  unique_event_types,
  last_activity
FROM aggregated_user_properties
WHERE last_activity >= CURRENT_DATE() - 30;
```

## 9. Advanced String Operations

**Q16: Create a query to clean and standardize company names using advanced string functions.**

```sql
WITH cleaned_companies AS (
  SELECT 
    company_id,
    original_name,
    -- Remove common suffixes and prefixes
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          UPPER(TRIM(original_name)),
          '\\s+(INC\\.?|LLC|LTD\\.?|CORP\\.?|CORPORATION|LIMITED|COMPANY|CO\\.?)\\s*$',
          ''
        ),
        '^(THE\\s+)',
        ''
      ),
      '\\s+',
      ' '
    ) as standardized_name,
    -- Calculate similarity for deduplication
    SOUNDEX(original_name) as soundex_code
  FROM companies
),
similarity_groups AS (
  SELECT 
    c1.company_id,
    c1.original_name,
    c1.standardized_name,
    c1.soundex_code,
    ARRAY_AGG(c2.company_id) WITHIN GROUP (ORDER BY c2.company_id) as similar_companies
  FROM cleaned_companies c1
  JOIN cleaned_companies c2 ON (
    c1.soundex_code = c2.soundex_code 
    OR EDITDISTANCE(c1.standardized_name, c2.standardized_name) <= 2
  )
  WHERE c1.company_id <= c2.company_id  -- Avoid duplicates
  GROUP BY c1.company_id, c1.original_name, c1.standardized_name, c1.soundex_code
)
SELECT 
  company_id,
  original_name,
  standardized_name,
  similar_companies,
  ARRAY_SIZE(similar_companies) as potential_duplicates
FROM similarity_groups
WHERE ARRAY_SIZE(similar_companies) > 1
ORDER BY potential_duplicates DESC;
```

## 10. Advanced Analytics Functions

**Q17: Implement a cohort retention analysis using advanced window functions.**

```sql
WITH user_cohorts AS (
  SELECT 
    user_id,
    DATE_TRUNC('month', MIN(registration_date)) as cohort_month
  FROM users
  GROUP BY user_id
),
user_activities AS (
  SELECT 
    uc.user_id,
    uc.cohort_month,
    DATE_TRUNC('month', ua.activity_date) as activity_month,
    DATEDIFF('month', uc.cohort_month, DATE_TRUNC('month', ua.activity_date)) as period_number
  FROM user_cohorts uc
  JOIN user_activity_log ua ON uc.user_id = ua.user_id
),
cohort_sizes AS (
  SELECT 
    cohort_month,
    COUNT(DISTINCT user_id) as cohort_size
  FROM user_cohorts
  GROUP BY cohort_month
),
retention_table AS (
  SELECT 
    ua.cohort_month,
    ua.period_number,
    COUNT(DISTINCT ua.user_id) as active_users,
    cs.cohort_size
  FROM user_activities ua
  JOIN cohort_sizes cs ON ua.cohort_month = cs.cohort_month
  GROUP BY ua.cohort_month, ua.period_number, cs.cohort_size
)
SELECT 
  cohort_month,
  cohort_size,
  period_number,
  active_users,
  ROUND(100.0 * active_users / cohort_size, 2) as retention_rate
FROM retention_table
ORDER BY cohort_month, period_number;
```

## 11-30. Additional Advanced Questions

**Q18: Multi-dimensional funnel analysis with conversion attribution**
**Q19: Time-series anomaly detection using statistical methods**
**Q20: Advanced customer segmentation using RFM analysis**
**Q21: Dynamic SQL generation for flexible reporting**
**Q22: Slowly changing dimensions Type 2 implementation**
**Q23: Advanced geographic analysis with spatial functions**
**Q24: Machine learning feature engineering in SQL**
**Q25: Complex data lineage tracking queries**
**Q26: Advanced partitioning and clustering strategies**
**Q27: Real-time streaming data processing patterns**
**Q28: Multi-tenant data isolation techniques**
**Q29: Advanced security and data masking implementations**
**Q30: Performance tuning for complex analytical workloads**

[Note: Each remaining question would follow similar depth and complexity, focusing on enterprise-level Analytics Engineering challenges]

---

## Key Interview Tips:

1. **Explain your approach**: Always walk through your thought process
2. **Discuss trade-offs**: Mention performance implications and alternatives
3. **Consider edge cases**: Address NULL handling, data quality issues
4. **Optimize for scale**: Think about how queries perform on large datasets
5. **Business context**: Connect technical solutions to business problems

## Performance Considerations:
- Always consider clustering keys for large tables
- Use appropriate warehouse sizing
- Leverage result caching when possible
- Consider query compilation time for complex queries
- Use EXPLAIN PLAN to understand query execution
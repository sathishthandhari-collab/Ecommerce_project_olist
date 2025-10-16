Here are 50 intermediate-to-advanced Snowflake SQL interview questions with concise answers and code-ready examples that focus on theory and practice in the Snowflake dialect. 

### Window functions- Q1: What are window functions and why are they useful in analytics?
  - A: Window functions compute values like running totals, moving averages, and rankings across sets of rows related to the current row without collapsing rows, enabling powerful analytics directly in SQL. 

- Q2: How do you calculate a running total per partition and order in Snowflake?
  - A: Use SUM with an OVER clause and a frame; for example, SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) yields a cumulative sum by customer and time. 
  ```sql
  SELECT customer_id, order_date, amount,
         SUM(amount) OVER (
           PARTITION BY customer_id
           ORDER BY order_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS running_total
  FROM fact_orders;
  ```

- Q3: How would you compute a 7-day moving average?
  - A: Apply AVG over a window frame defined relative to the current row, e.g., AVG(metric) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) for a 7-row rolling mean. 
  ```sql
  SELECT d, metric,
         AVG(metric) OVER (
           ORDER BY d
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
         ) AS ma_7
  FROM daily_metrics;
  ```

- Q4: What’s the difference between ROW_NUMBER, RANK, and DENSE_RANK?
  - A: ROW_NUMBER assigns unique sequential numbers, RANK gives ties the same rank with gaps, and DENSE_RANK gives ties the same rank without gaps, all computed as window functions with ORDER BY and optional PARTITION BY. 

- Q5: How do you compute percent of total within a partition?
  - A: Divide a row’s value by SUM(value) OVER (PARTITION BY key) to produce percentage contribution within each partition. 
  ```sql
  SELECT key, value,
         value / NULLIF(SUM(value) OVER (PARTITION BY key), 0) AS pct_of_total
  FROM t;
  ```

- Q6: When would you use RANGE vs ROWS in a window frame?
  - A: ROWS defines a physical row-count frame while RANGE defines a logical value-based frame (e.g., time range), with function-specific support and defaults described in Snowflake’s window function behavior. 

- Q7: How do you compute a lag/lead difference for trend analysis?
  - A: Use value - LAG(value) OVER (...) to produce period-over-period change for row-wise comparisons in ordered partitions. 
  ```sql
  SELECT d, value,
         value - LAG(value) OVER (ORDER BY d) AS delta
  FROM series;
  ```

- Q8: Why use variables in window expressions?
  - A: Variables are not part of SQL itself, but structuring a query so the same window expression is referenced once improves clarity; however, Snowflake allows referencing SELECT aliases in QUALIFY to avoid duplicating window logic. 

### QUALIFY- Q9: What does QUALIFY do in Snowflake?
  - A: QUALIFY filters a SELECT after window functions are evaluated, analogous to HAVING for aggregates but for window function results, and is evaluated after the window clause in execution order. 

- Q10: Show how to return the top row per group using QUALIFY and ROW_NUMBER.
  - A: Compute ROW_NUMBER() OVER (PARTITION BY key ORDER BY metric DESC) and QUALIFY row_num = 1 to keep the best row per group. 
  ```sql
  SELECT key, metric,
         ROW_NUMBER() OVER (PARTITION BY key ORDER BY metric DESC) AS rn
  FROM t
  QUALIFY rn = 1;
  ```

- Q11: Why is QUALIFY often preferable to subqueries?
  - A: QUALIFY avoids wrapping a SELECT with window functions inside an outer SELECT/WHERE, producing simpler and more readable SQL with equivalent semantics. 

- Q12: Can QUALIFY reference a column alias defined in the SELECT list?
  - A: Yes, expressions in the SELECT list, including window functions, can be referenced by their alias in QUALIFY, simplifying reuse of window results. 

- Q13: Can QUALIFY predicates include aggregates and subqueries?
  - A: Yes, QUALIFY supports aggregates and subqueries in its predicate, following rules similar to HAVING for aggregate usage. 

- Q14: What happens if you use QUALIFY without a window function?
  - A: QUALIFY requires at least one window function in the SELECT list or the QUALIFY predicate; otherwise, the statement is invalid. 

- Q15: Demonstrate keeping the two most recent orders per customer.
  - A: Use ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) and QUALIFY rn <= 2 to retain only the two latest per customer. 
  ```sql
  SELECT customer_id, order_id, order_date,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
  FROM orders
  QUALIFY rn <= 2;
  ```

- Q16: How do QUALIFY and WHERE interact in evaluation order?
  - A: WHERE filters before GROUP BY and window computation, while QUALIFY filters after the window phase, so QUALIFY sees the final window results. 

- Q17: Can QUALIFY be used alongside HAVING?
  - A: Yes, WHERE and GROUP BY and HAVING are evaluated before window functions, and QUALIFY executes after the window, so HAVING and QUALIFY can both appear to filter different phases. 

- Q18: Keep rows where a row’s value is greater than the partition average using QUALIFY.
  - A: Compare the value to AVG(value) OVER (PARTITION BY key) within QUALIFY to keep only above-average rows within each partition. 
  ```sql
  SELECT key, value
  FROM t
  QUALIFY value > AVG(value) OVER (PARTITION BY key);
  ```

- Q19: How do you select the first event per session with ties broken by timestamp?
  - A: Use ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_ts) and QUALIFY rn = 1 to keep the earliest in each session. 

- Q20: Return rows whose ranking is in the top 10 per category.
  - A: Use RANK() OVER (PARTITION BY category ORDER BY score DESC) and QUALIFY rank <= 10 for top-N-per-category selection. 

### PIVOT- Q21: What does PIVOT do in Snowflake?
  - A: PIVOT rotates rows into columns by turning distinct values from a value column into output columns while aggregating another column per pivoted value. 

- Q22: Show a basic PIVOT to sum sales by quarter for each employee.
  - A: Use PIVOT(SUM(amount) FOR quarter IN ('2023_Q1','2023_Q2','2023_Q3','2023_Q4')) after the FROM clause to widen quarterly metrics per employee. 
  ```sql
  SELECT *
  FROM quarterly_sales
  PIVOT (SUM(amount) FOR quarter IN ('2023_Q1','2023_Q2','2023_Q3','2023_Q4'))
  ORDER BY empid;
  ```

- Q23: How does dynamic pivoting work with ANY?
  - A: Use IN (ANY ORDER BY quarter) to pivot on all distinct values for the pivot column dynamically, optionally ordering pivoted columns. 

- Q24: How do you alias pivoted columns or aggregated outputs?
  - A: Provide aliases in the PIVOT subclause, e.g., '2023_Q1' AS q1 or SUM(amount) AS total to append suffixes like _TOTAL to pivot column names. 

- Q25: How can you substitute a default for NULL pivot cells?
  - A: Add DEFAULT ON NULL (<value>) within the PIVOT subclause to replace NULL results with a specified constant such as 0. 

- Q26: Can a subquery define which values to pivot?
  - A: Yes, use a subquery in the IN (...) list, e.g., IN (SELECT DISTINCT quarter FROM other_table WHERE condition ORDER BY quarter). 

- Q27: How can you emulate multiple aggregations with PIVOT?
  - A: Since PIVOT does one aggregation per query, UNION multiple PIVOT queries (e.g., AVG, MAX, MIN, COUNT, SUM) and label each aggregation in the projection. 

- Q28: How do you pivot on multiple columns or exclude columns first?
  - A: Use a CTE to preselect or exclude columns and use multiple PIVOT clauses as needed, then aggregate outer results if necessary. 

- Q29: What are best practices for dynamic pivot in views?
  - A: Dynamic pivot in view definitions can fail when output columns change as data evolves, so consider stability and deduplication behavior of dynamic pivot. 

- Q30: Demonstrate a pivot that orders columns dynamically by the pivot value.
  - A: Use IN (ANY ORDER BY quarter) to control column order directly in the PIVOT specification. 
  ```sql
  SELECT *
  FROM quarterly_sales
  PIVOT (SUM(amount) FOR quarter IN (ANY ORDER BY quarter))
  ORDER BY empid;
  ```

### MERGE- Q31: What does MERGE do in Snowflake?
  - A: MERGE performs inserts, updates, and deletes against a target table based on matches between target and source rows, enabling change-apply logic in one SQL statement. 

- Q32: Show a MERGE that updates on match and inserts on no match.
  - A: Use WHEN MATCHED THEN UPDATE SET ... and WHEN NOT MATCHED THEN INSERT (...) VALUES (...) to upsert based on a join condition. 
  ```sql
  MERGE INTO tgt
  USING src
  ON tgt.id = src.id
  WHEN MATCHED THEN UPDATE SET tgt.val = src.val
  WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val);
  ```

- Q33: How do you handle updates involving many columns with MERGE?
  - A: Use UPDATE ALL BY NAME to update all target columns from source by matching column names, independent of column order. 

- Q34: What are the semantics when multiple source rows match one target row?
  - A: Depending on session settings, conflicting updates/deletes can be nondeterministic or error; using GROUP BY in the source to reduce to one row per target key guarantees deterministic behavior. 

- Q35: Can MERGE include multiple WHEN MATCHED clauses?
  - A: Yes, multiple WHEN MATCHED and WHEN NOT MATCHED clauses are allowed, but the unconstrained final clause of each kind must appear last to avoid unreachable cases. 

- Q36: Show a MERGE that deletes when flagged and updates when a condition holds.
  - A: Combine WHEN MATCHED AND condition THEN DELETE and another WHEN MATCHED AND condition THEN UPDATE, plus an INSERT for non-matches. 
  ```sql
  MERGE INTO tgt
  USING src
  ON tgt.id = src.id
  WHEN MATCHED AND src.marked = 'Y' THEN DELETE
  WHEN MATCHED AND src.isnew = 1 THEN UPDATE SET tgt.val = src.newval
  WHEN MATCHED THEN UPDATE SET tgt.val = src.newval
  WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.newval);
  ```

- Q37: How does MERGE treat duplicate source rows when there is no match in target?
  - A: If WHEN NOT MATCHED THEN INSERT is present and multiple duplicate source rows exist, each duplicate inserts a row in the target. 

- Q38: Is MERGE semantically equivalent to separate DML statements?
  - A: Under deterministic conditions, MERGE behaves like coordinated INSERT/UPDATE/DELETE statements but in one atomic command with well-defined precedence. 

- Q39: Why is GROUP BY in the source often recommended before MERGE?
  - A: Aggregating or deduplicating the source ensures a single source row per target key, preventing ambiguous updates/deletes and ensuring deterministic results. 

- Q40: Can MERGE leverage ALL BY NAME for both matched and not matched cases?
  - A: Yes, WHEN MATCHED THEN UPDATE ALL BY NAME and WHEN NOT MATCHED THEN INSERT ALL BY NAME allow full-column mapping by name for updates and inserts. 

### COPY INTO- Q41: What does COPY INTO <table> do?
  - A: COPY INTO loads data from internal or external stages (or external locations) into an existing table, with rich file format and copy options for CSV, JSON, Avro, ORC, Parquet, and XML. 

- Q42: How do you select and transform staged file columns during load?
  - A: Use the SELECT form with positional columns like $1, $2 (and JSON path elements) inside the FROM stage reference and optionally map to target columns. 
  ```sql
  COPY INTO mydb.public.t (c1, c2)
  FROM (
    SELECT $1, $2
    FROM @mystage/prefix/
  )
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
  ```

- Q43: What does VALIDATION_MODE do in COPY?
  - A: VALIDATION_MODE instructs COPY to validate files without loading (e.g., RETURN_10_ROWS or RETURN_ERRORS), useful for preflight checks before actual ingestion. 

- Q44: How do FILES and PATTERN differ in COPY?
  - A: FILES explicitly lists staged file names (up to 1000), while PATTERN uses a regex to match file names/paths; using both together restricts to listed paths. 

- Q45: Name two common COPY options to handle data quality issues.
  - A: ON_ERROR controls error handling (e.g., CONTINUE or ABORT_STATEMENT) and NULL_IF maps specific strings to SQL NULL during load to normalize source values. 

- Q46: How do you specify the data file format for COPY?
  - A: Either reference a named FILE_FORMAT or specify TYPE with format options inline, but these two approaches are mutually exclusive in a single COPY. 

### EXPLAIN- Q47: What does EXPLAIN return in Snowflake?
  - A: EXPLAIN returns the logical execution plan, detailing operations like table scans and joins, object references, and expressions, without actually running the query.- Q48: Does EXPLAIN require an active warehouse or consume credits?
  - A: EXPLAIN compiles the statement and does not require a running warehouse, but compilation consumes Cloud Services credits similar to other metadata operations.

- Q49: What output formats can EXPLAIN produce?
  - A: EXPLAIN supports TABULAR (default), JSON, and TEXT output formats, allowing human readability or programmatic analysis of plan details.

- Q50: How can you post-process EXPLAIN output for analysis?
  - A: Generate JSON output and store it in a table to query later, or use TABLE functions to treat output as tabular data for downstream diagnostics and reporting.
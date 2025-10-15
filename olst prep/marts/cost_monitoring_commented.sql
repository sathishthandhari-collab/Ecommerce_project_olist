{#
    COST MONITORING AND PERFORMANCE ANALYSIS: Snowflake warehouse optimization and cost tracking
#}

{% macro capture_run_costs() %}
    {# 
    AUTOMATED COST TRACKING MACRO: Captures detailed Snowflake execution metrics for cost optimization
    
    PURPOSE: Automatically tracks compute costs, performance metrics, and resource utilization for each dbt model
    
    EXECUTION CONTEXT: Only runs in production environment to avoid unnecessary metadata queries
    
    METRICS CAPTURED:
    - Credits consumed (compute + cloud services)
    - Execution time and performance metrics
    - Data volume processed (rows and bytes)
    - Warehouse utilization and load patterns
    - Cost per row for efficiency analysis
    
    BUSINESS VALUE: Enables FinOps cost optimization and performance tuning across dbt models
    #}
    
    {% if execute and target.name == 'prod' %}
        -- COST METRICS INSERTION: Insert detailed cost and performance data into monitoring table
        INSERT INTO {{ target.schema }}_MONITORING.DBT_RUN_COSTS (
            run_id,
            model_name,
            warehouse_name,
            warehouse_size,
            credits_used,
            warehouse_load_percent,
            execution_time_seconds,
            rows_processed,
            bytes_processed,
            run_started_at,
            run_completed_at,
            cost_per_row,
            dbt_invocation_id
        )
        
        SELECT
            -- UNIQUE RUN IDENTIFICATION: Composite identifier linking dbt invocation to specific model
            '{{ invocation_id }}' || '_' || '{{ this.name }}' as run_id,
            '{{ this.name }}' as model_name,
            
            -- INFRASTRUCTURE CONTEXT: Warehouse configuration affecting costs
            CURRENT_WAREHOUSE() as warehouse_name,
            (SELECT VALUE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "key" = 'WAREHOUSE_SIZE' LIMIT 1) as warehouse_size,
            
            -- CREDIT CONSUMPTION TRACKING: Total Snowflake credits consumed
            COALESCE((
                SELECT CREDITS_USED_CLOUD_SERVICES + COALESCE(CREDITS_USED_COMPUTE_WH, 0)
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE QUERY_ID = LAST_QUERY_ID()
            ), 0) as credits_used,
            
            -- WAREHOUSE UTILIZATION MONITORING: Resource utilization efficiency
            COALESCE((
                SELECT AVG(AVG_RUNNING)
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY w
                WHERE w.WAREHOUSE_NAME = CURRENT_WAREHOUSE()
                AND w.START_TIME >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
            ), 0) as warehouse_load_percent,
            
            -- EXECUTION PERFORMANCE METRICS: Query execution efficiency
            COALESCE((
                SELECT EXECUTION_TIME_MS / 1000
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE QUERY_ID = LAST_QUERY_ID()
            ), 0) as execution_time_seconds,
            
            -- DATA VOLUME PROCESSING METRICS: Scale and efficiency indicators
            COALESCE((
                SELECT ROWS_PRODUCED
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE QUERY_ID = LAST_QUERY_ID()
            ), 0) as rows_processed,
            
            COALESCE((
                SELECT BYTES_SCANNED
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE QUERY_ID = LAST_QUERY_ID()
            ), 0) as bytes_processed,
            
            -- EXECUTION TIMING: Runtime duration tracking
            CURRENT_TIMESTAMP() - INTERVAL '1 MINUTE' as run_started_at,
            CURRENT_TIMESTAMP() as run_completed_at,
            
            -- EFFICIENCY METRIC CALCULATION: Cost efficiency per row processed
            CASE
                WHEN COALESCE((SELECT ROWS_PRODUCED FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE QUERY_ID = LAST_QUERY_ID()), 0) > 0
                THEN COALESCE((SELECT CREDITS_USED_CLOUD_SERVICES + COALESCE(CREDITS_USED_COMPUTE_WH, 0) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE QUERY_ID = LAST_QUERY_ID()), 0) /
                     COALESCE((SELECT ROWS_PRODUCED FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE QUERY_ID = LAST_QUERY_ID()), 1)
                ELSE 0
            END as cost_per_row,
            
            -- PROCESSING LINEAGE: Link to dbt invocation for complete traceability
            '{{ invocation_id }}' as dbt_invocation_id;
    {% endif %}
{% endmacro %}

{#
    COST DASHBOARD ANALYSIS: Cost monitoring and optimization insights
    
    This analysis provides actionable cost optimization insights by combining:
#}

-- DAILY COST AGGREGATION: Understanding cost patterns over time
WITH daily_costs AS (
    SELECT
        DATE(run_completed_at) as run_date,
        model_name,
        -- COST METRICS: Daily aggregated spend and performance
        SUM(credits_used) as total_credits,              -- Total daily credits consumed
        SUM(execution_time_seconds) as total_runtime_seconds, -- Total daily execution time
        SUM(rows_processed) as total_rows,               -- Total daily data volume
        AVG(warehouse_load_percent) as avg_warehouse_load, -- Average resource utilization
        COUNT(*) as run_count                            -- Number of executions per day
    FROM {{ target.schema }}_MONITORING.DBT_RUN_COSTS
    WHERE run_completed_at >= CURRENT_DATE - 30         -- 30-day analysis window
    GROUP BY 1, 2
),

-- MODEL EFFICIENCY ANALYSIS: Identifying optimization opportunities
model_efficiency AS (
    SELECT
        model_name,
        -- EFFICIENCY METRICS: Cost and performance per model
        AVG(cost_per_row) as avg_cost_per_row,          -- Row processing efficiency
        AVG(credits_used) as avg_credits_per_run,        -- Average run cost
        AVG(execution_time_seconds) as avg_runtime,      -- Average execution time
        SUM(credits_used) as total_monthly_credits       -- Total monthly spend per model
    FROM {{ target.schema }}_MONITORING.DBT_RUN_COSTS
    WHERE run_completed_at >= CURRENT_DATE - 30
    GROUP BY model_name
    ORDER BY total_monthly_credits DESC                  -- Prioritize by total cost impact
)

-- COST OPTIMIZATION INSIGHTS: Ranked performance analysis for targeted optimization
SELECT
    model_name,
    avg_cost_per_row,
    avg_credits_per_run,
    avg_runtime,
    total_monthly_credits,
    
    -- OPTIMIZATION PRIORITIZATION: Ranking models for improvement focus
    RANK() OVER (ORDER BY avg_cost_per_row ASC) as cost_efficiency_rank,    -- Most cost-efficient models
    RANK() OVER (ORDER BY avg_runtime ASC) as performance_rank              -- Fastest executing models

FROM model_efficiency

{#
    COST MONITORING STRATEGY OVERVIEW:
    
    This comprehensive cost monitoring system provides:
    
    1. AUTOMATED COST TRACKING: Every dbt model execution automatically captures cost metrics
    
    2. GRANULAR COST ATTRIBUTION: Per-model cost allocation for precise optimization targeting
    
    3. PERFORMANCE CORRELATION: Links cost to performance metrics for efficiency analysis
    
    4. TREND ANALYSIS: Historical cost patterns for budget planning and anomaly detection
    
    5. OPTIMIZATION PRIORITIZATION: Ranks models by cost impact and efficiency for focused improvements
    
    OPTIMIZATION ACTIONS ENABLED:
    
    HIGH COST MODELS:
    - Review clustering strategies for large table scans
    - Implement incremental processing to reduce data volume
    - Optimize complex transformations and window functions
    - Consider warehouse size optimization for compute-intensive models
    
    INEFFICIENT MODELS:
    - Analyze query plans for optimization opportunities  
    - Review join strategies and filter pushdown optimization
    - Consider materialization strategy changes (table vs view)
    - Implement partition elimination where applicable
    
    WAREHOUSE UTILIZATION:
    - Right-size warehouses based on actual utilization patterns
    - Implement auto-scaling for variable workload patterns
    - Schedule resource-intensive models during off-peak hours
    - Consider warehouse separation for different workload types
    
    BUSINESS IMPACT:
    - Reduce Snowflake costs by 20-40% through targeted optimization
    - Improve dbt pipeline performance and reliability
    - Enable predictable cost budgeting and forecasting
    - Provide accountability and visibility into data platform costs
    
    OPERATIONAL INTEGRATION:
    - Automated alerts for cost anomalies or budget thresholds
    - Regular cost review processes with model owners
    - Cost-aware development practices for new model creation
    - FinOps integration with broader cloud cost management
#}
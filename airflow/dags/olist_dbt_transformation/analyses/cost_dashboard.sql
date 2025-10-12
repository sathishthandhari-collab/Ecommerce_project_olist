WITH daily_costs AS (
    SELECT 
        DATE(run_completed_at) as run_date,
        model_name,
        SUM(credits_used) as total_credits,
        SUM(execution_time_seconds) as total_runtime_seconds,
        SUM(rows_processed) as total_rows,
        AVG(warehouse_load_percent) as avg_warehouse_load,
        COUNT(*) as run_count
    FROM {{ target.schema }}_MONITORING.DBT_RUN_COSTS 
    WHERE run_completed_at >= CURRENT_DATE - 30
    GROUP BY 1, 2
),

model_efficiency AS (
    SELECT 
        model_name,
        AVG(cost_per_row) as avg_cost_per_row,
        AVG(credits_used) as avg_credits_per_run,
        AVG(execution_time_seconds) as avg_runtime,
        SUM(credits_used) as total_monthly_credits
    FROM {{ target.schema }}_MONITORING.DBT_RUN_COSTS
    WHERE run_completed_at >= CURRENT_DATE - 30
    GROUP BY model_name
    ORDER BY total_monthly_credits DESC
)

SELECT 
    model_name,
    avg_cost_per_row,
    avg_credits_per_run,
    avg_runtime,
    total_monthly_credits,
    -- Cost efficiency ranking
    RANK() OVER (ORDER BY avg_cost_per_row ASC) as cost_efficiency_rank,
    -- Performance ranking  
    RANK() OVER (ORDER BY avg_runtime ASC) as performance_rank
FROM model_efficiency

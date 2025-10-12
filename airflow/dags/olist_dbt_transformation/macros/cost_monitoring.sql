{% macro capture_run_costs() %}
    {% if execute and target.name == 'prod' %}
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
            '{{ invocation_id }}' || '_' || '{{ this.name }}' as run_id,
            '{{ this.name }}' as model_name,
            CURRENT_WAREHOUSE() as warehouse_name,
            (SELECT VALUE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "key" = 'WAREHOUSE_SIZE' LIMIT 1) as warehouse_size,
            
            -- Get credits used from query history
            COALESCE((
                SELECT CREDITS_USED_CLOUD_SERVICES + COALESCE(CREDITS_USED_COMPUTE_WH, 0)
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
                WHERE QUERY_ID = LAST_QUERY_ID()
            ), 0) as credits_used,
            
            -- Get warehouse load
            COALESCE((
                SELECT AVG(AVG_RUNNING)
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY w
                WHERE w.WAREHOUSE_NAME = CURRENT_WAREHOUSE()
                  AND w.START_TIME >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
            ), 0) as warehouse_load_percent,
            
            -- Execution metrics
            COALESCE((
                SELECT EXECUTION_TIME_MS / 1000
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
                WHERE QUERY_ID = LAST_QUERY_ID()
            ), 0) as execution_time_seconds,
            
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
            
            CURRENT_TIMESTAMP() - INTERVAL '1 MINUTE' as run_started_at,
            CURRENT_TIMESTAMP() as run_completed_at,
            
            -- Cost per row calculation
            CASE 
                WHEN COALESCE((SELECT ROWS_PRODUCED FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE QUERY_ID = LAST_QUERY_ID()), 0) > 0
                THEN COALESCE((SELECT CREDITS_USED_CLOUD_SERVICES + COALESCE(CREDITS_USED_COMPUTE_WH, 0) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE QUERY_ID = LAST_QUERY_ID()), 0) / 
                     COALESCE((SELECT ROWS_PRODUCED FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE QUERY_ID = LAST_QUERY_ID()), 1)
                ELSE 0
            END as cost_per_row,
            
            '{{ invocation_id }}' as dbt_invocation_id;
    {% endif %}
{% endmacro %}

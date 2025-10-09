{{ config(
  materialized='table',
  schema='DBT_MONITORING',
  alias='DBT_RUN_COSTS',
  contract={"enforced": false}
) }}

with template as (
    select
        cast(null as varchar(50)) as run_id,
        cast(null as varchar(100)) as model_name,
        cast(null as varchar(100)) as warehouse_name,
        cast(null as varchar(20)) as warehouse_size,
        cast(null as decimal(10, 4)) as credits_used,
        cast(null as decimal(5, 2)) as warehouse_load_percent,
        cast(null as integer) as execution_time_seconds,
        cast(null as integer) as rows_processed,
        cast(null as bigint) as bytes_processed,
        cast(null as timestamp) as run_started_at,
        cast(null as timestamp) as run_completed_at,
        cast(null as decimal(10, 6)) as cost_per_row,
        cast(null as varchar(50)) as dbt_invocation_id
)

select *
from template
where 1 = 0

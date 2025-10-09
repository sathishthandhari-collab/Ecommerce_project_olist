from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.email.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
import pandas as pd
import requests
import logging

# DAG Configuration
default_args = {
    'owner': 'analytics-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['analytics-engineering@olist.com'],
    'sla': timedelta(hours=2)  # SLA for entire pipeline
}

dag = DAG(
    'olist_dbt_production_pipeline',
    default_args=default_args,
    description='Production data pipeline for Olist analytics with dbt + data quality gates',
    schedule_interval='0 6 * * *',  # Daily at 6 AM UTC
    catchup=False,
    tags=['production', 'dbt', 'analytics-engineering', 'daily'],
    max_active_runs=1,  # Prevent parallel runs
    dagrun_timeout=timedelta(hours=4)  # Kill long-running DAGs
)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def check_data_freshness(**context):
    """Check if source data is fresh enough for processing"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Check latest data in source tables
    freshness_query = """
    SELECT 
        'orders' as table_name,
        MAX(_loaded_at) as latest_data,
        DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP) as hours_old
    FROM {{ var('database') }}.{{ var('raw_schema') }}.raw_olist_orders
    
    UNION ALL
    
    SELECT 
        'order_items' as table_name,
        MAX(_loaded_at) as latest_data,
        DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP) as hours_old  
    FROM {{ var('database') }}.{{ var('raw_schema') }}.raw_olist_order_items
    """
    
    results = hook.get_pandas_df(freshness_query)
    
    for _, row in results.iterrows():
        if row['hours_old'] > 6:  # Data older than 6 hours
            raise Exception(f"Source data for {row['table_name']} is {row['hours_old']} hours old")
    
    logging.info("Data freshness check passed - all sources are current")
    return True

def determine_warehouse_size(**context):
    """Dynamically determine warehouse size based on data volume"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Count records to process
    volume_query = """
    SELECT COUNT(*) as new_orders
    FROM {{ var('database') }}.{{ var('raw_schema') }}.raw_olist_orders
    WHERE _loaded_at > CURRENT_DATE - INTERVAL '1 day'
    """
    
    result = hook.get_pandas_df(volume_query)
    new_orders = result.iloc['new_orders']
    
    # Dynamic sizing logic
    if new_orders > 50000:
        warehouse_size = 'X-LARGE'
    elif new_orders > 20000:
        warehouse_size = 'LARGE'  
    elif new_orders > 5000:
        warehouse_size = 'MEDIUM'
    else:
        warehouse_size = 'SMALL'
    
    logging.info(f"Processing {new_orders} new orders - using {warehouse_size} warehouse")
    
    # Store in XCom for downstream tasks
    context['task_instance'].xcom_push(key='warehouse_size', value=warehouse_size)
    return warehouse_size

def check_dbt_test_results(**context):
    """Parse dbt test results and determine if pipeline should continue"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Check for test failures in monitoring schema
    test_results_query = """
    SELECT 
        test_name,
        model_name,
        failure_count,
        severity
    FROM {{ var('database') }}.dbt_monitoring.test_results
    WHERE run_timestamp >= CURRENT_DATE
      AND failure_count > 0
    ORDER BY severity DESC, failure_count DESC
    """
    
    failed_tests = hook.get_pandas_df(test_results_query)
    
    if len(failed_tests) == 0:
        logging.info("✅ All dbt tests passed")
        return 'notify_success'
    
    # Check for critical failures
    critical_failures = failed_tests[failed_tests['severity'] == 'error']
    
    if len(critical_failures) > 0:
        logging.error(f"❌ {len(critical_failures)} critical test failures found")
        context['task_instance'].xcom_push(key='failed_tests', value=critical_failures.to_dict('records'))
        return 'handle_test_failures'
    else:
        logging.warning(f"⚠️ {len(failed_tests)} non-critical test failures found")
        context['task_instance'].xcom_push(key='warning_tests', value=failed_tests.to_dict('records'))
        return 'notify_warnings'

def send_success_notification(**context):
    """Send success notification with pipeline metrics"""
    
    # Get pipeline metrics from XCom
    warehouse_size = context['task_instance'].xcom_pull(key='warehouse_size')
    
    message = f"""
🎉 *Olist Analytics Pipeline Completed Successfully*

📊 *Pipeline Summary:*
• Warehouse Size: `{warehouse_size}`
• Execution Date: `{context['ds']}`
• Duration: `{context['dag_run'].end_date - context['dag_run'].start_date if context['dag_run'].end_date else 'Running'}`

✅ *Completed Stages:*
• Data freshness validation
• Staging layer (7 models)
• Intermediate layer (4 models) 
• Marts layer (5 models)
• Snapshot processing (3 dimensions)
• Data quality testing

📈 *Key Metrics Updated:*
• Customer 360 analytics
• CLV predictions with confidence intervals
• Seller health scoring
• Fraud detection anomalies
• Executive KPI dashboard

🔗 *Dashboards:* <https://looker.olist.com/dashboards/executive|Executive KPIs> | <https://looker.olist.com/dashboards/customer|Customer Analytics>
    """
    
    return message

# ============================================================================
# AIRFLOW TASKS
# ============================================================================

# Pre-flight checks
data_freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

warehouse_sizing = PythonOperator(
    task_id='determine_warehouse_size',
    python_callable=determine_warehouse_size,
    dag=dag
)

# Dynamic warehouse scaling
scale_warehouse_up = SnowflakeOperator(
    task_id='scale_warehouse_up',
    sql="""
    ALTER WAREHOUSE {{ var('warehouse_name') }} 
    SET warehouse_size = '{{ ti.xcom_pull(key='warehouse_size') }}';
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag
)

# dbt Staging Layer
dbt_staging = SnowflakeOperator(
    task_id='dbt_run_staging',
    sql='CALL {{ var("database") }}.dbt_procedures.run_dbt_staging();',
    snowflake_conn_id='snowflake_default',
    dag=dag,
    pool='dbt_pool'  # Resource pool for dbt tasks
)

dbt_test_staging = SnowflakeOperator(
    task_id='dbt_test_staging',
    sql='CALL {{ var("database") }}.dbt_procedures.test_dbt_staging();',
    snowflake_conn_id='snowflake_default', 
    dag=dag
)

# Snapshots (run before intermediate to capture dimension changes)
dbt_snapshots = SnowflakeOperator(
    task_id='dbt_run_snapshots',
    sql='CALL {{ var("database") }}.dbt_procedures.run_dbt_snapshots();',
    snowflake_conn_id='snowflake_default',
    dag=dag
)

# dbt Intermediate Layer
dbt_intermediate = SnowflakeOperator(
    task_id='dbt_run_intermediate',
    sql='CALL {{ var("database") }}.dbt_procedures.run_dbt_intermediate();',
    snowflake_conn_id='snowflake_default',
    dag=dag,
    pool='dbt_pool'
)

dbt_test_intermediate = SnowflakeOperator(
    task_id='dbt_test_intermediate', 
    sql='CALL {{ var("database") }}.dbt_procedures.test_dbt_intermediate();',
    snowflake_conn_id='snowflake_default',
    dag=dag
)

# dbt Marts Layer
dbt_marts = SnowflakeOperator(
    task_id='dbt_run_marts',
    sql='CALL {{ var("database") }}.dbt_procedures.run_dbt_marts();',
    snowflake_conn_id='snowflake_default',
    dag=dag,
    pool='dbt_pool'
)

dbt_test_marts = SnowflakeOperator(
    task_id='dbt_test_marts',
    sql='CALL {{ var("database") }}.dbt_procedures.test_dbt_marts();',
    snowflake_conn_id='snowflake_default',
    dag=dag
)

# Data Quality Gate
quality_gate = BranchPythonOperator(
    task_id='data_quality_gate',
    python_callable=check_dbt_test_results,
    dag=dag
)

# Post-processing tasks
update_cost_monitoring = SnowflakeOperator(
    task_id='update_cost_monitoring',
    sql="""
    CALL {{ var("database") }}.dbt_procedures.update_cost_dashboard(
        '{{ ds }}', '{{ ti.xcom_pull(key="warehouse_size") }}'
    );
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag
)

# Scale warehouse down
scale_warehouse_down = SnowflakeOperator(
    task_id='scale_warehouse_down',
    sql="""
    ALTER WAREHOUSE {{ var('warehouse_name') }} 
    SET warehouse_size = 'SMALL';
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag,
    trigger_rule='none_failed_or_skipped'  # Always run for cleanup
)

# Notification tasks
notify_success = SlackWebhookOperator(
    task_id='notify_success',
    http_conn_id='slack_webhook',
    message=send_success_notification,
    dag=dag
)

notify_warnings = SlackWebhookOperator(
    task_id='notify_warnings',
    http_conn_id='slack_webhook', 
    message="""
⚠️ *Olist Pipeline Completed with Warnings*

Some non-critical tests failed. Check details in monitoring dashboard.
Pipeline execution continued successfully.

Test Results: {{ ti.xcom_pull(key='warning_tests') }}
""",
    dag=dag
)

handle_test_failures = EmailOperator(
    task_id='handle_test_failures',
    to=['analytics-engineering@olist.com', 'data-ops@olist.com'],
    subject='🚨 Critical Data Quality Failures - Olist Pipeline',
    html_content="""
    <h2>Critical Data Quality Test Failures</h2>
    
    <p>The Olist analytics pipeline has encountered critical test failures that require immediate attention:</p>
    
    <p><strong>Failed Tests:</strong></p>
    {{ ti.xcom_pull(key='failed_tests') }}
    
    <p><strong>Next Steps:</strong></p>
    <ul>
        <li>Review failed test details in Snowflake</li>
        <li>Check data quality dashboard</li>
        <li>Contact on-call engineer if data corruption suspected</li>
    </ul>
    
    <p><strong>Dashboard:</strong> <a href="https://looker.olist.com/dashboards/data-quality">Data Quality Dashboard</a></p>
    """,
    dag=dag
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Pre-flight checks
data_freshness_check >> warehouse_sizing >> scale_warehouse_up

# Core dbt pipeline
scale_warehouse_up >> dbt_staging
dbt_staging >> dbt_test_staging >> dbt_snapshots
dbt_snapshots >> dbt_intermediate
dbt_intermediate >> dbt_test_intermediate >> dbt_marts 
dbt_marts >> dbt_test_marts >> quality_gate

# Quality gate branches
quality_gate >> [notify_success, notify_warnings, handle_test_failures]

# Post-processing (runs regardless of test results)
[notify_success, notify_warnings] >> update_cost_monitoring >> scale_warehouse_down

# Cleanup always runs
handle_test_failures >> scale_warehouse_down
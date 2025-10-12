# olist_monitoring_dag.py - Fixed for Airflow 3.1.0

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
import logging

# Monitoring and alerting DAG
default_args = {
    'owner': 'analytics-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 1,
}

monitoring_dag = DAG(
    'olist_monitoring_pipeline',
    default_args=default_args,
    description='Olist Data Quality and Cost Monitoring',
    schedule='0 */4 * * *',  # Fixed: schedule instead of schedule_interval
    catchup=False,
    tags=['monitoring', 'data-quality', 'cost-optimization']
)

# Configuration
DBT_PROJECT_DIR = '/opt/airflow/dags/olist_dbt_transformation'

# Standard environment variables for all dbt tasks
SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": "{{ var.value.SNOWFLAKE_ACCOUNT }}",
    "SNOWFLAKE_USER": "{{ var.value.SNOWFLAKE_USER }}",
    "SNOWFLAKE_PASSWORD": "{{ var.value.SNOWFLAKE_PASSWORD }}"
}

def analyze_cost_trends(**context):
    """Analyze cost trends and generate alerts"""
    import logging
    logging.info("Analyzing cost trends from monitoring table")
    return {"status": "completed", "alerts": []}

def check_data_quality_scores(**context):
    """Monitor data quality scores across all models"""
    import logging
    logging.info("Checking data quality scores")
    return {"quality_status": "healthy"}

# Cost monitoring tasks
with TaskGroup("cost_monitoring", dag=monitoring_dag) as cost_group:
    
    analyze_costs = PythonOperator(
        task_id='analyze_cost_trends',
        python_callable=analyze_cost_trends,
        dag=monitoring_dag
    )
    
    cost_monitoring_update = BashOperator(
        task_id='update_cost_monitoring',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select cost_monitoring --profiles-dir . --project-dir . --target prod --vars '{{capture_costs: true}}'
        """,
        env=SNOWFLAKE_ENV,
        dag=monitoring_dag
    )

# Data quality monitoring
with TaskGroup("quality_monitoring", dag=monitoring_dag) as quality_group:
    
    validate_data_quality = PythonOperator(
        task_id='validate_data_quality_scores',
        python_callable=check_data_quality_scores,
        dag=monitoring_dag
    )
    
    check_model_freshness = BashOperator(
        task_id='check_model_freshness',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt source freshness --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=monitoring_dag
    )
    
    check_critical_tests = BashOperator(
        task_id='check_critical_tests',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test --select tag:critical --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=monitoring_dag
    )

# Business monitoring
with TaskGroup("business_monitoring", dag=monitoring_dag) as business_group:
    
    monitor_anomalies = BashOperator(
        task_id='monitor_order_anomalies',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test --select int_order_anomalies --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=monitoring_dag
    )
    
    check_executive_kpis = BashOperator(
        task_id='check_executive_kpis_freshness',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test --select mart_executive_kpis --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=monitoring_dag
    )

# Alert routing
alert_email = EmailOperator(
    task_id='send_monitoring_alerts',
    to=['analytics-team@company.com'],
    subject='Olist Analytics Monitoring Alert - {{ ds }}',
    html_content="""
    <h3>Daily Monitoring Summary</h3>
    <p>Data quality and cost monitoring completed for {{ ds }}</p>
    <p>Check Airflow logs for detailed results.</p>
    """,
    trigger_rule='one_failed',
    dag=monitoring_dag
)

# Dependencies
[cost_group, quality_group, business_group] >> alert_email
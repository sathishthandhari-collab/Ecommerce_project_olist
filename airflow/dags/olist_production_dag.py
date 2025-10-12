# olist_production_dag.py - Fixed for Airflow 3.1.0

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.models import Variable
from airflow.sdk import TaskGroup
import logging

# DAG Configuration
default_args = {
    'owner': 'analytics-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['sathishthandhari@gmail.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'sla': timedelta(minutes=20),
    'execution_timeout': timedelta(minutes=15),
}

dag = DAG(
    'olist_production_pipeline',
    default_args=default_args,
    description='Olist Analytics Engineering Production Pipeline',
    # schedule='0 6 * * *',  # Fixed: schedule instead of schedule_interval
    catchup=False,
    max_active_runs=1,
    tags=['production', 'analytics-engineering', 'dbt', 'olist']
)

# Configuration
DBT_PROJECT_DIR = '/opt/airflow/dags/olist_dbt_transformation'

# Standard environment variables for all dbt tasks
SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": "{{ var.value.SNOWFLAKE_ACCOUNT }}",
    "SNOWFLAKE_USER": "{{ var.value.SNOWFLAKE_USER }}",
    "SNOWFLAKE_PASSWORD": "{{ var.value.SNOWFLAKE_PASSWORD }}"
}

def notify_success(**context):
    """Send success notification"""
    logging.info(f"Pipeline completed successfully for {context['ds']}")
    return "Success notification sent"

def notify_failure(**context):
    """Send failure notification"""
    logging.error(f"Pipeline failed for {context['ds']}")
    return "Failure notification sent"

# Pre-flight checks
with TaskGroup("preflight_checks", dag=dag) as preflight_group:
    
    dbt_deps_check = BashOperator(
        task_id='dbt_deps_install',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation && 
        dbt deps --profiles-dir . --project-dir . &&
        dbt debug --profiles-dir . --project-dir . &&
        dbt compile --profiles-dir . --project-dir .
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )

# Staging layer execution
with TaskGroup("staging_layer", dag=dag) as staging_group:

    staging_orders = BashOperator(
        task_id='run_stg_orders',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation && 
        dbt run --select stg_orders --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    staging_customers = BashOperator(
        task_id='run_stg_customers',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select stg_customers --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    

    staging_payments = BashOperator(
        task_id='run_stg_payments',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select stg_payments --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    staging_order_items = BashOperator(
        task_id='run_stg_order_items',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select stg_order_items --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    staging_products = BashOperator(
        task_id='run_stg_products',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select stg_products --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    staging_sellers = BashOperator(
        task_id='run_stg_sellers',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select stg_sellers --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    staging_reviews = BashOperator(
        task_id='run_stg_reviews',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select stg_reviews --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    staging_geolocation = BashOperator(
        task_id='run_stg_geolocation',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select stg_geolocation --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )

# Intermediate layer execution
with TaskGroup("intermediate_layer", dag=dag) as intermediate_group:
    
    int_customer_360 = BashOperator(
        task_id='run_int_customer_360',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select int_customer_360 --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    int_customer_clv = BashOperator(
        task_id='run_int_customer_lifetime_value',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select int_customer_lifetime_value --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    int_order_anomalies = BashOperator(
        task_id='run_int_order_anomalies',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select int_order_anomalies --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    int_seller_health = BashOperator(
        task_id='run_int_seller_health_score',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select int_seller_health_score --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )

# Marts layer execution
with TaskGroup("marts_layer", dag=dag) as marts_group:
    
    mart_executive_kpis = BashOperator(
        task_id='run_mart_executive_kpis',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select mart_executive_kpis --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    mart_customer_strategy = BashOperator(
        task_id='run_mart_customer_strategy',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select mart_customer_strategy --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    mart_financial_performance = BashOperator(
        task_id='run_mart_financial_performance',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select mart_financial_performance --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    mart_fraud_monitoring = BashOperator(
        task_id='run_mart_fraud_monitoring',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select mart_fraud_monitoring --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    mart_logistics_performance = BashOperator(
        task_id='run_mart_logistics_performance',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select mart_logistics_performance --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    mart_seller_management = BashOperator(
        task_id='run_mart_seller_management',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select mart_seller_management --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    mart_payment_performance = BashOperator(
        task_id='run_mart_payment_method_performance',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select mart_payment_method_performance --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    mart_unit_economics = BashOperator(
        task_id='run_mart_unit_economics',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt run --select mart_unit_economics --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )

# Data quality validation
with TaskGroup("data_quality_tests", dag=dag) as quality_group:
    
    run_dbt_tests = BashOperator(
        task_id='run_all_dbt_tests',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt test --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    # check_data_freshness = BashOperator(
    #     task_id='check_data_freshness',
    #     bash_command="""
    #     export PATH=/home/airflow/.local/bin:$PATH &&
    #     cd /opt/airflow/dags/olist_dbt_transformation &&
    #     dbt source freshness --profiles-dir . --project-dir . --target prod
    #     """,
    #     env=SNOWFLAKE_ENV,
    #     dag=dag
    # )

# Post-processing and notifications
with TaskGroup("post_processing", dag=dag) as post_group:
    
    generate_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&
        dbt docs generate --profiles-dir . --project-dir . --target prod
        """,
        env=SNOWFLAKE_ENV,
        dag=dag
    )
    
    # update_cost_monitoring = BashOperator(
    #     task_id='update_cost_monitoring',
    #     bash_command="""
    #     cd /opt/airflow/dags/olist_dbt_transformation &&
    #     dbt run --select cost_monitoring --profiles-dir . --project-dir . --target prod --vars '{{capture_costs: true}}'
    #     """,
    #     env=SNOWFLAKE_ENV,
    #     dag=dag
    # )
    
    success_notification = PythonOperator(
        task_id='success_notification',
        python_callable=notify_success,
        dag=dag
    )
    
    failure_notification = PythonOperator(
        task_id='failure_notification',
        python_callable=notify_failure,
        trigger_rule='one_failed',
        dag=dag
    )


preflight_group >> staging_group >> intermediate_group >> marts_group >> quality_group >> post_group

# Within staging group - independent execution
staging_orders >> [staging_customers, staging_payments, staging_order_items, 
 staging_products, staging_sellers, staging_reviews, staging_geolocation]

# Intermediate dependencies
staging_group >> int_customer_360 >> [int_customer_clv, int_order_anomalies, int_seller_health ]
int_customer_360 >> int_customer_clv
staging_orders >> [staging_customers, staging_payments, staging_order_items] >> int_customer_clv
staging_orders >> int_order_anomalies
[staging_sellers, staging_order_items] >> int_seller_health

# Marts dependencies 
int_customer_360 >> int_customer_clv >> mart_customer_strategy
int_customer_360 >> [int_customer_clv, int_order_anomalies] >> mart_executive_kpis
staging_group >> mart_financial_performance
int_order_anomalies >> mart_fraud_monitoring
staging_group >> mart_logistics_performance
int_seller_health >> mart_seller_management
staging_payments >> mart_payment_performance
mart_customer_strategy >> mart_unit_economics

# Quality tests after marts
marts_group >> run_dbt_tests

# Post-processing
quality_group >> generate_docs >> success_notification

# Set up failure notifications for critical tasks
for task in [mart_executive_kpis, mart_customer_strategy, mart_fraud_monitoring]:
    task >> failure_notification
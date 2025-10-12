# olist_development_dag.py - Fixed for Airflow 3.1.0

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Development DAG for testing and iteration
default_args = {
    'owner': 'analytics-engineering-dev',
    'depends_on_past': False,
    # 'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dev_dag = DAG(
    'olist_development_pipeline',
    default_args=default_args,
    description='Olist Development Pipeline - Sampled Data',
    schedule=None,  # Fixed: schedule instead of schedule_interval
    catchup=False,
    tags=['development', 'testing', 'dbt', 'olist']
)

# Configuration
DBT_PROJECT_DIR = '/opt/airflow/dags/olist_dbt_transformation'

# Standard environment variables for all dbt tasks
SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": "{{ var.value.SNOWFLAKE_ACCOUNT }}",
    "SNOWFLAKE_USER": "{{ var.value.SNOWFLAKE_USER }}",
    "SNOWFLAKE_PASSWORD": "{{ var.value.SNOWFLAKE_PASSWORD }}"
}

def validate_dev_sample(**context):
    """Validate development sampling is working correctly"""
    import logging
    logging.info("Development pipeline using sampled data for faster iteration")
    return True

# Development workflow with sampling
with TaskGroup("dev_quick_test", dag=dev_dag) as dev_group:

    dev_checks = BashOperator(
        task_id='dev_run_checks',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&  
        dbt debug --profiles-dir . --project-dir . &&
        dbt deps --profiles-dir . --project-dir . &&
        dbt compile --profiles-dir . --project-dir . --target dev
        """,
        env=SNOWFLAKE_ENV,
        dag=dev_dag
    )
    
    dev_staging = BashOperator(
        task_id='dev_run_staging',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&  
        dbt run --select tag:staging --profiles-dir . --project-dir . --target dev
        """,
        env=SNOWFLAKE_ENV,
        dag=dev_dag
    )
    
    dev_intermediate = BashOperator(
        task_id='dev_run_intermediate',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation &&  
        dbt run --select tag:intermediate --profiles-dir . --project-dir . --target dev
        """,
        env=SNOWFLAKE_ENV,
        dag=dev_dag
    )
    
    dev_selected_marts = BashOperator(
        task_id='dev_run_selected_marts',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation && 
        dbt run --select mart_executive_kpis mart_customer_strategy --profiles-dir . --project-dir . --target dev
        """,
        env=SNOWFLAKE_ENV,
        dag=dev_dag
    )
    
    dev_tests = BashOperator(
        task_id='dev_run_tests',
        bash_command="""
        export PATH=/home/airflow/.local/bin:$PATH &&
        cd /opt/airflow/dags/olist_dbt_transformation && 
        dbt test --select tag:staging tag:intermediate --profiles-dir . --project-dir . --target dev
        """,
        env=SNOWFLAKE_ENV,
        dag=dev_dag
    )
    
    validate_sampling = PythonOperator(
        task_id='validate_dev_sampling',
        python_callable=validate_dev_sample,
        dag=dev_dag
    )

# Development dependencies
dev_checks >> dev_staging >> dev_intermediate >> dev_selected_marts >> dev_tests >> validate_sampling
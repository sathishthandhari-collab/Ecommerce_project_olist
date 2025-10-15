# Apache Airflow Questions for Analytics Engineers

## 1. Core Concepts & Architecture

**Q1: Explain Airflow's core components and architecture.**

**Answer:**
- **Web Server**: UI for monitoring and managing workflows
- **Scheduler**: Orchestrates task execution based on dependencies and schedule
- **Executor**: Defines how tasks are executed (Sequential, Local, Celery, Kubernetes)
- **Metadata Database**: Stores DAG definitions, task states, and execution history
- **Workers**: Execute tasks (in distributed setups)

**Q2: What are DAGs and how do they differ from traditional workflows?**

**Answer:**
- **DAG (Directed Acyclic Graph)**: Collection of tasks with dependencies, no cycles allowed
- **Declarative**: Define what should happen, not how
- **Dynamic**: Can be generated programmatically
- **Idempotent**: Re-running should produce same results
- **Atomic**: Tasks should be independent units of work

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    'data_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
)
```

**Q3: Explain different types of Airflow operators and their use cases.**

**Answer:**
- **BashOperator**: Execute bash commands
- **PythonOperator**: Run Python functions
- **SQLOperator**: Execute SQL queries
- **EmailOperator**: Send notifications
- **S3Operator**: Interact with AWS S3
- **KubernetesPodOperator**: Run containers in Kubernetes
- **DBTOperator**: Execute DBT models
- **SensorOperators**: Wait for conditions to be met

## 2. DAG Design Patterns

**Q4: Implement a robust data pipeline DAG with error handling and monitoring.**

**Answer:**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.s3_key_sensor import S3KeySensor
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)  # Service Level Agreement
}

def extract_data(**context):
    """Extract data with error handling and logging"""
    try:
        hook = PostgresHook(postgres_conn_id='source_db')
        sql = """
        SELECT * FROM orders 
        WHERE date(created_at) = '{{ ds }}'
        """
        df = hook.get_pandas_df(sql)
        
        # Data quality checks
        if df.empty:
            raise ValueError(f"No data found for {context['ds']}")
            
        if df.duplicated(['order_id']).any():
            raise ValueError("Duplicate order IDs found")
            
        logging.info(f"Extracted {len(df)} records for {context['ds']}")
        return df.to_json()
        
    except Exception as e:
        logging.error(f"Data extraction failed: {str(e)}")
        raise

dag = DAG(
    'ecommerce_data_pipeline',
    default_args=default_args,
    description='Daily e-commerce data processing pipeline',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
    max_active_runs=1,
    tags=['ecommerce', 'daily', 'critical']
)

# Wait for source data
wait_for_data = S3KeySensor(
    task_id='wait_for_source_data',
    bucket_name='raw-data-bucket',
    bucket_key='orders/{{ ds }}/data.csv',
    timeout=3600,  # 1 hour timeout
    poke_interval=300,  # Check every 5 minutes
    dag=dag
)

# Extract data
extract_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_data,
    dag=dag
)

# Transform with DBT
transform_task = BashOperator(
    task_id='transform_data',
    bash_command='cd /opt/dbt && dbt run --models +fact_orders',
    dag=dag
)

# Data quality tests
test_task = BashOperator(
    task_id='run_data_tests',
    bash_command='cd /opt/dbt && dbt test --models fact_orders',
    dag=dag
)

# Success notification
success_notification = EmailOperator(
    task_id='send_success_email',
    to=['analytics-team@company.com'],
    subject='Pipeline Success - {{ ds }}',
    html_content="""
    <h3>Data Pipeline Completed Successfully</h3>
    <p>Date: {{ ds }}</p>
    <p>DAG: {{ dag.dag_id }}</p>
    <p>Run ID: {{ run_id }}</p>
    """,
    dag=dag
)

# Define dependencies
wait_for_data >> extract_task >> transform_task >> test_task >> success_notification
```

**Q5: How do you implement dynamic DAG generation?**

**Answer:**
```python
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Configuration for different environments
ENVIRONMENTS = {
    'dev': {'db': 'dev_db', 'schema': 'dev_analytics'},
    'staging': {'db': 'staging_db', 'schema': 'staging_analytics'},
    'prod': {'db': 'prod_db', 'schema': 'analytics'}
}

def create_dbt_dag(env_name, env_config):
    """Factory function to create environment-specific DAGs"""
    
    dag = DAG(
        f'dbt_pipeline_{env_name}',
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily' if env_name == 'prod' else None,
        catchup=False,
        tags=[env_name, 'dbt', 'analytics']
    )
    
    # Dynamic task creation based on DBT models
    dbt_models = ['staging', 'intermediate', 'marts']
    previous_task = None
    
    for model_type in dbt_models:
        task = BashOperator(
            task_id=f'dbt_run_{model_type}',
            bash_command=f'''
                cd /opt/dbt && 
                dbt run --models {model_type} 
                --target {env_name} 
                --vars '{{"target_database": "{env_config["db"]}", "target_schema": "{env_config["schema"]}"}}'
            ''',
            dag=dag
        )
        
        if previous_task:
            previous_task >> task
        previous_task = task
    
    return dag

# Generate DAGs for each environment
for env_name, env_config in ENVIRONMENTS.items():
    globals()[f'dbt_dag_{env_name}'] = create_dbt_dag(env_name, env_config)
```

## 3. Task Dependencies & XComs

**Q6: Implement complex task dependencies with branching and conditional logic.**

**Answer:**
```python
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

def check_data_quality(**context):
    """Determine processing path based on data quality"""
    # Simulate data quality check
    data_quality_score = 0.95  # In real scenario, calculate from data
    
    if data_quality_score >= 0.9:
        return 'high_quality_processing'
    elif data_quality_score >= 0.7:
        return 'standard_processing'
    else:
        return 'data_quality_failure'

def process_high_quality_data(**context):
    """Process high-quality data with advanced analytics"""
    ti = context['task_instance']
    # Get data from previous task
    raw_data = ti.xcom_pull(task_ids='extract_data')
    
    # Advanced processing logic
    processed_data = {"status": "high_quality", "records": 10000}
    
    # Push result to XCom
    ti.xcom_push(key='processed_records', value=processed_data['records'])
    return processed_data

# Branching logic
quality_check = BranchPythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

# Different processing paths
high_quality_task = PythonOperator(
    task_id='high_quality_processing',
    python_callable=process_high_quality_data,
    dag=dag
)

standard_task = PythonOperator(
    task_id='standard_processing',
    python_callable=lambda: print("Standard processing"),
    dag=dag
)

failure_task = PythonOperator(
    task_id='data_quality_failure',
    python_callable=lambda: print("Data quality failure"),
    dag=dag
)

# Convergence point - runs regardless of branch taken
join_task = DummyOperator(
    task_id='join_branches',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag
)

# Dependencies
extract_data >> quality_check
quality_check >> [high_quality_task, standard_task, failure_task]
[high_quality_task, standard_task, failure_task] >> join_task
```

**Q7: How do you handle large data transfers between tasks using XComs?**

**Answer:**
```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import pandas as pd

class S3XComBackend:
    """Custom XCom backend for large data storage"""
    
    @staticmethod
    def serialize_value(value, key, task_id, dag_id, run_id):
        if isinstance(value, pd.DataFrame):
            # Store large DataFrames in S3
            s3_key = f"xcom/{dag_id}/{run_id}/{task_id}/{key}.parquet"
            s3_hook = S3Hook(aws_conn_id='aws_default')
            
            # Convert to parquet and upload
            parquet_buffer = value.to_parquet()
            s3_hook.load_bytes(
                bytes_data=parquet_buffer,
                key=s3_key,
                bucket_name='airflow-xcom-bucket'
            )
            
            return json.dumps({
                'type': 'dataframe',
                's3_key': s3_key,
                'shape': value.shape
            })
        
        return json.dumps(value)
    
    @staticmethod
    def deserialize_value(result):
        data = json.loads(result)
        
        if isinstance(data, dict) and data.get('type') == 'dataframe':
            # Retrieve DataFrame from S3
            s3_hook = S3Hook(aws_conn_id='aws_default')
            obj = s3_hook.get_key(
                key=data['s3_key'],
                bucket_name='airflow-xcom-bucket'
            )
            return pd.read_parquet(obj.get()['Body'])
        
        return data

def process_large_dataset(**context):
    """Process large dataset and store efficiently"""
    # Generate large dataset
    df = pd.DataFrame({
        'id': range(1000000),
        'value': np.random.randn(1000000)
    })
    
    # This will automatically use S3 storage
    return df

def consume_large_dataset(**context):
    """Consume large dataset from previous task"""
    ti = context['task_instance']
    df = ti.xcom_pull(task_ids='process_large_dataset')
    
    print(f"Received DataFrame with shape: {df.shape}")
    return df.head().to_dict()
```

## 4. Error Handling & Monitoring

**Q8: Implement comprehensive error handling and alerting strategies.**

**Answer:**
```python
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
import logging

def task_fail_alert(context):
    """Custom failure callback function"""
    task_instance = context.get('task_instance')
    dag_run = context.get('dag_run')
    
    # Log detailed error information
    logging.error(f"""
    Task Failed: {task_instance.task_id}
    DAG: {task_instance.dag_id}
    Execution Date: {dag_run.execution_date}
    Log URL: {task_instance.log_url}
    """)
    
    # Send Slack notification
    slack_msg = f"""
    :red_circle: *Task Failed*
    *DAG:* {task_instance.dag_id}
    *Task:* {task_instance.task_id}
    *Execution Date:* {dag_run.execution_date}
    *Log URL:* {task_instance.log_url}
    """
    
    slack_alert = SlackWebhookOperator(
        task_id='slack_failed',
        http_conn_id='slack_connection',
        message=slack_msg,
        username='airflow'
    )
    
    return slack_alert.execute(context=context)

def retry_with_backoff(**context):
    """Implement exponential backoff for retries"""
    task_instance = context.get('task_instance')
    retry_number = task_instance.try_number
    
    if retry_number <= 3:
        # Exponential backoff: 2^retry_number minutes
        import time
        sleep_time = 2 ** retry_number * 60
        logging.info(f"Retry {retry_number}: waiting {sleep_time} seconds")
        time.sleep(sleep_time)
        
        # Your actual task logic here
        if retry_number == 3:  # Simulate success on final retry
            return "Success after retries"
        else:
            raise Exception(f"Simulated failure on retry {retry_number}")
    else:
        raise Exception("Max retries exceeded")

# Configure DAG with error handling
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': task_fail_alert,
    'email_on_failure': True,
    'email_on_retry': False,
}

# Task with custom retry logic
retry_task = PythonOperator(
    task_id='task_with_backoff',
    python_callable=retry_with_backoff,
    dag=dag
)

# Failure handling task
handle_failure = PythonOperator(
    task_id='handle_failure',
    python_callable=lambda: logging.info("Handling failure gracefully"),
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag
)
```

**Q9: How do you implement data quality monitoring in Airflow?**

**Answer:**
```python
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
import pandas as pd

def data_quality_check(**context):
    """Comprehensive data quality monitoring"""
    hook = PostgresHook(postgres_conn_id='warehouse_db')
    
    # Define quality checks
    quality_checks = [
        {
            'name': 'row_count',
            'sql': "SELECT COUNT(*) FROM fact_sales WHERE date = '{{ ds }}'",
            'expected_min': 1000,
            'expected_max': 50000
        },
        {
            'name': 'null_check', 
            'sql': "SELECT COUNT(*) FROM fact_sales WHERE customer_id IS NULL AND date = '{{ ds }}'",
            'expected_max': 0
        },
        {
            'name': 'duplicate_check',
            'sql': """
                SELECT COUNT(*) FROM (
                    SELECT customer_id, product_id, COUNT(*) 
                    FROM fact_sales 
                    WHERE date = '{{ ds }}'
                    GROUP BY customer_id, product_id 
                    HAVING COUNT(*) > 1
                ) duplicates
            """,
            'expected_max': 0
        }
    ]
    
    failed_checks = []
    
    for check in quality_checks:
        result = hook.get_first(check['sql'])[0]
        
        # Validate against expectations
        if 'expected_min' in check and result < check['expected_min']:
            failed_checks.append(f"{check['name']}: {result} < {check['expected_min']}")
        
        if 'expected_max' in check and result > check['expected_max']:
            failed_checks.append(f"{check['name']}: {result} > {check['expected_max']}")
        
        # Log results
        logging.info(f"Quality check {check['name']}: {result}")
    
    if failed_checks:
        error_msg = "Data quality checks failed:\n" + "\n".join(failed_checks)
        raise AirflowException(error_msg)
    
    return "All data quality checks passed"

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag
)
```

## 5. Advanced Operators & Hooks

**Q10: Create custom operators for specialized data processing tasks.**

**Answer:**
```python
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults
import pandas as pd

class SnowflakeToS3Operator(BaseOperator):
    """Custom operator to export Snowflake data to S3"""
    
    template_fields = ['sql', 's3_key']
    
    @apply_defaults
    def __init__(
        self,
        sql,
        s3_bucket,
        s3_key,
        snowflake_conn_id='snowflake_default',
        aws_conn_id='aws_default',
        file_format='parquet',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.snowflake_conn_id = snowflake_conn_id
        self.aws_conn_id = aws_conn_id
        self.file_format = file_format
    
    def execute(self, context):
        # Get data from Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        df = snowflake_hook.get_pandas_df(self.sql)
        
        self.log.info(f"Retrieved {len(df)} rows from Snowflake")
        
        # Upload to S3
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        
        if self.file_format == 'parquet':
            buffer = df.to_parquet()
            s3_hook.load_bytes(
                bytes_data=buffer,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )
        elif self.file_format == 'csv':
            csv_buffer = df.to_csv(index=False)
            s3_hook.load_string(
                string_data=csv_buffer,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )
        
        self.log.info(f"Successfully uploaded to s3://{self.s3_bucket}/{self.s3_key}")
        return f"s3://{self.s3_bucket}/{self.s3_key}"

# Usage
export_task = SnowflakeToS3Operator(
    task_id='export_sales_data',
    sql="""
        SELECT * FROM fact_sales 
        WHERE date = '{{ ds }}'
    """,
    s3_bucket='data-exports',
    s3_key='sales/{{ ds }}/sales_data.parquet',
    dag=dag
)
```

**Q11: Implement a sensor for monitoring external data sources.**

**Answer:**
```python
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import requests
import json

class APIDataSensor(BaseSensorOperator):
    """Sensor to wait for data availability via API"""
    
    template_fields = ['api_url', 'expected_records']
    
    @apply_defaults
    def __init__(
        self,
        api_url,
        api_key,
        expected_records=None,
        http_conn_id=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_url = api_url
        self.api_key = api_key
        self.expected_records = expected_records
        self.http_conn_id = http_conn_id
    
    def poke(self, context):
        """Check if data is available"""
        try:
            headers = {'Authorization': f'Bearer {self.api_key}'}
            response = requests.get(self.api_url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            available_records = data.get('count', 0)
            
            self.log.info(f"Found {available_records} records")
            
            if self.expected_records:
                return available_records >= self.expected_records
            else:
                return available_records > 0
                
        except requests.exceptions.RequestException as e:
            self.log.error(f"API request failed: {e}")
            return False
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            return False

# Usage
wait_for_api_data = APIDataSensor(
    task_id='wait_for_external_data',
    api_url='https://api.example.com/data/{{ ds }}',
    api_key='your_api_key',
    expected_records=1000,
    timeout=3600,  # 1 hour
    poke_interval=300,  # 5 minutes
    dag=dag
)
```

## 6-30. Additional Advanced Questions

**Q12: Implementing distributed task execution with Celery/Kubernetes**
**Q13: Advanced scheduling patterns and timezone handling**
**Q14: Airflow security and access control implementation**
**Q15: Performance optimization for large-scale DAGs**
**Q16: Integration with external monitoring systems**
**Q17: Custom executors for specialized workloads**
**Q18: Advanced templating and Jinja usage**
**Q19: Airflow REST API usage and automation**
**Q20: Data lineage tracking across DAGs**
**Q21: Multi-tenant Airflow architectures**
**Q22: Advanced configuration management**
**Q23: Disaster recovery and backup strategies**
**Q24: CI/CD pipelines for Airflow DAGs**
**Q25: Advanced logging and debugging techniques**
**Q26: Integration with data catalogs and metadata systems**
**Q27: Real-time processing patterns with Airflow**
**Q28: Advanced testing strategies for DAGs**
**Q29: Airflow plugins and custom UI extensions**
**Q30: Migration strategies and version upgrades**

---

## Key Airflow Best Practices:

1. **DAG Design**: Keep DAGs simple, modular, and testable
2. **Error Handling**: Implement comprehensive error handling and monitoring
3. **Resource Management**: Right-size tasks and manage resource consumption
4. **Security**: Implement proper authentication, authorization, and secret management
5. **Monitoring**: Set up comprehensive monitoring and alerting
6. **Testing**: Test DAGs in isolated environments before production
7. **Documentation**: Document complex logic and dependencies
8. **Performance**: Optimize for scale and efficiency

## Advanced Concepts for Senior AE Roles:
- Custom operator and sensor development
- Advanced scheduling and dependency patterns
- Performance optimization for large-scale deployments
- Integration with cloud-native data platforms
- Security and compliance implementation
- Monitoring and observability frameworks
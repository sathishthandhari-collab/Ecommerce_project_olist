# Apache Airflow Interview Questions for Analytics Engineers

## 30 Comprehensive Airflow Orchestration Questions

### Core Concepts and Architecture (1-10)

**Q1: Explain Airflow's architecture components and how they work together in a production Analytics Engineering environment.**

**Answer**:
**Airflow Core Components**:

1. **Web Server**: UI/API for monitoring and management
2. **Scheduler**: Orchestrates task execution and dependency management
3. **Executor**: Determines how tasks are executed (sequential, parallel, distributed)
4. **Metadata Database**: Stores DAG definitions, task states, and execution history
5. **Workers**: Execute actual tasks (can be distributed across multiple machines)

**Production Architecture for Analytics Engineering**:
```python
# Example production configuration
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Production DAG with proper resource management
default_args = {
    'owner': 'analytics_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'pool': 'dbt_pool',  # Resource pool management
    'queue': 'analytics_queue'  # Queue for distributed execution
}

dag = DAG(
    'analytics_pipeline',
    default_args=default_args,
    description='Production analytics pipeline',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['production', 'analytics', 'dbt']
)
```

**Component Interactions**:
- **Scheduler** reads DAG files and creates DagRun instances
- **Executor** picks up tasks and distributes to **Workers**
- **Workers** report task status back via **Metadata Database**
- **Web Server** provides real-time monitoring interface
- **External systems** (Snowflake, dbt Cloud) integrate via providers

**High Availability Setup**:
```yaml
# docker-compose.yml for HA setup
version: '3.8'
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: airflow
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
    volumes:
      - postgres_db_volume:/var/lib/postgresql/data
    
  redis:
    image: redis:latest
    
  airflow-webserver:
    build: .
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      - postgres
      - redis
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
```

---

**Q2: How do you design DAGs for complex Analytics Engineering workflows with proper dependency management and error handling?**

**Answer**:
**Complex DAG Design Patterns**:

```python
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

# Advanced DAG with comprehensive error handling
def check_data_quality(**context):
    """Dynamic branching based on data quality checks"""
    # Get data quality score from XCom
    quality_score = context['task_instance'].xcom_pull(task_ids='data_quality_check')
    
    if quality_score >= 0.95:
        return 'high_quality_processing'
    elif quality_score >= 0.85:
        return 'standard_processing_with_alerts'
    else:
        return 'data_quality_failure_handling'

def send_failure_notification(context):
    """Custom failure callback with rich context"""
    task_instance = context.get('task_instance')
    dag_run = context.get('dag_run')
    
    failure_info = {
        'dag_id': dag_run.dag_id,
        'task_id': task_instance.task_id,
        'execution_date': dag_run.execution_date,
        'log_url': task_instance.log_url,
        'duration': task_instance.duration,
        'try_number': task_instance.try_number
    }
    
    # Send to Slack, PagerDuty, etc.
    logging.error(f"Task failure: {failure_info}")
    return failure_info

# Complex analytics pipeline DAG
default_args = {
    'owner': 'analytics_engineering',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
    'on_failure_callback': send_failure_notification,
    'sla': timedelta(hours=4),  # SLA monitoring
}

dag = DAG(
    'complex_analytics_pipeline',
    default_args=default_args,
    description='Complex analytics pipeline with advanced patterns',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=['complex', 'analytics', 'production']
)

# Start dummy task
start = DummyOperator(task_id='start', dag=dag)

# Data ingestion task group
with TaskGroup('data_ingestion', dag=dag) as ingestion_group:
    # Wait for upstream data availability
    wait_for_source_data = HttpSensor(
        task_id='wait_for_source_data',
        http_conn_id='source_system_api',
        endpoint='data-availability-check',
        timeout=3600,
        poke_interval=300,
        mode='reschedule'  # Don't block worker slots
    )
    
    # Parallel data extraction tasks
    extract_orders = SnowflakeOperator(
        task_id='extract_orders',
        snowflake_conn_id='snowflake_default',
        sql="""
            COPY INTO staging.raw_orders
            FROM @external_stage/orders/{{ ds }}
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """,
        parameters={'execution_date': '{{ ds }}'}
    )
    
    extract_customers = SnowflakeOperator(
        task_id='extract_customers', 
        snowflake_conn_id='snowflake_default',
        sql="""
            COPY INTO staging.raw_customers  
            FROM @external_stage/customers/{{ ds }}
            FILE_FORMAT = (TYPE = 'CSV')
            ON_ERROR = 'CONTINUE';
        """
    )
    
    wait_for_source_data >> [extract_orders, extract_customers]

# Data quality assessment
data_quality_check = SnowflakeOperator(
    task_id='data_quality_check',
    snowflake_conn_id='snowflake_default',
    sql="""
        SELECT 
            (
                -- Completeness check (non-null primary keys)
                (SELECT COUNT(*) - COUNT(order_id) FROM staging.raw_orders) / 
                NULLIF(COUNT(*), 0) +
                
                -- Validity check (reasonable date ranges)
                (SELECT COUNT(*) FROM staging.raw_orders 
                 WHERE order_date BETWEEN '2020-01-01' AND CURRENT_DATE + 1) / 
                NULLIF(COUNT(*), 0) +
                
                -- Consistency check (referential integrity)
                (SELECT COUNT(*) FROM staging.raw_orders o 
                 JOIN staging.raw_customers c ON o.customer_id = c.customer_id) / 
                NULLIF(COUNT(*), 0)
            ) / 3.0 AS quality_score
        FROM staging.raw_orders;
    """,
    do_xcom_push=True,
    dag=dag
)

# Dynamic branching based on data quality
quality_branch = BranchPythonOperator(
    task_id='quality_branch',
    python_callable=check_data_quality,
    dag=dag
)

# High quality processing path
with TaskGroup('high_quality_processing', dag=dag) as high_quality_group:
    dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command="""
            cd /opt/analytics &&
            dbt run --select tag:staging --target prod
        """,
        env={'DBT_PROFILES_DIR': '/opt/dbt_profiles'}
    )
    
    dbt_intermediate = BashOperator(
        task_id='dbt_intermediate',
        bash_command="""
            cd /opt/analytics &&
            dbt run --select tag:intermediate --target prod
        """
    )
    
    dbt_marts = BashOperator(
        task_id='dbt_marts',
        bash_command="""
            cd /opt/analytics &&
            dbt run --select tag:marts --target prod
        """
    )
    
    dbt_tests = BashOperator(
        task_id='dbt_tests',
        bash_command="""
            cd /opt/analytics &&
            dbt test --target prod
        """
    )
    
    dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_tests

# Standard processing with alerts
standard_processing_with_alerts = BashOperator(
    task_id='standard_processing_with_alerts',
    bash_command="""
        cd /opt/analytics &&
        dbt run --select tag:staging tag:intermediate --target prod &&
        echo "Data quality below optimal threshold - alerting stakeholders"
    """,
    dag=dag
)

# Failure handling path
data_quality_failure_handling = BashOperator(
    task_id='data_quality_failure_handling',
    bash_command="""
        echo "Data quality too low - triggering failure workflow"
        # Implement data quality incident response
        exit 1
    """,
    dag=dag
)

# Post-processing task group
with TaskGroup('post_processing', dag=dag) as post_processing_group:
    update_metadata = SnowflakeOperator(
        task_id='update_metadata',
        snowflake_conn_id='snowflake_default',
        sql="""
            INSERT INTO metadata.pipeline_runs
            VALUES ('{{ dag.dag_id }}', '{{ ts }}', 'SUCCESS', CURRENT_TIMESTAMP());
        """
    )
    
    refresh_bi_cache = BashOperator(
        task_id='refresh_bi_cache',
        bash_command="""
            curl -X POST "{{ var.value.bi_refresh_endpoint }}" \
            -H "Authorization: Bearer {{ var.value.bi_api_token }}"
        """
    )
    
    [update_metadata, refresh_bi_cache]

# End dummy task
end = DummyOperator(
    task_id='end',
    trigger_rule='none_failed_min_one_success',  # Flexible completion
    dag=dag
)

# Define dependencies
start >> ingestion_group >> data_quality_check >> quality_branch
quality_branch >> high_quality_group >> post_processing_group >> end
quality_branch >> standard_processing_with_alerts >> post_processing_group >> end  
quality_branch >> data_quality_failure_handling >> end
```

**Advanced Dependency Patterns**:
```python
# Cross-DAG dependencies
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream_dag = ExternalTaskSensor(
    task_id='wait_for_upstream_dag',
    external_dag_id='upstream_data_pipeline',
    external_task_id='data_validation_complete',
    timeout=7200,
    poke_interval=300,
    dag=dag
)

# Dynamic task generation based on configuration
def create_processing_tasks():
    """Generate tasks dynamically based on configuration"""
    config = Variable.get('processing_config', deserialize_json=True)
    tasks = []
    
    for table_config in config['tables']:
        task = SnowflakeOperator(
            task_id=f"process_{table_config['name']}",
            sql=table_config['sql_template'].format(**table_config['params']),
            dag=dag
        )
        tasks.append(task)
    
    return tasks

# Generate and connect dynamic tasks
processing_tasks = create_processing_tasks()
for i, task in enumerate(processing_tasks):
    if i > 0:
        processing_tasks[i-1] >> task
```

---

**Q3: How do you implement monitoring, alerting, and SLA management for production Analytics Engineering workflows?**

**Answer**:
**Comprehensive Monitoring Strategy**:

1. **SLA Configuration and Monitoring**:
```python
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.callbacks import slack_callback
import logging

# SLA callback function
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Comprehensive SLA miss handling"""
    sla_info = {
        'dag_id': dag.dag_id,
        'missed_tasks': [task.task_id for task in task_list],
        'blocking_tasks': [task.task_id for task in blocking_task_list],
        'execution_date': slas[0].execution_date if slas else None,
        'timestamp': datetime.now()
    }
    
    logging.error(f"SLA MISS: {sla_info}")
    
    # Send to multiple channels
    send_slack_alert(sla_info, severity='HIGH')
    send_pagerduty_alert(sla_info, severity='HIGH')
    update_dashboard_status(sla_info)
    
    return sla_info

# Production DAG with comprehensive SLA management
dag = DAG(
    'analytics_pipeline_with_sla',
    default_args={
        'owner': 'analytics_engineering',
        'start_date': days_ago(1),
        'sla': timedelta(hours=2),  # Global SLA
        'on_failure_callback': failure_callback,
        'on_success_callback': success_callback,
        'on_retry_callback': retry_callback
    },
    sla_miss_callback=sla_miss_callback,
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['production', 'sla_critical']
)

# Task-specific SLA configuration
critical_dbt_run = BashOperator(
    task_id='critical_dbt_run',
    bash_command='dbt run --select tag:critical',
    sla=timedelta(minutes=45),  # Task-specific SLA
    dag=dag
)

standard_dbt_run = BashOperator(
    task_id='standard_dbt_run', 
    bash_command='dbt run --select tag:standard',
    sla=timedelta(hours=1, minutes=30),
    dag=dag
)
```

2. **Advanced Alerting System**:
```python
# Custom alerting framework
import requests
import json
from airflow.hooks.base import BaseHook

def send_slack_alert(context, severity='MEDIUM'):
    """Rich Slack notifications with context"""
    task_instance = context.get('task_instance')
    dag_run = context.get('dag_run')
    
    # Determine color based on severity
    color_map = {
        'LOW': '#36a64f',      # Green
        'MEDIUM': '#ff9500',   # Orange  
        'HIGH': '#ff0000',     # Red
        'CRITICAL': '#8B0000'  # Dark Red
    }
    
    # Build rich message
    slack_message = {
        'channel': '#analytics-alerts',
        'attachments': [{
            'color': color_map.get(severity, '#ff9500'),
            'title': f"Airflow Alert - {severity}",
            'fields': [
                {
                    'title': 'DAG',
                    'value': dag_run.dag_id,
                    'short': True
                },
                {
                    'title': 'Task', 
                    'value': task_instance.task_id,
                    'short': True
                },
                {
                    'title': 'Execution Date',
                    'value': dag_run.execution_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'short': True
                },
                {
                    'title': 'Duration',
                    'value': f"{task_instance.duration} seconds" if task_instance.duration else "N/A",
                    'short': True
                },
                {
                    'title': 'Log URL',
                    'value': f"<{task_instance.log_url}|View Logs>",
                    'short': False
                }
            ],
            'footer': 'Airflow Monitoring',
            'ts': int(context['ts'].timestamp())
        }]
    }
    
    # Send to Slack
    slack_webhook = BaseHook.get_connection('slack_webhook').password
    response = requests.post(slack_webhook, json=slack_message)
    
    if response.status_code != 200:
        logging.error(f"Failed to send Slack alert: {response.text}")

def send_pagerduty_alert(context, severity='MEDIUM'):
    """PagerDuty integration for critical failures"""
    if severity in ['HIGH', 'CRITICAL']:
        task_instance = context.get('task_instance')
        dag_run = context.get('dag_run')
        
        pagerduty_payload = {
            'routing_key': Variable.get('pagerduty_routing_key'),
            'event_action': 'trigger',
            'dedup_key': f"{dag_run.dag_id}-{task_instance.task_id}-{dag_run.execution_date}",
            'payload': {
                'summary': f"Airflow Task Failure: {dag_run.dag_id}.{task_instance.task_id}",
                'source': 'airflow',
                'severity': severity.lower(),
                'custom_details': {
                    'dag_id': dag_run.dag_id,
                    'task_id': task_instance.task_id,
                    'execution_date': str(dag_run.execution_date),
                    'log_url': task_instance.log_url,
                    'try_number': task_instance.try_number
                }
            }
        }
        
        response = requests.post(
            'https://events.pagerduty.com/v2/enqueue',
            json=pagerduty_payload
        )
        
        if response.status_code != 202:
            logging.error(f"Failed to send PagerDuty alert: {response.text}")

# Custom monitoring operator
class DataQualityMonitor(BaseOperator):
    """Custom operator for data quality monitoring"""
    
    def __init__(self, sql_check, threshold, **kwargs):
        super().__init__(**kwargs)
        self.sql_check = sql_check
        self.threshold = threshold
    
    def execute(self, context):
        # Execute quality check
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        result = hook.get_first(self.sql_check)
        quality_score = result[0] if result else 0
        
        # Store in XCom for downstream tasks
        context['task_instance'].xcom_push(key='quality_score', value=quality_score)
        
        # Alert if below threshold
        if quality_score < self.threshold:
            alert_context = {
                'quality_score': quality_score,
                'threshold': self.threshold,
                'check_sql': self.sql_check
            }
            send_slack_alert(alert_context, severity='HIGH')
            
        return quality_score

# Usage in DAG
quality_monitor = DataQualityMonitor(
    task_id='monitor_data_quality',
    sql_check="""
        SELECT 
            (COUNT(*) - COUNT(CASE WHEN customer_id IS NULL THEN 1 END)) / 
            COUNT(*) * 100 as completeness_pct
        FROM staging.customers
    """,
    threshold=95.0,  # 95% completeness required
    dag=dag
)
```

3. **Performance and Cost Monitoring**:
```python
# Performance monitoring DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator

def collect_performance_metrics(**context):
    """Collect comprehensive performance metrics"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Query execution metrics
    execution_metrics = hook.get_pandas_df("""
        SELECT 
            query_text,
            execution_time_ms,
            credits_used,
            rows_produced,
            bytes_scanned,
            warehouse_name,
            start_time
        FROM snowflake.account_usage.query_history
        WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
        AND query_text ILIKE '%dbt%'
    """)
    
    # Cost analysis
    cost_metrics = hook.get_pandas_df("""
        SELECT 
            warehouse_name,
            SUM(credits_used) as total_credits,
            AVG(execution_time_ms) as avg_execution_time,
            COUNT(*) as query_count
        FROM snowflake.account_usage.query_history
        WHERE start_time >= CURRENT_DATE - 1
        GROUP BY warehouse_name
    """)
    
    # Alert on cost anomalies
    for _, row in cost_metrics.iterrows():
        if row['total_credits'] > float(Variable.get('max_daily_credits', 100)):
            send_cost_alert({
                'warehouse': row['warehouse_name'],
                'credits': row['total_credits'],
                'queries': row['query_count']
            })
    
    # Store metrics for dashboard
    context['task_instance'].xcom_push(key='performance_metrics', value=execution_metrics.to_json())
    context['task_instance'].xcom_push(key='cost_metrics', value=cost_metrics.to_json())
    
    return {
        'total_queries': len(execution_metrics),
        'total_credits': cost_metrics['total_credits'].sum(),
        'avg_execution_time': execution_metrics['execution_time_ms'].mean()
    }

performance_monitoring = PythonOperator(
    task_id='collect_performance_metrics',
    python_callable=collect_performance_metrics,
    dag=dag
)
```

4. **Health Check and Status Dashboard**:
```python
def generate_health_dashboard(**context):
    """Generate comprehensive health dashboard data"""
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.state import State
    from datetime import datetime, timedelta
    
    # Get recent DAG run statistics
    recent_runs = session.query(DagRun).filter(
        DagRun.execution_date >= datetime.now() - timedelta(days=7)
    ).all()
    
    # Calculate success rates
    dag_health = {}
    for run in recent_runs:
        dag_id = run.dag_id
        if dag_id not in dag_health:
            dag_health[dag_id] = {'success': 0, 'failed': 0, 'total': 0}
        
        dag_health[dag_id]['total'] += 1
        if run.state == State.SUCCESS:
            dag_health[dag_id]['success'] += 1
        elif run.state == State.FAILED:
            dag_health[dag_id]['failed'] += 1
    
    # Calculate health scores
    for dag_id in dag_health:
        metrics = dag_health[dag_id]
        metrics['success_rate'] = metrics['success'] / max(metrics['total'], 1) * 100
        
        # Health status classification
        if metrics['success_rate'] >= 95:
            metrics['health_status'] = 'EXCELLENT'
        elif metrics['success_rate'] >= 85:
            metrics['health_status'] = 'GOOD'
        elif metrics['success_rate'] >= 70:
            metrics['health_status'] = 'FAIR'
        else:
            metrics['health_status'] = 'POOR'
    
    # Store for external dashboard consumption
    health_summary = {
        'timestamp': datetime.now().isoformat(),
        'dag_health': dag_health,
        'overall_health': sum(d['success_rate'] for d in dag_health.values()) / len(dag_health)
    }
    
    # Send to external monitoring system
    requests.post(
        Variable.get('monitoring_dashboard_endpoint'),
        json=health_summary,
        headers={'Authorization': f"Bearer {Variable.get('dashboard_api_token')}"}
    )
    
    return health_summary

health_check = PythonOperator(
    task_id='generate_health_dashboard',
    python_callable=generate_health_dashboard,
    dag=dag
)
```

**Monitoring Best Practices**:
- **Proactive Alerting**: Alert before SLA breaches, not after
- **Context-Rich Notifications**: Include logs, metrics, and remediation steps
- **Tiered Alert Severity**: Different response protocols for different severities
- **Cost Monitoring**: Track compute costs and optimize resource usage
- **Health Dashboards**: Executive-level visibility into pipeline health
- **Automated Recovery**: Self-healing mechanisms for transient failures

---

### Task Management and Operators (11-20)

**Q4: How do you implement custom operators for Analytics Engineering-specific tasks and integrate with external systems?**

**Answer**:
**Custom Operator Development Framework**:

1. **dbt Operator with Advanced Features**:
```python
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.subprocess import SubprocessHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from typing import Dict, List, Optional, Any
import json
import os
import subprocess
import tempfile

class AdvancedDbtOperator(BaseOperator):
    """
    Advanced dbt operator with comprehensive features:
    - Environment-specific configuration
    - Detailed logging and metrics collection
    - Failure analysis and recommendations
    - Performance monitoring
    - Cost tracking integration
    """
    
    template_fields = ['dbt_command', 'vars', 'target', 'select', 'exclude']
    
    def __init__(
        self,
        dbt_command: str,
        dbt_dir: str = '/opt/dbt',
        target: str = 'prod',
        vars: Optional[Dict[str, Any]] = None,
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        full_refresh: bool = False,
        fail_fast: bool = True,
        collect_metrics: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.dbt_command = dbt_command
        self.dbt_dir = dbt_dir
        self.target = target
        self.vars = vars or {}
        self.select = select
        self.exclude = exclude
        self.full_refresh = full_refresh
        self.fail_fast = fail_fast
        self.collect_metrics = collect_metrics
    
    def execute(self, context):
        """Execute dbt command with comprehensive monitoring"""
        
        # Build dbt command
        cmd_parts = ['dbt', self.dbt_command]
        
        # Add target
        cmd_parts.extend(['--target', self.target])
        
        # Add selection criteria
        if self.select:
            cmd_parts.extend(['--select', self.select])
        if self.exclude:
            cmd_parts.extend(['--exclude', self.exclude])
        
        # Add variables
        if self.vars:
            for key, value in self.vars.items():
                cmd_parts.extend(['--vars', f'{key}: {value}'])
        
        # Add flags
        if self.full_refresh:
            cmd_parts.append('--full-refresh')
        if self.fail_fast:
            cmd_parts.append('--fail-fast')
        
        # Execute with monitoring
        start_time = time.time()
        
        try:
            # Run dbt command
            result = self._run_dbt_command(cmd_parts, context)
            
            # Collect and store metrics
            if self.collect_metrics:
                metrics = self._collect_dbt_metrics(result, start_time)
                context['task_instance'].xcom_push(key='dbt_metrics', value=metrics)
            
            # Parse results for downstream tasks
            parsed_results = self._parse_dbt_results(result)
            context['task_instance'].xcom_push(key='dbt_results', value=parsed_results)
            
            return parsed_results
            
        except subprocess.CalledProcessError as e:
            # Enhanced error handling with analysis
            error_analysis = self._analyze_dbt_failure(e, context)
            
            # Log detailed failure information
            self.log.error(f"dbt command failed: {error_analysis}")
            
            # Store failure context for debugging
            context['task_instance'].xcom_push(key='failure_analysis', value=error_analysis)
            
            raise AirflowException(f"dbt command failed: {error_analysis['summary']}")
    
    def _run_dbt_command(self, cmd_parts: List[str], context) -> subprocess.CompletedProcess:
        """Execute dbt command with proper environment setup"""
        
        # Set up environment
        env = os.environ.copy()
        env.update({
            'DBT_PROFILES_DIR': '/opt/dbt/profiles',
            'DBT_PROJECT_DIR': self.dbt_dir,
            'AIRFLOW_EXECUTION_DATE': context['ds'],
            'AIRFLOW_DAG_ID': context['dag'].dag_id,
            'AIRFLOW_TASK_ID': context['task'].task_id
        })
        
        # Execute command
        self.log.info(f"Executing: {' '.join(cmd_parts)}")
        
        result = subprocess.run(
            cmd_parts,
            cwd=self.dbt_dir,
            env=env,
            capture_output=True,
            text=True,
            check=True  # Raise exception on non-zero exit
        )
        
        # Log output
        if result.stdout:
            self.log.info(f"dbt stdout:\n{result.stdout}")
        if result.stderr:
            self.log.warning(f"dbt stderr:\n{result.stderr}")
        
        return result
    
    def _collect_dbt_metrics(self, result: subprocess.CompletedProcess, start_time: float) -> Dict:
        """Collect comprehensive dbt execution metrics"""
        end_time = time.time()
        
        metrics = {
            'execution_time_seconds': end_time - start_time,
            'command': self.dbt_command,
            'target': self.target,
            'start_time': start_time,
            'end_time': end_time
        }
        
        # Parse dbt output for model-specific metrics
        try:
            # Look for dbt run results
            if 'run_results.json' in os.listdir(f"{self.dbt_dir}/target"):
                with open(f"{self.dbt_dir}/target/run_results.json", 'r') as f:
                    run_results = json.load(f)
                
                metrics.update({
                    'models_run': len([r for r in run_results['results'] if r['resource_type'] == 'model']),
                    'tests_run': len([r for r in run_results['results'] if r['resource_type'] == 'test']),
                    'success_count': len([r for r in run_results['results'] if r['status'] == 'success']),
                    'error_count': len([r for r in run_results['results'] if r['status'] == 'error']),
                    'skip_count': len([r for r in run_results['results'] if r['status'] == 'skipped'])
                })
                
                # Collect model-level timing
                model_timings = {}
                for result in run_results['results']:
                    if result['resource_type'] == 'model':
                        model_timings[result['unique_id']] = result['execution_time']
                
                metrics['model_timings'] = model_timings
        
        except Exception as e:
            self.log.warning(f"Failed to collect detailed dbt metrics: {str(e)}")
        
        return metrics
    
    def _analyze_dbt_failure(self, error: subprocess.CalledProcessError, context) -> Dict:
        """Analyze dbt failures and provide remediation suggestions"""
        
        error_output = error.stderr or error.stdout or ""
        
        analysis = {
            'command': ' '.join(error.cmd) if error.cmd else 'Unknown',
            'return_code': error.returncode,
            'error_output': error_output,
            'summary': 'dbt command failed',
            'category': 'UNKNOWN',
            'suggestions': []
        }
        
        # Categorize common dbt errors
        if 'compilation error' in error_output.lower():
            analysis.update({
                'category': 'COMPILATION_ERROR',
                'summary': 'dbt compilation failed - likely SQL syntax or Jinja templating error',
                'suggestions': [
                    'Check SQL syntax in affected models',
                    'Verify Jinja template variables and macros',
                    'Review model dependencies and references'
                ]
            })
        
        elif 'database error' in error_output.lower():
            analysis.update({
                'category': 'DATABASE_ERROR', 
                'summary': 'Database connection or execution error',
                'suggestions': [
                    'Verify database connection and credentials',
                    'Check warehouse capacity and concurrency limits',
                    'Review SQL for database-specific syntax issues'
                ]
            })
        
        elif 'test' in error_output.lower() and 'fail' in error_output.lower():
            analysis.update({
                'category': 'TEST_FAILURE',
                'summary': 'dbt tests failed - data quality issues detected',
                'suggestions': [
                    'Review failed test results in dbt logs',
                    'Investigate upstream data quality',
                    'Check for recent changes in source data'
                ]
            })
        
        elif 'timeout' in error_output.lower():
            analysis.update({
                'category': 'TIMEOUT_ERROR',
                'summary': 'dbt command timed out',
                'suggestions': [
                    'Increase warehouse size for large models',
                    'Optimize model SQL for performance',
                    'Consider breaking large models into smaller chunks'
                ]
            })
        
        # Extract specific error details
        try:
            # Parse specific model errors
            if 'models/' in error_output:
                import re
                model_pattern = r'models/([^/]+/)*([^/\s]+)\.sql'
                matches = re.findall(model_pattern, error_output)
                if matches:
                    analysis['failed_models'] = [match[-1] for match in matches]
        
        except Exception as e:
            self.log.warning(f"Failed to extract detailed error information: {str(e)}")
        
        return analysis
    
    def _parse_dbt_results(self, result: subprocess.CompletedProcess) -> Dict:
        """Parse dbt command results for downstream task consumption"""
        
        parsed = {
            'success': True,
            'return_code': result.returncode,
            'command': self.dbt_command
        }
        
        # Parse manifest for model information
        try:
            manifest_path = f"{self.dbt_dir}/target/manifest.json"
            if os.path.exists(manifest_path):
                with open(manifest_path, 'r') as f:
                    manifest = json.load(f)
                
                parsed['models'] = list(manifest.get('nodes', {}).keys())
                parsed['sources'] = list(manifest.get('sources', {}).keys())
        
        except Exception as e:
            self.log.warning(f"Failed to parse dbt manifest: {str(e)}")
        
        return parsed
```

2. **Snowflake Cost Monitoring Operator**:
```python
class SnowflakeCostMonitorOperator(BaseOperator):
    """Monitor Snowflake costs and alert on anomalies"""
    
    def __init__(
        self,
        snowflake_conn_id: str = 'snowflake_default',
        cost_threshold_daily: float = 100.0,
        warehouse_pattern: str = '%',
        alert_webhook: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.cost_threshold_daily = cost_threshold_daily
        self.warehouse_pattern = warehouse_pattern
        self.alert_webhook = alert_webhook
    
    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        
        # Query current day costs
        cost_query = """
            SELECT 
                warehouse_name,
                SUM(credits_used) as daily_credits,
                COUNT(DISTINCT query_id) as query_count,
                AVG(execution_time_ms) as avg_execution_ms,
                SUM(credits_used) * 3.0 as estimated_cost_usd  -- Approx $3 per credit
            FROM snowflake.account_usage.query_history
            WHERE start_time >= CURRENT_DATE
              AND warehouse_name LIKE %s
              AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
            ORDER BY daily_credits DESC
        """
        
        results = hook.get_pandas_df(cost_query, parameters=[self.warehouse_pattern])
        
        # Check for cost anomalies
        total_daily_cost = results['estimated_cost_usd'].sum()
        anomalies = results[results['estimated_cost_usd'] > self.cost_threshold_daily]
        
        cost_summary = {
            'total_daily_cost': float(total_daily_cost),
            'total_credits': float(results['daily_credits'].sum()),
            'warehouse_count': len(results),
            'anomalies': len(anomalies),
            'top_consumers': results.head(5).to_dict('records')
        }
        
        # Alert on anomalies
        if not anomalies.empty or total_daily_cost > self.cost_threshold_daily * 2:
            self._send_cost_alert(cost_summary, anomalies, context)
        
        # Store results for downstream consumption
        context['task_instance'].xcom_push(key='cost_summary', value=cost_summary)
        
        return cost_summary
    
    def _send_cost_alert(self, summary, anomalies, context):
        """Send cost anomaly alerts"""
        alert_message = {
            'alert_type': 'COST_ANOMALY',
            'total_daily_cost': summary['total_daily_cost'],
            'threshold': self.cost_threshold_daily,
            'anomalous_warehouses': anomalies['warehouse_name'].tolist(),
            'dag_id': context['dag'].dag_id,
            'execution_date': context['ds']
        }
        
        if self.alert_webhook:
            requests.post(self.alert_webhook, json=alert_message)
        
        self.log.warning(f"Cost anomaly detected: {alert_message}")
```

3. **Data Quality Validation Operator**:
```python
class DataQualityValidationOperator(BaseOperator):
    """Comprehensive data quality validation with configurable rules"""
    
    def __init__(
        self,
        table_name: str,
        validation_rules: Dict[str, Any],
        snowflake_conn_id: str = 'snowflake_default',
        fail_on_violation: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.validation_rules = validation_rules
        self.snowflake_conn_id = snowflake_conn_id
        self.fail_on_violation = fail_on_violation
    
    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        
        validation_results = {
            'table_name': self.table_name,
            'validation_timestamp': datetime.now().isoformat(),
            'rules_checked': len(self.validation_rules),
            'rules_passed': 0,
            'rules_failed': 0,
            'failures': []
        }
        
        for rule_name, rule_config in self.validation_rules.items():
            try:
                result = self._execute_validation_rule(hook, rule_name, rule_config)
                if result['passed']:
                    validation_results['rules_passed'] += 1
                else:
                    validation_results['rules_failed'] += 1
                    validation_results['failures'].append(result)
                    
            except Exception as e:
                self.log.error(f"Failed to execute validation rule {rule_name}: {str(e)}")
                validation_results['failures'].append({
                    'rule_name': rule_name,
                    'error': str(e),
                    'passed': False
                })
                validation_results['rules_failed'] += 1
        
        # Calculate overall quality score
        total_rules = validation_results['rules_checked']
        passed_rules = validation_results['rules_passed']
        quality_score = (passed_rules / total_rules) * 100 if total_rules > 0 else 0
        
        validation_results['quality_score'] = quality_score
        
        # Store results
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        
        # Fail task if configured and quality is poor
        if self.fail_on_violation and validation_results['rules_failed'] > 0:
            raise AirflowException(f"Data quality validation failed: {validation_results['failures']}")
        
        return validation_results
    
    def _execute_validation_rule(self, hook, rule_name: str, rule_config: Dict) -> Dict:
        """Execute individual validation rule"""
        
        rule_type = rule_config.get('type')
        
        if rule_type == 'not_null':
            return self._check_not_null(hook, rule_name, rule_config)
        elif rule_type == 'unique':
            return self._check_unique(hook, rule_name, rule_config)  
        elif rule_type == 'range':
            return self._check_range(hook, rule_name, rule_config)
        elif rule_type == 'custom_sql':
            return self._check_custom_sql(hook, rule_name, rule_config)
        else:
            raise ValueError(f"Unknown validation rule type: {rule_type}")
    
    def _check_not_null(self, hook, rule_name: str, rule_config: Dict) -> Dict:
        """Check for null values in specified columns"""
        columns = rule_config.get('columns', [])
        threshold = rule_config.get('max_null_percentage', 0)
        
        for column in columns:
            query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT({column}) as non_null_rows,
                    (COUNT(*) - COUNT({column})) / COUNT(*) * 100 as null_percentage
                FROM {self.table_name}
            """
            
            result = hook.get_first(query)
            null_percentage = result[2] if result else 100
            
            if null_percentage > threshold:
                return {
                    'rule_name': rule_name,
                    'passed': False,
                    'message': f"Column {column} has {null_percentage:.2f}% nulls (threshold: {threshold}%)",
                    'details': {'null_percentage': null_percentage, 'threshold': threshold}
                }
        
        return {'rule_name': rule_name, 'passed': True, 'message': 'All null checks passed'}
    
    def _check_unique(self, hook, rule_name: str, rule_config: Dict) -> Dict:
        """Check for unique values in specified columns"""
        columns = rule_config.get('columns', [])
        
        for column in columns:
            query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT {column}) as unique_values
                FROM {self.table_name}
            """
            
            result = hook.get_first(query)
            if result and result[0] != result[1]:
                return {
                    'rule_name': rule_name,
                    'passed': False,
                    'message': f"Column {column} has duplicate values",
                    'details': {'total_rows': result[0], 'unique_values': result[1]}
                }
        
        return {'rule_name': rule_name, 'passed': True, 'message': 'All uniqueness checks passed'}
    
    def _check_custom_sql(self, hook, rule_name: str, rule_config: Dict) -> Dict:
        """Execute custom SQL validation"""
        sql = rule_config.get('sql')
        expected_result = rule_config.get('expected_result', 0)
        
        if not sql:
            raise ValueError("Custom SQL validation requires 'sql' parameter")
        
        # Replace table placeholder
        sql = sql.replace('{{table_name}}', self.table_name)
        
        result = hook.get_first(sql)
        actual_result = result[0] if result else None
        
        passed = actual_result == expected_result
        
        return {
            'rule_name': rule_name,
            'passed': passed,
            'message': f"Custom SQL check: expected {expected_result}, got {actual_result}",
            'details': {'expected': expected_result, 'actual': actual_result}
        }

# Usage examples
dbt_run = AdvancedDbtOperator(
    task_id='run_dbt_models',
    dbt_command='run',
    select='tag:daily',
    target='prod',
    vars={'start_date': '{{ ds }}'},
    collect_metrics=True,
    dag=dag
)

cost_monitor = SnowflakeCostMonitorOperator(
    task_id='monitor_costs',
    cost_threshold_daily=200.0,
    warehouse_pattern='%ANALYTICS%',
    alert_webhook=Variable.get('cost_alert_webhook'),
    dag=dag
)

data_quality = DataQualityValidationOperator(
    task_id='validate_customer_data',
    table_name='marts.dim_customers',
    validation_rules={
        'primary_key_check': {
            'type': 'unique',
            'columns': ['customer_id']
        },
        'completeness_check': {
            'type': 'not_null', 
            'columns': ['customer_id', 'email'],
            'max_null_percentage': 1.0
        },
        'business_rule_check': {
            'type': 'custom_sql',
            'sql': 'SELECT COUNT(*) FROM {{table_name}} WHERE created_date > CURRENT_DATE',
            'expected_result': 0
        }
    },
    dag=dag
)
```

These custom operators provide:
- **Advanced dbt integration** with metrics collection and failure analysis
- **Cost monitoring** with automated alerting on anomalies
- **Comprehensive data quality validation** with flexible rule configuration
- **Rich error handling** with actionable remediation suggestions
- **Performance tracking** and optimization recommendations

---

### Scaling and Performance (21-30)

**Q5: How do you design Airflow for high-scale Analytics Engineering workloads with proper resource management and optimization?**

**Answer**:
**High-Scale Architecture Design**:

1. **Distributed Execution with Kubernetes**:
```yaml
# kubernetes_executor.yaml - Production Kubernetes setup
apiVersion: v1
kind: ConfigMap
metadata:
  name: airflow-config
data:
  airflow.cfg: |
    [core]
    executor = KubernetesExecutor
    parallelism = 32
    dag_concurrency = 16
    max_active_runs_per_dag = 3
    
    [kubernetes]
    namespace = airflow
    delete_worker_pods = True
    delete_worker_pods_on_start = True
    worker_container_repository = my-registry/airflow-worker
    worker_container_tag = latest
    
    [kubernetes_secrets]
    SNOWFLAKE_PRIVATE_KEY = airflow-secrets=snowflake_private_key
    
    [kubernetes_environment_variables]
    AIRFLOW_VAR_ENVIRONMENT = production
---
# Worker pod template for analytics workloads
apiVersion: v1
kind: Pod
metadata:
  name: analytics-worker-template
spec:
  containers:
  - name: base
    image: my-registry/airflow-analytics:latest
    resources:
      requests:
        memory: "2Gi"
        cpu: "1000m"
      limits:
        memory: "8Gi" 
        cpu: "4000m"
    env:
    - name: DBT_PROFILES_DIR
      value: "/opt/airflow/dbt_profiles"
    volumeMounts:
    - name: dbt-profiles
      mountPath: /opt/airflow/dbt_profiles
      readOnly: true
    - name: analytics-code
      mountPath: /opt/analytics
      readOnly: true
  volumes:
  - name: dbt-profiles
    secret:
      secretName: dbt-profiles
  - name: analytics-code
    configMap:
      name: analytics-dbt-code
  nodeSelector:
    workload: analytics
  tolerations:
  - key: analytics-workload
    operator: Equal
    value: "true"
    effect: NoSchedule
```

2. **Advanced Resource Management**:
```python
from airflow.models import Pool
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Resource pool configuration for different workload types
def create_resource_pools():
    """Create resource pools for workload isolation"""
    pools = [
        {'pool': 'dbt_staging_pool', 'slots': 8, 'description': 'Staging layer processing'},
        {'pool': 'dbt_marts_pool', 'slots': 4, 'description': 'Mart layer processing'}, 
        {'pool': 'heavy_compute_pool', 'slots': 2, 'description': 'Resource-intensive tasks'},
        {'pool': 'data_quality_pool', 'slots': 6, 'description': 'Data quality checks'},
        {'pool': 'external_api_pool', 'slots': 10, 'description': 'External API calls'}
    ]
    
    for pool_config in pools:
        Pool.create_or_update_pool(**pool_config)

# Dynamic resource allocation based on workload
def get_task_resources(task_type: str, data_volume: str = 'medium') -> dict:
    """Dynamic resource allocation based on task characteristics"""
    
    resource_map = {
        'dbt_staging': {
            'small': {'cpu': '500m', 'memory': '1Gi', 'pool': 'dbt_staging_pool'},
            'medium': {'cpu': '1000m', 'memory': '2Gi', 'pool': 'dbt_staging_pool'}, 
            'large': {'cpu': '2000m', 'memory': '4Gi', 'pool': 'dbt_staging_pool'}
        },
        'dbt_marts': {
            'small': {'cpu': '1000m', 'memory': '2Gi', 'pool': 'dbt_marts_pool'},
            'medium': {'cpu': '2000m', 'memory': '4Gi', 'pool': 'dbt_marts_pool'},
            'large': {'cpu': '4000m', 'memory': '8Gi', 'pool': 'heavy_compute_pool'}
        },
        'data_quality': {
            'small': {'cpu': '500m', 'memory': '1Gi', 'pool': 'data_quality_pool'},
            'medium': {'cpu': '1000m', 'memory': '2Gi', 'pool': 'data_quality_pool'},
            'large': {'cpu': '1500m', 'memory': '3Gi', 'pool': 'data_quality_pool'}
        }
    }
    
    return resource_map.get(task_type, {}).get(data_volume, resource_map[task_type]['medium'])

# High-scale analytics DAG with optimized resource usage
default_args = {
    'owner': 'analytics_engineering',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'pool': 'default_pool'  # Default pool
}

dag = DAG(
    'high_scale_analytics_pipeline',
    default_args=default_args,
    description='High-scale analytics pipeline with optimized resources',
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=16,  # DAG-level concurrency limit
    tags=['high-scale', 'production', 'optimized']
)

# Staging layer with parallel processing
with TaskGroup('staging_layer', dag=dag) as staging_group:
    staging_tables = ['customers', 'orders', 'products', 'payments', 'reviews']
    
    for table in staging_tables:
        resources = get_task_resources('dbt_staging', 'medium')
        
        staging_task = BashOperator(
            task_id=f'stage_{table}',
            bash_command=f'dbt run --select staging.stg_{table} --target prod',
            pool=resources['pool'],
            # Kubernetes pod override for resource allocation
            executor_config={
                'KubernetesExecutor': {
                    'request_memory': resources['memory'],
                    'request_cpu': resources['cpu'],
                    'limit_memory': resources['memory'], 
                    'limit_cpu': resources['cpu']
                }
            }
        )

# Intermediate layer with dependency-aware scheduling
with TaskGroup('intermediate_layer', dag=dag) as intermediate_group:
    # Customer analytics (heavy compute)
    customer_360 = BashOperator(
        task_id='customer_360',
        bash_command='dbt run --select intermediate.int_customer_360 --target prod',
        pool='heavy_compute_pool',
        executor_config={
            'KubernetesExecutor': {
                'request_memory': '4Gi',
                'request_cpu': '2000m', 
                'limit_memory': '8Gi',
                'limit_cpu': '4000m',
                'env_vars': {
                    'SNOWFLAKE_WAREHOUSE': 'ANALYTICS_LARGE_WH'  # Use larger warehouse
                }
            }
        }
    )
    
    # Product analytics (standard compute)
    product_metrics = BashOperator(
        task_id='product_metrics',
        bash_command='dbt run --select intermediate.int_product_metrics --target prod', 
        pool='dbt_staging_pool',
        executor_config={
            'KubernetesExecutor': {
                'request_memory': '2Gi',
                'request_cpu': '1000m'
            }
        }
    )
    
    # Set dependencies within intermediate layer
    [customer_360, product_metrics]

# Mart layer with optimized materialization
with TaskGroup('mart_layer', dag=dag) as mart_group:
    mart_resources = get_task_resources('dbt_marts', 'large')
    
    # Critical business marts
    critical_marts = BashOperator(
        task_id='critical_marts',
        bash_command='dbt run --select tag:critical --target prod',
        pool=mart_resources['pool'],
        priority_weight=10,  # High priority
        executor_config={
            'KubernetesExecutor': {
                'request_memory': mart_resources['memory'],
                'request_cpu': mart_resources['cpu'],
                'limit_memory': '16Gi',  # Higher limit for critical workloads
                'limit_cpu': '8000m'
            }
        }
    )
    
    # Standard marts (parallel execution)
    standard_marts = BashOperator(
        task_id='standard_marts',
        bash_command='dbt run --select tag:standard --target prod --threads 8',
        pool='dbt_marts_pool',
        executor_config={
            'KubernetesExecutor': {
                'request_memory': '4Gi',
                'request_cpu': '2000m'
            }
        }
    )
    
    critical_marts >> standard_marts
```

3. **Performance Monitoring and Auto-scaling**:
```python
# Performance monitoring and auto-scaling logic
from airflow.operators.python import PythonOperator
from airflow.providers.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes import client, config
import psutil
import time

def monitor_and_scale_resources(**context):
    """Monitor resource usage and trigger auto-scaling"""
    
    # Get current resource utilization
    cpu_percent = psutil.cpu_percent(interval=1)
    memory_percent = psutil.virtual_memory().percent
    
    # Get current active tasks
    from airflow.models import TaskInstance
    from airflow.utils.state import State
    
    active_tasks = session.query(TaskInstance).filter(
        TaskInstance.state == State.RUNNING
    ).count()
    
    resource_metrics = {
        'cpu_percent': cpu_percent,
        'memory_percent': memory_percent, 
        'active_tasks': active_tasks,
        'timestamp': datetime.now().isoformat()
    }
    
    # Auto-scaling logic
    if cpu_percent > 80 or memory_percent > 85:
        # Scale up - increase pool sizes
        increase_pool_capacity(['dbt_staging_pool', 'dbt_marts_pool'], factor=1.5)
        
        # Trigger larger Snowflake warehouse
        context['task_instance'].xcom_push(key='warehouse_scaling', value='scale_up')
        
    elif cpu_percent < 30 and memory_percent < 40 and active_tasks < 5:
        # Scale down - decrease pool sizes
        decrease_pool_capacity(['dbt_staging_pool', 'dbt_marts_pool'], factor=0.8)
        
        context['task_instance'].xcom_push(key='warehouse_scaling', value='scale_down')
    
    # Store metrics for analysis
    context['task_instance'].xcom_push(key='resource_metrics', value=resource_metrics)
    
    return resource_metrics

def increase_pool_capacity(pool_names: list, factor: float = 1.5):
    """Increase pool capacity dynamically"""
    for pool_name in pool_names:
        pool = Pool.get_pool(pool_name)
        if pool:
            new_slots = int(pool.slots * factor)
            Pool.create_or_update_pool(
                pool=pool_name,
                slots=new_slots,
                description=f"Auto-scaled to {new_slots} slots"
            )
            logging.info(f"Scaled up pool {pool_name} to {new_slots} slots")

# Resource monitoring task
resource_monitor = PythonOperator(
    task_id='monitor_resources',
    python_callable=monitor_and_scale_resources,
    dag=dag
)

# Kubernetes horizontal pod autoscaler configuration
hpa_config = """
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: airflow-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: airflow-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 100
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 600
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
"""
```

4. **Optimized Task Scheduling and Batching**:
```python
# Advanced task scheduling and batching strategies
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class BatchProcessingOperator(BaseOperator):
    """Custom operator for batch processing large datasets"""
    
    def __init__(
        self,
        processing_function,
        batch_size: int = 1000,
        max_workers: int = 4,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.processing_function = processing_function
        self.batch_size = batch_size
        self.max_workers = max_workers
    
    def execute(self, context):
        """Execute batch processing with parallel workers"""
        
        # Get data to process (example: customer IDs)
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        data_query = "SELECT customer_id FROM staging.customers ORDER BY customer_id"
        customer_ids = [row[0] for row in hook.get_records(data_query)]
        
        # Create batches
        batches = [
            customer_ids[i:i + self.batch_size] 
            for i in range(0, len(customer_ids), self.batch_size)
        ]
        
        # Process batches in parallel
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit batch processing jobs
            future_to_batch = {
                executor.submit(self.processing_function, batch, context): batch 
                for batch in batches
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    result = future.result()
                    results.append({
                        'batch_size': len(batch),
                        'processed_records': result.get('processed_records', 0),
                        'processing_time': result.get('processing_time', 0)
                    })
                except Exception as exc:
                    self.log.error(f'Batch processing failed: {exc}')
                    raise
        
        # Aggregate results
        total_processed = sum(r['processed_records'] for r in results)
        total_time = max(r['processing_time'] for r in results) if results else 0
        
        processing_summary = {
            'total_batches': len(batches),
            'total_processed_records': total_processed,
            'total_processing_time': total_time,
            'parallel_efficiency': len(batches) / (total_time / 60) if total_time > 0 else 0
        }
        
        context['task_instance'].xcom_push(key='batch_results', value=processing_summary)
        
        return processing_summary

# Usage in DAG
def process_customer_batch(customer_batch, context):
    """Process a batch of customers"""
    start_time = time.time()
    
    # Batch processing logic
    processed_count = 0
    for customer_id in customer_batch:
        # Process individual customer (placeholder)
        processed_count += 1
        time.sleep(0.01)  # Simulate processing time
    
    processing_time = time.time() - start_time
    
    return {
        'processed_records': processed_count,
        'processing_time': processing_time
    }

batch_processor = BatchProcessingOperator(
    task_id='batch_process_customers',
    processing_function=process_customer_batch,
    batch_size=500,
    max_workers=8,
    pool='heavy_compute_pool',
    dag=dag
)
```

5. **Cache Optimization and State Management**:
```python
# Advanced caching and state management
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import redis
import pickle

class SmartCacheOperator(BaseOperator):
    """Operator with intelligent caching for expensive computations"""
    
    def __init__(
        self,
        computation_function,
        cache_key: str,
        cache_ttl: int = 3600,  # 1 hour default
        force_refresh: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.computation_function = computation_function
        self.cache_key = cache_key
        self.cache_ttl = cache_ttl
        self.force_refresh = force_refresh
        
        # Initialize Redis connection
        redis_conn = BaseHook.get_connection('redis_default')
        self.redis_client = redis.Redis(
            host=redis_conn.host,
            port=redis_conn.port,
            password=redis_conn.password,
            decode_responses=False
        )
    
    def execute(self, context):
        """Execute with intelligent caching"""
        
        # Check cache first (unless force refresh)
        if not self.force_refresh:
            cached_result = self._get_from_cache(context)
            if cached_result is not None:
                self.log.info(f"Retrieved result from cache for key: {self.cache_key}")
                return cached_result
        
        # Execute computation
        self.log.info(f"Cache miss - executing computation for key: {self.cache_key}")
        result = self.computation_function(context)
        
        # Store in cache
        self._store_in_cache(result, context)
        
        return result
    
    def _get_cache_key(self, context) -> str:
        """Generate context-aware cache key"""
        execution_date = context['ds']
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id
        
        return f"{self.cache_key}:{dag_id}:{task_id}:{execution_date}"
    
    def _get_from_cache(self, context):
        """Retrieve from cache with error handling"""
        try:
            cache_key = self._get_cache_key(context)
            cached_data = self.redis_client.get(cache_key)
            
            if cached_data:
                return pickle.loads(cached_data)
            return None
            
        except Exception as e:
            self.log.warning(f"Cache retrieval failed: {str(e)}")
            return None
    
    def _store_in_cache(self, result, context):
        """Store in cache with TTL"""
        try:
            cache_key = self._get_cache_key(context)
            serialized_data = pickle.dumps(result)
            
            self.redis_client.setex(
                cache_key,
                self.cache_ttl,
                serialized_data
            )
            
            self.log.info(f"Stored result in cache with TTL {self.cache_ttl}s")
            
        except Exception as e:
            self.log.warning(f"Cache storage failed: {str(e)}")

# State management for complex workflows
class WorkflowStateManager:
    """Manage complex workflow state across task executions"""
    
    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.state_key = f"workflow_state:{dag_id}"
        
        redis_conn = BaseHook.get_connection('redis_default')
        self.redis_client = redis.Redis(
            host=redis_conn.host,
            port=redis_conn.port,
            password=redis_conn.password
        )
    
    def get_state(self, execution_date: str) -> dict:
        """Get workflow state for execution date"""
        state_key = f"{self.state_key}:{execution_date}"
        
        try:
            state_data = self.redis_client.get(state_key)
            if state_data:
                return json.loads(state_data)
            return {}
        except Exception as e:
            logging.error(f"Failed to get workflow state: {str(e)}")
            return {}
    
    def update_state(self, execution_date: str, state_updates: dict):
        """Update workflow state"""
        current_state = self.get_state(execution_date)
        current_state.update(state_updates)
        
        state_key = f"{self.state_key}:{execution_date}"
        
        try:
            self.redis_client.setex(
                state_key,
                86400,  # 24 hour TTL
                json.dumps(current_state)
            )
        except Exception as e:
            logging.error(f"Failed to update workflow state: {str(e)}")
    
    def mark_checkpoint(self, execution_date: str, checkpoint_name: str):
        """Mark workflow checkpoint for recovery"""
        self.update_state(execution_date, {
            f"checkpoint_{checkpoint_name}": datetime.now().isoformat(),
            "last_checkpoint": checkpoint_name
        })

# Usage example
def expensive_computation(context):
    """Example expensive computation that benefits from caching"""
    # Simulate expensive computation
    time.sleep(30)
    
    result = {
        'computed_value': random.random(),
        'computation_time': 30,
        'execution_date': context['ds']
    }
    
    return result

cached_computation = SmartCacheOperator(
    task_id='cached_expensive_task',
    computation_function=expensive_computation,
    cache_key='expensive_computation_v1',
    cache_ttl=7200,  # 2 hours
    dag=dag
)
```

This high-scale architecture provides:
- **Kubernetes-native execution** with auto-scaling capabilities
- **Dynamic resource allocation** based on workload characteristics
- **Intelligent caching** to avoid redundant computations
- **Batch processing optimization** for large datasets
- **Advanced monitoring** with automated scaling triggers
- **State management** for complex, long-running workflows
- **Resource pool management** for workload isolation and prioritization

The system can handle hundreds of concurrent tasks while maintaining optimal resource utilization and cost efficiency.

---

These Airflow questions demonstrate mastery of:
- **Production-grade orchestration** architecture and best practices
- **Advanced DAG design patterns** with error handling and monitoring
- **Custom operator development** for Analytics Engineering use cases
- **High-scale deployment** with Kubernetes and resource optimization
- **Performance monitoring** and automated scaling strategies
- **Integration patterns** with modern data stack components (dbt, Snowflake, etc.)

Each question includes practical examples that show hands-on experience with enterprise-scale data orchestration challenges.

import json
import boto3
import os
from datetime import datetime

# Initialize AWS clients
emr_client = boto3.client('emr', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    '''
    Lambda function triggered by S3 events to start EMR PySpark job
    Handles batch file uploads to Olist dataset
    '''

    # Parse S3 event
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        print(f"Processing file: s3://{bucket}/{key}")

        # Filter for CSV files only
        if not key.endswith('.csv'):
            print(f"Skipping non-CSV file: {key}")
            return {
                'statusCode': 200,
                'body': json.dumps('File is not CSV, skipping')
            }

        # Determine table name from file path
        file_name = key.split('/')[-1]
        table_name = file_name.replace('.csv', '')

        # Start EMR cluster and run PySpark job
        response = start_emr_job(bucket, key, table_name)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'EMR job started successfully',
                'cluster_id': response['JobFlowId'],
                'file': key
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def start_emr_job(bucket, key, table_name):
    '''
    Start EMR cluster with PySpark job configuration
    '''

    # EMR cluster configuration
    cluster_config = {
        'Name': f'Olist-Ingestion-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        'ReleaseLabel': 'emr-6.15.0',
        'Applications': [
            {'Name': 'Spark'},
            {'Name': 'Hadoop'}
        ],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': 'Core',
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                    'BidPrice': 'OnDemandPrice'
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': os.environ.get('EMR_SUBNET_ID'),
            'Ec2KeyName': os.environ.get('EMR_KEY_PAIR')
        },
        'Steps': [
            {
                'Name': 'Copy PySpark script from S3',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'aws', 's3', 'cp',
                        f's3://{os.environ.get("SCRIPTS_BUCKET")}/pyspark_ingestion.py',
                        '/home/hadoop/'
                    ]
                }
            },
            {
                'Name': 'Run PySpark Ingestion Job',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--master', 'yarn',
                        '--conf', 'spark.executor.memory=4g',
                        '--conf', 'spark.executor.cores=2',
                        '--conf', 'spark.driver.memory=4g',
                        '--conf', 'spark.sql.adaptive.enabled=true',
                        '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
                        '--jars', f's3://{os.environ.get("JARS_BUCKET")}/snowflake-jdbc-3.13.30.jar,'
                                  f's3://{os.environ.get("JARS_BUCKET")}/spark-snowflake_2.12-2.12.0-spark_3.3.jar',
                        '/home/hadoop/pyspark_ingestion.py',
                        '--source-bucket', bucket,
                        '--source-key', key,
                        '--table-name', table_name
                    ]
                }
            }
        ],
        'LogUri': f's3://{os.environ.get("EMR_LOGS_BUCKET")}/emr-logs/',
        'JobFlowRole': os.environ.get('EMR_EC2_ROLE'),
        'ServiceRole': os.environ.get('EMR_SERVICE_ROLE'),
        'VisibleToAllUsers': True,
        'Tags': [
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Project', 'Value': 'Olist-Ingestion'},
            {'Key': 'ManagedBy', 'Value': 'Lambda'}
        ]
    }

    # Launch EMR cluster
    response = emr_client.run_job_flow(**cluster_config)

    return response


# Terraform Configuration for Olist PySpark Ingestion Pipeline
# AWS S3 → Lambda → EMR → Snowflake

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Variables
variable "aws_region" {
  description = "AWS Region"
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name for resource naming"
  default     = "olist-ingestion"
}

variable "environment" {
  description = "Environment (dev/prod)"
  default     = "prod"
}

# S3 Buckets
resource "aws_s3_bucket" "olist_data" {
  bucket = "${var.project_name}-data-${var.environment}"

  tags = {
    Name        = "Olist Data Bucket"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_s3_bucket" "emr_scripts" {
  bucket = "${var.project_name}-scripts-${var.environment}"

  tags = {
    Name        = "EMR Scripts Bucket"
    Environment = var.environment
  }
}

resource "aws_s3_bucket" "emr_logs" {
  bucket = "${var.project_name}-logs-${var.environment}"

  tags = {
    Name        = "EMR Logs Bucket"
    Environment = var.environment
  }
}

resource "aws_s3_bucket" "emr_jars" {
  bucket = "${var.project_name}-jars-${var.environment}"

  tags = {
    Name        = "EMR JARs Bucket"
    Environment = var.environment
  }
}

# S3 Bucket Notification for Lambda Trigger
resource "aws_s3_bucket_notification" "olist_data_notification" {
  bucket = aws_s3_bucket.olist_data.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.emr_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "incoming/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "emr:RunJobFlow",
          "emr:DescribeCluster",
          "emr:ListClusters"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.olist_data.arn,
          "\${aws_s3_bucket.olist_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          aws_iam_role.emr_service_role.arn,
          aws_iam_role.emr_ec2_role.arn
        ]
      }
    ]
  })
}

# Lambda Function
resource "aws_lambda_function" "emr_trigger" {
  filename         = "lambda_deployment.zip"
  function_name    = "${var.project_name}-emr-trigger"
  role            = aws_iam_role.lambda_execution_role.arn
  handler         = "lambda_s3_trigger.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 256

  environment {
    variables = {
      AWS_REGION        = var.aws_region
      SCRIPTS_BUCKET    = aws_s3_bucket.emr_scripts.id
      JARS_BUCKET       = aws_s3_bucket.emr_jars.id
      EMR_LOGS_BUCKET   = aws_s3_bucket.emr_logs.id
      EMR_SERVICE_ROLE  = aws_iam_role.emr_service_role.arn
      EMR_EC2_ROLE      = aws_iam_instance_profile.emr_ec2_instance_profile.arn
      EMR_SUBNET_ID     = var.emr_subnet_id
      EMR_KEY_PAIR      = var.emr_key_pair
    }
  }

  tags = {
    Name        = "EMR Trigger Lambda"
    Environment = var.environment
  }
}

# Lambda Permission for S3
resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.emr_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.olist_data.arn
}

# IAM Roles for EMR
resource "aws_iam_role" "emr_service_role" {
  name = "${var.project_name}-emr-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2_role" {
  name = "${var.project_name}-emr-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2_policy" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_ec2_instance_profile" {
  name = "${var.project_name}-emr-ec2-instance-profile"
  role = aws_iam_role.emr_ec2_role.name
}

# EventBridge Rule for Monitoring (Optional)
resource "aws_cloudwatch_event_rule" "emr_state_change" {
  name        = "${var.project_name}-emr-state-change"
  description = "Capture EMR cluster state changes"

  event_pattern = jsonencode({
    source      = ["aws.emr"]
    detail-type = ["EMR Cluster State Change"]
  })
}

# SNS Topic for Notifications
resource "aws_sns_topic" "pipeline_notifications" {
  name = "${var.project_name}-notifications"

  tags = {
    Name        = "Pipeline Notifications"
    Environment = var.environment
  }
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.emr_trigger.function_name}"
  retention_in_days = 14

  tags = {
    Name        = "Lambda Logs"
    Environment = var.environment
  }
}

# Outputs
output "s3_data_bucket" {
  value       = aws_s3_bucket.olist_data.bucket
  description = "S3 bucket for Olist data"
}

output "lambda_function_arn" {
  value       = aws_lambda_function.emr_trigger.arn
  description = "Lambda function ARN"
}

output "emr_service_role_arn" {
  value       = aws_iam_role.emr_service_role.arn
  description = "EMR service role ARN"
}

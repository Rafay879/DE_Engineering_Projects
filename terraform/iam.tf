data "aws_caller_identity" "current" {}

# ── Glue Execution Role ───────────────────────────────────────────────────────
resource "aws_iam_role" "glue" {
  name = "GlueExecutionRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "GlueExecutionPolicy"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "BronzeReadWrite"
        Effect = "Allow"
        Action = ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket","s3:GetBucketLocation"]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*"
        ]
      },
      {
        Sid    = "SilverReadWrite"
        Effect = "Allow"
        Action = ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket","s3:GetBucketLocation"]
        Resource = [
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*"
        ]
      },
      {
        Sid    = "GoldRead"
        Effect = "Allow"
        Action = ["s3:GetObject","s3:ListBucket","s3:GetBucketLocation"]
        Resource = [
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        Sid    = "GlueAssets"
        Effect = "Allow"
        Action = ["s3:GetObject","s3:PutObject","s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.glue_assets_bucket}",
          "arn:aws:s3:::${var.glue_assets_bucket}/*"
        ]
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase","glue:GetDatabases",
          "glue:CreateTable","glue:GetTable","glue:GetTables",
          "glue:UpdateTable","glue:DeleteTable",
          "glue:GetPartition","glue:GetPartitions",
          "glue:CreatePartition","glue:BatchCreatePartition",
          "glue:GetTableVersions","glue:GetTableVersion"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${var.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${var.account_id}:database/bronze_db",
          "arn:aws:glue:${var.aws_region}:${var.account_id}:database/silver_db",
          "arn:aws:glue:${var.aws_region}:${var.account_id}:table/bronze_db/*",
          "arn:aws:glue:${var.aws_region}:${var.account_id}:table/silver_db/*"
        ]
      },
      {
        Sid      = "CloudWatch"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

# ── dbt Execution Role ────────────────────────────────────────────────────────
resource "aws_iam_role" "dbt" {
  name = "dbtExecutionRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "dbt_policy" {
  name = "dbtExecutionPolicy"
  role = aws_iam_role.dbt.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SilverRead"
        Effect = "Allow"
        Action = ["s3:GetObject","s3:ListBucket","s3:GetBucketLocation"]
        Resource = [
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*"
        ]
      },
      {
        Sid    = "GoldReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject","s3:PutObject","s3:DeleteObject",
          "s3:ListBucket","s3:GetBucketLocation",
          "s3:AbortMultipartUpload","s3:ListMultipartUploadParts"
        ]
        Resource = [
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        Sid    = "AthenaResults"
        Effect = "Allow"
        Action = ["s3:GetObject","s3:PutObject","s3:ListBucket","s3:GetBucketLocation"]
        Resource = [
          aws_s3_bucket.athena_result.arn,
          "${aws_s3_bucket.athena_result.arn}/*"
        ]
      },
      {
        Sid    = "ExplicitDenyBronze"
        Effect = "Deny"
        Action = "s3:*"
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*"
        ]
      },
      {
        Sid    = "Athena"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution","athena:GetQueryExecution",
          "athena:GetQueryResults","athena:StopQueryExecution",
          "athena:ListQueryExecutions","athena:GetWorkGroup"
        ]
        Resource = "*"
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase","glue:GetDatabases",
          "glue:CreateTable","glue:GetTable","glue:GetTables",
          "glue:UpdateTable","glue:DeleteTable",
          "glue:GetPartition","glue:GetPartitions",
          "glue:CreatePartition","glue:BatchCreatePartition",
          "glue:GetTableVersions","glue:GetTableVersion","glue:BatchGetPartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${var.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${var.account_id}:database/silver_db",
          "arn:aws:glue:${var.aws_region}:${var.account_id}:table/silver_db/*",
          "arn:aws:glue:${var.aws_region}:${var.account_id}:database/gold_db",
          "arn:aws:glue:${var.aws_region}:${var.account_id}:table/gold_db/*"
        ]
      },
      {
        Sid    = "ECR"
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ]
        Resource = "*"
      },
      {
        Sid    = "CloudWatch"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

# ── Step Functions Role ───────────────────────────────────────────────────────
resource "aws_iam_role" "stepfunctions" {
  name = "StepFunctionsRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "stepfunctions_policy" {
  name = "StepFunctionsPolicy"
  role = aws_iam_role.stepfunctions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueStartJob"
        Effect = "Allow"
        Action = ["glue:StartJobRun","glue:GetJobRun","glue:GetJobRuns","glue:BatchStopJobRun"]
        Resource = ["arn:aws:glue:${var.aws_region}:${var.account_id}:job/*"]
      },
      {
        Sid    = "ECSRunTask"
        Effect = "Allow"
        Action = ["ecs:RunTask","ecs:StopTask","ecs:DescribeTasks"]
        Resource = ["arn:aws:ecs:${var.aws_region}:${var.account_id}:task-definition/*"]
      },
      {
        Sid    = "ECSPassRole"
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [aws_iam_role.dbt.arn]
        Condition = {
          StringLike = { "iam:PassedToService" = "ecs-tasks.amazonaws.com" }
        }
      },
      {
        Sid      = "SNS"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.pipeline_alerts.arn]
      },
      {
        Sid    = "EventBridge"
        Effect = "Allow"
        Action = [
          "events:PutTargets","events:PutRule",
          "events:DescribeRule","events:DeleteRule","events:RemoveTargets"
        ]
        Resource = [
          "arn:aws:events:${var.aws_region}:${var.account_id}:rule/StepFunctionsGetEventsForGlueJobsRule",
          "arn:aws:events:${var.aws_region}:${var.account_id}:rule/StepFunctionsGetEventsForECSTaskRule"
        ]
      },
      {
        Sid      = "CloudWatch"
        Effect   = "Allow"
        Action   = [
          "logs:CreateLogDelivery","logs:GetLogDelivery",
          "logs:UpdateLogDelivery","logs:DeleteLogDelivery",
          "logs:ListLogDeliveries","logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies","logs:DescribeLogGroups"
        ]
        Resource = "*"
      },
      {
        Sid      = "XRay"
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments","xray:PutTelemetryRecords"]
        Resource = "*"
          },
      {
      Sid    = "DynamoDBRunTracking"
      Effect = "Allow"
      Action = [
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:GetItem"
      ]
      Resource = [
        "arn:aws:dynamodb:${var.aws_region}:${var.account_id}:table/lakehouse-pipeline-state"
      ]
    }
    ]
  })
}

# ── Athena Query Role ─────────────────────────────────────────────────────────
resource "aws_iam_role" "athena" {
  name = "AthenaQueryRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${var.account_id}:root" }
      Action    = "sts:AssumeRole"
      Condition = { Bool = { "aws:MultiFactorAuthPresent" = "true" } }
    }]
  })
}

resource "aws_iam_role_policy" "athena_policy" {
  name = "AthenaQueryPolicy"
  role = aws_iam_role.athena.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GoldReadOnly"
        Effect = "Allow"
        Action = ["s3:GetObject","s3:ListBucket"]
        Resource = [
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        Sid      = "Athena"
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution","athena:GetQueryResults","athena:GetQueryExecution"]
        Resource = "*"
      }
    ]
  })
}
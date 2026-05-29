resource "aws_ecs_cluster" "lakehouse" {
  name = "lakehouse-cluster"
}

resource "aws_ecs_task_definition" "dbt" {
  family                   = "lakehouse-dbt-task"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.dbt.arn
  task_role_arn            = aws_iam_role.dbt.arn

  container_definitions = jsonencode([{
    name  = "dbt-container"
    image = "${var.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com/lakehouse-dbt:${var.ecr_image_tag}"
    essential = true
    environment = [
      { name = "AWS_REGION",            value = var.aws_region },
      { name = "DBT_ATHENA_WORKGROUP",  value = "primary" },
      { name = "DBT_S3_STAGING_DIR",    value = "s3://${var.project}-athena-result/" },
      { name = "DBT_TARGET",            value = "prod" }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/lakehouse/dbt"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "dbt"
        "awslogs-create-group"  = "true"
      }
    }
  }])
}
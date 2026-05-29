output "bronze_bucket" {
  value = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
  value = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
  value = aws_s3_bucket.gold.bucket
}

output "athena_result_bucket" {
  value = aws_s3_bucket.athena_result.bucket
}

output "ecr_repository_url" {
  value = aws_ecr_repository.lakehouse_dbt.repository_url
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.lakehouse.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.lakehouse_pipeline.arn
}

output "sns_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}

output "glue_bronze_job" {
  value = aws_glue_job.bronze.name
}

output "glue_silver_job" {
  value = aws_glue_job.silver.name
}
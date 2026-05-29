variable "aws_region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "account_id" {
  description = "AWS account ID"
  default     = "959666773387"
}

variable "project" {
  description = "Project name prefix"
  default     = "retail-lakehouse"
}

variable "glue_assets_bucket" {
  description = "Glue assets bucket (auto-created by AWS)"
  default     = "aws-glue-assets-959666773387-us-east-1"
}

variable "ecr_image_tag" {
  description = "dbt Docker image tag in ECR"
  default     = "v2"
}

variable "alert_email" {
  description = "Email for pipeline alerts"
  default     = "879abdulrafay@gmail.com"
}

variable "ecs_subnet_ids" {
  description = "Subnets for ECS Fargate tasks"
  default     = [
    "subnet-0b5b1ffc81b82aeb0",
    "subnet-054b83e4abe0fea1f"
  ]
}

variable "ecs_security_group_ids" {
  description = "Security groups for ECS tasks"
  default     = ["sg-021d18077abd81454"]
}
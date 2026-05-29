resource "aws_dynamodb_table" "pipeline_state" {
  name         = "lakehouse-pipeline-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pipeline_name"
  range_key    = "run_date"

  attribute {
    name = "pipeline_name"
    type = "S"
  }

  attribute {
    name = "run_date"
    type = "S"
  }

  tags = {
    Project = var.project
  }
}
resource "aws_ecr_repository" "lakehouse_dbt" {
  name                 = "lakehouse-dbt"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}
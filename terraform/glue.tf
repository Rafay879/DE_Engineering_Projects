resource "aws_glue_job" "bronze" {
  name         = "bronze-ingestion"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
  timeout      = 5

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_assets_bucket}/scripts/bronze-ingestion.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                    = "iceberg"
    "--enable-job-insights"                 = "true"
    "--enable-continuous-cloudwatch-log"    = "true"
    "--enable-metrics"                      = "true"
    "--conf"                                = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${var.project}-bronze/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--run_date"                            = "2026-05-16"
    "--source"                              = "olist"
  }
}

resource "aws_glue_job" "silver" {
  name         = "silver-transform"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
  timeout      = 5

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_assets_bucket}/scripts/silver-transform.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                    = "iceberg"
    "--enable-job-insights"                 = "true"
    "--enable-continuous-cloudwatch-log"    = "true"
    "--enable-metrics"                      = "true"
    "--conf"                                = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${var.project}-silver/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--run_date"                            = "2026-05-16"
    "--source"                              = "olist"
  }
}
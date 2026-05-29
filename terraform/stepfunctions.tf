resource "aws_sfn_state_machine" "lakehouse_pipeline" {
  name     = "Retail_Lakehouse_Pipeline"
  role_arn = aws_iam_role.stepfunctions.arn

  definition = jsonencode({
    Comment = "Retail Lakehouse Pipeline: Bronze → Silver → Gold"
    StartAt = "StartBronzeGlueJob"
    States = {

      StartBronzeGlueJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze.name
          Arguments = {
            "--run_date.$" = "$.run_date"
            "--source.$"   = "$.source"
          }
        }
        ResultPath = "$.bronze_result"
        Next       = "StartSilverGlueJob"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "RecordFailure"
          ResultPath  = "$.error"
        }]
      }

      StartSilverGlueJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver.name
          Arguments = {
            "--run_date.$" = "$.run_date"
            "--source.$"   = "$.source"
          }
        }
        ResultPath = "$.silver_result"
        Next       = "StartdbtECSTask"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "RecordFailure"
          ResultPath  = "$.error"
        }]
      }

      StartdbtECSTask = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync"
        Parameters = {
          Cluster        = aws_ecs_cluster.lakehouse.arn
          TaskDefinition = aws_ecs_task_definition.dbt.arn
          LaunchType     = "FARGATE"
          NetworkConfiguration = {
            AwsvpcConfiguration = {
              Subnets        = var.ecs_subnet_ids
              SecurityGroups = var.ecs_security_group_ids
              AssignPublicIp = "ENABLED"
            }
          }
          Overrides = {
            ContainerOverrides = [{
              Name = "dbt-container"
              Environment = [{
                Name      = "RUN_DATE"
                "Value.$" = "$.run_date"
              }]
            }]
          }
        }
        ResultPath = "$.ecs_result"
        Next       = "RecordSuccess"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "RecordFailure"
          ResultPath  = "$.error"
        }]
      }

      RecordSuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"
        Parameters = {
          TableName = aws_dynamodb_table.pipeline_state.name
          Item = {
            pipeline_name = { "S.$" = "States.Format('retail_lakehouse_{}', $.source)" }
            run_date      = { "S.$" = "$.run_date" }
            status        = { S = "SUCCEEDED" }
            execution_arn = { "S.$" = "$$.Execution.Id" }
            started_at    = { "S.$" = "$$.Execution.StartTime" }
          }
        }
        ResultPath = "$.dynamo_result"
        Next       = "NotifySuccess"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifySuccess"
          ResultPath  = "$.dynamo_error"
        }]
      }

      RecordFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"
        Parameters = {
          TableName = aws_dynamodb_table.pipeline_state.name
          Item = {
            pipeline_name = { "S.$" = "States.Format('retail_lakehouse_{}', $.source)" }
            run_date      = { "S.$" = "$.run_date" }
            status        = { S = "FAILED" }
            execution_arn = { "S.$" = "$$.Execution.Id" }
            started_at    = { "S.$" = "$$.Execution.StartTime" }
          }
        }
        ResultPath = "$.dynamo_result"
        Next       = "NotifyFailure"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.dynamo_error"
        }]
      }

      NotifySuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.pipeline_alerts.arn
          Subject     = "Lakehouse Pipeline SUCCESS"
          "Message.$" = "States.Format('Pipeline completed successfully for run_date: {}', $.run_date)"
        }
        Next = "PipelineComplete"
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.pipeline_alerts.arn
          Subject     = "Lakehouse Pipeline FAILED"
          "Message.$" = "States.Format('Pipeline FAILED for run_date: {}', $.run_date)"
        }
        Next = "PipelineFailed"
      }

      PipelineComplete = { Type = "Succeed" }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "One or more pipeline stages failed"
      }
    }
  })
}
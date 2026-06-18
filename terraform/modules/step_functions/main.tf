# Step Functions Module - Main Configuration
# Pokemon Data Pipeline - State machine definition and configuration

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.name_prefix}-pipeline"
  role_arn = var.stepfunctions_role_arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment = "Pokemon Data Pipeline - Orchestrates ingestion and transformation stages"
    StartAt = "IngestData"
    States = {
      IngestData = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.lambda_function_arn
          "Payload.$"  = "$"
        }
        ResultPath      = "$.ingestion_result"
        ResultSelector = {
          "status.$"        = "$.Payload.status"
          "pokemon_count.$" = "$.Payload.pokemon_count"
          "failed_pokemon.$" = "$.Payload.failed_pokemon"
          "s3_prefix.$"     = "$.Payload.s3_prefix"
        }
        TimeoutSeconds = 900
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "PipelineFailed"
            ResultPath  = "$.error_info"
          }
        ]
        Next = "CheckIngestionStatus"
      }

      CheckIngestionStatus = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.ingestion_result.status"
            StringEquals = "success"
            Next         = "TransformBronzeToSilver"
          },
          {
            Variable     = "$.ingestion_result.status"
            StringEquals = "partial_failure"
            Next         = "EvaluatePartialFailure"
          }
        ]
        Default = "PipelineFailed"
      }

      EvaluatePartialFailure = {
        Type    = "Choice"
        Comment = "Proceed if >90% of Pokemon were successfully ingested"
        Choices = [
          {
            Variable              = "$.ingestion_result.pokemon_count"
            NumericGreaterThan    = 0
            Next                  = "TransformBronzeToSilver"
          }
        ]
        Default = "PipelineFailed"
      }

      TransformBronzeToSilver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = var.bronze_to_silver_job_name
        }
        ResultPath     = "$.bronze_to_silver_result"
        TimeoutSeconds = 3600
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "PipelineFailed"
            ResultPath  = "$.error_info"
          }
        ]
        Next = "TransformSilverToGold"
      }

      TransformSilverToGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = var.silver_to_gold_job_name
        }
        ResultPath     = "$.silver_to_gold_result"
        TimeoutSeconds = 3600
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "PipelineFailed"
            ResultPath  = "$.error_info"
          }
        ]
        Next = "PipelineSuccess"
      }

      PipelineSuccess = {
        Type = "Succeed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineExecutionFailed"
        Cause = "Pipeline execution failed. Check execution history for details including step name, error message, and execution ARN."
      }
    }
  })

  tags = var.common_tags
}

# IAM Module - Outputs
# Pokemon Data Pipeline - Exported values from IAM module

output "lambda_role_arn" {
  description = "ARN of the IAM role for the Lambda ingestion function"
  value       = aws_iam_role.lambda.arn
}

output "lambda_role_name" {
  description = "Name of the IAM role for the Lambda ingestion function"
  value       = aws_iam_role.lambda.name
}

output "glue_role_arn" {
  description = "ARN of the IAM role for Glue transformation jobs"
  value       = aws_iam_role.glue.arn
}

output "glue_role_name" {
  description = "Name of the IAM role for Glue transformation jobs"
  value       = aws_iam_role.glue.name
}

output "stepfunctions_role_arn" {
  description = "ARN of the IAM role for the Step Functions state machine"
  value       = aws_iam_role.stepfunctions.arn
}

output "stepfunctions_role_name" {
  description = "Name of the IAM role for the Step Functions state machine"
  value       = aws_iam_role.stepfunctions.name
}

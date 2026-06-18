# Lambda Module - Outputs
# Pokemon Data Pipeline - Exported values from Lambda module

output "function_name" {
  description = "Name of the Lambda ingestion function"
  value       = aws_lambda_function.ingestion.function_name
}

output "function_arn" {
  description = "ARN of the Lambda ingestion function"
  value       = aws_lambda_function.ingestion.arn
}

output "invoke_arn" {
  description = "Invoke ARN of the Lambda ingestion function (for API Gateway or Step Functions)"
  value       = aws_lambda_function.ingestion.invoke_arn
}

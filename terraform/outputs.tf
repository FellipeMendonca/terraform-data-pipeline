# Root Outputs Configuration
# Pokemon Data Pipeline - Key resource identifiers exposed from the root module
# These outputs provide references to important resources after deployment.

output "s3_bucket_name" {
  description = "Name of the data pipeline S3 bucket"
  value       = module.s3.bucket_name
}

output "s3_bucket_arn" {
  description = "ARN of the data pipeline S3 bucket"
  value       = module.s3.bucket_arn
}

output "lambda_function_arn" {
  description = "ARN of the Lambda ingestion function"
  value       = module.lambda.function_arn
}

output "lambda_function_name" {
  description = "Name of the Lambda ingestion function"
  value       = module.lambda.function_name
}

output "step_function_arn" {
  description = "ARN of the Step Functions state machine"
  value       = module.step_functions.state_machine_arn
}

output "step_function_name" {
  description = "Name of the Step Functions state machine"
  value       = module.step_functions.state_machine_name
}

output "glue_database_name" {
  description = "Name of the Glue Catalog database"
  value       = module.glue.catalog_database_name
}

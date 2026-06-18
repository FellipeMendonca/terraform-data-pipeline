# Step Functions Module - Variables
# Pokemon Data Pipeline - Input variables for Step Functions module

variable "name_prefix" {
  description = "Prefix for resource naming derived from workspace (e.g., dev-pokemon, prd-pokemon)"
  type        = string
}

variable "stepfunctions_role_arn" {
  description = "ARN of the IAM role for the Step Functions state machine"
  type        = string
}

variable "lambda_function_arn" {
  description = "ARN of the Lambda ingestion function to invoke"
  type        = string
}

variable "bronze_to_silver_job_name" {
  description = "Name of the Glue job for Bronze to Silver transformation"
  type        = string
}

variable "silver_to_gold_job_name" {
  description = "Name of the Glue job for Silver to Gold transformation"
  type        = string
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

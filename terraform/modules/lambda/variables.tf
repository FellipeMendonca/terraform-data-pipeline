# Lambda Module - Variables
# Pokemon Data Pipeline - Input variables for Lambda module

variable "name_prefix" {
  description = "Prefix for resource names (e.g., dev-pokemon)"
  type        = string
}

variable "lambda_memory_size" {
  description = "Amount of memory in MB allocated to the Lambda function"
  type        = number
}

variable "lambda_timeout" {
  description = "Maximum execution time for the Lambda function in seconds"
  type        = number
}

variable "lambda_role_arn" {
  description = "ARN of the IAM role for the Lambda function"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for storing ingested data"
  type        = string
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "glue_database_name" {
  description = "Name of the Glue Catalog database for partition registration"
  type        = string
}

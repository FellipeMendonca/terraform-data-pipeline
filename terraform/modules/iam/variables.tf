# IAM Module - Variables
# Pokemon Data Pipeline - Input variables for IAM module

variable "name_prefix" {
  description = "Name prefix for all IAM resources (e.g., dev-pokemon)"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket used by the pipeline"
  type        = string
}

variable "aws_region" {
  description = "AWS region where resources are deployed"
  type        = string
}

variable "account_id" {
  description = "AWS account ID for constructing resource ARNs"
  type        = string
}

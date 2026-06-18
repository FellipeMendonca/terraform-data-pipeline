# S3 Module - Variables
# Pokemon Data Pipeline - Input variables for S3 module

variable "name_prefix" {
  description = "Prefix for resource naming derived from workspace (e.g., dev-pokemon, prd-pokemon)"
  type        = string
}

variable "account_id" {
  description = "AWS account ID used in bucket naming to ensure global uniqueness"
  type        = string
}

variable "common_tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
}

variable "s3_bronze_retention_days" {
  description = "Number of days to retain objects in the Bronze layer before expiration"
  type        = number
}

variable "s3_silver_retention_days" {
  description = "Number of days to retain objects in the Silver layer before expiration"
  type        = number
}

variable "s3_gold_retention_days" {
  description = "Number of days to retain objects in the Gold layer before expiration"
  type        = number
}

variable "s3_noncurrent_expiration_days" {
  description = "Number of days to retain non-current object versions before expiration"
  type        = number
}

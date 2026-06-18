# Glue Module - Variables
# Pokemon Data Pipeline - Input variables for Glue module

variable "env" {
  description = "Environment identifier derived from terraform.workspace (e.g., dev, prd)"
  type        = string
}

variable "name_prefix" {
  description = "Prefix for resource naming derived from workspace (e.g., dev-pokemon, prd-pokemon)"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket where pipeline data is stored"
  type        = string
}

variable "glue_role_arn" {
  description = "ARN of the IAM role for Glue transformation jobs"
  type        = string
}

variable "glue_worker_type" {
  description = "Worker type for Glue transformation jobs (e.g., G.1X, G.2X)"
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of workers for Glue transformation jobs"
  type        = number
  default     = 2
}

variable "glue_timeout" {
  description = "Timeout in minutes for Glue transformation jobs"
  type        = number
  default     = 60
}

variable "common_tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
}

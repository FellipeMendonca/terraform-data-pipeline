variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
}

variable "lambda_memory_size" {
  description = "Memory size in MB for the Lambda ingestion function"
  type        = number
}

variable "lambda_timeout" {
  description = "Timeout in seconds for the Lambda ingestion function"
  type        = number
}

variable "glue_number_of_workers" {
  description = "Number of workers for Glue transformation jobs"
  type        = number
}

variable "glue_worker_type" {
  description = "Worker type for Glue transformation jobs (e.g., G.1X, G.2X)"
  type        = string
}

variable "glue_timeout" {
  description = "Timeout in minutes for Glue transformation jobs"
  type        = number
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

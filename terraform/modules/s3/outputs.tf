# S3 Module - Outputs
# Pokemon Data Pipeline - Exported values from S3 module

output "bucket_name" {
  description = "Name of the data pipeline S3 bucket"
  value       = aws_s3_bucket.data.bucket
}

output "bucket_arn" {
  description = "ARN of the data pipeline S3 bucket"
  value       = aws_s3_bucket.data.arn
}

output "bucket_id" {
  description = "ID of the data pipeline S3 bucket"
  value       = aws_s3_bucket.data.id
}

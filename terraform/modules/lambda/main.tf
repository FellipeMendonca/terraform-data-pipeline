# Lambda Module - Main Configuration
# Pokemon Data Pipeline - Lambda function, packaging, and environment variables

# Package Lambda source code into a zip archive
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/lambda"
  output_path = "${path.module}/../../../dist/lambda_ingestion.zip"
}

# Lambda function for PokeAPI data ingestion
resource "aws_lambda_function" "ingestion" {
  function_name    = "${var.name_prefix}-ingestion"
  role             = var.lambda_role_arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  memory_size      = var.lambda_memory_size
  timeout          = var.lambda_timeout
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  environment {
    variables = {
      S3_BUCKET_NAME = var.s3_bucket_name
    }
  }

  tags = var.common_tags
}

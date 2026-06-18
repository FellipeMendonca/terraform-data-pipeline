# Root Terraform Configuration
# Pokemon Data Pipeline - Main configuration file
# This file wires together all modules and defines the provider configuration.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Data source for current AWS account ID
data "aws_caller_identity" "current" {}

# -----------------------------------------------------------------------------
# Module: IAM
# Creates least-privilege IAM roles for Lambda, Glue, and Step Functions.
# Depends on: S3 module (needs bucket ARN for policy scoping)
# -----------------------------------------------------------------------------
module "iam" {
  source = "./modules/iam"

  name_prefix   = local.name_prefix
  s3_bucket_arn = module.s3.bucket_arn
  aws_region    = var.aws_region
  account_id    = data.aws_caller_identity.current.account_id
}

# -----------------------------------------------------------------------------
# Module: S3
# Creates the data pipeline bucket with lifecycle rules and encryption.
# -----------------------------------------------------------------------------
module "s3" {
  source = "./modules/s3"

  name_prefix                   = local.name_prefix
  account_id                    = data.aws_caller_identity.current.account_id
  common_tags                   = local.common_tags
  s3_bronze_retention_days      = var.s3_bronze_retention_days
  s3_silver_retention_days      = var.s3_silver_retention_days
  s3_gold_retention_days        = var.s3_gold_retention_days
  s3_noncurrent_expiration_days = var.s3_noncurrent_expiration_days
}

# -----------------------------------------------------------------------------
# Module: Lambda
# Deploys the ingestion function that fetches data from PokeAPI into S3 Bronze.
# Depends on: IAM module (role ARN), S3 module (bucket name)
# -----------------------------------------------------------------------------
module "lambda" {
  source = "./modules/lambda"

  name_prefix        = local.name_prefix
  lambda_memory_size = var.lambda_memory_size
  lambda_timeout     = var.lambda_timeout
  lambda_role_arn    = module.iam.lambda_role_arn
  s3_bucket_name     = module.s3.bucket_name
  common_tags        = local.common_tags
}

# -----------------------------------------------------------------------------
# Module: Glue
# Creates Glue Jobs and Catalog resources for Bronze→Silver→Gold transformations.
# Depends on: IAM module (role ARN), S3 module (bucket name)
# -----------------------------------------------------------------------------
module "glue" {
  source = "./modules/glue"

  env                    = local.env
  name_prefix            = local.name_prefix
  s3_bucket_name         = module.s3.bucket_name
  glue_role_arn          = module.iam.glue_role_arn
  glue_worker_type       = var.glue_worker_type
  glue_number_of_workers = var.glue_number_of_workers
  glue_timeout           = var.glue_timeout
  common_tags            = local.common_tags
}

# -----------------------------------------------------------------------------
# Module: Step Functions
# Orchestrates the pipeline: Lambda ingestion → Glue Bronze→Silver → Glue Silver→Gold.
# Depends on: IAM module (role ARN), Lambda module (function ARN), Glue module (job names)
# -----------------------------------------------------------------------------
module "step_functions" {
  source = "./modules/step_functions"

  name_prefix               = local.name_prefix
  stepfunctions_role_arn    = module.iam.stepfunctions_role_arn
  lambda_function_arn       = module.lambda.function_arn
  bronze_to_silver_job_name = module.glue.bronze_to_silver_job_name
  silver_to_gold_job_name   = module.glue.silver_to_gold_job_name
  common_tags               = local.common_tags
}

# S3 Module - Main Configuration
# Pokemon Data Pipeline - S3 bucket, prefixes, encryption, and access controls

# Single data pipeline bucket with prefix-based layer separation
resource "aws_s3_bucket" "data" {
  bucket = "${var.name_prefix}-data-pipeline-${var.account_id}"

  tags = var.common_tags
}

# Enable versioning on the bucket
resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption (SSE-S3)
resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

# Create prefix objects for each Medallion Architecture layer
resource "aws_s3_object" "bronze_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = "bronze/"
  content = ""

  tags = var.common_tags
}

resource "aws_s3_object" "silver_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = "silver/"
  content = ""

  tags = var.common_tags
}

resource "aws_s3_object" "gold_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = "gold/"
  content = ""

  tags = var.common_tags
}

# Lifecycle policies for Medallion Architecture layers
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  # Bronze layer retention
  rule {
    id     = "bronze-retention"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    expiration {
      days = var.s3_bronze_retention_days
    }
  }

  # Silver layer retention
  rule {
    id     = "silver-retention"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    expiration {
      days = var.s3_silver_retention_days
    }
  }

  # Gold layer retention
  rule {
    id     = "gold-retention"
    status = "Enabled"

    filter {
      prefix = "gold/"
    }

    expiration {
      days = var.s3_gold_retention_days
    }
  }

  # Non-current version expiration (applies to all objects)
  rule {
    id     = "noncurrent-version-expiration"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.s3_noncurrent_expiration_days
    }
  }
}

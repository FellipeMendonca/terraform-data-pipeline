# Production environment - full capacity for workloads
aws_region                    = "us-east-1"
lambda_memory_size            = 512
lambda_timeout                = 900    # 15 minutes
glue_number_of_workers        = 5
glue_worker_type              = "G.1X"
glue_timeout                  = 60     # minutes
s3_bronze_retention_days      = 90
s3_silver_retention_days      = 180
s3_gold_retention_days        = 365
s3_noncurrent_expiration_days = 30

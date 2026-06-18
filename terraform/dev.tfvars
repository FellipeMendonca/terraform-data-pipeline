# Development environment - reduced sizing for cost efficiency
aws_region                    = "us-east-1"
lambda_memory_size            = 256
lambda_timeout                = 600    # 10 minutes
glue_number_of_workers        = 2
glue_worker_type              = "G.1X"
glue_timeout                  = 30     # minutes
s3_bronze_retention_days      = 30
s3_silver_retention_days      = 60
s3_gold_retention_days        = 90
s3_noncurrent_expiration_days = 7

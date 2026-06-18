# Implementation Plan: Terraform Data Pipeline

## Overview

This plan implements a serverless data pipeline on AWS using Terraform for infrastructure provisioning. The pipeline ingests Pokémon data from PokeAPI via Lambda, transforms it through the Medallion Architecture (Bronze → Silver → Gold) using Glue Jobs, and is orchestrated by Step Functions. CI/CD is handled via GitHub Actions with branch-to-environment mapping using Terraform workspaces.

## Tasks

- [x] 1. Set up project structure, Terraform backend, and shared locals
  - [x] 1.1 Create directory structure and root Terraform configuration
    - Create `terraform/` directory with `main.tf`, `variables.tf`, `outputs.tf`, `backend.tf`, `locals.tf`
    - Create module directories: `modules/s3/`, `modules/lambda/`, `modules/glue/`, `modules/step_functions/`, `modules/iam/`
    - Each module directory must contain `main.tf`, `variables.tf`, `outputs.tf`
    - Create `src/lambda/`, `src/glue/transformations/`, `src/glue/utils/`, `tests/unit/lambda/`, `tests/unit/glue/`, `tests/property/`
    - _Requirements: 6.1, 6.2_

  - [x] 1.2 Configure Terraform remote backend and workspace locals
    - Implement `backend.tf` with S3 backend and DynamoDB locking (bucket: `terraform-state-pokemon-pipeline`, key: `pokemon-data-pipeline/terraform.tfstate`)
    - Implement `locals.tf` with workspace-derived locals (`env = terraform.workspace`, `name_prefix`, `common_tags` including Environment tag)
    - Define root `variables.tf` with all parameterized variables (lambda_memory_size, lambda_timeout, glue_number_of_workers, glue_worker_type, glue_timeout, s3 retention days, etc.)
    - _Requirements: 6.5, 6.6, 10.1, 10.3, 10.4, 10.7_

  - [x] 1.3 Create environment-specific tfvars files
    - Create `terraform/dev.tfvars` with reduced sizing (Lambda 256MB/10min, Glue 2 DPUs/30min, Bronze 30d/Silver 60d/Gold 90d retention)
    - Create `terraform/prd.tfvars` with production sizing (Lambda 512MB/15min, Glue 5 DPUs/60min, Bronze 90d/Silver 180d/Gold 365d retention)
    - _Requirements: 6.4, 10.2, 10.5, 10.6_

- [x] 2. Implement IAM module with least-privilege policies
  - [x] 2.1 Create IAM roles and policies for Lambda, Glue, and Step Functions
    - Implement `modules/iam/main.tf` with dedicated roles per service (`${env}-pokemon-lambda-role`, `${env}-pokemon-glue-role`, `${env}-pokemon-stepfunctions-role`)
    - Lambda role: permissions for S3 write to bronze prefix, CloudWatch Logs, and invoke from Step Functions
    - Glue role: permissions for S3 read/write to bronze/silver/gold prefixes, Glue Catalog, CloudWatch Logs
    - Step Functions role: permissions to invoke Lambda and start Glue Jobs
    - All policies scoped to specific resource ARNs using `name_prefix` variable
    - _Requirements: 6.3_

- [x] 3. Implement S3 storage module
  - [x] 3.1 Create S3 bucket with layer prefixes, encryption, and access controls
    - Implement `modules/s3/main.tf` with single bucket (`${name_prefix}-data-pipeline-${account_id}`)
    - Configure prefixes for `bronze/`, `silver/`, `gold/`
    - Enable SSE-S3 or SSE-KMS server-side encryption
    - Configure Block Public Access with all four options enabled
    - Enable versioning on the bucket
    - _Requirements: 9.1, 9.4, 9.5_

  - [x] 3.2 Configure lifecycle policies for each layer
    - Add lifecycle rules: Bronze retention (var), Silver retention (var), Gold retention (var)
    - Add lifecycle rule to expire non-current versions after configurable days
    - All retention periods driven by tfvars variables
    - _Requirements: 9.2, 9.3_

- [x] 4. Implement Glue Catalog module and tables
  - [x] 4.1 Create Glue Catalog database and tables for all layers
    - Create database: `${env}_pokemon_data_pipeline`
    - Create `bronze_pokemon` table: JSON SerDe, partition columns (year, month, day)
    - Create `silver_pokemon` table: Parquet SerDe, partition column (primary_type)
    - Create `gold_pokemon_stats` table: Parquet SerDe, partition column (type)
    - All within `modules/glue/main.tf`
    - _Requirements: 4.1, 4.3, 4.4, 10.8_

- [x] 5. Checkpoint - Ensure Terraform validates
  - Ensure `terraform validate` passes on all modules, ask the user if questions arise.

- [x] 6. Implement Lambda ingestion function
  - [x] 6.1 Create PokeAPI client with rate limiting and retry logic
    - Implement `src/lambda/pokeapi_client.py` with `RateLimiter` class (max 100 req/min, 500ms between requests)
    - Implement exponential backoff retry (3 attempts, base 1s) for HTTP 4xx/5xx errors and timeouts (>30s)
    - Implement pagination through `/api/v2/pokemon` endpoint
    - _Requirements: 1.1, 1.4, 1.6_

  - [x] 6.2 Create S3 writer with date-based partitioning
    - Implement `src/lambda/s3_writer.py` with `generate_s3_key(date, name)` function
    - Write one JSON file per Pokémon at `bronze/YYYY/MM/DD/{pokemon_name}.json`
    - _Requirements: 1.2, 1.3_

  - [x] 6.3 Create Lambda handler with timeout monitoring and partial failure support
    - Implement `src/lambda/handler.py` as entry point
    - Accept input `{ "execution_date": "YYYY-MM-DD" }` from Step Functions
    - Monitor execution time; save partial results at 80% timeout threshold
    - Continue processing remaining Pokémon on individual failures after retries
    - Return structured output: `{ "status": "success|partial_failure|failure", "pokemon_count": int, "failed_pokemon": [str], "s3_prefix": str }`
    - _Requirements: 1.5, 1.7, 8.1_

  - [x]* 6.4 Write property test for S3 key path generation
    - **Property 1: S3 Key Path Generation**
    - Test that for any valid execution date and Pokémon name, the generated S3 key matches `bronze/{year}/{month}/{day}/{name}.json`
    - Use Hypothesis to generate varied dates and name strings
    - **Validates: Requirements 1.3**

  - [x]* 6.5 Write property test for rate limiter timing guarantees
    - **Property 2: Rate Limiter Timing Guarantees**
    - Test that consecutive request timestamps differ by >= 500ms and no 60-second window contains > 100 requests
    - Use Hypothesis to generate sequences of request calls
    - **Validates: Requirements 1.6**

  - [x]* 6.6 Write unit tests for Lambda functions
    - Test PokeAPI client: mock HTTP responses (success, errors, pagination, timeout)
    - Test S3 writer: mock boto3 S3 client
    - Test handler: mock dependencies, test partial failure at timeout threshold, test graceful continuation on individual failures
    - Use `unittest.mock` for all external service mocks
    - _Requirements: 8.4, 8.5_

- [x] 7. Implement Glue transformation jobs
  - [x] 7.1 Implement Bronze to Silver transformation functions
    - Implement `src/glue/transformations/deduplication.py` with `deduplicate(df, key="id")` pure function
    - Implement `src/glue/transformations/type_standardization.py` with `standardize_types(df)` pure function (numeric→int, name→lowercase, lists→typed arrays)
    - Implement `src/glue/transformations/null_handling.py` with `handle_nulls(df)` pure function (null numerics→0, null text→"")
    - _Requirements: 2.2, 2.3, 2.6, 8.6_

  - [x] 7.2 Implement Bronze to Silver Glue job entry point
    - Implement `src/glue/bronze_to_silver.py` that orchestrates the transformation pipeline
    - Read JSON from Bronze layer, apply transformations (deduplicate → standardize → handle nulls)
    - Write Parquet to Silver layer partitioned by primary_type
    - Update Glue Catalog schema on success
    - Log errors with step name, exception, and timestamp on failure
    - _Requirements: 2.1, 2.4, 2.5, 2.7, 4.2_

  - [x] 7.3 Implement Silver to Gold transformation and aggregation
    - Implement `src/glue/transformations/aggregations.py` with `aggregate_by_type(df)` pure function
    - Calculate count, avg, min, max for numeric attributes grouped by type
    - Implement `src/glue/silver_to_gold.py` entry point
    - Write Parquet to Gold layer partitioned by type
    - Ensure no duplicate aggregation rows in output
    - Update Glue Catalog schema on success
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 4.2_

  - [x]* 7.4 Write property test for deduplication
    - **Property 3: Deduplication by ID**
    - Test that for any DataFrame with duplicate IDs, output has no duplicates and every unique ID appears exactly once
    - Use Hypothesis with PySpark DataFrames and local SparkSession
    - **Validates: Requirements 2.2**

  - [x]* 7.5 Write property test for type standardization
    - **Property 4: Type Standardization**
    - Test that numeric fields become integers, name becomes lowercase, and list fields become typed arrays
    - Use Hypothesis with varied data representations
    - **Validates: Requirements 2.3**

  - [x]* 7.6 Write property test for null value handling
    - **Property 5: Null Value Handling**
    - Test that no numeric field is null (replaced by 0) and no text field is null (replaced by "")
    - Use Hypothesis to generate DataFrames with nulls in various positions
    - **Validates: Requirements 2.6**

  - [x]* 7.7 Write property test for aggregation correctness
    - **Property 6: Aggregation Correctness and Uniqueness**
    - Test that output has one row per unique type, count matches input records, and avg/min/max are mathematically correct
    - Use Hypothesis with Silver-layer DataFrames
    - **Validates: Requirements 3.2, 3.5**

  - [x]* 7.8 Write unit tests for Glue transformation functions
    - Create `tests/unit/glue/conftest.py` with SparkSession fixture (local[*])
    - Test each transformation with representative datasets (min 10 records per scenario)
    - Test edge cases: empty DataFrames, single record, all nulls
    - Use `chispa` library for DataFrame equality assertions
    - _Requirements: 8.7, 8.8_

- [x] 8. Checkpoint - Ensure all Python code and tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement Lambda Terraform module
  - [x] 9.1 Create Lambda module with function definition and configuration
    - Implement `modules/lambda/main.tf` with Lambda function resource
    - Configure runtime (Python 3.12), memory (variable), timeout (variable)
    - Set environment variables for S3 bucket name and bronze prefix
    - Package Lambda source code from `src/lambda/`
    - Wire IAM role from IAM module
    - _Requirements: 6.1, 6.2, 6.4_

- [x] 10. Implement Glue Terraform module
  - [x] 10.1 Create Glue module with job definitions and catalog resources
    - Implement `modules/glue/main.tf` with two Glue Job resources (bronze_to_silver, silver_to_gold)
    - Configure worker type (variable), number of workers (variable), timeout (variable)
    - Reference Glue Catalog database and tables created in task 4.1
    - Package Glue scripts from `src/glue/`
    - Wire IAM role from IAM module
    - _Requirements: 6.1, 6.2, 6.4_

- [x] 11. Implement Step Functions Terraform module
  - [x] 11.1 Create Step Functions state machine definition
    - Implement `modules/step_functions/main.tf` with Standard Workflow state machine
    - Define states: IngestData (Lambda invoke, 15min timeout), CheckIngestionStatus (Choice), TransformBronzeToSilver (Glue sync, 60min timeout), TransformSilverToGold (Glue sync, 60min timeout), PipelineSuccess, PipelineFailed
    - Configure error handling: catch failures and transition to PipelineFailed state
    - PipelineFailed state includes diagnostic output (step name, error message, execution ARN)
    - Wire IAM role from IAM module
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

- [x] 12. Wire all Terraform modules together in root configuration
  - [x] 12.1 Integrate all modules in root main.tf with proper dependencies
    - Wire `modules/iam` outputs into Lambda, Glue, and Step Functions modules
    - Wire `modules/s3` outputs (bucket name, ARN) into Lambda, Glue, and IAM modules
    - Wire `modules/lambda` and `modules/glue` outputs into Step Functions module
    - Define root `outputs.tf` with key resource identifiers (bucket name, Lambda ARN, Step Function ARN)
    - Ensure all resources use `local.common_tags` for tagging
    - _Requirements: 6.1, 6.2, 6.4, 10.3, 10.7, 10.8_

- [x] 13. Checkpoint - Ensure Terraform plan succeeds
  - Ensure `terraform plan -var-file=dev.tfvars` succeeds without errors, ask the user if questions arise.

- [x] 14. Implement GitHub Actions CI/CD workflows
  - [x] 14.1 Create Terraform plan workflow for pull requests
    - Create `.github/workflows/terraform-plan.yml`
    - Trigger on PR to `main` and `develop` branches
    - Determine workspace and tfvars from target branch (`github.base_ref`)
    - Steps: checkout, setup Terraform, configure AWS credentials (secrets), `terraform fmt -check`, `terraform validate`, `terraform workspace select`, `terraform plan -var-file=$TF_VAR_FILE`
    - Post plan output as PR comment
    - Fail pipeline and report errors if fmt/validate/plan fails
    - _Requirements: 7.1, 7.2, 7.5, 7.6, 7.7, 7.8_

  - [x] 14.2 Create Terraform apply workflow for merges
    - Create `.github/workflows/terraform-apply.yml`
    - Trigger on push to `main` and `develop` branches
    - Determine workspace and tfvars from branch name (`github.ref_name`)
    - Steps: checkout, setup Terraform, configure AWS credentials (secrets), `terraform fmt -check`, `terraform validate`, run pytest, `terraform workspace select`, `terraform plan`, `terraform apply -auto-approve -var-file=$TF_VAR_FILE`
    - Fail pipeline and report errors without applying if any step fails
    - _Requirements: 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 8.2, 8.3_

- [x] 15. Final checkpoint - Ensure all configuration is valid
  - Ensure all tests pass and Terraform validates successfully, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document using Hypothesis
- Unit tests validate specific examples and edge cases using pytest and chispa
- All Python code (Lambda and Glue) uses pure functions to enable isolated testing
- Terraform modules are designed for reuse with workspace-based environment isolation

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3"] },
    { "id": 1, "tasks": ["2.1", "3.1"] },
    { "id": 2, "tasks": ["3.2", "4.1"] },
    { "id": 3, "tasks": ["6.1", "6.2", "7.1"] },
    { "id": 4, "tasks": ["6.3", "7.2", "7.3"] },
    { "id": 5, "tasks": ["6.4", "6.5", "6.6", "7.4", "7.5", "7.6", "7.7", "7.8"] },
    { "id": 6, "tasks": ["9.1", "10.1", "11.1"] },
    { "id": 7, "tasks": ["12.1"] },
    { "id": 8, "tasks": ["14.1", "14.2"] }
  ]
}
```

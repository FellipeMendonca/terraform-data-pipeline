# Glue Module - Main Configuration
# Pokemon Data Pipeline - Glue Catalog database, tables, and ETL jobs

# -----------------------------------------------------------------------------
# Glue Catalog Database
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_database" "pipeline" {
  name = "${var.env}_pokemon_data_pipeline"

  description = "Pokemon Data Pipeline catalog database for ${var.env} environment"
}

# -----------------------------------------------------------------------------
# Bronze Layer Table - Raw JSON data from PokeAPI
# Partitioned by year/month/day based on ingestion date
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_table" "bronze_pokemon" {
  name          = "bronze_pokemon"
  database_name = aws_glue_catalog_database.pipeline.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/bronze/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "id"
      type = "int"
    }

    columns {
      name = "name"
      type = "string"
    }

    columns {
      name = "height"
      type = "int"
    }

    columns {
      name = "weight"
      type = "int"
    }

    columns {
      name = "base_experience"
      type = "int"
    }

    columns {
      name = "types"
      type = "string"
    }

    columns {
      name = "abilities"
      type = "string"
    }

    columns {
      name = "stats"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }

  partition_keys {
    name = "month"
    type = "string"
  }

  partition_keys {
    name = "day"
    type = "string"
  }
}

# -----------------------------------------------------------------------------
# Silver Layer Table - Cleaned and standardized Parquet data
# Partitioned by primary Pokémon type
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_table" "silver_pokemon" {
  name          = "silver_pokemon"
  database_name = aws_glue_catalog_database.pipeline.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/silver/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "id"
      type = "int"
    }

    columns {
      name = "name"
      type = "string"
    }

    columns {
      name = "height"
      type = "int"
    }

    columns {
      name = "weight"
      type = "int"
    }

    columns {
      name = "base_experience"
      type = "int"
    }

    columns {
      name = "secondary_type"
      type = "string"
    }

    columns {
      name = "types"
      type = "array<string>"
    }

    columns {
      name = "abilities"
      type = "array<string>"
    }

    columns {
      name = "hp"
      type = "int"
    }

    columns {
      name = "attack"
      type = "int"
    }

    columns {
      name = "defense"
      type = "int"
    }

    columns {
      name = "special_attack"
      type = "int"
    }

    columns {
      name = "special_defense"
      type = "int"
    }

    columns {
      name = "speed"
      type = "int"
    }
  }

  partition_keys {
    name = "primary_type"
    type = "string"
  }
}

# -----------------------------------------------------------------------------
# Gold Layer Table - Aggregated stats by Pokémon type
# Partitioned by type
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_table" "gold_pokemon_stats" {
  name          = "gold_pokemon_stats"
  database_name = aws_glue_catalog_database.pipeline.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/gold/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "count"
      type = "int"
    }

    columns {
      name = "avg_height"
      type = "double"
    }

    columns {
      name = "min_height"
      type = "int"
    }

    columns {
      name = "max_height"
      type = "int"
    }

    columns {
      name = "avg_weight"
      type = "double"
    }

    columns {
      name = "min_weight"
      type = "int"
    }

    columns {
      name = "max_weight"
      type = "int"
    }

    columns {
      name = "avg_base_experience"
      type = "double"
    }

    columns {
      name = "avg_hp"
      type = "double"
    }

    columns {
      name = "avg_attack"
      type = "double"
    }

    columns {
      name = "avg_defense"
      type = "double"
    }

    columns {
      name = "avg_speed"
      type = "double"
    }
  }

  partition_keys {
    name = "type"
    type = "string"
  }
}

# -----------------------------------------------------------------------------
# S3 Objects for Glue Scripts
# Upload Glue job scripts to S3 for execution
# -----------------------------------------------------------------------------

resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = var.s3_bucket_name
  key    = "glue-scripts/bronze_to_silver.py"
  source = "${path.module}/../../../src/glue/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../../../src/glue/bronze_to_silver.py")

  tags = var.common_tags
}

resource "aws_s3_object" "silver_to_gold_script" {
  bucket = var.s3_bucket_name
  key    = "glue-scripts/silver_to_gold.py"
  source = "${path.module}/../../../src/glue/silver_to_gold.py"
  etag   = filemd5("${path.module}/../../../src/glue/silver_to_gold.py")

  tags = var.common_tags
}

# -----------------------------------------------------------------------------
# Glue Job: Bronze to Silver
# Transforms raw JSON data into cleaned/standardized Parquet
# -----------------------------------------------------------------------------

resource "aws_glue_job" "bronze_to_silver" {
  name     = "${var.name_prefix}-bronze-to-silver"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_name}/${aws_s3_object.bronze_to_silver_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-enable"
    "--database_name"       = aws_glue_catalog_database.pipeline.name
    "--s3_bucket"           = var.s3_bucket_name
    "--bronze_prefix"       = "bronze/"
    "--silver_prefix"       = "silver/"
    "--gold_prefix"         = "gold/"
  }

  tags = var.common_tags
}

# -----------------------------------------------------------------------------
# Glue Job: Silver to Gold
# Aggregates cleaned data into analytics-ready summaries
# -----------------------------------------------------------------------------

resource "aws_glue_job" "silver_to_gold" {
  name     = "${var.name_prefix}-silver-to-gold"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_name}/${aws_s3_object.silver_to_gold_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-enable"
    "--database_name"       = aws_glue_catalog_database.pipeline.name
    "--s3_bucket"           = var.s3_bucket_name
    "--bronze_prefix"       = "bronze/"
    "--silver_prefix"       = "silver/"
    "--gold_prefix"         = "gold/"
  }

  tags = var.common_tags
}

# Glue Module - Outputs
# Pokemon Data Pipeline - Exported values from Glue module

output "catalog_database_name" {
  description = "Name of the Glue Catalog database"
  value       = aws_glue_catalog_database.pipeline.name
}

output "bronze_table_name" {
  description = "Name of the Bronze layer Glue Catalog table"
  value       = aws_glue_catalog_table.bronze_pokemon.name
}

output "silver_table_name" {
  description = "Name of the Silver layer Glue Catalog table"
  value       = aws_glue_catalog_table.silver_pokemon.name
}

output "gold_table_name" {
  description = "Name of the Gold layer Glue Catalog table"
  value       = aws_glue_catalog_table.gold_pokemon_stats.name
}

output "bronze_to_silver_job_name" {
  description = "Name of the Bronze to Silver Glue ETL job"
  value       = aws_glue_job.bronze_to_silver.name
}

output "silver_to_gold_job_name" {
  description = "Name of the Silver to Gold Glue ETL job"
  value       = aws_glue_job.silver_to_gold.name
}

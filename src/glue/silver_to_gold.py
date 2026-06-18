"""Glue Job entry point: Silver to Gold layer transformation.

Reads cleaned Pokémon data from the Silver layer, applies aggregation
by type, and writes the results to the Gold layer in Parquet format
partitioned by type. Updates the Glue Catalog on success.
"""

import sys
import logging
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.glue.transformations.aggregations import aggregate_by_type

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    """Execute the Silver to Gold transformation pipeline."""
    step_name = "initialization"
    try:
        # Initialize Glue context
        step_name = "glue_context_init"
        args = getResolvedOptions(
            sys.argv, ["JOB_NAME", "database_name", "s3_bucket", "silver_prefix", "gold_prefix"]
        )

        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(args["JOB_NAME"], args)

        database_name = args["database_name"]
        s3_bucket = args["s3_bucket"]
        silver_prefix = args["silver_prefix"]
        gold_prefix = args["gold_prefix"]

        silver_path = f"s3://{s3_bucket}/{silver_prefix}"
        gold_path = f"s3://{s3_bucket}/{gold_prefix}"

        # Read Silver layer data
        step_name = "read_silver_data"
        logger.info(f"Reading Silver layer data from: {silver_path}")
        silver_df = spark.read.parquet(silver_path)

        # Apply aggregation transformation
        step_name = "aggregate_by_type"
        logger.info("Applying aggregation by type transformation")
        gold_df = aggregate_by_type(silver_df)

        # Write Gold layer data partitioned by type
        step_name = "write_gold_data"
        logger.info(f"Writing Gold layer data to: {gold_path}")
        gold_df.write.mode("overwrite").partitionBy("type").parquet(gold_path)

        # Update Glue Catalog schema
        step_name = "update_glue_catalog"
        logger.info(f"Updating Glue Catalog table: {database_name}.gold_pokemon_stats")
        _update_catalog_schema(glue_context, database_name, "gold_pokemon_stats", gold_path)

        logger.info("Silver to Gold transformation completed successfully")
        job.commit()

    except Exception as e:
        timestamp = datetime.now(timezone.utc).isoformat()
        logger.error(
            f"Silver to Gold transformation failed at step '{step_name}': "
            f"{type(e).__name__}: {str(e)} | timestamp: {timestamp}"
        )
        raise


def _update_catalog_schema(glue_context, database_name, table_name, location):
    """Update the Glue Catalog table schema to reflect current data.

    Args:
        glue_context: The GlueContext instance.
        database_name: Name of the Glue Catalog database.
        table_name: Name of the table to update.
        location: S3 location of the table data.
    """
    import boto3

    client = boto3.client("glue")

    # Read back the written data schema
    spark = glue_context.spark_session
    df = spark.read.parquet(location)

    # Build column list from DataFrame schema (excluding partition column)
    columns = []
    for field in df.schema.fields:
        if field.name != "type":
            columns.append({
                "Name": field.name,
                "Type": _spark_type_to_glue_type(field.dataType),
            })

    # Update the table with the new schema
    try:
        client.update_table(
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                },
                "PartitionKeys": [{"Name": "type", "Type": "string"}],
                "TableType": "EXTERNAL_TABLE",
            },
        )
        logger.info(f"Successfully updated Glue Catalog table: {database_name}.{table_name}")
    except Exception as e:
        logger.error(f"Failed to update Glue Catalog table: {e}")
        raise


def _spark_type_to_glue_type(spark_type):
    """Convert a PySpark data type to a Glue Catalog type string.

    Args:
        spark_type: A PySpark DataType instance.

    Returns:
        A string representing the equivalent Glue Catalog type.
    """
    from pyspark.sql import types as T

    type_mapping = {
        T.IntegerType: "int",
        T.LongType: "bigint",
        T.DoubleType: "double",
        T.FloatType: "float",
        T.StringType: "string",
        T.BooleanType: "boolean",
    }

    for spark_cls, glue_type in type_mapping.items():
        if isinstance(spark_type, spark_cls):
            return glue_type

    return "string"


if __name__ == "__main__":
    main()

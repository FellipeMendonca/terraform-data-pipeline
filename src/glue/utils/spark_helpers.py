"""Shared PySpark utility functions for Glue jobs.

Provides helper functions for common operations across Glue job entry points,
including Glue Catalog schema updates and structured error logging.
"""

import logging
import sys
from datetime import datetime, timezone

import boto3
from pyspark.sql import DataFrame
from pyspark.sql import types as T

logger = logging.getLogger(__name__)


def get_glue_catalog_columns(schema: T.StructType) -> list[dict]:
    """Convert a PySpark StructType schema to Glue Catalog column definitions.

    Maps PySpark data types to Glue/Hive type strings for catalog registration.

    Args:
        schema: PySpark StructType representing the DataFrame schema.

    Returns:
        A list of dicts with 'Name' and 'Type' keys for Glue Catalog API.
    """
    type_mapping = {
        "IntegerType": "int",
        "LongType": "bigint",
        "DoubleType": "double",
        "FloatType": "float",
        "StringType": "string",
        "BooleanType": "boolean",
        "TimestampType": "timestamp",
        "DateType": "date",
    }

    columns = []
    for field in schema.fields:
        type_name = type(field.dataType).__name__
        if type_name == "ArrayType":
            element_type = type(field.dataType.elementType).__name__
            hive_type = f"array<{type_mapping.get(element_type, 'string')}>"
        else:
            hive_type = type_mapping.get(type_name, "string")
        columns.append({"Name": field.name, "Type": hive_type})

    return columns


def update_glue_catalog_table(
    database_name: str,
    table_name: str,
    columns: list[dict],
    location: str,
    partition_keys: list[dict],
    data_format: str = "parquet",
) -> None:
    """Update Glue Catalog table schema with the provided column definitions.

    Args:
        database_name: Name of the Glue Catalog database.
        table_name: Name of the table to update.
        columns: List of column definitions (Name/Type dicts).
        location: S3 location of the table data.
        partition_keys: List of partition key definitions (Name/Type dicts).
        data_format: Data format (parquet or json). Defaults to "parquet".
    """
    client = boto3.client("glue")

    serde_info = {
        "parquet": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
        "json": {
            "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
            "Parameters": {"serialization.format": "1"},
        },
    }

    input_format = {
        "parquet": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "json": "org.apache.hadoop.mapred.TextInputFormat",
    }

    output_format = {
        "parquet": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "json": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    }

    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": location,
            "InputFormat": input_format.get(data_format, input_format["parquet"]),
            "OutputFormat": output_format.get(data_format, output_format["parquet"]),
            "SerdeInfo": serde_info.get(data_format, serde_info["parquet"]),
        },
        "PartitionKeys": partition_keys,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": data_format},
    }

    client.update_table(
        DatabaseName=database_name,
        TableInput=table_input,
    )
    logger.info("Updated Glue Catalog table %s.%s schema", database_name, table_name)


def log_transformation_error(step_name: str, exception: Exception) -> None:
    """Log a structured error message for a failed transformation step.

    Logs the step name, exception message, and current UTC timestamp to stderr
    for CloudWatch capture.

    Args:
        step_name: Name of the transformation step that failed.
        exception: The exception that was raised.
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    error_msg = (
        f"[TRANSFORMATION_ERROR] "
        f"step={step_name} | "
        f"exception={type(exception).__name__}: {exception} | "
        f"timestamp={timestamp}"
    )
    logger.error(error_msg)
    print(error_msg, file=sys.stderr)

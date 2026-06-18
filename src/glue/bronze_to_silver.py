"""Bronze to Silver Glue Job entry point.

Orchestrates the transformation pipeline from the Bronze layer (raw JSON)
to the Silver layer (cleaned Parquet). Applies transformations in order:
deduplicate → standardize_types → handle_nulls, then partitions output
by primary Pokémon type.

This script is designed to run as an AWS Glue Job (PySpark).
"""

import sys
from datetime import date, datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

try:
    # Glue runtime with extra-py-files
    from transformations import deduplicate, handle_nulls, standardize_types
    from utils.spark_helpers import (
        get_glue_catalog_columns,
        log_transformation_error,
        update_glue_catalog_table,
    )
except ImportError:
    # Local/test environment
    from src.glue.transformations import deduplicate, handle_nulls, standardize_types
    from src.glue.utils.spark_helpers import (
        get_glue_catalog_columns,
        log_transformation_error,
        update_glue_catalog_table,
    )


def extract_primary_type(df):
    """Extract primary_type from the types array as a new column.

    The primary type is the first element of the types array. If types is
    empty or null, primary_type defaults to "unknown".

    Args:
        df: DataFrame with a 'types' column containing an array of type name strings.

    Returns:
        DataFrame with an additional 'primary_type' column.
    """
    return df.withColumn(
        "primary_type",
        F.when(
            (F.col("types").isNull()) | (F.size(F.col("types")) == 0),
            F.lit("unknown"),
        ).otherwise(F.col("types").getItem(0)),
    )


def _extract_stats(df):
    """Extract individual stat values from the nested stats array.

    The PokeAPI stats field is an array of structs like:
    [{"base_stat": 45, "effort": 0, "stat": {"name": "hp", "url": "..."}}, ...]

    This function extracts each stat into its own column: hp, attack, defense,
    special_attack, special_defense, speed.

    Args:
        df: DataFrame with a 'stats' column (array of structs or JSON string).

    Returns:
        DataFrame with individual stat columns added.
    """
    from pyspark.sql import types as T

    if "stats" not in df.columns:
        # If stats column doesn't exist, add zero-value columns
        for stat_name in ["hp", "attack", "defense", "special_attack", "special_defense", "speed"]:
            df = df.withColumn(stat_name, F.lit(0))
        return df

    # Check if stats is a string (needs parsing) or already structured
    stats_col = df.schema["stats"]
    if isinstance(stats_col.dataType, T.StringType):
        # Parse JSON string
        stats_schema = T.ArrayType(
            T.StructType([
                T.StructField("base_stat", T.IntegerType()),
                T.StructField("effort", T.IntegerType()),
                T.StructField("stat", T.StructType([
                    T.StructField("name", T.StringType()),
                    T.StructField("url", T.StringType()),
                ])),
            ])
        )
        df = df.withColumn("stats_parsed", F.from_json(F.col("stats"), stats_schema))
    else:
        df = df.withColumn("stats_parsed", F.col("stats"))

    # Map stat names to column names
    stat_mapping = {
        "hp": "hp",
        "attack": "attack",
        "defense": "defense",
        "special-attack": "special_attack",
        "special-defense": "special_defense",
        "speed": "speed",
    }

    # Extract each stat by filtering the array
    for api_name, col_name in stat_mapping.items():
        df = df.withColumn(
            col_name,
            F.coalesce(
                F.filter(F.col("stats_parsed"), lambda x: x.getField("stat").getField("name") == api_name)[0].getField("base_stat"),
                F.lit(0),
            ),
        )

    # Drop the temporary parsed column
    df = df.drop("stats_parsed")

    return df


def run_bronze_to_silver(glue_context, args):
    """Execute the Bronze to Silver transformation pipeline.

    Reads JSON data from the Bronze layer, applies all transformation steps,
    extracts primary_type for partitioning, writes Parquet to the Silver layer,
    and updates the Glue Catalog schema.

    Args:
        glue_context: The GlueContext for this job.
        args: Resolved job arguments containing database_name, s3_bucket,
              bronze_prefix, and silver_prefix.

    Raises:
        Exception: Re-raises any exception after logging the failure details.
    """
    spark = glue_context.spark_session

    database_name = args["database_name"]
    s3_bucket = args["s3_bucket"]
    bronze_prefix = args.get("bronze_prefix", "bronze")
    silver_prefix = args.get("silver_prefix", "silver")

    # Determine bronze input path (use execution date if provided, else today)
    execution_date_str = args.get("execution_date", "")
    if execution_date_str:
        exec_date = datetime.strptime(execution_date_str, "%Y-%m-%d").date()
    else:
        exec_date = date.today()

    bronze_path = (
        f"s3://{s3_bucket}/{bronze_prefix}/"
        f"year={exec_date.year}/month={exec_date.month:02d}/day={exec_date.day:02d}/"
    )
    silver_path = f"s3://{s3_bucket}/{silver_prefix}/"

    current_step = "read_bronze"
    try:
        # Step 1: Read JSON from Bronze layer
        df = spark.read.json(bronze_path)

        # Step 2: Deduplicate by Pokémon ID
        current_step = "deduplicate"
        df = deduplicate(df, key="id")

        # Step 3: Standardize data types
        current_step = "standardize_types"
        df = standardize_types(df)

        # Step 3.5: Extract individual stats from the stats array
        current_step = "extract_stats"
        df = _extract_stats(df)

        # Step 4: Handle null values
        current_step = "handle_nulls"
        df = handle_nulls(df)

        # Step 5: Extract primary_type for partitioning
        current_step = "extract_primary_type"
        df = extract_primary_type(df)

        # Step 6: Write Parquet to Silver layer partitioned by primary_type
        current_step = "write_silver"
        df.write.mode("overwrite").partitionBy("primary_type").parquet(silver_path)

        # Step 7: Update Glue Catalog schema on success
        current_step = "update_catalog"
        # Get schema excluding the partition column for catalog columns
        output_schema = df.schema
        non_partition_columns = get_glue_catalog_columns(
            type(output_schema)(
                [f for f in output_schema.fields if f.name != "primary_type"]
            )
        )
        partition_keys = [{"Name": "primary_type", "Type": "string"}]

        update_glue_catalog_table(
            database_name=database_name,
            table_name="silver_pokemon",
            columns=non_partition_columns,
            location=silver_path,
            partition_keys=partition_keys,
            data_format="parquet",
        )

    except Exception as e:
        log_transformation_error(step_name=current_step, exception=e)
        raise


def main():
    """Glue Job main entry point.

    Initializes the Spark and Glue contexts, resolves job arguments,
    and executes the Bronze to Silver transformation pipeline.
    """
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "database_name",
            "s3_bucket",
            "bronze_prefix",
            "silver_prefix",
        ],
    )

    # execution_date is optional, defaults to today
    try:
        extra_args = getResolvedOptions(sys.argv, ["execution_date"])
        args["execution_date"] = extra_args["execution_date"]
    except Exception:
        args["execution_date"] = date.today().isoformat()

    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    run_bronze_to_silver(glue_context, args)

    job.commit()


if __name__ == "__main__":
    main()

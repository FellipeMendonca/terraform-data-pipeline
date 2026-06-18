"""Aggregation transformation for Silver to Gold layer.

Groups Pokémon data by primary type and calculates summary statistics
(count, average, minimum, maximum) for numeric attributes.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Numeric attributes to aggregate with full stats (avg, min, max)
FULL_STATS_COLUMNS = ["height", "weight"]

# Numeric attributes to aggregate with average only
AVG_ONLY_COLUMNS = ["base_experience", "hp", "attack", "defense", "speed"]


def aggregate_by_type(df: DataFrame) -> DataFrame:
    """Aggregate Pokémon statistics grouped by primary type.

    Groups the input DataFrame by `primary_type` and calculates:
    - count: number of Pokémon per type
    - avg, min, max for height and weight
    - avg for base_experience, hp, attack, defense, speed

    The output column `primary_type` is renamed to `type`.

    Args:
        df: Input PySpark DataFrame from the Silver layer containing
            individual Pokémon records with numeric attributes.

    Returns:
        A new DataFrame with one row per unique type containing
        aggregated statistics. No duplicate rows will exist since
        grouping by type produces unique rows naturally.
    """
    agg_exprs = [F.count("*").alias("count")]

    # Full stats columns: avg, min, max
    for col_name in FULL_STATS_COLUMNS:
        agg_exprs.append(F.avg(F.col(col_name)).alias(f"avg_{col_name}"))
        agg_exprs.append(F.min(F.col(col_name)).alias(f"min_{col_name}"))
        agg_exprs.append(F.max(F.col(col_name)).alias(f"max_{col_name}"))

    # Average-only columns
    for col_name in AVG_ONLY_COLUMNS:
        agg_exprs.append(F.avg(F.col(col_name)).alias(f"avg_{col_name}"))

    result = df.groupBy("primary_type").agg(*agg_exprs)

    # Rename primary_type to type for Gold layer schema
    result = result.withColumnRenamed("primary_type", "type")

    return result

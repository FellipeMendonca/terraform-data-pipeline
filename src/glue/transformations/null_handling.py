"""Null value handling transformation for Bronze to Silver layer.

Replaces null values in numeric columns with 0 and null values in text columns
with empty string.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Numeric columns that should have nulls replaced with 0
NUMERIC_COLUMNS = [
    "height",
    "weight",
    "base_experience",
    "hp",
    "attack",
    "defense",
    "special_attack",
    "special_defense",
    "speed",
]

# Text columns that should have nulls replaced with empty string
TEXT_COLUMNS = ["name"]


def handle_nulls(df: DataFrame) -> DataFrame:
    """Replace null values with appropriate defaults.

    Applies the following null handling:
    - Numeric columns (height, weight, base_experience, hp, attack, defense,
      special_attack, special_defense, speed): null → 0
    - Text columns (name): null → ""

    Args:
        df: Input PySpark DataFrame potentially containing null values.

    Returns:
        A new DataFrame with null values replaced by defaults.
    """
    result = df

    # Replace nulls in numeric columns with 0
    for col_name in NUMERIC_COLUMNS:
        if col_name in result.columns:
            result = result.withColumn(
                col_name, F.when(F.col(col_name).isNull(), F.lit(0)).otherwise(F.col(col_name))
            )

    # Replace nulls in text columns with empty string
    for col_name in TEXT_COLUMNS:
        if col_name in result.columns:
            result = result.withColumn(
                col_name, F.when(F.col(col_name).isNull(), F.lit("")).otherwise(F.col(col_name))
            )

    return result

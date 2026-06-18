"""Deduplication transformation for Bronze to Silver layer.

Removes duplicate records from a DataFrame based on a specified key column.
"""

from pyspark.sql import DataFrame


def deduplicate(df: DataFrame, key: str = "id") -> DataFrame:
    """Remove duplicate records based on the specified key column.

    Keeps the first occurrence of each key value and drops subsequent duplicates.

    Args:
        df: Input PySpark DataFrame potentially containing duplicate records.
        key: Column name to use as deduplication key. Defaults to "id".

    Returns:
        A new DataFrame with duplicates removed based on the key column.
    """
    return df.dropDuplicates([key])

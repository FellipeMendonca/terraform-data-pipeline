"""Type standardization transformation for Bronze to Silver layer.

Standardizes data types: numeric fields to integers, name to lowercase,
and nested JSON list fields (types, abilities) to typed string arrays.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def standardize_types(df: DataFrame) -> DataFrame:
    """Standardize data types in the DataFrame.

    Applies the following transformations:
    - Casts height, weight, base_experience to IntegerType
    - Converts name to lowercase
    - Converts types field (JSON nested list) to array of type name strings
    - Converts abilities field (JSON nested list) to array of ability name strings

    Args:
        df: Input PySpark DataFrame with raw Bronze layer data.

    Returns:
        A new DataFrame with standardized types.
    """
    result = df

    # Cast numeric fields to IntegerType
    for col_name in ["height", "weight", "base_experience"]:
        if col_name in result.columns:
            result = result.withColumn(col_name, F.col(col_name).cast(T.IntegerType()))

    # Convert name to lowercase
    if "name" in result.columns:
        result = result.withColumn("name", F.lower(F.col("name")))

    # Convert types field: extract type names from nested JSON structure
    # Expected input format: [{"slot": 1, "type": {"name": "grass", "url": "..."}}, ...]
    if "types" in result.columns:
        types_col = result.schema["types"]
        if isinstance(types_col.dataType, T.StringType):
            # If types is a JSON string, parse it and extract type names
            schema = T.ArrayType(
                T.StructType([
                    T.StructField("slot", T.IntegerType()),
                    T.StructField("type", T.StructType([
                        T.StructField("name", T.StringType()),
                        T.StructField("url", T.StringType()),
                    ])),
                ])
            )
            result = result.withColumn("types", F.from_json(F.col("types"), schema))
            result = result.withColumn(
                "types",
                F.transform(F.col("types"), lambda x: x.getField("type").getField("name")),
            )
        elif isinstance(types_col.dataType, T.ArrayType):
            element_type = types_col.dataType.elementType
            if isinstance(element_type, T.StructType) and "type" in element_type.fieldNames():
                # Already parsed as array of structs with nested type field
                result = result.withColumn(
                    "types",
                    F.transform(F.col("types"), lambda x: x.getField("type").getField("name")),
                )
            # If already array of strings, leave as-is

    # Convert abilities field: extract ability names from nested JSON structure
    # Expected input format: [{"ability": {"name": "overgrow", "url": "..."}, ...}, ...]
    if "abilities" in result.columns:
        abilities_col = result.schema["abilities"]
        if isinstance(abilities_col.dataType, T.StringType):
            # If abilities is a JSON string, parse it and extract ability names
            schema = T.ArrayType(
                T.StructType([
                    T.StructField("ability", T.StructType([
                        T.StructField("name", T.StringType()),
                        T.StructField("url", T.StringType()),
                    ])),
                    T.StructField("is_hidden", T.BooleanType()),
                    T.StructField("slot", T.IntegerType()),
                ])
            )
            result = result.withColumn("abilities", F.from_json(F.col("abilities"), schema))
            result = result.withColumn(
                "abilities",
                F.transform(F.col("abilities"), lambda x: x.getField("ability").getField("name")),
            )
        elif isinstance(abilities_col.dataType, T.ArrayType):
            element_type = abilities_col.dataType.elementType
            if isinstance(element_type, T.StructType) and "ability" in element_type.fieldNames():
                # Already parsed as array of structs with nested ability field
                result = result.withColumn(
                    "abilities",
                    F.transform(
                        F.col("abilities"), lambda x: x.getField("ability").getField("name")
                    ),
                )
            # If already array of strings, leave as-is

    return result

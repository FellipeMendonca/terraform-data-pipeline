"""Unit tests for the null handling transformation."""

import pytest
from pyspark.sql import Row
from pyspark.sql import types as T

from src.glue.transformations.null_handling import handle_nulls


class TestHandleNulls:
    """Tests for handle_nulls(df)."""

    def test_null_numerics_become_zero(self, spark):
        """Normal case: null values in numeric columns are replaced with 0."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("height", T.IntegerType()),
            T.StructField("weight", T.IntegerType()),
            T.StructField("base_experience", T.IntegerType()),
            T.StructField("hp", T.IntegerType()),
            T.StructField("attack", T.IntegerType()),
            T.StructField("defense", T.IntegerType()),
            T.StructField("special_attack", T.IntegerType()),
            T.StructField("special_defense", T.IntegerType()),
            T.StructField("speed", T.IntegerType()),
        ])
        data = [
            (1, "bulbasaur", 7, 69, 64, 45, 49, 49, 65, 65, 45),
            (2, "ivysaur", None, 130, 142, 60, 62, 63, 80, 80, 60),
            (3, "venusaur", 20, None, 263, 80, 82, 83, 100, 100, 80),
            (4, "charmander", 6, 85, None, 39, 52, 43, 60, 50, 65),
            (5, "charmeleon", 11, 190, 142, None, 64, 58, 80, 65, 80),
            (6, "charizard", 17, 905, 267, 78, None, 78, 109, 85, 100),
            (7, "squirtle", 5, 90, 63, 44, 48, None, 50, 64, 43),
            (8, "wartortle", 10, 225, 142, 59, 63, 80, None, 80, 58),
            (9, "blastoise", 16, 855, 265, 79, 83, 100, 85, None, 78),
            (10, "caterpie", 3, 29, 39, 45, 30, 35, 20, 20, None),
        ]
        df = spark.createDataFrame(data, schema)

        result = handle_nulls(df)
        rows = {row.id: row for row in result.collect()}

        # Verify nulls replaced with 0
        assert rows[2].height == 0
        assert rows[3].weight == 0
        assert rows[4].base_experience == 0
        assert rows[5].hp == 0
        assert rows[6].attack == 0
        assert rows[7].defense == 0
        assert rows[8].special_attack == 0
        assert rows[9].special_defense == 0
        assert rows[10].speed == 0

        # Verify non-null values preserved
        assert rows[1].height == 7
        assert rows[1].weight == 69
        assert rows[1].hp == 45

    def test_null_strings_become_empty(self, spark):
        """Null values in text columns are replaced with empty string."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("height", T.IntegerType()),
        ])
        data = [
            (1, None, 7),
            (2, None, 10),
            (3, "venusaur", 20),
            (4, None, 6),
            (5, "charmeleon", 11),
            (6, "charizard", 17),
            (7, None, 5),
            (8, "wartortle", 10),
            (9, "blastoise", 16),
            (10, "caterpie", 3),
        ]
        df = spark.createDataFrame(data, schema)

        result = handle_nulls(df)
        rows = {row.id: row for row in result.collect()}

        # Null names become empty string
        assert rows[1].name == ""
        assert rows[2].name == ""
        assert rows[4].name == ""
        assert rows[7].name == ""

        # Non-null names preserved
        assert rows[3].name == "venusaur"
        assert rows[5].name == "charmeleon"
        assert rows[8].name == "wartortle"

    def test_empty_dataframe(self, spark):
        """Edge case: empty DataFrame returns empty DataFrame with same schema."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("height", T.IntegerType()),
            T.StructField("weight", T.IntegerType()),
            T.StructField("base_experience", T.IntegerType()),
        ])
        df = spark.createDataFrame([], schema)

        result = handle_nulls(df)

        assert result.count() == 0
        assert result.schema == df.schema

    def test_no_nulls_preserves_all_values(self, spark):
        """When there are no null values, all data is preserved unchanged."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("height", T.IntegerType()),
            T.StructField("weight", T.IntegerType()),
            T.StructField("base_experience", T.IntegerType()),
            T.StructField("hp", T.IntegerType()),
            T.StructField("attack", T.IntegerType()),
            T.StructField("defense", T.IntegerType()),
            T.StructField("special_attack", T.IntegerType()),
            T.StructField("special_defense", T.IntegerType()),
            T.StructField("speed", T.IntegerType()),
        ])
        data = [
            (1, "bulbasaur", 7, 69, 64, 45, 49, 49, 65, 65, 45),
            (2, "ivysaur", 10, 130, 142, 60, 62, 63, 80, 80, 60),
            (3, "venusaur", 20, 1000, 263, 80, 82, 83, 100, 100, 80),
            (4, "charmander", 6, 85, 62, 39, 52, 43, 60, 50, 65),
            (5, "charmeleon", 11, 190, 142, 60, 64, 58, 80, 65, 80),
            (6, "charizard", 17, 905, 267, 78, 84, 78, 109, 85, 100),
            (7, "squirtle", 5, 90, 63, 44, 48, 65, 50, 64, 43),
            (8, "wartortle", 10, 225, 142, 59, 63, 80, 65, 80, 58),
            (9, "blastoise", 16, 855, 265, 79, 83, 100, 85, 105, 78),
            (10, "caterpie", 3, 29, 39, 45, 30, 35, 20, 20, 45),
        ]
        df = spark.createDataFrame(data, schema)

        result = handle_nulls(df)
        rows = {row.id: row for row in result.collect()}

        # All values should be unchanged
        assert rows[1].height == 7
        assert rows[1].name == "bulbasaur"
        assert rows[5].attack == 64
        assert rows[10].speed == 45

    def test_columns_not_in_schema_are_ignored(self, spark):
        """Columns not in the known lists are left untouched even if null."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("custom_field", T.StringType()),
        ])
        data = [
            (1, "bulbasaur", None),
            (2, "ivysaur", "value"),
        ]
        df = spark.createDataFrame(data, schema)

        result = handle_nulls(df)
        rows = {row.id: row for row in result.collect()}

        # custom_field is not in TEXT_COLUMNS so null remains
        assert rows[1].custom_field is None
        assert rows[2].custom_field == "value"
        # name is in TEXT_COLUMNS so null would be handled, but here it's not null
        assert rows[1].name == "bulbasaur"

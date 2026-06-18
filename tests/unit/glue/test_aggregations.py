"""Unit tests for the aggregation transformation."""

import pytest
from pyspark.sql import Row
from pyspark.sql import types as T

from src.glue.transformations.aggregations import aggregate_by_type


class TestAggregateByType:
    """Tests for aggregate_by_type(df)."""

    def test_correct_count_avg_min_max(self, spark):
        """Normal case: verify count, avg, min, max are mathematically correct."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("primary_type", T.StringType()),
            T.StructField("height", T.IntegerType()),
            T.StructField("weight", T.IntegerType()),
            T.StructField("base_experience", T.IntegerType()),
            T.StructField("hp", T.IntegerType()),
            T.StructField("attack", T.IntegerType()),
            T.StructField("defense", T.IntegerType()),
            T.StructField("speed", T.IntegerType()),
        ])
        data = [
            # 4 grass Pokémon: heights 7, 10, 20, 8 → avg=11.25, min=7, max=20
            (1, "bulbasaur", "grass", 7, 69, 64, 45, 49, 49, 45),
            (2, "ivysaur", "grass", 10, 130, 142, 60, 62, 63, 60),
            (3, "venusaur", "grass", 20, 1000, 263, 80, 82, 83, 80),
            (11, "oddish", "grass", 8, 54, 64, 45, 50, 55, 30),
            # 3 fire Pokémon: heights 6, 11, 17 → avg=11.333, min=6, max=17
            (4, "charmander", "fire", 6, 85, 62, 39, 52, 43, 65),
            (5, "charmeleon", "fire", 11, 190, 142, 58, 64, 58, 80),
            (6, "charizard", "fire", 17, 905, 267, 78, 84, 78, 100),
            # 3 water Pokémon: heights 5, 10, 16 → avg=10.333, min=5, max=16
            (7, "squirtle", "water", 5, 90, 63, 44, 48, 65, 43),
            (8, "wartortle", "water", 10, 225, 142, 59, 63, 80, 58),
            (9, "blastoise", "water", 16, 855, 265, 79, 83, 100, 78),
        ]
        df = spark.createDataFrame(data, schema)

        result = aggregate_by_type(df)
        rows = {row.type: row for row in result.collect()}

        # Verify grass type aggregations
        assert rows["grass"]["count"] == 4
        assert rows["grass"].avg_height == pytest.approx(11.25)
        assert rows["grass"].min_height == 7
        assert rows["grass"].max_height == 20
        assert rows["grass"].avg_weight == pytest.approx((69 + 130 + 1000 + 54) / 4)
        assert rows["grass"].min_weight == 54
        assert rows["grass"].max_weight == 1000

        # Verify fire type aggregations
        assert rows["fire"]["count"] == 3
        assert rows["fire"].avg_height == pytest.approx((6 + 11 + 17) / 3)
        assert rows["fire"].min_height == 6
        assert rows["fire"].max_height == 17

        # Verify water type aggregations
        assert rows["water"]["count"] == 3
        assert rows["water"].avg_height == pytest.approx((5 + 10 + 16) / 3)
        assert rows["water"].min_height == 5
        assert rows["water"].max_height == 16

    def test_empty_dataframe(self, spark):
        """Edge case: empty DataFrame produces empty result."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("primary_type", T.StringType()),
            T.StructField("height", T.IntegerType()),
            T.StructField("weight", T.IntegerType()),
            T.StructField("base_experience", T.IntegerType()),
            T.StructField("hp", T.IntegerType()),
            T.StructField("attack", T.IntegerType()),
            T.StructField("defense", T.IntegerType()),
            T.StructField("speed", T.IntegerType()),
        ])
        df = spark.createDataFrame([], schema)

        result = aggregate_by_type(df)

        assert result.count() == 0

    def test_single_type_produces_one_row(self, spark):
        """When all Pokémon share the same type, result has one row."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("primary_type", T.StringType()),
            T.StructField("height", T.IntegerType()),
            T.StructField("weight", T.IntegerType()),
            T.StructField("base_experience", T.IntegerType()),
            T.StructField("hp", T.IntegerType()),
            T.StructField("attack", T.IntegerType()),
            T.StructField("defense", T.IntegerType()),
            T.StructField("speed", T.IntegerType()),
        ])
        data = [
            (1, "bulbasaur", "grass", 7, 69, 64, 45, 49, 49, 45),
            (2, "ivysaur", "grass", 10, 130, 142, 60, 62, 63, 60),
            (3, "venusaur", "grass", 20, 1000, 263, 80, 82, 83, 80),
            (11, "oddish", "grass", 8, 54, 64, 45, 50, 55, 30),
            (12, "gloom", "grass", 8, 86, 138, 60, 65, 70, 40),
            (13, "vileplume", "grass", 12, 186, 245, 75, 80, 85, 50),
            (14, "bellsprout", "grass", 7, 40, 60, 50, 75, 35, 40),
            (15, "weepinbell", "grass", 10, 64, 137, 65, 90, 50, 55),
            (16, "victreebel", "grass", 17, 155, 245, 80, 105, 65, 70),
            (17, "exeggcute", "grass", 4, 25, 65, 60, 40, 80, 40),
        ]
        df = spark.createDataFrame(data, schema)

        result = aggregate_by_type(df)

        assert result.count() == 1
        row = result.collect()[0]
        assert row.type == "grass"
        assert row["count"] == 10

        # Mathematically verify avg_height: sum of heights / 10
        expected_avg_height = (7 + 10 + 20 + 8 + 8 + 12 + 7 + 10 + 17 + 4) / 10
        assert row.avg_height == pytest.approx(expected_avg_height)
        assert row.min_height == 4
        assert row.max_height == 20

    def test_avg_only_columns_have_averages(self, spark):
        """Columns in AVG_ONLY_COLUMNS produce only avg_ aggregations."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("primary_type", T.StringType()),
            T.StructField("height", T.IntegerType()),
            T.StructField("weight", T.IntegerType()),
            T.StructField("base_experience", T.IntegerType()),
            T.StructField("hp", T.IntegerType()),
            T.StructField("attack", T.IntegerType()),
            T.StructField("defense", T.IntegerType()),
            T.StructField("speed", T.IntegerType()),
        ])
        data = [
            (1, "bulbasaur", "grass", 7, 69, 64, 45, 49, 49, 45),
            (2, "ivysaur", "grass", 10, 130, 142, 60, 62, 63, 60),
            (3, "venusaur", "grass", 20, 1000, 263, 80, 82, 83, 80),
            (4, "charmander", "fire", 6, 85, 62, 39, 52, 43, 65),
            (5, "charmeleon", "fire", 11, 190, 142, 58, 64, 58, 80),
            (6, "charizard", "fire", 17, 905, 267, 78, 84, 78, 100),
            (7, "squirtle", "water", 5, 90, 63, 44, 48, 65, 43),
            (8, "wartortle", "water", 10, 225, 142, 59, 63, 80, 58),
            (9, "blastoise", "water", 16, 855, 265, 79, 83, 100, 78),
            (10, "caterpie", "bug", 3, 29, 39, 45, 30, 35, 45),
        ]
        df = spark.createDataFrame(data, schema)

        result = aggregate_by_type(df)
        result_columns = result.columns

        # AVG_ONLY columns should have avg_ but not min_ or max_
        assert "avg_base_experience" in result_columns
        assert "avg_hp" in result_columns
        assert "avg_attack" in result_columns
        assert "avg_defense" in result_columns
        assert "avg_speed" in result_columns

        # These should NOT exist
        assert "min_base_experience" not in result_columns
        assert "max_base_experience" not in result_columns
        assert "min_hp" not in result_columns
        assert "max_hp" not in result_columns

        # FULL_STATS columns should have avg, min, max
        assert "avg_height" in result_columns
        assert "min_height" in result_columns
        assert "max_height" in result_columns
        assert "avg_weight" in result_columns
        assert "min_weight" in result_columns
        assert "max_weight" in result_columns

        # Verify avg values for bug type (single Pokémon)
        bug_row = result.filter("type = 'bug'").collect()[0]
        assert bug_row.avg_base_experience == pytest.approx(39.0)
        assert bug_row.avg_hp == pytest.approx(45.0)
        assert bug_row.avg_speed == pytest.approx(45.0)

    def test_output_renames_primary_type_to_type(self, spark):
        """The primary_type column is renamed to 'type' in the output."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("primary_type", T.StringType()),
            T.StructField("height", T.IntegerType()),
            T.StructField("weight", T.IntegerType()),
            T.StructField("base_experience", T.IntegerType()),
            T.StructField("hp", T.IntegerType()),
            T.StructField("attack", T.IntegerType()),
            T.StructField("defense", T.IntegerType()),
            T.StructField("speed", T.IntegerType()),
        ])
        data = [
            (1, "bulbasaur", "grass", 7, 69, 64, 45, 49, 49, 45),
            (4, "charmander", "fire", 6, 85, 62, 39, 52, 43, 65),
        ]
        df = spark.createDataFrame(data, schema)

        result = aggregate_by_type(df)

        assert "type" in result.columns
        assert "primary_type" not in result.columns

"""Unit tests for the deduplication transformation."""

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row

from src.glue.transformations.deduplication import deduplicate


class TestDeduplicate:
    """Tests for deduplicate(df, key='id')."""

    def test_removes_duplicate_ids(self, spark):
        """Normal case: removes rows with duplicate IDs, keeping first occurrence."""
        data = [
            Row(id=1, name="bulbasaur", height=7),
            Row(id=2, name="ivysaur", height=10),
            Row(id=3, name="venusaur", height=20),
            Row(id=1, name="bulbasaur_dup", height=7),
            Row(id=4, name="charmander", height=6),
            Row(id=5, name="charmeleon", height=11),
            Row(id=2, name="ivysaur_dup", height=10),
            Row(id=6, name="charizard", height=17),
            Row(id=7, name="squirtle", height=5),
            Row(id=8, name="wartortle", height=10),
            Row(id=3, name="venusaur_dup", height=20),
            Row(id=9, name="blastoise", height=16),
        ]
        df = spark.createDataFrame(data)

        result = deduplicate(df, key="id")
        result_ids = sorted([row.id for row in result.collect()])

        assert result.count() == 9
        assert result_ids == [1, 2, 3, 4, 5, 6, 7, 8, 9]

    def test_empty_dataframe(self, spark):
        """Edge case: empty DataFrame returns empty DataFrame with same schema."""
        schema = "id INT, name STRING, height INT"
        df = spark.createDataFrame([], schema)

        result = deduplicate(df, key="id")

        assert result.count() == 0
        assert result.schema == df.schema

    def test_no_duplicates_returns_same_data(self, spark):
        """When there are no duplicates, all rows are preserved."""
        data = [
            Row(id=1, name="bulbasaur", height=7),
            Row(id=2, name="ivysaur", height=10),
            Row(id=3, name="venusaur", height=20),
            Row(id=4, name="charmander", height=6),
            Row(id=5, name="charmeleon", height=11),
            Row(id=6, name="charizard", height=17),
            Row(id=7, name="squirtle", height=5),
            Row(id=8, name="wartortle", height=10),
            Row(id=9, name="blastoise", height=16),
            Row(id=10, name="caterpie", height=3),
        ]
        df = spark.createDataFrame(data)

        result = deduplicate(df, key="id")

        assert_df_equality(result, df, ignore_row_order=True)

    def test_custom_key_column(self, spark):
        """Deduplication works with a custom key column."""
        data = [
            Row(id=1, name="bulbasaur", primary_type="grass"),
            Row(id=2, name="ivysaur", primary_type="grass"),
            Row(id=3, name="venusaur", primary_type="grass"),
            Row(id=4, name="charmander", primary_type="fire"),
            Row(id=5, name="charmeleon", primary_type="fire"),
            Row(id=6, name="charizard", primary_type="fire"),
            Row(id=7, name="squirtle", primary_type="water"),
            Row(id=8, name="wartortle", primary_type="water"),
            Row(id=9, name="blastoise", primary_type="water"),
            Row(id=10, name="caterpie", primary_type="bug"),
        ]
        df = spark.createDataFrame(data)

        result = deduplicate(df, key="primary_type")

        # Should keep only one row per primary_type
        assert result.count() == 4
        result_types = sorted([row.primary_type for row in result.collect()])
        assert result_types == ["bug", "fire", "grass", "water"]

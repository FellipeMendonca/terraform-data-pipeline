"""Unit tests for the type standardization transformation."""

import json

import pytest
from pyspark.sql import Row
from pyspark.sql import types as T

from src.glue.transformations.type_standardization import standardize_types


class TestStandardizeTypes:
    """Tests for standardize_types(df)."""

    def test_converts_types_array_of_structs_to_string_array(self, spark):
        """Normal case: types as array of structs (PokeAPI format) becomes array<string>."""
        types_schema = T.ArrayType(
            T.StructType([
                T.StructField("slot", T.IntegerType()),
                T.StructField("type", T.StructType([
                    T.StructField("name", T.StringType()),
                    T.StructField("url", T.StringType()),
                ])),
            ])
        )
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("height", T.StringType()),
            T.StructField("weight", T.StringType()),
            T.StructField("base_experience", T.StringType()),
            T.StructField("types", types_schema),
        ])

        data = [
            (1, "Bulbasaur", "7", "69", "64", [{"slot": 1, "type": {"name": "grass", "url": "u1"}}, {"slot": 2, "type": {"name": "poison", "url": "u2"}}]),
            (2, "Ivysaur", "10", "130", "142", [{"slot": 1, "type": {"name": "grass", "url": "u1"}}, {"slot": 2, "type": {"name": "poison", "url": "u2"}}]),
            (3, "Venusaur", "20", "1000", "263", [{"slot": 1, "type": {"name": "grass", "url": "u1"}}, {"slot": 2, "type": {"name": "poison", "url": "u2"}}]),
            (4, "Charmander", "6", "85", "62", [{"slot": 1, "type": {"name": "fire", "url": "u3"}}]),
            (5, "Charmeleon", "11", "190", "142", [{"slot": 1, "type": {"name": "fire", "url": "u3"}}]),
            (6, "Charizard", "17", "905", "267", [{"slot": 1, "type": {"name": "fire", "url": "u3"}}, {"slot": 2, "type": {"name": "flying", "url": "u4"}}]),
            (7, "Squirtle", "5", "90", "63", [{"slot": 1, "type": {"name": "water", "url": "u5"}}]),
            (8, "Wartortle", "10", "225", "142", [{"slot": 1, "type": {"name": "water", "url": "u5"}}]),
            (9, "Blastoise", "16", "855", "265", [{"slot": 1, "type": {"name": "water", "url": "u5"}}]),
            (10, "Caterpie", "3", "29", "39", [{"slot": 1, "type": {"name": "bug", "url": "u6"}}]),
        ]
        df = spark.createDataFrame(data, schema)

        result = standardize_types(df)

        # Verify types column is now array<string>
        types_field = result.schema["types"]
        assert isinstance(types_field.dataType, T.ArrayType)
        assert isinstance(types_field.dataType.elementType, T.StringType)

        # Verify actual values
        row = result.filter("id = 1").collect()[0]
        assert row.types == ["grass", "poison"]

        row = result.filter("id = 4").collect()[0]
        assert row.types == ["fire"]

    def test_empty_dataframe(self, spark):
        """Edge case: empty DataFrame with types column returns empty with correct schema."""
        types_schema = T.ArrayType(
            T.StructType([
                T.StructField("slot", T.IntegerType()),
                T.StructField("type", T.StructType([
                    T.StructField("name", T.StringType()),
                    T.StructField("url", T.StringType()),
                ])),
            ])
        )
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("height", T.StringType()),
            T.StructField("weight", T.StringType()),
            T.StructField("base_experience", T.StringType()),
            T.StructField("types", types_schema),
        ])
        df = spark.createDataFrame([], schema)

        result = standardize_types(df)

        assert result.count() == 0
        types_field = result.schema["types"]
        assert isinstance(types_field.dataType, T.ArrayType)
        assert isinstance(types_field.dataType.elementType, T.StringType)

    def test_numeric_fields_cast_to_integer(self, spark):
        """Height, weight, base_experience are cast from string to IntegerType."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("height", T.StringType()),
            T.StructField("weight", T.StringType()),
            T.StructField("base_experience", T.StringType()),
        ])
        data = [
            (1, "Bulbasaur", "7", "69", "64"),
            (2, "Ivysaur", "10", "130", "142"),
            (3, "Venusaur", "20", "1000", "263"),
            (4, "Charmander", "6", "85", "62"),
            (5, "Charmeleon", "11", "190", "142"),
            (6, "Charizard", "17", "905", "267"),
            (7, "Squirtle", "5", "90", "63"),
            (8, "Wartortle", "10", "225", "142"),
            (9, "Blastoise", "16", "855", "265"),
            (10, "Caterpie", "3", "29", "39"),
        ]
        df = spark.createDataFrame(data, schema)

        result = standardize_types(df)

        # Verify types are now IntegerType
        assert result.schema["height"].dataType == T.IntegerType()
        assert result.schema["weight"].dataType == T.IntegerType()
        assert result.schema["base_experience"].dataType == T.IntegerType()

        # Verify values
        row = result.filter("id = 1").collect()[0]
        assert row.height == 7
        assert row.weight == 69
        assert row.base_experience == 64

    def test_name_converted_to_lowercase(self, spark):
        """Name column is converted to lowercase."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
        ])
        data = [
            (1, "Bulbasaur"),
            (2, "IVYSAUR"),
            (3, "VeNuSaUr"),
            (4, "charmander"),
            (5, "CHARMELEON"),
            (6, "Charizard"),
            (7, "SQUIRTLE"),
            (8, "Wartortle"),
            (9, "BLASTOISE"),
            (10, "Caterpie"),
        ]
        df = spark.createDataFrame(data, schema)

        result = standardize_types(df)

        rows = {row.id: row.name for row in result.collect()}
        assert rows[1] == "bulbasaur"
        assert rows[2] == "ivysaur"
        assert rows[3] == "venusaur"
        assert rows[4] == "charmander"
        assert rows[7] == "squirtle"

    def test_types_from_json_string(self, spark):
        """Types column as JSON string is parsed and extracted to array<string>."""
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("types", T.StringType()),
        ])
        types_json_1 = json.dumps([
            {"slot": 1, "type": {"name": "grass", "url": "u1"}},
            {"slot": 2, "type": {"name": "poison", "url": "u2"}},
        ])
        types_json_2 = json.dumps([
            {"slot": 1, "type": {"name": "fire", "url": "u3"}},
        ])
        data = [
            (1, "bulbasaur", types_json_1),
            (2, "charmander", types_json_2),
        ]
        df = spark.createDataFrame(data, schema)

        result = standardize_types(df)

        types_field = result.schema["types"]
        assert isinstance(types_field.dataType, T.ArrayType)
        assert isinstance(types_field.dataType.elementType, T.StringType)

        rows = {row.id: row.types for row in result.collect()}
        assert rows[1] == ["grass", "poison"]
        assert rows[2] == ["fire"]

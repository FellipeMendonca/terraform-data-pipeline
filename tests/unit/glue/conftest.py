"""Shared fixtures for Glue transformation unit tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    yield spark
    spark.stop()

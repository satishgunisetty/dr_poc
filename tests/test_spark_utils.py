"""Tests for the spark utilities module."""

import pytest
from pyspark.sql import SparkSession

from src.utils.spark_utils import get_spark_session, read_data, write_data


@pytest.fixture
def spark() -> SparkSession:
    """Create a Spark session for testing."""
    return (
        SparkSession.builder.appName("test")
        .master("local[1]")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )


def test_get_spark_session() -> None:
    """Test that we can create a Spark session."""
    spark = get_spark_session()
    assert spark is not None
    assert isinstance(spark, SparkSession)


def test_read_write_data(spark: SparkSession, tmp_path: pytest.TempPathFactory) -> None:
    """Test reading and writing data using the utility functions."""
    # Create test data
    test_data = [(1, "test1"), (2, "test2"), (3, "test3")]
    test_df = spark.createDataFrame(test_data, ["id", "value"])

    # Test writing data
    output_path = str(tmp_path / "test_output")
    write_data(test_df, output_path, "parquet")

    # Test reading data
    read_df = read_data(output_path, "parquet")
    assert read_df is not None

    # Verify data
    result_data = read_df.collect()
    assert len(result_data) == 3
    assert all(row["id"] > 0 for row in result_data)
    assert all(row["value"].startswith("test") for row in result_data)

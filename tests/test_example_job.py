"""Tests for the example job module."""

import pytest
from pyspark.sql import SparkSession

from src.jobs.example_job import ExampleJob


@pytest.fixture(scope="session")  # type: ignore[misc]
def spark() -> SparkSession:
    """Create a Spark session for testing."""
    return (
        SparkSession.builder.appName("test")
        .master("local[1]")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )


def test_example_job_initialization() -> None:
    """Test that ExampleJob can be initialized."""
    job = ExampleJob()
    assert job is not None


def test_example_job_process_data(spark: SparkSession) -> None:
    """Test the process_data method of ExampleJob."""
    # Create test data
    test_data = [(1, "test1"), (2, "test2"), (3, "test3")]
    test_df = spark.createDataFrame(test_data, ["id", "value"])

    # Initialize and run job
    job = ExampleJob()
    result_df = job.process_data(test_df)

    # Verify results
    result_data = result_df.collect()
    assert len(result_data) == 3
    assert all(row["id"] > 0 for row in result_data)
    assert all(row["value"].startswith("test") for row in result_data)

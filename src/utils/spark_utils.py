"""Utility functions for Spark operations."""

from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_spark_session(app_name: str = "Spark Utils") -> SparkSession:
    """Create and configure a Spark session.

    Args:
        app_name: Name of the Spark application

    Returns:
        Configured SparkSession instance
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )


def read_data(path: str, format_type: str = "parquet") -> Optional[DataFrame]:
    """Read data from a file.

    Args:
        path: Path to the data file
        format_type: File format (e.g., parquet, csv)

    Returns:
        DataFrame containing the read data
    """
    spark = get_spark_session()
    try:
        return (
            spark.read.format(format_type)
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path)
        )
    except Exception:
        return None


def write_data(df: DataFrame, path: str, format_type: str = "parquet") -> None:
    """Write data to a file.

    Args:
        df: DataFrame to write
        path: Target path for the file
        format_type: File format (e.g., parquet, csv)
    """
    df.write.format(format_type).option("header", "true").mode("overwrite").save(path)


def read_delta_table(spark: SparkSession, path: str) -> DataFrame:
    """Read data from a Delta table."""
    return spark.read.format("delta").load(path)


def write_delta_table(df: DataFrame, path: str) -> None:
    """Write data to a Delta table."""
    df.write.format("delta").mode("overwrite").save(path)


def create_schema(fields: List[Dict[str, Any]]) -> StructType:
    """Create a Spark schema from field definitions."""
    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "boolean": BooleanType(),
    }

    return StructType(
        [
            StructField(
                field["name"],
                type_mapping.get(field["type"], StringType()),
                field.get("nullable", True),
            )
            for field in fields
        ]
    )

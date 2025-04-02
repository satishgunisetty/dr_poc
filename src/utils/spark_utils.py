"""
Utility functions for Spark operations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

def get_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a Spark session with common configurations.
    
    Args:
        app_name (str): Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def read_delta_table(spark: SparkSession, table_path: str) -> DataFrame:
    """
    Read a Delta table from the specified path.
    
    Args:
        spark (SparkSession): Active Spark session
        table_path (str): Path to the Delta table
        
    Returns:
        DataFrame: Spark DataFrame containing the table data
    """
    return spark.read.format("delta").load(table_path)

def write_delta_table(df: DataFrame, table_path: str, mode: str = "overwrite") -> None:
    """
    Write a DataFrame to a Delta table.
    
    Args:
        df (DataFrame): Spark DataFrame to write
        table_path (str): Path where the Delta table should be written
        mode (str): Write mode (overwrite, append, etc.)
    """
    df.write.format("delta").mode(mode).save(table_path) 
"""
Example ETL job for Databricks.
"""

from pyspark.sql import SparkSession
from src.utils.spark_utils import get_spark_session, read_delta_table, write_delta_table
from src.config.settings import *
import logging

# Configure logging
logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT
)
logger = logging.getLogger(__name__)

def main():
    """
    Main function to run the ETL job.
    """
    try:
        # Initialize Spark session
        spark = get_spark_session("Example ETL Job")
        logger.info("Spark session initialized successfully")

        # Example: Read source data
        source_df = read_delta_table(spark, f"dbfs:/{STORAGE_ACCOUNT}/{CONTAINER_NAME}/source_table")
        logger.info(f"Read {source_df.count()} records from source table")

        # Example: Transform data
        transformed_df = source_df.filter(source_df["status"] == "active")
        logger.info(f"Transformed data: {transformed_df.count()} records")

        # Example: Write transformed data
        output_path = f"dbfs:/{STORAGE_ACCOUNT}/{CONTAINER_NAME}/output_table"
        write_delta_table(transformed_df, output_path)
        logger.info(f"Successfully wrote data to {output_path}")

    except Exception as e:
        logger.error(f"Error in ETL job: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main() 
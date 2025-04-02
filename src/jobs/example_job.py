"""Example job for demonstrating project structure and functionality."""

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from src.config.settings import CONTAINER_NAME, LOG_FORMAT, LOG_LEVEL, STORAGE_ACCOUNT

# Configure logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


class ExampleJob:
    """Example job class for data processing."""

    def __init__(self) -> None:
        """Initialize the job."""
        self.spark = (
            SparkSession.builder.appName("Example ETL Job")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
        )

    def read_data(self, path: str) -> Optional[DataFrame]:
        """Read data from Azure Data Lake Storage.

        Args:
            path: Path to the data in ADLS

        Returns:
            DataFrame containing the read data
        """
        try:
            adls_path = (
                f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}"
                f".dfs.core.windows.net/{path}"
            )
            return (
                self.spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(adls_path)
            )
        except Exception as e:
            logger.error("Error reading data: %s", str(e))
            return None

    def write_data(self, df: DataFrame, path: str) -> None:
        """Write data to Azure Data Lake Storage.

        Args:
            df: DataFrame to write
            path: Target path in ADLS
        """
        try:
            adls_path = (
                f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}"
                f".dfs.core.windows.net/{path}"
            )
            df.write.format("csv").option("header", "true").mode("overwrite").save(
                adls_path
            )
        except Exception as e:
            logger.error("Error writing data: %s", str(e))
            raise

    def process_data(self, df: DataFrame) -> DataFrame:
        """Process the input DataFrame.

        Args:
            df: Input DataFrame to process

        Returns:
            Processed DataFrame
        """
        return df.filter("id > 0")

    def run(self) -> None:
        """Run the ETL job."""
        try:
            # Read source data
            source_df = self.read_data("source_table")
            if source_df is None:
                return
            logger.info("Read %d records from source table", source_df.count())

            # Transform data
            transformed_df = self.process_data(source_df)
            logger.info("Transformed %d records", transformed_df.count())

            # Write transformed data
            self.write_data(transformed_df, "output_table")
            logger.info("Successfully wrote data to output_table")

        except Exception as e:
            logger.error("Job failed: %s", str(e))
            raise


if __name__ == "__main__":
    job = ExampleJob()
    job.run()

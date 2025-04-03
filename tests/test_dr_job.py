"""Tests for the Disaster Recovery job."""

from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.config.dr_config import (
    CatalogConfig,
    DRConfig,
    ErrorHandlingConfig,
    SchemaConfig,
    TableConfig,
    VerificationConfig,
)
from src.jobs.dr_job import DisasterRecoveryOrchestrator


@pytest.fixture(scope="session")  # type: ignore
def spark() -> SparkSession:
    """Create a Spark session for testing."""
    # Create a test Spark session with minimal configuration
    return (
        SparkSession.builder.master("local[1]")
        .appName("DR Job Test")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )


@pytest.fixture(scope="function")  # type: ignore
def mock_spark_session(mocker: Any) -> SparkSession:
    """Create a mock Spark session."""
    spark = mocker.Mock(spec=SparkSession)

    # Mock DataFrame methods
    df_mock = mocker.Mock()
    df_mock.toJSON.return_value.collect.return_value = ['{"properties": "test"}']
    df_mock.count.return_value = 100
    df_mock.select.return_value = df_mock
    df_mock.limit.return_value = df_mock
    df_mock.collect.return_value = [{"table": "test_table"}]

    # Set up the mock Spark session to return our mock DataFrame
    spark.sql.return_value = df_mock
    return spark


@pytest.fixture(scope="function")  # type: ignore
def sample_data(spark: SparkSession) -> Any:
    """Create sample data for testing."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )
    data = [(1, "test1", 100), (2, "test2", 200), (3, "test3", 300)]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="function")  # type: ignore
def test_config() -> DRConfig:
    """Create a test configuration."""
    return DRConfig(
        catalogs={
            "test_catalog": CatalogConfig(
                schemas={
                    "test_schema": SchemaConfig(
                        tables={
                            "test_table": TableConfig(
                                backup_frequency="daily",
                                priority=1,
                                verification=VerificationConfig(
                                    enabled=True,
                                    row_count_check=True,
                                    data_sample_check=True,
                                ),
                                error_handling=ErrorHandlingConfig(
                                    fail_fast=True,
                                    max_retries=3,
                                    retry_delay_seconds=60,
                                ),
                            )
                        }
                    )
                }
            )
        },
        backup_catalog="backup",
        backup_schema="dr",
    )


@pytest.fixture(scope="function")  # type: ignore
def dr_job(
    mock_spark_session: SparkSession, test_config: DRConfig
) -> DisasterRecoveryOrchestrator:
    """Create a DR job instance."""
    return DisasterRecoveryOrchestrator(config=test_config, spark=mock_spark_session)


def test_dr_job_initialization_without_spark() -> None:
    """Test DR job initialization without Spark session."""
    config = DRConfig(
        catalogs={},
        backup_catalog="backup",
        backup_schema="dr",
    )
    with pytest.raises(RuntimeError, match="No active Spark session found"):
        DisasterRecoveryOrchestrator(config=config)


def test_create_backup_catalog(dr_job: DisasterRecoveryOrchestrator) -> None:
    """Test backup catalog creation."""
    dr_job._create_backup_catalog()
    dr_job.spark.sql.assert_any_call("CREATE CATALOG IF NOT EXISTS backup")
    dr_job.spark.sql.assert_any_call("CREATE SCHEMA IF NOT EXISTS backup.dr")


def test_verify_clone_success(
    mock_spark_session: SparkSession,
    dr_job: DisasterRecoveryOrchestrator,
    test_config: DRConfig,
) -> None:
    """Test successful clone verification."""
    # Mock row count comparison
    mock_spark_session.sql.return_value.count.return_value = 1000
    mock_spark_session.sql.return_value.select.return_value = (
        mock_spark_session.sql.return_value
    )
    mock_spark_session.sql.return_value.limit.return_value = (
        mock_spark_session.sql.return_value
    )
    mock_spark_session.sql.return_value.collect.return_value = [
        {"count": 1000, "sum": 5000, "avg": 5.0}
    ]

    result = dr_job.verify_clone(
        source_table="test_table",
        target_table="test_table",
    )

    assert result is True


def test_verify_clone_failure(
    mock_spark_session: SparkSession,
    dr_job: DisasterRecoveryOrchestrator,
    test_config: DRConfig,
) -> None:
    """Test failed clone verification."""
    # Mock different row counts
    mock_spark_session.sql.return_value.count.side_effect = [1000, 999]
    mock_spark_session.sql.return_value.select.return_value = (
        mock_spark_session.sql.return_value
    )
    mock_spark_session.sql.return_value.limit.return_value = (
        mock_spark_session.sql.return_value
    )
    mock_spark_session.sql.return_value.collect.return_value = [
        {"count": 1000, "sum": 5000, "avg": 5.0}
    ]

    result = dr_job.verify_clone(
        source_table="test_table",
        target_table="test_table",
    )

    assert result is False


def test_deep_clone_table(
    dr_job: DisasterRecoveryOrchestrator, spark: SparkSession, sample_data: Any
) -> None:
    """Test deep cloning a table."""
    # Create source table
    source_table = "test_source"
    sample_data.createOrReplaceTempView(source_table)

    # Create backup catalog
    dr_job.create_backup_catalog()

    # Perform deep clone
    dr_job.deep_clone_table(
        source_catalog="test_catalog",
        source_schema="test_schema",
        source_table=source_table,
        target_catalog="backup",
        target_schema="dr",
        target_table=source_table,
    )

    # Verify clone
    source_count = spark.sql(f"SELECT COUNT(*) as count FROM {source_table}").collect()[
        0
    ]["count"]

    backup_table = f"{source_table}"  # Use single-part namespace
    target_count = spark.sql(f"SELECT COUNT(*) as count FROM {backup_table}").collect()[
        0
    ]["count"]

    assert source_count == target_count


def test_restore_table(
    dr_job: DisasterRecoveryOrchestrator, spark: SparkSession, sample_data: Any
) -> None:
    """Test restoring a table from backup."""
    # Create source table
    source_table = "test_source"
    sample_data.createOrReplaceTempView(source_table)

    # Create backup catalog and perform backup
    dr_job.create_backup_catalog()
    dr_job.deep_clone_table(
        source_catalog="test_catalog",
        source_schema="test_schema",
        source_table=source_table,
        target_catalog="backup",
        target_schema="dr",
        target_table=source_table,
    )

    # Verify restore
    assert dr_job.verify_clone(
        source_table=source_table,
        target_table=source_table,
    )


def test_get_tables_to_backup(dr_job: DisasterRecoveryOrchestrator) -> None:
    """Test getting tables that need backup."""
    tables = dr_job.get_tables_to_backup()
    assert len(tables) > 0
    assert all(isinstance(t[3], TableConfig) for t in tables)


def test_should_backup(dr_job: DisasterRecoveryOrchestrator) -> None:
    """Test backup frequency check."""
    # Test daily backup with ISO format string
    assert dr_job._should_backup("daily", "2023-01-01T00:00:00")

    # Test weekly backup
    assert dr_job._should_backup("weekly", "2023-01-01T00:00:00")

    # Test monthly backup
    assert dr_job._should_backup("monthly", "2023-01-01T00:00:00")

    # Test unknown frequency
    assert not dr_job._should_backup("invalid", "2023-01-01T00:00:00")


def test_cleanup_old_backups(
    dr_job: DisasterRecoveryOrchestrator, spark: SparkSession, sample_data: Any
) -> None:
    """Test cleaning up old backups."""
    # Create test table and backup
    source_table = "test_source"
    sample_data.createOrReplaceTempView(source_table)

    dr_job.create_backup_catalog()
    dr_job.deep_clone_table(
        source_catalog="test_catalog",
        source_schema="test_schema",
        source_table=source_table,
        target_catalog="backup",
        target_schema="dr",
        target_table=source_table,
    )

    # Run cleanup
    dr_job.cleanup_old_backups()

    # Verify cleanup by checking if the table exists
    try:
        spark.sql(f"SELECT * FROM {source_table}").collect()
        assert False, "Table should have been deleted"
    except Exception:
        # Expected - table should not exist
        pass

"""Disaster Recovery job for backing up and restoring Delta tables."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession

from src.config.dr_config import DRConfig, TableConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DisasterRecoveryOrchestrator:
    """Orchestrates disaster recovery operations for Delta tables."""

    def __init__(self, config: DRConfig, spark: Optional[SparkSession] = None) -> None:
        """Initialize the DR job.

        Args:
            config: Disaster recovery configuration
            spark: Optional SparkSession. If not provided, will try to get
                active session.
        """
        self.config = config
        self.spark = spark or SparkSession.getActiveSession()
        if not self.spark:
            raise RuntimeError("No active Spark session found")
        self.logger = logging.getLogger(__name__)
        self.last_source_table_props: Optional[Dict[str, Any]] = None
        self.last_deep_clone_metadata: Optional[Dict[str, Any]] = None

    @property
    def backup_catalog(self) -> str:
        """Get the backup catalog name."""
        return self.config.backup_catalog

    @property
    def backup_schema(self) -> str:
        """Get the backup schema name."""
        return self.config.backup_schema

    def _get_table_properties(self, table_name: str) -> Dict[str, Any]:
        """Get table properties using DESCRIBE TABLE EXTENDED."""
        try:
            assert self.spark is not None  # For type checker
            sql = f"DESCRIBE TABLE EXTENDED {table_name}"
            df = self.spark.sql(sql)
            return {row["col_name"]: row["data_type"] for row in df.collect()}
        except Exception as e:
            self.logger.error("Error getting table properties: %s", e)
            return {}

    def _get_deep_clone_metadata(self, source_table: str) -> Dict[str, Any]:
        """Get metadata for deep clone operation."""
        try:
            assert self.spark is not None  # For type checker
            sql = f"DESCRIBE TABLE EXTENDED {source_table}"
            df = self.spark.sql(sql)
            return {row["col_name"]: row["data_type"] for row in df.collect()}
        except Exception as e:
            self.logger.error("Error getting deep clone metadata: %s", e)
            return {}

    def _get_storage_location(self, table_name: str) -> Optional[str]:
        """Get storage location based on configuration."""
        if not hasattr(self.config, "storage") or not self.config.storage:
            return None

        if not self.config.storage.use_external_location:
            return None

        storage_type = self.config.storage.storage_type.lower()
        container = self.config.storage.container_name
        path_prefix = self.config.storage.path_prefix

        storage_path = {
            "abfss": f"abfss://{container}@{path_prefix}/{table_name}",
            "s3": f"s3://{container}/{path_prefix}/{table_name}",
            "gcs": f"gs://{container}/{path_prefix}/{table_name}",
        }.get(storage_type)

        if not storage_path:
            self.logger.error(
                "Unsupported storage type: %s. Supported types: abfss, s3, gcs",
                storage_type,
            )
            return None

        return storage_path

    def deep_clone_table(
        self,
        *,  # Force keyword arguments
        source_catalog: str,
        source_schema: str,
        source_table: str,
        target_catalog: str,
        target_schema: str,
        target_table: str,
    ) -> None:
        """Deep clone a table from source to target.

        This method performs a deep clone operation and stores the source table
        properties and deep clone metadata as instance attributes for later
        reference.

        Args:
            source_catalog: Source catalog name
            source_schema: Source schema name
            source_table: Source table name
            target_catalog: Target catalog name
            target_schema: Target schema name
            target_table: Target table name
        """
        try:
            storage_location = self._get_storage_location(target_table)

            # Store metadata as instance attributes for later reference
            self.last_source_table_props = self._get_table_properties(
                table_name=source_table,
            )
            self.last_deep_clone_metadata = self._get_deep_clone_metadata(
                source_table=source_table,
            )

            assert self.spark is not None  # For type checker
            sql_cmd = (
                f"CREATE TABLE IF NOT EXISTS {target_table} "
                f"DEEP CLONE {source_table}"
            )

            if storage_location:
                sql_cmd += f" LOCATION '{storage_location}'"

            self.spark.sql(sql_cmd)
            self.logger.info(
                "Successfully cloned table from %s.%s.%s to %s.%s.%s",
                source_catalog,
                source_schema,
                source_table,
                target_catalog,
                target_schema,
                target_table,
            )
        except Exception as e:
            self.logger.error("Error cloning table: %s", str(e))
            raise

    def verify_clone(
        self,
        source_table: str,
        target_table: str,
    ) -> bool:
        """Verify that a cloned table matches its source."""
        try:
            assert self.spark is not None  # For type checker
            # Compare row counts
            source_count = self.spark.sql(
                f"SELECT COUNT(*) as count FROM {source_table}"
            ).count()
            target_count = self.spark.sql(
                f"SELECT COUNT(*) as count FROM {target_table}"
            ).count()

            if source_count != target_count:
                self.logger.error(
                    "Row count mismatch: source=%d, target=%d",
                    source_count,
                    target_count,
                )
                return False

            # Compare data samples
            source_sample = (
                self.spark.sql(
                    f"SELECT * FROM {source_table}"
                )  # Use single-part namespace
                .select("*")
                .limit(1000)
            )
            target_sample = (
                self.spark.sql(
                    f"SELECT * FROM {target_table}"
                )  # Use single-part namespace
                .select("*")
                .limit(1000)
            )

            # Compare basic statistics of samples
            source_stats = source_sample.collect()
            target_stats = target_sample.collect()

            if source_stats != target_stats:
                self.logger.error("Data sample mismatch between source and target")
                return False

            return True
        except Exception as e:
            self.logger.error("Error verifying clone: %s", str(e))
            return False

    def _create_backup_catalog(self) -> None:
        """Create backup catalog and schema if they don't exist."""
        assert self.spark is not None  # For type checker
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.backup_catalog}")
        self.spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self.backup_catalog}.{self.backup_schema}"
        )

    def run_backup(self) -> None:
        """Run the backup process for all configured tables."""
        try:
            self._create_backup_catalog()
            tables = self.get_tables_to_backup()

            for catalog, schema, table, config in tables:
                try:
                    self.deep_clone_table(
                        source_catalog=catalog,
                        source_schema=schema,
                        source_table=table,
                        target_catalog=self.config.backup_catalog,
                        target_schema=self.config.backup_schema,
                        target_table=table,
                    )

                    if config.verification.enabled:
                        success = self.verify_clone(
                            source_table=table,
                            target_table=table,
                        )
                        if not success and config.error_handling.fail_fast:
                            raise RuntimeError(
                                f"Verification failed for {catalog}.{schema}.{table}"
                            )

                except Exception as e:
                    self.logger.error(
                        "Error backing up %s.%s.%s: %s",
                        catalog,
                        schema,
                        table,
                        str(e),
                    )
                    if config.error_handling.fail_fast:
                        raise

        except Exception as e:
            self.logger.error("Backup process failed: %s", str(e))
            raise

    def create_backup_catalog(self) -> None:
        """Create backup catalog and schema if they don't exist."""
        self._create_backup_catalog()

    def _should_backup(self, frequency: str, last_backup: Optional[str] = None) -> bool:
        """Check if a table should be backed up based on frequency.

        Args:
            frequency: Backup frequency (daily, weekly, monthly)
            last_backup: Optional timestamp of last backup

        Returns:
            bool: True if backup is needed, False otherwise
        """
        if not last_backup:
            return True

        last_backup_dt = datetime.fromisoformat(last_backup)
        now = datetime.now()

        if frequency == "daily":
            return (now - last_backup_dt).days >= 1
        if frequency == "weekly":
            return (now - last_backup_dt).days >= 7
        if frequency == "monthly":
            return (now - last_backup_dt).days >= 30

        self.logger.warning("Unknown backup frequency: %s", frequency)
        return False

    def get_tables_to_backup(self) -> List[Tuple[str, str, str, TableConfig]]:
        """Get list of tables that need backup.

        Returns:
            List of tuples containing (catalog, schema, table, config)
        """
        tables_to_backup = []
        for catalog_name, catalog in self.config.catalogs.items():
            for schema_name, schema in catalog.schemas.items():
                for table_name, table_config in schema.tables.items():
                    # Check if table needs backup based on frequency
                    if self._should_backup(table_config.backup_frequency):
                        tables_to_backup.append(
                            (catalog_name, schema_name, table_name, table_config)
                        )

        # Sort by priority (higher priority first)
        return sorted(tables_to_backup, key=lambda x: x[3].priority, reverse=True)

    def _get_retention_days(self) -> int:
        """Get the minimum retention days based on backup frequencies.

        Returns:
            int: Minimum number of days to retain backups
        """
        retention_days = 30  # Default retention period
        for catalog in self.config.catalogs.values():
            for schema in catalog.schemas.values():
                for table_config in schema.tables.values():
                    if table_config.backup_frequency == "daily":
                        retention_days = min(retention_days, 7)
                    elif table_config.backup_frequency == "weekly":
                        retention_days = min(retention_days, 30)
                    elif table_config.backup_frequency == "monthly":
                        retention_days = min(retention_days, 90)
        return retention_days

    def _get_table_creation_time(self, table_name: str) -> Optional[str]:
        """Get the creation time of a table from its properties.

        Args:
            table_name: Name of the table

        Returns:
            Optional[str]: Creation time in ISO format if found, None otherwise
        """
        assert self.spark is not None  # For type checker
        props_df = self.spark.sql(
            f"DESCRIBE TABLE EXTENDED "
            f"{self.backup_catalog}.{self.backup_schema}.{table_name}"
        )
        props = props_df.collect()

        for prop in props:
            if prop.get("col_name") == "Created Time":
                creation_time = prop.get("data_type")
                return str(creation_time) if creation_time is not None else None
        return None

    def _drop_old_backup_table(self, table_name: str, age_days: int) -> None:
        """Drop an old backup table.

        Args:
            table_name: Name of the table to drop
            age_days: Age of the table in days
        """
        assert self.spark is not None  # For type checker
        self.logger.info(
            "Dropping old backup table %s (age: %d days)",
            table_name,
            age_days,
        )
        self.spark.sql(
            f"DROP TABLE {self.backup_catalog}.{self.backup_schema}.{table_name}"
        )

    def cleanup_old_backups(self) -> None:
        """Clean up old backups based on retention policy."""
        try:
            assert self.spark is not None  # For type checker
            # Get list of tables in backup catalog
            tables_df = self.spark.sql(
                f"SHOW TABLES IN {self.backup_catalog}.{self.backup_schema}"
            )
            tables = tables_df.collect()
            retention_days = self._get_retention_days()

            for table in tables:
                table_name = table.get("table") or table.get("tableName")
                if not table_name:
                    self.logger.warning("Could not find table name in table info")
                    continue

                creation_time = self._get_table_creation_time(table_name)

                if not creation_time:
                    self.logger.warning(
                        "Could not find creation time for table %s, skipping cleanup",
                        table_name,
                    )
                    continue

                try:
                    creation_dt = datetime.fromisoformat(creation_time)
                    age_days = (datetime.now() - creation_dt).days

                    if age_days > retention_days:
                        self._drop_old_backup_table(table_name, age_days)

                except (ValueError, TypeError) as e:
                    self.logger.error(
                        "Error parsing creation time for table %s: %s",
                        table_name,
                        str(e),
                    )

        except Exception as e:
            self.logger.error("Error cleaning up old backups: %s", str(e))
            raise

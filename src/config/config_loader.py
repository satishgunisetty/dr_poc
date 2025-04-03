"""Configuration loader for disaster recovery."""

from typing import Dict

import yaml  # type: ignore[import-untyped, unused-ignore]  # noqa: F401

from src.config.dr_config import (
    CatalogConfig,
    DRConfig,
    ErrorHandlingConfig,
    SchemaConfig,
    StorageConfig,
    TableConfig,
    VerificationConfig,
)


class ConfigLoader:
    """Loads and validates DR configuration from YAML."""

    @staticmethod
    def load_config(config_path: str) -> DRConfig:
        """Load configuration from YAML file.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            DRConfig: Loaded configuration
        """
        with open(config_path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)

        # Load catalogs
        catalogs: Dict[str, CatalogConfig] = {}
        for catalog_name, catalog_data in config_data["catalogs"].items():
            schemas: Dict[str, SchemaConfig] = {}
            for schema_name, schema_data in catalog_data["schemas"].items():
                tables: Dict[str, TableConfig] = {}
                for table_name, table_data in schema_data["tables"].items():
                    # Create verification config
                    verification = VerificationConfig(
                        enabled=table_data.get("verification", {}).get("enabled", True),
                        row_count_check=table_data.get("verification", {}).get(
                            "row_count_check", True
                        ),
                        data_sample_check=table_data.get("verification", {}).get(
                            "data_sample_check", True
                        ),
                    )

                    # Create error handling config
                    error_handling = ErrorHandlingConfig(
                        fail_fast=table_data.get("error_handling", {}).get(
                            "fail_fast", True
                        ),
                        max_retries=table_data.get("error_handling", {}).get(
                            "max_retries", 3
                        ),
                        retry_delay_seconds=table_data.get("error_handling", {}).get(
                            "retry_delay_seconds", 60
                        ),
                    )

                    # Create storage config if present
                    storage = None
                    if "storage" in table_data:
                        storage = StorageConfig(
                            storage_type=table_data["storage"]["type"],
                            container_name=table_data["storage"]["container_name"],
                            path_prefix=table_data["storage"]["path_prefix"],
                            use_external_location=table_data["storage"].get(
                                "use_external_location", False
                            ),
                        )

                    # Create table config
                    tables[table_name] = TableConfig(
                        backup_frequency=table_data["backup_frequency"],
                        priority=table_data["priority"],
                        verification=verification,
                        error_handling=error_handling,
                        storage=storage,
                    )

                schemas[schema_name] = SchemaConfig(tables=tables)
            catalogs[catalog_name] = CatalogConfig(schemas=schemas)

        return DRConfig(
            catalogs=catalogs,
            backup_catalog=config_data["backup_catalog"],
            backup_schema=config_data["backup_schema"],
        )

"""Configuration classes for disaster recovery."""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class VerificationConfig:
    """Configuration for verification settings."""

    enabled: bool = True
    row_count_check: bool = True
    data_sample_check: bool = True


@dataclass
class ErrorHandlingConfig:
    """Configuration for error handling settings."""

    fail_fast: bool = True
    max_retries: int = 3
    retry_delay_seconds: int = 60


@dataclass
class StorageConfig:
    """Configuration for storage settings."""

    storage_type: str  # "abfss", "s3", "gcs"
    container_name: str
    path_prefix: str
    use_external_location: bool = False


@dataclass
class TableConfig:
    """Configuration for a specific table."""

    backup_frequency: str  # e.g., "daily", "weekly", "monthly"
    priority: int  # Higher number = higher priority
    verification: VerificationConfig
    error_handling: ErrorHandlingConfig
    storage: Optional[StorageConfig] = None


@dataclass
class SchemaConfig:
    """Configuration for a schema."""

    tables: Dict[str, TableConfig]


@dataclass
class CatalogConfig:
    """Configuration for a catalog."""

    schemas: Dict[str, SchemaConfig]


@dataclass
class DRConfig:
    """Configuration for disaster recovery."""

    catalogs: Dict[str, CatalogConfig]
    backup_catalog: str
    backup_schema: str
    storage: Optional[StorageConfig] = None


# Example configuration
example_config = DRConfig(
    catalogs={
        "sales": CatalogConfig(
            schemas={
                "transactions": SchemaConfig(
                    tables={
                        "orders": TableConfig(
                            backup_frequency="daily",
                            priority=1,
                            verification=VerificationConfig(),
                            error_handling=ErrorHandlingConfig(),
                        ),
                        "customers": TableConfig(
                            backup_frequency="weekly",
                            priority=2,
                            verification=VerificationConfig(),
                            error_handling=ErrorHandlingConfig(),
                        ),
                    },
                ),
            },
        ),
        "inventory": CatalogConfig(
            schemas={
                "stock": SchemaConfig(
                    tables={
                        "products": TableConfig(
                            backup_frequency="daily",
                            priority=1,
                            verification=VerificationConfig(),
                            error_handling=ErrorHandlingConfig(),
                        ),
                        "stock": TableConfig(
                            backup_frequency="daily",
                            priority=1,
                            verification=VerificationConfig(),
                            error_handling=ErrorHandlingConfig(),
                        ),
                    },
                ),
            },
        ),
        "analytics": CatalogConfig(
            schemas={
                "reports": SchemaConfig(
                    tables={
                        "reports": TableConfig(
                            backup_frequency="weekly",
                            priority=3,
                            verification=VerificationConfig(),
                            error_handling=ErrorHandlingConfig(),
                        ),
                    },
                ),
            },
        ),
    },
    backup_catalog="backup",
    backup_schema="dr",
)

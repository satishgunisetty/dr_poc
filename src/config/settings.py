"""Project configuration settings."""

import logging
import os
from dataclasses import dataclass
from typing import Literal


@dataclass
class Settings:
    """Project configuration settings."""

    app_name: str = "databricks-project"
    environment: Literal["development", "production"] = "development"

    def __post_init__(self) -> None:
        """Validate settings after initialization."""
        if self.environment not in ["development", "production"]:
            raise ValueError(
                f"Invalid environment: {self.environment}. "
                "Must be one of: development, production"
            )


# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", logging.INFO)
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Azure Storage configuration
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT", "your-storage-account")
CONTAINER_NAME = os.getenv("CONTAINER_NAME", "your-container")

# Databricks workspace settings
WORKSPACE_NAME = "your-workspace-name"
ENVIRONMENT = "development"  # or "production"

# Database settings
DATABASE_NAME = "your-database-name"
TABLE_PREFIX = "your_table_prefix"

# Job settings
JOB_TIMEOUT_MINUTES = 120
MAX_RETRIES = 3

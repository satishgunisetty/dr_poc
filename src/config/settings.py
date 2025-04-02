"""
Common configuration settings for the Databricks project.
"""

# Databricks workspace settings
WORKSPACE_NAME = "your-workspace-name"
ENVIRONMENT = "development"  # or "production"

# Storage settings
STORAGE_ACCOUNT = "your-storage-account"
CONTAINER_NAME = "your-container"

# Database settings
DATABASE_NAME = "your_database"
SCHEMA_NAME = "your_schema"

# Job settings
JOB_TIMEOUT_MINUTES = 120
MAX_RETRIES = 3

# Logging settings
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s" 
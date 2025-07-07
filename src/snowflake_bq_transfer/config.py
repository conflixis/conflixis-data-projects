"""Configuration management for Snowflake to BigQuery transfer."""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


class Config:
    """Configuration class for Snowflake to BigQuery transfer."""
    
    def __init__(self, env_path: Optional[Path] = None):
        """Initialize configuration from environment variables.
        
        Args:
            env_path: Optional path to .env file. If not provided, will look for
                     common/.env in the project root.
        """
        if env_path is None:
            # Find project root (where pyproject.toml is)
            current_path = Path(__file__).resolve()
            project_root = None
            for parent in current_path.parents:
                if (parent / "pyproject.toml").exists():
                    project_root = parent
                    break
            
            if project_root:
                env_path = project_root / "common" / ".env"
        
        if env_path and env_path.exists():
            load_dotenv(env_path)
        
        # Snowflake Configuration
        self.snowflake_user = os.getenv("SNOWFLAKE_USER")
        self.snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
        self.snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA")
        self.snowflake_pat = os.getenv("SNOWFLAKE_PAT")
        
        # Snowflake Staging Configuration
        self.snowflake_staging_database = os.getenv("SNOWFLAKE_STAGING_DATABASE", "CONFLIXIS_STAGE")
        self.snowflake_staging_schema = os.getenv("SNOWFLAKE_STAGING_SCHEMA", "PUBLIC")
        self.gcs_bucket = os.getenv("SNOWFLAKE_GCS_BUCKET", "snowflake_dh_bq")
        self.gcs_stage_name = os.getenv("SNOWFLAKE_GCS_STAGE_NAME", "snowflake_dh_bq")
        self.storage_integration = os.getenv("SNOWFLAKE_STORAGE_INTEGRATION", "GCS_INT")
        
        # BigQuery Configuration
        self.gcp_project_id = os.getenv("BQ_PROJECT_ID", os.getenv("GOOGLE_CLOUD_PROJECT"))
        self.bq_target_dataset = os.getenv("SNOWFLAKE_BQ_TARGET_DATASET", "CONFLIXIS_309340")
        self.google_application_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        
    def validate(self) -> None:
        """Validate that all required configuration values are present.
        
        Raises:
            ValueError: If any required configuration is missing.
        """
        required_fields = [
            ("SNOWFLAKE_USER", self.snowflake_user),
            ("SNOWFLAKE_ACCOUNT", self.snowflake_account),
            ("SNOWFLAKE_WAREHOUSE", self.snowflake_warehouse),
            ("SNOWFLAKE_DATABASE", self.snowflake_database),
            ("SNOWFLAKE_SCHEMA", self.snowflake_schema),
            ("SNOWFLAKE_PAT", self.snowflake_pat),
            ("GCP_PROJECT_ID", self.gcp_project_id),
            ("GOOGLE_APPLICATION_CREDENTIALS", self.google_application_credentials),
        ]
        
        missing_fields = []
        for field_name, field_value in required_fields:
            if not field_value:
                missing_fields.append(field_name)
        
        if missing_fields:
            raise ValueError(
                f"Missing required configuration fields: {', '.join(missing_fields)}. "
                "Please check your .env file."
            )
        
        # Validate Google credentials file exists
        if self.google_application_credentials:
            creds_path = Path(self.google_application_credentials)
            if not creds_path.exists():
                raise ValueError(
                    f"Google credentials file not found: {self.google_application_credentials}"
                )
    
    def get_snowflake_connection_params(self) -> dict:
        """Get Snowflake connection parameters.
        
        Returns:
            Dictionary with Snowflake connection parameters.
        """
        return {
            "user": self.snowflake_user,
            "password": self.snowflake_pat,
            "account": self.snowflake_account,
            "warehouse": self.snowflake_warehouse,
            "database": self.snowflake_database,
            "schema": self.snowflake_schema,
        }
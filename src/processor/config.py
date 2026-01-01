# =============================================================================
# Application Configuration
# =============================================================================
"""
Pydantic-based configuration using environment variables.
Supports both AWS and LocalStack environments.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    All settings can be overridden via environment variables.
    Prefix: none (direct mapping).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # -------------------------------------------------------------------------
    # Environment
    # -------------------------------------------------------------------------
    environment: Literal["local", "dev", "staging", "prod"] = Field(
        default="local",
        description="Deployment environment",
    )

    # -------------------------------------------------------------------------
    # AWS Configuration
    # -------------------------------------------------------------------------
    aws_region: str = Field(
        default="us-west-2",
        description="AWS region",
    )
    aws_endpoint_url: str | None = Field(
        default=None,
        description="Custom AWS endpoint URL (LocalStack)",
    )
    aws_access_key_id: str | None = Field(
        default=None,
        description="AWS access key (optional, uses default chain)",
    )
    aws_secret_access_key: str | None = Field(
        default=None,
        description="AWS secret key (optional, uses default chain)",
    )

    # -------------------------------------------------------------------------
    # Resource Configuration
    # -------------------------------------------------------------------------
    sqs_queue_url: str = Field(
        description="URL of the SQS processing queue",
    )
    sns_topic_arn: str = Field(
        description="ARN of the SNS file events topic",
    )
    s3_bucket_name: str = Field(
        description="Name of the S3 files bucket",
    )
    dynamodb_table_name: str = Field(
        description="Name of the DynamoDB metadata table",
    )
    kms_key_id: str = Field(
        description="KMS key ID/ARN/alias for encryption",
    )

    # -------------------------------------------------------------------------
    # SQS Consumer Settings
    # -------------------------------------------------------------------------
    sqs_max_messages: int = Field(
        default=10,
        ge=1,
        le=10,
        description="Max messages per SQS receive call",
    )
    sqs_wait_time_seconds: int = Field(
        default=20,
        ge=0,
        le=20,
        description="SQS long-polling wait time",
    )
    sqs_visibility_timeout: int = Field(
        default=300,
        ge=30,
        le=43200,
        description="SQS message visibility timeout",
    )

    # -------------------------------------------------------------------------
    # Processing Settings
    # -------------------------------------------------------------------------
    processing_max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Max retries for failed processing",
    )
    processing_retry_delay_seconds: int = Field(
        default=5,
        ge=1,
        le=300,
        description="Delay between retries",
    )
    max_file_size_bytes: int = Field(
        default=100 * 1024 * 1024,  # 100 MB
        description="Maximum allowed file size",
    )

    # -------------------------------------------------------------------------
    # Logging
    # -------------------------------------------------------------------------
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level",
    )
    log_format: Literal["json", "console"] = Field(
        default="json",
        description="Log output format",
    )

    # -------------------------------------------------------------------------
    # Validators
    # -------------------------------------------------------------------------
    @field_validator("environment", mode="before")
    @classmethod
    def lowercase_environment(cls, v: str) -> str:
        return v.lower() if isinstance(v, str) else v

    @field_validator("log_level", mode="before")
    @classmethod
    def uppercase_log_level(cls, v: str) -> str:
        return v.upper() if isinstance(v, str) else v

    # -------------------------------------------------------------------------
    # Computed Properties
    # -------------------------------------------------------------------------
    @property
    def is_local(self) -> bool:
        """Check if running in local environment."""
        return self.environment == "local"

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "prod"

    @property
    def use_json_logging(self) -> bool:
        """Check if JSON logging should be used."""
        return self.log_format == "json"


@lru_cache
def get_settings() -> Settings:
    """
    Get cached application settings.

    Settings are loaded once and cached for the lifetime of the application.
    """
    return Settings()

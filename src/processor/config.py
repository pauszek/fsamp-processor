"""
Pydantic-based configuration using environment variables.
Supports both AWS Lambda and LocalStack/ECS environments.
"""

from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

SUPPORTED_FIPS_ENDPOINT_REGION = "us-west-2"


def is_fips_endpoint_region(region: str) -> bool:
    """Return True when the project permits AWS FIPS endpoint usage in region."""
    return region == SUPPORTED_FIPS_ENDPOINT_REGION


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    All settings can be overridden via environment variables.
    Supports both Lambda and ECS/Container deployments.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )
    environment: Literal["local", "dev", "staging", "prod"] = Field(
        default="local",
        description="Deployment environment",
    )
    aws_lambda_function_name: str = Field(
        default="",
        alias="AWS_LAMBDA_FUNCTION_NAME",
        description="AWS Lambda runtime function name, when present",
        exclude=True,
    )
    is_lambda: bool = Field(
        default=False,
        description="Whether running as AWS Lambda (auto-detected)",
    )
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
    use_fips_endpoint: bool = Field(
        default=True,
        description="Use AWS FIPS endpoints in the supported us-west-2 deployment region",
    )

    fips_required: bool | None = Field(
        default=None,
        description="Require FIPS mode for crypto/TLS (defaults to non-local only)",
        alias="FIPS_REQUIRED",
    )
    sqs_queue_url: str = Field(
        default="",
        description="URL of the SQS processing queue",
    )
    sns_topic_arn: str = Field(
        default="",
        description="Fallback ARN of the SNS topic used for published outbox events",
    )
    file_events_topic_arn: str = Field(
        default="",
        description="ARN of the SNS file-events topic that feeds the processing queue",
    )
    processing_events_topic_arn: str = Field(
        default="",
        description="ARN of the SNS processing-events topic for processor result events",
    )
    s3_bucket_name: str = Field(
        default="",
        description="Name of the S3 files bucket",
    )
    dynamodb_table_name: str = Field(
        default="",
        description="Name of the DynamoDB metadata table",
    )
    outbox_table_name: str = Field(
        default="",
        description="Name of the DynamoDB outbox table (Outbox Pattern)",
    )
    kms_key_id: str = Field(
        default="",
        description="KMS key ID/ARN/alias for encryption",
    )
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
    processing_claim_ttl_seconds: int = Field(
        default=330,
        ge=30,
        le=7200,
        description="Token-fenced processing lease duration",
    )
    max_file_size_bytes: int = Field(
        default=100 * 1024 * 1024,  # 100 MB
        ge=1,
        le=100 * 1024 * 1024,
        description="Maximum allowed file size",
    )
    outbox_max_retry_count: int = Field(
        default=5,
        ge=1,
        le=100,
        alias="MAX_RETRY_COUNT",
        description="Maximum number of outbox publication attempts",
    )
    publish_claim_ttl_seconds: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Outbox publisher lease duration",
    )
    outbox_retention_seconds: int = Field(
        default=30 * 24 * 60 * 60,
        ge=24 * 60 * 60,
        le=365 * 24 * 60 * 60,
        description="Published outbox row retention period",
    )
    quarantine_prefix: str = Field(
        default="quarantine",
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9!_.*'()/-]+$",
        description="Same-bucket prefix for denied files",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level",
    )
    log_format: Literal["json", "console"] = Field(
        default="json",
        description="Log output format",
    )

    @field_validator("environment", mode="before")
    @classmethod
    def lowercase_environment(cls, v: str) -> str:
        return v.lower() if isinstance(v, str) else v

    @field_validator("log_level", mode="before")
    @classmethod
    def uppercase_log_level(cls, v: str) -> str:
        return v.upper() if isinstance(v, str) else v

    @model_validator(mode="after")
    def detect_lambda_runtime(self) -> Self:
        if self.aws_lambda_function_name:
            self.is_lambda = True
        return self

    @model_validator(mode="after")
    def validate_deployment_baseline(self) -> Self:
        if self.environment != "local":
            if self.aws_endpoint_url:
                raise ValueError("Custom AWS endpoints are allowed only in local mode")
            if self.aws_region != SUPPORTED_FIPS_ENDPOINT_REGION:
                raise ValueError(
                    f"Non-local deployments require region {SUPPORTED_FIPS_ENDPOINT_REGION}"
                )
            if not self.use_fips_endpoint:
                raise ValueError("Non-local deployments must enable AWS FIPS endpoints")
            if self.fips_required is False:
                raise ValueError("Non-local deployments cannot disable FIPS enforcement")
        return self

    def validate_processor_runtime(self) -> None:
        """Fail before wiring a processor with incomplete resource configuration."""
        required = {
            "S3_BUCKET_NAME": self.s3_bucket_name,
            "DYNAMODB_TABLE_NAME": self.dynamodb_table_name,
            "KMS_KEY_ID": self.kms_key_id,
        }
        if not self.is_lambda:
            required["SQS_QUEUE_URL"] = self.sqs_queue_url
        if self.environment != "local":
            required["OUTBOX_TABLE_NAME"] = self.outbox_table_name
        elif not self.outbox_table_name:
            required["SNS_TOPIC_ARN"] = self.sns_topic_arn
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(f"Missing required processor configuration: {', '.join(missing)}")

    def validate_outbox_runtime(self) -> None:
        """Fail before wiring a publisher without its table or result topic."""
        missing: list[str] = []
        if not self.outbox_table_name:
            missing.append("OUTBOX_TABLE_NAME")
        if not (
            self.sns_topic_arn or self.file_events_topic_arn or self.processing_events_topic_arn
        ):
            missing.append("SNS topic ARN")
        if missing:
            raise ValueError(f"Missing required outbox configuration: {', '.join(missing)}")

    @property
    def is_local(self) -> bool:
        """Check if running in local environment."""
        return self.environment == "local"

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "prod"

    @property
    def should_use_fips(self) -> bool:
        """
        Check if FIPS endpoints should be used.

        Real AWS deployments are pinned to the supported project baseline
        region and FIPS endpoints are never used for LocalStack/custom endpoints.
        """
        if self.is_local or self.aws_endpoint_url:
            return False
        if not is_fips_endpoint_region(self.aws_region):
            raise ValueError(
                "FSAMP active AWS deployments are pinned to the "
                f"{SUPPORTED_FIPS_ENDPOINT_REGION} FIPS endpoint baseline: {self.aws_region}"
            )
        return self.use_fips_endpoint

    @property
    def should_require_fips(self) -> bool:
        """
        Check if FIPS mode must be enforced for crypto/TLS.

        Defaults to true for non-local environments unless overridden.
        """
        if self.fips_required is not None:
            return self.fips_required
        return not self.is_local and not self.aws_endpoint_url

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

"""
Factory for creating AWS clients with proper configuration.
Supports both real AWS and LocalStack endpoints.

FIPS 140-3-oriented posture:
- Uses AWS FIPS endpoints in the us-west-2 deployment region
- Configurable via USE_FIPS_ENDPOINT environment variable
- Automatically disabled for LocalStack
"""

from functools import lru_cache
from typing import TYPE_CHECKING, Any

import boto3
import structlog
from botocore.config import Config

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_kms import KMSClient
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_sns import SNSClient
    from mypy_boto3_sqs import SQSClient

logger = structlog.get_logger(__name__)

DEFAULT_CONFIG = Config(
    retries={
        "max_attempts": 3,
        "mode": "adaptive",
    },
    connect_timeout=5,
    read_timeout=30,
)

FIPS_CONFIG = Config(
    retries={
        "max_attempts": 3,
        "mode": "adaptive",
    },
    connect_timeout=5,
    read_timeout=30,
    use_fips_endpoint=True,
)

SUPPORTED_FIPS_ENDPOINT_REGION = "us-west-2"


def is_fips_endpoint_region(region: str) -> bool:
    """Return True when the project permits AWS FIPS endpoint usage in region."""
    return region == SUPPORTED_FIPS_ENDPOINT_REGION


class AWSClientFactory:
    """Cached AWS clients with shared retry, timeout and endpoint settings.

    Real AWS clients stay in us-west-2 and use FIPS endpoints when use_fips is
    set; LocalStack endpoints never do.
    """

    def __init__(
        self,
        region: str = "us-west-2",
        endpoint_url: str | None = None,
        config: Config | None = None,
        use_fips: bool = False,
    ) -> None:
        """
        Initialize AWS Client Factory.

        Args:
            region: AWS region.
            endpoint_url: Custom endpoint URL (e.g., LocalStack).
            config: Custom botocore Config.
            use_fips: Whether to use AWS FIPS endpoints.
        """
        self._region = region
        self._endpoint_url = endpoint_url
        self._is_local = endpoint_url is not None

        if not self._is_local and not is_fips_endpoint_region(region):
            raise ValueError(
                "FSAMP active AWS deployments are pinned to the "
                f"{SUPPORTED_FIPS_ENDPOINT_REGION} FIPS endpoint baseline: {region}"
            )

        self._use_fips = use_fips and not self._is_local

        if config:
            self._config = config
        elif self._use_fips:
            self._config = FIPS_CONFIG
        else:
            self._config = DEFAULT_CONFIG

        logger.info(
            "AWS Client Factory initialized",
            region=region,
            endpoint_url=endpoint_url or "AWS (default)",
            is_local=self._is_local,
            fips_enabled=self._use_fips,
        )

    def _get_client_kwargs(self) -> dict[str, Any]:
        """Get common kwargs for boto3 client creation."""
        kwargs: dict[str, Any] = {
            "region_name": self._region,
            "config": self._config,
        }

        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url

        return kwargs

    @lru_cache(maxsize=1)
    def get_s3_client(self) -> S3Client:
        """Get cached S3 client."""
        logger.debug("Creating S3 client")
        return boto3.client("s3", **self._get_client_kwargs())

    @lru_cache(maxsize=1)
    def get_sqs_client(self) -> SQSClient:
        """Get cached SQS client."""
        logger.debug("Creating SQS client")
        return boto3.client("sqs", **self._get_client_kwargs())

    @lru_cache(maxsize=1)
    def get_sns_client(self) -> SNSClient:
        """Get cached SNS client."""
        logger.debug("Creating SNS client")
        return boto3.client("sns", **self._get_client_kwargs())

    @lru_cache(maxsize=1)
    def get_dynamodb_client(self) -> DynamoDBClient:
        """Get cached DynamoDB client."""
        logger.debug("Creating DynamoDB client")
        return boto3.client("dynamodb", **self._get_client_kwargs())

    @lru_cache(maxsize=1)
    def get_kms_client(self) -> KMSClient:
        """Get cached KMS client."""
        logger.debug("Creating KMS client")
        return boto3.client("kms", **self._get_client_kwargs())

    def clear_cache(self) -> None:
        """Clear all cached clients."""
        self.get_s3_client.cache_clear()
        self.get_sqs_client.cache_clear()
        self.get_sns_client.cache_clear()
        self.get_dynamodb_client.cache_clear()
        self.get_kms_client.cache_clear()
        logger.debug("Client cache cleared")

    @property
    def is_local(self) -> bool:
        """Check if using LocalStack endpoint."""
        return self._is_local

    def verify_connectivity(self) -> dict[str, bool]:
        """
        Verify connectivity to all AWS services.

        Returns:
            Dict mapping service name to connectivity status.
        """
        results: dict[str, bool] = {}

        try:
            self.get_s3_client().list_buckets()
            results["s3"] = True
        except Exception as e:
            logger.warning("S3 connectivity check failed", error=str(e))
            results["s3"] = False

        try:
            self.get_sqs_client().list_queues()
            results["sqs"] = True
        except Exception as e:
            logger.warning("SQS connectivity check failed", error=str(e))
            results["sqs"] = False

        try:
            self.get_sns_client().list_topics()
            results["sns"] = True
        except Exception as e:
            logger.warning("SNS connectivity check failed", error=str(e))
            results["sns"] = False

        try:
            self.get_dynamodb_client().list_tables()
            results["dynamodb"] = True
        except Exception as e:
            logger.warning("DynamoDB connectivity check failed", error=str(e))
            results["dynamodb"] = False

        try:
            self.get_kms_client().list_keys()
            results["kms"] = True
        except Exception as e:
            logger.warning("KMS connectivity check failed", error=str(e))
            results["kms"] = False

        logger.info("AWS connectivity verified", results=results)
        return results

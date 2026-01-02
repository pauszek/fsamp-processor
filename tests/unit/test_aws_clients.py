# =============================================================================
# Unit Tests for AWS Client Factory
# =============================================================================
"""Tests for AWS Client Factory."""

from unittest.mock import MagicMock, patch

import pytest

from processor.infrastructure.aws_clients import (
    DEFAULT_CONFIG,
    FIPS_CONFIG,
    AWSClientFactory,
)


class TestAWSClientFactoryInit:
    """Tests for AWSClientFactory initialization."""

    def test_init_default(self) -> None:
        """Test initialization with defaults."""
        factory = AWSClientFactory()

        assert factory._region == "us-west-2"
        assert factory._endpoint_url is None
        assert factory._is_local is False
        assert factory._use_fips is False
        assert factory._config is DEFAULT_CONFIG

    def test_init_with_endpoint_url(self) -> None:
        """Test initialization with endpoint URL (LocalStack)."""
        factory = AWSClientFactory(
            region="us-east-1",
            endpoint_url="http://localhost:4566",
        )

        assert factory._endpoint_url == "http://localhost:4566"
        assert factory._is_local is True
        assert factory._use_fips is False  # FIPS disabled for local

    def test_init_with_fips_us_region(self) -> None:
        """Test initialization with FIPS in us-* region."""
        factory = AWSClientFactory(
            region="us-west-2",
            use_fips=True,
        )

        assert factory._use_fips is True
        assert factory._config is FIPS_CONFIG

    def test_init_with_fips_non_us_region(self) -> None:
        """Test FIPS disabled for non-us regions."""
        factory = AWSClientFactory(
            region="eu-west-1",
            use_fips=True,
        )

        assert factory._use_fips is False  # FIPS only for us-* regions

    def test_init_with_fips_local_disabled(self) -> None:
        """Test FIPS disabled for local endpoint."""
        factory = AWSClientFactory(
            region="us-west-2",
            endpoint_url="http://localhost:4566",
            use_fips=True,
        )

        assert factory._use_fips is False  # FIPS disabled for local

    def test_init_with_custom_config(self) -> None:
        """Test initialization with custom config."""
        from botocore.config import Config

        custom_config = Config(retries={"max_attempts": 5})

        factory = AWSClientFactory(config=custom_config)

        assert factory._config is custom_config


class TestAWSClientFactoryGetClientKwargs:
    """Tests for _get_client_kwargs method."""

    def test_get_client_kwargs_default(self) -> None:
        """Test default client kwargs."""
        factory = AWSClientFactory(region="us-east-1")

        kwargs = factory._get_client_kwargs()

        assert kwargs["region_name"] == "us-east-1"
        assert kwargs["config"] is DEFAULT_CONFIG
        assert "endpoint_url" not in kwargs

    def test_get_client_kwargs_with_endpoint(self) -> None:
        """Test client kwargs with endpoint URL."""
        factory = AWSClientFactory(
            region="us-west-2",
            endpoint_url="http://localhost:4566",
        )

        kwargs = factory._get_client_kwargs()

        assert kwargs["endpoint_url"] == "http://localhost:4566"


class TestAWSClientFactoryClients:
    """Tests for client getters."""

    @pytest.fixture
    def factory(self) -> AWSClientFactory:
        """Create factory for testing."""
        return AWSClientFactory()

    def test_get_s3_client(self, factory: AWSClientFactory) -> None:
        """Test S3 client creation."""
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_s3_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "s3"

    def test_get_s3_client_cached(self, factory: AWSClientFactory) -> None:
        """Test S3 client is cached."""
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client1 = factory.get_s3_client()
            client2 = factory.get_s3_client()

            assert client1 is client2
            assert mock_client.call_count == 1

    def test_get_sqs_client(self, factory: AWSClientFactory) -> None:
        """Test SQS client creation."""
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_sqs_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "sqs"

    def test_get_sns_client(self, factory: AWSClientFactory) -> None:
        """Test SNS client creation."""
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_sns_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "sns"

    def test_get_dynamodb_client(self, factory: AWSClientFactory) -> None:
        """Test DynamoDB client creation."""
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_dynamodb_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "dynamodb"

    def test_get_kms_client(self, factory: AWSClientFactory) -> None:
        """Test KMS client creation."""
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_kms_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "kms"


class TestAWSClientFactoryClearCache:
    """Tests for clear_cache method."""

    def test_clear_cache(self) -> None:
        """Test cache clearing."""
        factory = AWSClientFactory()

        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            # Create clients
            factory.get_s3_client()
            factory.get_sqs_client()

            # Clear cache
            factory.clear_cache()

            # Create again - should call boto3.client again
            factory.get_s3_client()
            factory.get_sqs_client()

            # Should have been called 4 times (2 before clear, 2 after)
            assert mock_client.call_count == 4


class TestAWSClientFactoryIsLocal:
    """Tests for is_local property."""

    def test_is_local_true(self) -> None:
        """Test is_local returns True for local endpoint."""
        factory = AWSClientFactory(endpoint_url="http://localhost:4566")
        assert factory.is_local is True

    def test_is_local_false(self) -> None:
        """Test is_local returns False for AWS."""
        factory = AWSClientFactory()
        assert factory.is_local is False


class TestAWSClientFactoryVerifyConnectivity:
    """Tests for verify_connectivity method."""

    def test_verify_connectivity_all_success(self) -> None:
        """Test connectivity verification when all services succeed."""
        factory = AWSClientFactory()

        with patch("boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_sqs = MagicMock()
            mock_sns = MagicMock()
            mock_dynamodb = MagicMock()
            mock_kms = MagicMock()

            def get_mock_client(service, **kwargs):
                return {
                    "s3": mock_s3,
                    "sqs": mock_sqs,
                    "sns": mock_sns,
                    "dynamodb": mock_dynamodb,
                    "kms": mock_kms,
                }[service]

            mock_client.side_effect = get_mock_client

            results = factory.verify_connectivity()

            assert results["s3"] is True
            assert results["sqs"] is True
            assert results["sns"] is True
            assert results["dynamodb"] is True
            assert results["kms"] is True

    def test_verify_connectivity_s3_failure(self) -> None:
        """Test connectivity verification when S3 fails."""
        factory = AWSClientFactory()

        with patch("boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_s3.list_buckets.side_effect = Exception("Connection failed")

            mock_sqs = MagicMock()
            mock_sns = MagicMock()
            mock_dynamodb = MagicMock()
            mock_kms = MagicMock()

            def get_mock_client(service, **kwargs):
                return {
                    "s3": mock_s3,
                    "sqs": mock_sqs,
                    "sns": mock_sns,
                    "dynamodb": mock_dynamodb,
                    "kms": mock_kms,
                }[service]

            mock_client.side_effect = get_mock_client

            results = factory.verify_connectivity()

            assert results["s3"] is False
            assert results["sqs"] is True

    def test_verify_connectivity_multiple_failures(self) -> None:
        """Test connectivity verification with multiple failures."""
        factory = AWSClientFactory()

        with patch("boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_s3.list_buckets.side_effect = Exception("Failed")

            mock_sqs = MagicMock()
            mock_sqs.list_queues.side_effect = Exception("Failed")

            mock_sns = MagicMock()
            mock_dynamodb = MagicMock()
            mock_kms = MagicMock()

            def get_mock_client(service, **kwargs):
                return {
                    "s3": mock_s3,
                    "sqs": mock_sqs,
                    "sns": mock_sns,
                    "dynamodb": mock_dynamodb,
                    "kms": mock_kms,
                }[service]

            mock_client.side_effect = get_mock_client

            results = factory.verify_connectivity()

            assert results["s3"] is False
            assert results["sqs"] is False
            assert results["sns"] is True
            assert results["dynamodb"] is True
            assert results["kms"] is True

from unittest.mock import MagicMock, patch

import pytest
from botocore.config import Config

from processor.infrastructure.aws_clients import (
    DEFAULT_CONFIG,
    FIPS_CONFIG,
    AWSClientFactory,
)


class TestAWSClientFactoryInit:
    def test_init_default(self) -> None:
        factory = AWSClientFactory()

        assert factory._region == "us-west-2"
        assert factory._endpoint_url is None
        assert factory._is_local is False
        assert factory._use_fips is False
        assert factory._config is DEFAULT_CONFIG

    def test_init_with_endpoint_url(self) -> None:
        factory = AWSClientFactory(
            region="us-east-1",
            endpoint_url="http://localhost:4566",
        )

        assert factory._endpoint_url == "http://localhost:4566"
        assert factory._is_local is True
        assert factory._use_fips is False  # FIPS disabled for local

    def test_init_with_fips_us_west_2_region(self) -> None:
        factory = AWSClientFactory(
            region="us-west-2",
            use_fips=True,
        )

        assert factory._use_fips is True
        assert factory._config is FIPS_CONFIG

    @pytest.mark.parametrize("region", ["us-east-1", "eu-west-1"])
    def test_init_with_fips_unsupported_region_fails_closed(self, region: str) -> None:
        with pytest.raises(ValueError, match="FIPS endpoints requested"):
            AWSClientFactory(
                region=region,
                use_fips=True,
            )

    def test_init_with_fips_disabled_allows_unsupported_region(self) -> None:
        factory = AWSClientFactory(
            region="eu-west-1",
            use_fips=False,
        )

        assert factory._use_fips is False

    def test_init_with_fips_local_disabled(self) -> None:
        factory = AWSClientFactory(
            region="us-west-2",
            endpoint_url="http://localhost:4566",
            use_fips=True,
        )

        assert factory._use_fips is False  # FIPS disabled for local

    def test_init_with_custom_config(self) -> None:
        custom_config = Config(retries={"max_attempts": 5})

        factory = AWSClientFactory(config=custom_config)

        assert factory._config is custom_config


class TestAWSClientFactoryGetClientKwargs:
    def test_get_client_kwargs_default(self) -> None:
        factory = AWSClientFactory(region="us-east-1")

        kwargs = factory._get_client_kwargs()

        assert kwargs["region_name"] == "us-east-1"
        assert kwargs["config"] is DEFAULT_CONFIG
        assert "endpoint_url" not in kwargs

    def test_get_client_kwargs_with_endpoint(self) -> None:
        factory = AWSClientFactory(
            region="us-west-2",
            endpoint_url="http://localhost:4566",
        )

        kwargs = factory._get_client_kwargs()

        assert kwargs["endpoint_url"] == "http://localhost:4566"


class TestAWSClientFactoryClients:
    @pytest.fixture
    def factory(self) -> AWSClientFactory:
        return AWSClientFactory()

    def test_get_s3_client(self, factory: AWSClientFactory) -> None:
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_s3_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "s3"

    def test_get_s3_client_cached(self, factory: AWSClientFactory) -> None:
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client1 = factory.get_s3_client()
            client2 = factory.get_s3_client()

            assert client1 is client2
            assert mock_client.call_count == 1

    def test_get_sqs_client(self, factory: AWSClientFactory) -> None:
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_sqs_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "sqs"

    def test_get_sns_client(self, factory: AWSClientFactory) -> None:
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_sns_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "sns"

    def test_get_dynamodb_client(self, factory: AWSClientFactory) -> None:
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_dynamodb_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "dynamodb"

    def test_get_kms_client(self, factory: AWSClientFactory) -> None:
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            client = factory.get_kms_client()

            mock_client.assert_called_once()
            call_args = mock_client.call_args
            assert call_args[0][0] == "kms"


class TestAWSClientFactoryClearCache:
    def test_clear_cache(self) -> None:
        factory = AWSClientFactory()

        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()

            factory.get_s3_client()
            factory.get_sqs_client()

            factory.clear_cache()

            factory.get_s3_client()
            factory.get_sqs_client()

            assert mock_client.call_count == 4


class TestAWSClientFactoryIsLocal:
    def test_is_local_true(self) -> None:
        factory = AWSClientFactory(endpoint_url="http://localhost:4566")
        assert factory.is_local is True

    def test_is_local_false(self) -> None:
        factory = AWSClientFactory()
        assert factory.is_local is False


class TestAWSClientFactoryVerifyConnectivity:
    def test_verify_connectivity_all_success(self) -> None:
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

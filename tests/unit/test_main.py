import os
from unittest.mock import MagicMock, patch

import pytest

from processor.config import Settings
from processor.main import create_application, main


class TestCreateApplication:
    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        os.environ["AWS_REGION"] = "us-west-2"
        os.environ["SQS_QUEUE_URL"] = "http://localhost:4566/queue/test"
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test"
        os.environ["DYNAMODB_TABLE_NAME"] = "test-table"
        os.environ["KMS_KEY_ID"] = "test-key-id"
        os.environ["S3_BUCKET_NAME"] = "test-bucket"
        os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"
        yield
        for key in [
            "AWS_REGION",
            "SQS_QUEUE_URL",
            "SNS_TOPIC_ARN",
            "DYNAMODB_TABLE_NAME",
            "KMS_KEY_ID",
            "S3_BUCKET_NAME",
            "AWS_ENDPOINT_URL",
        ]:
            os.environ.pop(key, None)

    @patch("processor.main.AWSClientFactory")
    @patch("processor.main.KMSCryptoProvider")
    @patch("processor.main.S3FileStorage")
    @patch("processor.main.DynamoDBMetadataRepository")
    @patch("processor.main.DynamoDBOutboxRepository")
    @patch("processor.main.SNSEventPublisher")
    @patch("processor.main.FileProcessorService")
    @patch("processor.main.SQSConsumer")
    def test_create_application_success(
        self,
        mock_consumer,
        mock_processor,
        mock_publisher,
        mock_outbox_repo,
        mock_repo,
        mock_storage,
        mock_crypto,
        mock_factory,
    ) -> None:
        mock_factory_instance = MagicMock()
        mock_factory_instance.verify_connectivity.return_value = {
            "s3": True,
            "sqs": True,
            "sns": True,
            "dynamodb": True,
            "kms": True,
        }
        mock_factory.return_value = mock_factory_instance

        mock_crypto_instance = MagicMock()
        mock_crypto_instance.verify_key_access.return_value = True
        mock_crypto.return_value = mock_crypto_instance

        settings = Settings(
            aws_region="us-west-2",
            sqs_queue_url="http://queue",
            sns_topic_arn="arn:aws:sns:test",
            dynamodb_table_name="test-table",
            kms_key_id="test-key",
        )

        consumer = create_application(settings)

        mock_factory.assert_called_once()
        mock_crypto.assert_called_once()
        mock_outbox_repo.assert_not_called()
        mock_consumer.assert_called_once()

    @patch("processor.main.AWSClientFactory")
    @patch("processor.main.KMSCryptoProvider")
    @patch("processor.main.S3FileStorage")
    @patch("processor.main.DynamoDBMetadataRepository")
    @patch("processor.main.DynamoDBOutboxRepository")
    @patch("processor.main.SNSEventPublisher")
    @patch("processor.main.FileProcessorService")
    @patch("processor.main.SQSConsumer")
    def test_create_application_with_outbox(
        self,
        mock_consumer,
        mock_processor,
        mock_publisher,
        mock_outbox_repo,
        mock_repo,
        mock_storage,
        mock_crypto,
        mock_factory,
    ) -> None:
        mock_factory_instance = MagicMock()
        mock_factory_instance.verify_connectivity.return_value = {
            "s3": True,
            "sqs": True,
            "sns": True,
            "dynamodb": True,
            "kms": True,
        }
        mock_factory.return_value = mock_factory_instance

        mock_crypto_instance = MagicMock()
        mock_crypto_instance.verify_key_access.return_value = True
        mock_crypto.return_value = mock_crypto_instance

        settings = Settings(
            aws_region="us-west-2",
            sqs_queue_url="http://queue",
            sns_topic_arn="arn:aws:sns:test",
            dynamodb_table_name="test-table",
            outbox_table_name="test-outbox",
            kms_key_id="test-key",
        )

        create_application(settings)

        dynamodb_client = mock_factory_instance.get_dynamodb_client.return_value
        mock_outbox_repo.assert_called_once_with(
            dynamodb_client=dynamodb_client,
            metadata_table_name="test-table",
            outbox_table_name="test-outbox",
            retention_seconds=settings.outbox_retention_seconds,
        )
        mock_processor.assert_called_once()
        assert mock_processor.call_args.kwargs["outbox_repo"] == mock_outbox_repo.return_value
        assert mock_processor.call_args.kwargs["use_outbox_pattern"] is True

    @patch("processor.main.AWSClientFactory")
    def test_create_application_connectivity_failure(
        self,
        mock_factory,
    ) -> None:
        mock_factory_instance = MagicMock()
        mock_factory_instance.verify_connectivity.return_value = {
            "s3": False,
            "sqs": True,
            "sns": True,
            "dynamodb": True,
            "kms": True,
        }
        mock_factory.return_value = mock_factory_instance

        settings = Settings(
            aws_region="us-west-2",
            sqs_queue_url="http://queue",
            sns_topic_arn="arn:aws:sns:test",
            dynamodb_table_name="test-table",
            kms_key_id="test-key",
        )

        with pytest.raises(RuntimeError) as exc_info:
            create_application(settings)

        assert "Failed to connect to AWS services" in str(exc_info.value)

    @patch("processor.main.AWSClientFactory")
    @patch("processor.main.KMSCryptoProvider")
    @patch("processor.main.S3FileStorage")
    @patch("processor.main.DynamoDBMetadataRepository")
    @patch("processor.main.SNSEventPublisher")
    def test_create_application_kms_key_access_failure(
        self,
        mock_publisher,
        mock_repo,
        mock_storage,
        mock_crypto,
        mock_factory,
    ) -> None:
        mock_factory_instance = MagicMock()
        mock_factory_instance.verify_connectivity.return_value = {
            "s3": True,
            "sqs": True,
            "sns": True,
            "dynamodb": True,
            "kms": True,
        }
        mock_factory.return_value = mock_factory_instance

        mock_crypto_instance = MagicMock()
        mock_crypto_instance.verify_key_access.return_value = False
        mock_crypto.return_value = mock_crypto_instance

        settings = Settings(
            aws_region="us-west-2",
            sqs_queue_url="http://queue",
            sns_topic_arn="arn:aws:sns:test",
            dynamodb_table_name="test-table",
            kms_key_id="test-key",
        )

        with pytest.raises(RuntimeError) as exc_info:
            create_application(settings)

        assert "Cannot access KMS key" in str(exc_info.value)


class TestMain:
    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        os.environ["AWS_REGION"] = "us-west-2"
        os.environ["SQS_QUEUE_URL"] = "http://localhost:4566/queue/test"
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test"
        os.environ["DYNAMODB_TABLE_NAME"] = "test-table"
        os.environ["KMS_KEY_ID"] = "test-key-id"
        os.environ["S3_BUCKET_NAME"] = "test-bucket"
        os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"
        os.environ["LOG_LEVEL"] = "INFO"
        yield
        for key in [
            "AWS_REGION",
            "SQS_QUEUE_URL",
            "SNS_TOPIC_ARN",
            "DYNAMODB_TABLE_NAME",
            "KMS_KEY_ID",
            "S3_BUCKET_NAME",
            "AWS_ENDPOINT_URL",
            "LOG_LEVEL",
        ]:
            os.environ.pop(key, None)

    @patch("processor.main.create_application")
    @patch("processor.main.get_settings")
    @patch("processor.main.configure_logging")
    def test_main_success(
        self,
        mock_logging,
        mock_settings,
        mock_create_app,
    ) -> None:
        mock_consumer = MagicMock()
        mock_create_app.return_value = mock_consumer

        mock_settings.return_value = Settings(
            aws_region="us-west-2",
            sqs_queue_url="http://queue",
            sns_topic_arn="arn:aws:sns:test",
            dynamodb_table_name="test-table",
            kms_key_id="test-key",
        )

        result = main()

        assert result == 0
        mock_consumer.start_blocking.assert_called_once()

    @patch("processor.main.get_settings")
    def test_main_keyboard_interrupt(
        self,
        mock_settings,
    ) -> None:
        mock_settings.side_effect = KeyboardInterrupt()

        result = main()

        assert result == 0

    @patch("processor.main.get_settings")
    def test_main_exception(
        self,
        mock_settings,
    ) -> None:
        mock_settings.side_effect = Exception("Test error")

        result = main()

        assert result == 1

    @patch("processor.main.create_application")
    @patch("processor.main.get_settings")
    @patch("processor.main.configure_logging")
    def test_main_create_app_failure(
        self,
        mock_logging,
        mock_settings,
        mock_create_app,
    ) -> None:
        mock_create_app.side_effect = RuntimeError("Failed to create app")

        mock_settings.return_value = Settings(
            aws_region="us-west-2",
            sqs_queue_url="http://queue",
            sns_topic_arn="arn:aws:sns:test",
            dynamodb_table_name="test-table",
            kms_key_id="test-key",
        )

        result = main()

        assert result == 1

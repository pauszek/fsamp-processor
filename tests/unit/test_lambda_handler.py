import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from aws_lambda_powertools.utilities.batch import EventType as BatchEventType

import processor.lambda_handler as handler_module
from processor.domain.events import FileEvent

VALID_KMS_KEY_ID = "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"


class TestGetFileProcessor:
    def test_get_file_processor_creates_service(self) -> None:
        handler_module._file_processor = None
        handler_module._settings = None

        mock_settings = MagicMock()
        mock_settings.aws_region = "us-west-2"
        mock_settings.aws_endpoint_url = None
        mock_settings.should_use_fips = False
        mock_settings.should_require_fips = False
        mock_settings.kms_key_id = VALID_KMS_KEY_ID
        mock_settings.dynamodb_table_name = "test-table"
        mock_settings.sns_topic_arn = "arn:aws:sns:us-west-2:123456789012:test"
        mock_settings.outbox_table_name = None
        mock_settings.max_file_size_bytes = 100 * 1024 * 1024
        mock_settings.processing_claim_ttl_seconds = 330

        with (
            patch.object(handler_module, "get_settings", return_value=mock_settings),
            patch.object(handler_module, "AWSClientFactory") as mock_factory_class,
            patch.object(handler_module, "S3FileStorage"),
            patch.object(handler_module, "DynamoDBMetadataRepository"),
            patch.object(handler_module, "SNSEventPublisher"),
            patch.object(handler_module, "KMSCryptoProvider"),
            patch.object(handler_module, "FileProcessorService") as mock_service_class,
        ):
            mock_factory = MagicMock()
            mock_factory_class.return_value = mock_factory
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            result = handler_module.get_file_processor()

            assert result == mock_service
            mock_factory_class.assert_called_once()
            mock_service_class.assert_called_once()
            assert mock_service_class.call_args.kwargs["processing_claim_ttl_seconds"] == 330

    def test_get_file_processor_returns_cached(self) -> None:
        mock_service = MagicMock()
        handler_module._file_processor = mock_service

        result = handler_module.get_file_processor()

        assert result == mock_service
        handler_module._file_processor = None

    def test_get_file_processor_with_outbox(self) -> None:
        handler_module._file_processor = None
        handler_module._settings = None

        mock_settings = MagicMock()
        mock_settings.aws_region = "us-west-2"
        mock_settings.aws_endpoint_url = None
        mock_settings.should_use_fips = False
        mock_settings.should_require_fips = False
        mock_settings.kms_key_id = VALID_KMS_KEY_ID
        mock_settings.dynamodb_table_name = "test-table"
        mock_settings.sns_topic_arn = "arn:aws:sns:us-west-2:123456789012:test"
        mock_settings.outbox_table_name = "outbox-table"
        mock_settings.max_file_size_bytes = 100 * 1024 * 1024
        mock_settings.processing_claim_ttl_seconds = 330

        with (
            patch.object(handler_module, "get_settings", return_value=mock_settings),
            patch.object(handler_module, "AWSClientFactory") as mock_factory_class,
            patch.object(handler_module, "S3FileStorage"),
            patch.object(handler_module, "DynamoDBMetadataRepository"),
            patch.object(handler_module, "SNSEventPublisher"),
            patch.object(handler_module, "KMSCryptoProvider"),
            patch.object(handler_module, "DynamoDBOutboxRepository") as mock_outbox_class,
            patch.object(handler_module, "FileProcessorService") as mock_service_class,
        ):
            mock_factory = MagicMock()
            mock_factory_class.return_value = mock_factory
            mock_outbox = MagicMock()
            mock_outbox_class.return_value = mock_outbox
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            result = handler_module.get_file_processor()

            assert result == mock_service
            mock_outbox_class.assert_called_once()
            assert mock_service_class.call_args.kwargs["processing_claim_ttl_seconds"] == 330


class TestRecordHandlerLogic:
    @pytest.fixture
    def sample_file_event_dict(self) -> dict:
        return {
            "schema_version": "1.2.0",
            "file_id": str(uuid4()),
            "event_id": str(uuid4()),
            "correlation_id": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
            "source": "fsamp-gateway",
            "event_type": "FILE_UPLOADED",
            "file_metadata": {
                "original_filename": "test.pdf",
                "file_size_bytes": 1024,
                "mime_type": "application/pdf",
                "checksum_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            },
            "storage_location": {
                "bucket_name": "test-bucket",
                "object_key": "uploads/test.pdf",
            },
            "security_context": {
                "is_encrypted": True,
                "encryption_algorithm": "AES/GCM/NoPadding",
                "kms_key_id": VALID_KMS_KEY_ID,
            },
        }

    def test_json_parsing_direct_event(self, sample_file_event_dict: dict) -> None:
        body = json.dumps(sample_file_event_dict)
        data = json.loads(body)

        assert "Message" not in data
        assert "TopicArn" not in data

    def test_json_parsing_sns_wrapped(self, sample_file_event_dict: dict) -> None:
        sns_message = {
            "Type": "Notification",
            "TopicArn": "arn:aws:sns:us-west-2:123456789012:test",
            "Message": json.dumps(sample_file_event_dict),
        }
        body = json.dumps(sns_message)
        data = json.loads(body)

        assert "Message" in data
        assert "TopicArn" in data

        event_data = json.loads(data["Message"])
        assert event_data["event_type"] == "FILE_UPLOADED"

    def test_file_event_validation(self, sample_file_event_dict: dict) -> None:
        event = FileEvent.model_validate(sample_file_event_dict)

        assert event.event_type.value == "FILE_UPLOADED"
        assert event.file_metadata.original_filename == "test.pdf"


class TestLambdaHandlerConstants:
    def test_logger_service_name(self) -> None:
        assert handler_module.logger.service == "fsamp-processor"

    def test_processor_event_type(self) -> None:
        assert handler_module.processor.event_type == BatchEventType.SQS

    def test_global_state_initially_none(self) -> None:
        original_processor = handler_module._file_processor
        original_settings = handler_module._settings

        handler_module._file_processor = None
        handler_module._settings = None

        assert handler_module._file_processor is None
        assert handler_module._settings is None

        handler_module._file_processor = original_processor
        handler_module._settings = original_settings

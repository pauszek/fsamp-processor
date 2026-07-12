import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

import processor.lambda_handler as handler_module
from processor.domain.events import SCHEMA_VERSION, EventType, FileEvent
from processor.domain.exceptions import NonRetryableError, ProcessingError
from processor.domain.models import ProcessingResult, ProcessingStatus
from processor.lambda_handler import lambda_handler, record_handler

SAMPLE_CHECKSUM_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
SAMPLE_KMS_ARN = "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"


class MockContext:
    function_name = "test-processor"
    memory_limit_in_mb = 512
    invoked_function_arn = "arn:aws:lambda:us-west-2:123456789:function:test-processor"
    aws_request_id = "test-request-id-12345"
    log_group_name = "/aws/lambda/test-processor"
    log_stream_name = "2024/01/15/[$LATEST]abcd1234"

    def get_remaining_time_in_millis(self) -> int:
        return 300000


def create_sqs_event(file_events: list[dict[str, Any]]) -> dict[str, Any]:
    records = []
    for i, event_data in enumerate(file_events):
        records.append(
            {
                "messageId": f"msg-{i}",
                "receiptHandle": f"receipt-{i}",
                "body": json.dumps(event_data),
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1705316400000",
                },
                "messageAttributes": {},
                "md5OfBody": "test",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-west-2:123456789012:test-queue",
                "awsRegion": "us-west-2",
            }
        )

    return {"Records": records}


def create_file_event_dict(
    event_id: str | None = None,
    correlation_id: str | None = None,
    event_type: str = "FILE_UPLOADED",
    filename: str = "test.pdf",
) -> dict[str, Any]:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "fileId": str(event_id or uuid4()),
        "eventId": str(event_id or uuid4()),
        "correlationId": str(correlation_id or uuid4()),
        "timestamp": datetime.now(UTC).isoformat(),
        "source": "fsamp-gateway",
        "eventType": event_type,
        "fileMetadata": {
            "originalFilename": filename,
            "fileSizeBytes": 1024,
            "mimeType": "application/pdf",
            "checksumSHA256": SAMPLE_CHECKSUM_SHA256,
        },
        "storageLocation": {
            "bucketName": "fsamp-test-bucket",
            "objectKey": f"uploads/{filename}",
        },
        "securityContext": {
            "isEncrypted": True,
            "encryptionAlgorithm": "AES/GCM/NoPadding",
            "kmsKeyId": SAMPLE_KMS_ARN,
        },
    }


class TestLambdaHandler:
    @pytest.fixture
    def mock_processor(self):
        processor = MagicMock()
        processor.handle.return_value = ProcessingResult(
            event_id=str(uuid4()),
            correlation_id=str(uuid4()),
            status=ProcessingStatus.COMPLETED,
            started_at=datetime.now(UTC),
        )
        return processor

    @pytest.fixture
    def mock_settings(self):
        settings = MagicMock()
        settings.aws_region = "us-west-2"
        settings.aws_endpoint_url = None
        settings.kms_key_id = "alias/test-key"
        settings.dynamodb_table_name = "test-table"
        settings.sns_topic_arn = "arn:aws:sns:us-west-2:123456789:test-topic"
        settings.max_file_size_bytes = 100 * 1024 * 1024
        return settings

    @patch("processor.lambda_handler.get_file_processor")
    def test_handler_processes_single_message(self, mock_get_processor, mock_processor):
        mock_get_processor.return_value = mock_processor

        event = create_sqs_event([create_file_event_dict()])
        context = MockContext()

        result = lambda_handler(event, context)

        assert "batchItemFailures" in result
        assert len(result["batchItemFailures"]) == 0

        mock_processor.handle.assert_called_once()

    @patch("processor.lambda_handler.get_file_processor")
    def test_handler_processes_batch(self, mock_get_processor, mock_processor):
        mock_get_processor.return_value = mock_processor

        events = [create_file_event_dict(filename=f"file{i}.pdf") for i in range(3)]
        event = create_sqs_event(events)
        context = MockContext()

        result = lambda_handler(event, context)

        assert mock_processor.handle.call_count == 3
        assert len(result["batchItemFailures"]) == 0

    @patch("processor.lambda_handler.get_file_processor")
    def test_handler_reports_partial_batch_failures(self, mock_get_processor, mock_processor):
        mock_processor.handle.side_effect = [
            ProcessingResult(
                event_id=str(uuid4()),
                correlation_id=str(uuid4()),
                status=ProcessingStatus.COMPLETED,
                started_at=datetime.now(UTC),
            ),
            ProcessingError(
                message="Test error",
                event_id=str(uuid4()),
                correlation_id=str(uuid4()),
                retryable=True,
            ),
            ProcessingResult(
                event_id=str(uuid4()),
                correlation_id=str(uuid4()),
                status=ProcessingStatus.COMPLETED,
                started_at=datetime.now(UTC),
            ),
        ]
        mock_get_processor.return_value = mock_processor

        events = [create_file_event_dict(filename=f"file{i}.pdf") for i in range(3)]
        event = create_sqs_event(events)
        context = MockContext()

        result = lambda_handler(event, context)

        assert "batchItemFailures" in result

    @patch("processor.lambda_handler.get_file_processor")
    def test_handler_unwraps_sns_notification(self, mock_get_processor, mock_processor):
        mock_get_processor.return_value = mock_processor

        inner_event = create_file_event_dict()
        sns_wrapped = {
            "Type": "Notification",
            "MessageId": "sns-msg-123",
            "TopicArn": "arn:aws:sns:us-west-2:123456789:test-topic",
            "Message": json.dumps(inner_event),
            "Timestamp": "2024-01-15T10:30:00.000Z",
        }

        event = create_sqs_event([sns_wrapped])
        context = MockContext()

        result = lambda_handler(event, context)

        mock_processor.handle.assert_called_once()
        assert len(result.get("batchItemFailures", [])) == 0


class TestRecordHandler:
    @patch("processor.lambda_handler.get_file_processor")
    def test_record_handler_parses_event(self, mock_get_processor):
        mock_processor = MagicMock()
        mock_processor.handle.return_value = ProcessingResult(
            event_id=str(uuid4()),
            correlation_id=str(uuid4()),
            status=ProcessingStatus.COMPLETED,
            started_at=datetime.now(UTC),
        )
        mock_get_processor.return_value = mock_processor

        event_dict = create_file_event_dict()
        record_data = {
            "messageId": "msg-123",
            "receiptHandle": "receipt-123",
            "body": json.dumps(event_dict),
            "attributes": {},
            "messageAttributes": {},
            "md5OfBody": "test",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-west-2:123456789012:test-queue",
            "awsRegion": "us-west-2",
        }
        record = SQSRecord(record_data)

        result = record_handler(record)

        assert result["status"] == "success"
        assert result["messageId"] == "msg-123"

        call_args = mock_processor.handle.call_args[0][0]
        assert isinstance(call_args, FileEvent)
        assert call_args.event_type == EventType.FILE_UPLOADED

    @patch("processor.lambda_handler.get_file_processor")
    def test_record_handler_records_safe_file_metric(self, mock_get_processor):
        mock_processor = MagicMock()
        mock_processor.handle.return_value = ProcessingResult(
            event_id=str(uuid4()),
            correlation_id=str(uuid4()),
            status=ProcessingStatus.COMPLETED,
            started_at=datetime.now(UTC),
            metadata={"is_safe": True},
        )
        mock_get_processor.return_value = mock_processor

        event_dict = create_file_event_dict()
        record = SQSRecord(
            {
                "messageId": "msg-safe",
                "receiptHandle": "receipt-safe",
                "body": json.dumps(event_dict),
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "test",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-west-2:123456789012:test-queue",
                "awsRegion": "us-west-2",
            }
        )

        result = record_handler(record)

        assert result["status"] == "success"
        mock_processor.handle.assert_called_once()

    @patch("processor.lambda_handler.get_file_processor")
    def test_record_handler_records_unsafe_file_metric(self, mock_get_processor):
        mock_processor = MagicMock()
        mock_processor.handle.return_value = ProcessingResult(
            event_id=str(uuid4()),
            correlation_id=str(uuid4()),
            status=ProcessingStatus.COMPLETED,
            started_at=datetime.now(UTC),
            metadata={"is_safe": False},
        )
        mock_get_processor.return_value = mock_processor

        event_dict = create_file_event_dict()
        record = SQSRecord(
            {
                "messageId": "msg-unsafe",
                "receiptHandle": "receipt-unsafe",
                "body": json.dumps(event_dict),
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "test",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-west-2:123456789012:test-queue",
                "awsRegion": "us-west-2",
            }
        )

        result = record_handler(record)

        assert result["status"] == "success"
        mock_processor.handle.assert_called_once()

    @patch("processor.lambda_handler.get_file_processor")
    def test_record_handler_rethrows_non_retryable_error_for_dlq_redrive(self, mock_get_processor):
        mock_processor = MagicMock()
        mock_processor.handle.side_effect = NonRetryableError("permanent validation failure")
        mock_get_processor.return_value = mock_processor

        event_dict = create_file_event_dict()
        record = SQSRecord(
            {
                "messageId": "msg-non-retryable",
                "receiptHandle": "receipt-non-retryable",
                "body": json.dumps(event_dict),
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "test",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-west-2:123456789012:test-queue",
                "awsRegion": "us-west-2",
            }
        )

        with pytest.raises(NonRetryableError, match="permanent validation failure"):
            record_handler(record)


class TestColdStart:
    def test_file_processor_singleton(self):
        handler_module._file_processor = None
        handler_module._settings = None

        assert handler_module._file_processor is None

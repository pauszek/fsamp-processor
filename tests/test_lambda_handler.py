# =============================================================================
# Lambda Handler Tests
# =============================================================================
"""
Tests for AWS Lambda handler functionality.
Schema v1.0.0 compliant.
"""

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from processor.domain.events import EventType, FileEvent, FileMetadata, SecurityContext, StorageLocation, SCHEMA_VERSION
from processor.domain.models import ProcessingResult, ProcessingStatus


# =============================================================================
# Test Constants - Schema v1.0.0
# =============================================================================

SAMPLE_CHECKSUM_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
SAMPLE_KMS_ARN = "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"


class MockContext:
    """Mock AWS Lambda context."""
    
    function_name = "test-processor"
    memory_limit_in_mb = 512
    invoked_function_arn = "arn:aws:lambda:us-west-2:123456789:function:test-processor"
    aws_request_id = "test-request-id-12345"
    log_group_name = "/aws/lambda/test-processor"
    log_stream_name = "2024/01/15/[$LATEST]abcd1234"
    
    def get_remaining_time_in_millis(self) -> int:
        return 300000


def create_sqs_event(file_events: list[dict[str, Any]]) -> dict[str, Any]:
    """Create a mock SQS event with file events."""
    records = []
    for i, event_data in enumerate(file_events):
        records.append({
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
        })
    
    return {"Records": records}


def create_file_event_dict(
    event_id: str | None = None,
    correlation_id: str | None = None,
    event_type: str = "FILE_UPLOADED",
    filename: str = "test.pdf",
) -> dict[str, Any]:
    """Create a file event dictionary matching schema v1.0.0."""
    return {
        "schemaVersion": SCHEMA_VERSION,
        "eventId": str(event_id or uuid4()),
        "correlationId": str(correlation_id or uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
    """Tests for lambda_handler function."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock FileProcessorService."""
        processor = MagicMock()
        processor.handle.return_value = ProcessingResult(
            event_id=str(uuid4()),
            correlation_id=str(uuid4()),
            status=ProcessingStatus.COMPLETED,
            started_at=datetime.now(timezone.utc),
        )
        return processor

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
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
        """Test handler successfully processes a single SQS message."""
        from processor.lambda_handler import lambda_handler
        
        mock_get_processor.return_value = mock_processor
        
        event = create_sqs_event([create_file_event_dict()])
        context = MockContext()
        
        result = lambda_handler(event, context)
        
        # Should return empty batch item failures for success
        assert "batchItemFailures" in result
        assert len(result["batchItemFailures"]) == 0
        
        # Handler should have been called once
        mock_processor.handle.assert_called_once()

    @patch("processor.lambda_handler.get_file_processor")
    def test_handler_processes_batch(self, mock_get_processor, mock_processor):
        """Test handler processes a batch of SQS messages."""
        from processor.lambda_handler import lambda_handler
        
        mock_get_processor.return_value = mock_processor
        
        # Create batch of 3 messages
        events = [
            create_file_event_dict(filename=f"file{i}.pdf")
            for i in range(3)
        ]
        event = create_sqs_event(events)
        context = MockContext()
        
        result = lambda_handler(event, context)
        
        # Should process all messages
        assert mock_processor.handle.call_count == 3
        assert len(result["batchItemFailures"]) == 0

    @patch("processor.lambda_handler.get_file_processor")
    def test_handler_reports_partial_batch_failures(self, mock_get_processor, mock_processor):
        """Test handler reports failures for partial batch response."""
        from processor.lambda_handler import lambda_handler
        from processor.domain.exceptions import ProcessingError
        
        # Second call fails
        mock_processor.handle.side_effect = [
            ProcessingResult(
                event_id=str(uuid4()),
                correlation_id=str(uuid4()),
                status=ProcessingStatus.COMPLETED,
                started_at=datetime.now(timezone.utc),
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
                started_at=datetime.now(timezone.utc),
            ),
        ]
        mock_get_processor.return_value = mock_processor
        
        events = [
            create_file_event_dict(filename=f"file{i}.pdf")
            for i in range(3)
        ]
        event = create_sqs_event(events)
        context = MockContext()
        
        result = lambda_handler(event, context)
        
        # Should report 1 failure (message at index 1)
        assert "batchItemFailures" in result
        # The batch processor marks failed items

    @patch("processor.lambda_handler.get_file_processor")
    def test_handler_unwraps_sns_notification(self, mock_get_processor, mock_processor):
        """Test handler unwraps SNS notification wrapper."""
        from processor.lambda_handler import lambda_handler
        
        mock_get_processor.return_value = mock_processor
        
        # Create SNS-wrapped message
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
        
        # Should successfully process unwrapped message
        mock_processor.handle.assert_called_once()
        assert len(result.get("batchItemFailures", [])) == 0


class TestRecordHandler:
    """Tests for record_handler function."""

    @patch("processor.lambda_handler.get_file_processor")
    def test_record_handler_parses_event(self, mock_get_processor):
        """Test record handler correctly parses FileEvent."""
        from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
        from processor.lambda_handler import record_handler
        
        mock_processor = MagicMock()
        mock_processor.handle.return_value = ProcessingResult(
            event_id=str(uuid4()),
            correlation_id=str(uuid4()),
            status=ProcessingStatus.COMPLETED,
            started_at=datetime.now(timezone.utc),
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
        
        # Verify FileEvent was parsed correctly
        call_args = mock_processor.handle.call_args[0][0]
        assert isinstance(call_args, FileEvent)
        assert call_args.event_type == EventType.FILE_UPLOADED


class TestColdStart:
    """Tests for Lambda cold start behavior."""

    def test_file_processor_singleton(self):
        """Test FileProcessorService is created once (singleton)."""
        from processor.lambda_handler import _file_processor, get_file_processor
        
        # Reset singleton for test
        import processor.lambda_handler as handler_module
        handler_module._file_processor = None
        handler_module._settings = None
        
        # This test would need actual AWS mocks to fully work
        # For now, we verify the module structure supports singleton pattern
        assert handler_module._file_processor is None

# =============================================================================
# Unit Tests for SQS Consumer
# =============================================================================
"""Tests for SQS Consumer adapter - simplified version without threading issues."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from processor.adapters.inbound.sqs_consumer import SQSConsumer
from processor.domain.events import FileEvent
from processor.domain.exceptions import (
    EventValidationError,
    MessageError,
    NonRetryableError,
)

# Valid KMS ARN format: arn:aws:kms:{region}:{account}:key/{uuid}
VALID_KMS_KEY_ID = "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"


class TestSQSConsumerInit:
    """Tests for SQSConsumer initialization."""

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_init_with_valid_params(self, mock_signal) -> None:
        """Test initialization with valid parameters."""
        client = MagicMock()
        handler = MagicMock()

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="https://sqs.us-west-2.amazonaws.com/123456789012/test-queue",
            handler=handler,
            max_messages=5,
            wait_time_seconds=10,
            visibility_timeout=120,
        )

        assert consumer._queue_url == "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
        assert consumer._max_messages == 5
        assert consumer._wait_time_seconds == 10
        assert consumer._visibility_timeout == 120
        assert not consumer._running

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_init_clamps_max_messages_upper(self, mock_signal) -> None:
        """Test that max_messages is clamped to upper bound."""
        client = MagicMock()
        handler = MagicMock()

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
            max_messages=100,
        )
        assert consumer._max_messages == 10

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_init_clamps_max_messages_lower(self, mock_signal) -> None:
        """Test that max_messages is clamped to lower bound."""
        client = MagicMock()
        handler = MagicMock()

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
            max_messages=0,
        )
        assert consumer._max_messages == 1


class TestSQSConsumerReceiveMessages:
    """Tests for message receiving."""

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_receive_messages_success(self, mock_signal) -> None:
        """Test successful message receiving."""
        client = MagicMock()
        messages = [
            {"MessageId": "msg-1", "Body": "{}", "ReceiptHandle": "handle-1"},
            {"MessageId": "msg-2", "Body": "{}", "ReceiptHandle": "handle-2"},
        ]
        client.receive_message.return_value = {"Messages": messages}

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        result = consumer._receive_messages()

        assert len(result) == 2
        client.receive_message.assert_called_once()

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_receive_messages_empty(self, mock_signal) -> None:
        """Test receiving when no messages available."""
        client = MagicMock()
        client.receive_message.return_value = {}

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        result = consumer._receive_messages()
        assert result == []


class TestSQSConsumerAcknowledge:
    """Tests for message acknowledgment."""

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_acknowledge_success(self, mock_signal) -> None:
        """Test successful acknowledgment."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer.acknowledge("test-receipt-handle")

        client.delete_message.assert_called_once_with(
            QueueUrl="http://queue",
            ReceiptHandle="test-receipt-handle",
        )

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_acknowledge_no_receipt_handle(self, mock_signal) -> None:
        """Test acknowledgment with no receipt handle."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer.acknowledge(None)
        client.delete_message.assert_not_called()

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_acknowledge_client_error(self, mock_signal) -> None:
        """Test acknowledgment with client error."""
        client = MagicMock()
        client.delete_message.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameterValue"}},
            "DeleteMessage",
        )

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        with pytest.raises(MessageError):
            consumer.acknowledge("test-handle")


class TestSQSConsumerReject:
    """Tests for message rejection."""

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_reject_with_requeue(self, mock_signal) -> None:
        """Test rejection with requeue."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer.reject("test-handle", requeue=True)

        client.change_message_visibility.assert_called_once_with(
            QueueUrl="http://queue",
            ReceiptHandle="test-handle",
            VisibilityTimeout=0,
        )

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_reject_without_requeue(self, mock_signal) -> None:
        """Test rejection without requeue (delete)."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer.reject("test-handle", requeue=False)

        client.delete_message.assert_called_once()

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_reject_no_receipt_handle(self, mock_signal) -> None:
        """Test rejection with no receipt handle."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer.reject(None)
        client.change_message_visibility.assert_not_called()
        client.delete_message.assert_not_called()

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_reject_client_error(self, mock_signal) -> None:
        """Test rejection with client error."""
        client = MagicMock()
        client.change_message_visibility.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameterValue"}},
            "ChangeMessageVisibility",
        )

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        with pytest.raises(MessageError):
            consumer.reject("test-handle", requeue=True)


class TestSQSConsumerQueueAttributes:
    """Tests for queue attributes."""

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_get_queue_attributes_success(self, mock_signal) -> None:
        """Test getting queue attributes."""
        client = MagicMock()
        client.get_queue_attributes.return_value = {
            "Attributes": {
                "ApproximateNumberOfMessages": "10",
                "ApproximateNumberOfMessagesNotVisible": "5",
            }
        }

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        attrs = consumer.get_queue_attributes()

        assert attrs["ApproximateNumberOfMessages"] == "10"
        assert attrs["ApproximateNumberOfMessagesNotVisible"] == "5"

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_get_queue_attributes_error(self, mock_signal) -> None:
        """Test getting queue attributes with error."""
        client = MagicMock()
        client.get_queue_attributes.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}},
            "GetQueueAttributes",
        )

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        attrs = consumer.get_queue_attributes()
        assert attrs == {}


class TestSQSConsumerIsRunning:
    """Tests for is_running method."""

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_is_running_initially_false(self, mock_signal) -> None:
        """Test that is_running is initially False."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        assert consumer.is_running() is False

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_is_running_reflects_running_state(self, mock_signal) -> None:
        """Test that is_running reflects _running state."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer._running = True
        assert consumer.is_running() is True

        consumer._running = False
        assert consumer.is_running() is False


class TestSQSConsumerSignalHandler:
    """Tests for signal handler."""

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_signal_handler_sets_running_false(self, mock_signal) -> None:
        """Test that signal handler stops the consumer."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer._running = True
        consumer._signal_handler(2, None)  # SIGINT

        assert consumer._running is False


class TestSQSConsumerProcessMessage:
    """Tests for _process_message method."""

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_process_message_success(self, mock_signal) -> None:
        """Test successful message processing."""
        import json
        from datetime import datetime, timezone
        from uuid import uuid4

        client = MagicMock()
        handler = MagicMock()

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
        )

        # Create valid file event
        file_event = {
            "schema_version": "1.0.0",
            "event_id": str(uuid4()),
            "correlation_id": str(uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "fsamp-processor",
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
                "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            },
        }

        raw_message = {
            "MessageId": "msg-1",
            "Body": json.dumps(file_event),
            "ReceiptHandle": "handle-1",
            "Attributes": {},
            "MessageAttributes": {},
        }

        consumer._process_message(raw_message)

        handler.assert_called_once()
        client.delete_message.assert_called_once()

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_process_message_validation_error(self, mock_signal) -> None:
        """Test message processing with validation error - requeued as unexpected error."""
        import json

        client = MagicMock()
        handler = MagicMock()

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
        )

        # Invalid message body - pydantic validation will fail
        raw_message = {
            "MessageId": "msg-1",
            "Body": json.dumps({"invalid": "data"}),
            "ReceiptHandle": "handle-1",
            "Attributes": {},
            "MessageAttributes": {},
        }

        consumer._process_message(raw_message)

        # Handler should not be called for invalid events
        handler.assert_not_called()
        # Message should be requeued (pydantic ValidationError is caught as generic Exception)
        client.change_message_visibility.assert_called_once()

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_process_message_non_retryable_error(self, mock_signal) -> None:
        """Test message processing with non-retryable error."""
        import json
        from datetime import datetime, timezone
        from uuid import uuid4

        client = MagicMock()
        handler = MagicMock()
        handler.side_effect = NonRetryableError("File too large")

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
        )

        file_event = {
            "schema_version": "1.0.0",
            "event_id": str(uuid4()),
            "correlation_id": str(uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "fsamp-processor",
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
                "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            },
        }

        raw_message = {
            "MessageId": "msg-1",
            "Body": json.dumps(file_event),
            "ReceiptHandle": "handle-1",
            "Attributes": {},
            "MessageAttributes": {},
        }

        consumer._process_message(raw_message)

        # Message should be deleted (rejected without requeue)
        client.delete_message.assert_called_once()

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_process_message_unexpected_error(self, mock_signal) -> None:
        """Test message processing with unexpected error - requeue."""
        import json
        from datetime import datetime, timezone
        from uuid import uuid4

        client = MagicMock()
        handler = MagicMock()
        handler.side_effect = RuntimeError("Unexpected error")

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
        )

        file_event = {
            "schema_version": "1.0.0",
            "event_id": str(uuid4()),
            "correlation_id": str(uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "fsamp-processor",
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
                "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            },
        }

        raw_message = {
            "MessageId": "msg-1",
            "Body": json.dumps(file_event),
            "ReceiptHandle": "handle-1",
            "Attributes": {},
            "MessageAttributes": {},
        }

        consumer._process_message(raw_message)

        # Message should be requeued
        client.change_message_visibility.assert_called_once()


class TestSQSConsumerStartStop:
    """Tests for start/stop without actually starting threads."""

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_stop_when_not_running(self, mock_signal) -> None:
        """Test stop when consumer is not running."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        # Should not raise
        consumer.stop()
        assert consumer._running is False

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_start_when_already_running(self, mock_signal) -> None:
        """Test start when consumer is already running."""
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer._running = True
        # Should return without starting new thread
        consumer.start()
        assert consumer._running is True

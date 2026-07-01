import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from processor.adapters.inbound.sqs_consumer import SQSConsumer
from processor.domain.exceptions import MessageError, NonRetryableError

VALID_KMS_KEY_ID = "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"


class TestSQSConsumerInit:
    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_init_with_valid_params(self, mock_signal) -> None:
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
    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_receive_messages_success(self, mock_signal) -> None:
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
    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_acknowledge_success(self, mock_signal) -> None:
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
    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_reject_with_requeue(self, mock_signal) -> None:
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
    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_get_queue_attributes_success(self, mock_signal) -> None:
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
    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_is_running_initially_false(self, mock_signal) -> None:
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        assert consumer.is_running() is False

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_is_running_reflects_running_state(self, mock_signal) -> None:
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
    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_signal_handler_sets_running_false(self, mock_signal) -> None:
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
    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_process_messages_stops_after_shutdown_request(self, mock_signal) -> None:
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )
        messages = [
            {"MessageId": "msg-1"},
            {"MessageId": "msg-2"},
        ]

        def request_shutdown(_message: dict[str, str]) -> None:
            consumer._shutdown_event.set()

        consumer._process_message = MagicMock(side_effect=request_shutdown)  # type: ignore[method-assign]

        consumer._process_messages(messages)

        consumer._process_message.assert_called_once_with(messages[0])

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_process_message_success(self, mock_signal) -> None:
        client = MagicMock()
        handler = MagicMock()

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
        )

        file_event = {
            "schema_version": "1.1.2",
            "file_id": str(uuid4()),
            "event_id": str(uuid4()),
            "correlation_id": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
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
                "kms_key_id": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
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
        client = MagicMock()
        handler = MagicMock()

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
        )

        raw_message = {
            "MessageId": "msg-1",
            "Body": json.dumps({"invalid": "data"}),
            "ReceiptHandle": "handle-1",
            "Attributes": {},
            "MessageAttributes": {},
        }

        consumer._process_message(raw_message)

        handler.assert_not_called()
        client.change_message_visibility.assert_called_once()

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_process_message_non_retryable_error(self, mock_signal) -> None:
        client = MagicMock()
        handler = MagicMock()
        handler.side_effect = NonRetryableError("File too large")

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
        )

        file_event = {
            "schema_version": "1.1.2",
            "file_id": str(uuid4()),
            "event_id": str(uuid4()),
            "correlation_id": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
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
                "kms_key_id": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
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

        client.delete_message.assert_called_once()

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_process_message_unexpected_error(self, mock_signal) -> None:
        client = MagicMock()
        handler = MagicMock()
        handler.side_effect = RuntimeError("Unexpected error")

        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=handler,
        )

        file_event = {
            "schema_version": "1.1.2",
            "file_id": str(uuid4()),
            "event_id": str(uuid4()),
            "correlation_id": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
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
                "kms_key_id": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
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

        client.change_message_visibility.assert_called_once()


class TestSQSConsumerStartStop:
    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_stop_when_not_running(self, mock_signal) -> None:
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer.stop()
        assert consumer._running is False

    @patch("processor.adapters.inbound.sqs_consumer.signal.signal")
    def test_start_when_already_running(self, mock_signal) -> None:
        client = MagicMock()
        consumer = SQSConsumer(
            sqs_client=client,
            queue_url="http://queue",
            handler=MagicMock(),
        )

        consumer._running = True
        consumer.start()
        assert consumer._running is True

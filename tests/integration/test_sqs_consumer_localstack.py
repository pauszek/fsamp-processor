# =============================================================================
# SQS Consumer Integration Tests (LocalStack)
# =============================================================================
"""
Integration tests for SQS Consumer with real LocalStack.

Tests actual SQS behavior:
- Long-polling message retrieval
- Message visibility timeout
- Message acknowledgment (deletion)
- Error handling and DLQ behavior
"""

import json
import time
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from processor.adapters.inbound.sqs_consumer import SQSConsumer
from processor.domain.events import (
    SCHEMA_VERSION,
    EventSource,
    EventType,
    FileEvent,
    FileMetadata,
    SecurityContext,
    StorageLocation,
)

# Import LocalStack fixtures
pytest_plugins = ["tests.integration.conftest_localstack"]


@pytest.mark.integration
class TestSQSConsumerIntegration:
    """Integration tests for SQS Consumer."""

    def test_receives_and_processes_message(
        self,
        localstack_sqs_client,
        localstack_queue_url,
    ) -> None:
        """Test that consumer receives and processes SQS message."""
        # given
        processed_events: list[FileEvent] = []
        
        def handler(event: FileEvent) -> None:
            processed_events.append(event)
        
        consumer = SQSConsumer(
            sqs_client=localstack_sqs_client,
            queue_url=localstack_queue_url,
            handler=handler,
            max_messages=1,
            wait_time_seconds=1,
            visibility_timeout=30,
        )
        
        # Send test message
        event = create_test_event()
        message_body = create_sns_wrapped_message(event)
        
        localstack_sqs_client.send_message(
            QueueUrl=localstack_queue_url,
            MessageBody=message_body,
        )
        
        # when - poll for messages
        consumer._poll_and_process()
        
        # then
        assert len(processed_events) == 1
        assert processed_events[0].event_id == event.event_id
        
        # Verify message was deleted from queue
        response = localstack_sqs_client.receive_message(
            QueueUrl=localstack_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
        )
        assert "Messages" not in response or len(response["Messages"]) == 0

    def test_message_returns_to_queue_on_handler_error(
        self,
        localstack_sqs_client,
        localstack_queue_url,
    ) -> None:
        """Test that message becomes visible again if handler fails."""
        # given
        def failing_handler(event: FileEvent) -> None:
            raise RuntimeError("Processing failed")
        
        consumer = SQSConsumer(
            sqs_client=localstack_sqs_client,
            queue_url=localstack_queue_url,
            handler=failing_handler,
            max_messages=1,
            wait_time_seconds=1,
            visibility_timeout=1,  # Short timeout for test
        )
        
        # Send test message
        event = create_test_event()
        message_body = create_sns_wrapped_message(event)
        
        localstack_sqs_client.send_message(
            QueueUrl=localstack_queue_url,
            MessageBody=message_body,
        )
        
        # when - process fails
        try:
            consumer._poll_and_process()
        except Exception:
            pass  # Expected to fail
        
        # then - wait for visibility timeout
        time.sleep(2)
        
        # Message should be visible again
        response = localstack_sqs_client.receive_message(
            QueueUrl=localstack_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
        )
        assert "Messages" in response
        assert len(response["Messages"]) == 1

    def test_processes_multiple_messages_in_batch(
        self,
        localstack_sqs_client,
        localstack_queue_url,
    ) -> None:
        """Test batch processing of multiple messages."""
        # given
        processed_events: list[FileEvent] = []
        
        def handler(event: FileEvent) -> None:
            processed_events.append(event)
        
        consumer = SQSConsumer(
            sqs_client=localstack_sqs_client,
            queue_url=localstack_queue_url,
            handler=handler,
            max_messages=10,
            wait_time_seconds=1,
            visibility_timeout=30,
        )
        
        # Send 5 test messages
        for i in range(5):
            event = create_test_event()
            message_body = create_sns_wrapped_message(event)
            localstack_sqs_client.send_message(
                QueueUrl=localstack_queue_url,
                MessageBody=message_body,
            )
        
        # when - poll (may need multiple polls)
        for _ in range(3):
            consumer._poll_and_process()
        
        # then
        assert len(processed_events) == 5

    def test_handles_empty_queue_gracefully(
        self,
        localstack_sqs_client,
        localstack_queue_url,
    ) -> None:
        """Test that consumer handles empty queue without errors."""
        # given
        processed_events: list[FileEvent] = []
        
        def handler(event: FileEvent) -> None:
            processed_events.append(event)
        
        consumer = SQSConsumer(
            sqs_client=localstack_sqs_client,
            queue_url=localstack_queue_url,
            handler=handler,
            max_messages=1,
            wait_time_seconds=1,
            visibility_timeout=30,
        )
        
        # when - poll empty queue
        consumer._poll_and_process()
        
        # then
        assert len(processed_events) == 0

    def test_respects_visibility_timeout(
        self,
        localstack_sqs_client,
    ) -> None:
        """Test that visibility timeout prevents duplicate processing."""
        # Create queue with specific visibility timeout
        queue_name = f"test-visibility-{uuid.uuid4().hex[:8]}"
        response = localstack_sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={"VisibilityTimeout": "5"},
        )
        queue_url = response["QueueUrl"]
        
        # Send message
        event = create_test_event()
        message_body = create_sns_wrapped_message(event)
        localstack_sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
        )
        
        # First receive - message should be invisible
        response1 = localstack_sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=0,
        )
        assert "Messages" in response1
        
        # Immediate second receive - should be empty (message invisible)
        response2 = localstack_sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=0,
        )
        assert "Messages" not in response2 or len(response2["Messages"]) == 0
        
        # Cleanup
        localstack_sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.mark.integration
class TestSQSSNSIntegration:
    """Test SQS receiving messages from SNS subscription."""

    def test_receives_sns_notification(
        self,
        localstack_sqs_client,
        localstack_sns_client,
        localstack_queue_url,
        localstack_topic_arn,
    ) -> None:
        """Test SQS receives message published to subscribed SNS topic."""
        # given - subscribe queue to topic
        queue_arn = localstack_sqs_client.get_queue_attributes(
            QueueUrl=localstack_queue_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]
        
        localstack_sns_client.subscribe(
            TopicArn=localstack_topic_arn,
            Protocol="sqs",
            Endpoint=queue_arn,
        )
        
        # when - publish to SNS
        event = create_test_event()
        localstack_sns_client.publish(
            TopicArn=localstack_topic_arn,
            Message=event.model_dump_json(),
            MessageAttributes={
                "eventType": {
                    "DataType": "String",
                    "StringValue": event.event_type.value,
                },
            },
        )
        
        # then - SQS should receive the message
        time.sleep(1)  # Allow propagation
        
        response = localstack_sqs_client.receive_message(
            QueueUrl=localstack_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        
        assert "Messages" in response
        assert len(response["Messages"]) == 1
        
        # SNS wraps the message
        body = json.loads(response["Messages"][0]["Body"])
        assert "Message" in body  # SNS envelope


# =============================================================================
# Helper Functions
# =============================================================================

def create_test_event() -> FileEvent:
    """Create a test FileEvent."""
    return FileEvent(
        schema_version=SCHEMA_VERSION,
        event_id=uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        timestamp=datetime.now(UTC),
        source=EventSource.GATEWAY,
        event_type=EventType.FILE_UPLOADED,
        file_metadata=FileMetadata(
            original_filename="test-file.pdf",
            file_size_bytes=1024,
            mime_type="application/pdf",
            checksum_sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
        storage_location=StorageLocation(
            bucket_name="test-bucket",
            object_key=f"uploads/2025/01/{uuid.uuid4()}.pdf",
        ),
        security_context=SecurityContext(
            is_encrypted=True,
            encryption_algorithm="AES/GCM/NoPadding",
            kms_key_id="arn:aws:kms:us-west-2:123456789012:key/test-key",
        ),
    )


def create_sns_wrapped_message(event: FileEvent) -> str:
    """Create SNS-wrapped message as SQS would receive it."""
    # SNS wraps the message in an envelope
    sns_envelope = {
        "Type": "Notification",
        "MessageId": str(uuid.uuid4()),
        "TopicArn": "arn:aws:sns:us-west-2:000000000000:file-events",
        "Subject": None,
        "Message": event.model_dump_json(),
        "Timestamp": datetime.now(UTC).isoformat(),
        "SignatureVersion": "1",
        "Signature": "fake-signature",
        "SigningCertURL": "https://sns.us-west-2.amazonaws.com/fake.pem",
        "UnsubscribeURL": "https://sns.us-west-2.amazonaws.com/unsubscribe",
        "MessageAttributes": {
            "eventType": {
                "Type": "String",
                "Value": event.event_type.value,
            },
        },
    }
    return json.dumps(sns_envelope)

# =============================================================================
# Unit Tests for SNS Publisher
# =============================================================================
"""Tests for SNS Event Publisher adapter."""

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from processor.adapters.outbound.sns_publisher import SNSEventPublisher
from processor.domain.events import FileEvent
from processor.domain.exceptions import MessageError


class TestSNSEventPublisherInit:
    """Tests for SNSEventPublisher initialization."""

    def test_init_with_valid_params(self) -> None:
        """Test initialization with valid parameters."""
        client = MagicMock()
        topic_arn = "arn:aws:sns:us-west-2:123456789012:test-topic"

        publisher = SNSEventPublisher(
            sns_client=client,
            topic_arn=topic_arn,
        )

        assert publisher._topic_arn == topic_arn
        assert publisher._client is client


class TestSNSEventPublisherPublish:
    """Tests for single event publishing."""

    @pytest.fixture
    def publisher(self) -> SNSEventPublisher:
        """Create a publisher for testing."""
        client = MagicMock()
        client.publish.return_value = {"MessageId": "test-message-id"}
        return SNSEventPublisher(
            sns_client=client,
            topic_arn="arn:aws:sns:us-west-2:123456789012:test-topic",
        )

    def test_publish_success(
        self, publisher: SNSEventPublisher, sample_file_event: FileEvent
    ) -> None:
        """Test successful event publishing."""
        message_id = publisher.publish(sample_file_event)

        assert message_id == "test-message-id"
        publisher._client.publish.assert_called_once()

        call_kwargs = publisher._client.publish.call_args.kwargs
        assert call_kwargs["TopicArn"] == publisher._topic_arn
        assert "Message" in call_kwargs
        assert "MessageAttributes" in call_kwargs

    def test_publish_includes_message_attributes(
        self, publisher: SNSEventPublisher, sample_file_event: FileEvent
    ) -> None:
        """Test that publish includes correct message attributes."""
        publisher.publish(sample_file_event)

        call_kwargs = publisher._client.publish.call_args.kwargs
        attrs = call_kwargs["MessageAttributes"]

        assert "eventType" in attrs
        assert attrs["eventType"]["DataType"] == "String"
        assert "correlationId" in attrs
        assert attrs["correlationId"]["DataType"] == "String"

    def test_publish_client_error(
        self, sample_file_event: FileEvent
    ) -> None:
        """Test publish with client error."""
        client = MagicMock()
        client.publish.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "Internal error"}},
            "Publish",
        )

        publisher = SNSEventPublisher(
            sns_client=client,
            topic_arn="arn:aws:sns:us-west-2:123456789012:test-topic",
        )

        with pytest.raises(MessageError) as exc_info:
            publisher.publish(sample_file_event)

        assert "Failed to publish event" in str(exc_info.value)


class TestSNSEventPublisherPublishBatch:
    """Tests for batch event publishing."""

    @pytest.fixture
    def publisher(self) -> SNSEventPublisher:
        """Create a publisher for testing."""
        client = MagicMock()
        client.publish.return_value = {"MessageId": "test-message-id"}
        return SNSEventPublisher(
            sns_client=client,
            topic_arn="arn:aws:sns:us-west-2:123456789012:test-topic",
        )

    def test_publish_batch_success(
        self, publisher: SNSEventPublisher, sample_file_event: FileEvent
    ) -> None:
        """Test successful batch publishing."""
        events = [sample_file_event, sample_file_event, sample_file_event]

        message_ids = publisher.publish_batch(events)

        assert len(message_ids) == 3
        assert publisher._client.publish.call_count == 3

    def test_publish_batch_partial_failure(
        self, sample_file_event: FileEvent
    ) -> None:
        """Test batch publishing with partial failures."""
        client = MagicMock()
        
        # First two succeed, third fails
        client.publish.side_effect = [
            {"MessageId": "msg-1"},
            {"MessageId": "msg-2"},
            ClientError(
                {"Error": {"Code": "InternalError"}},
                "Publish",
            ),
        ]

        publisher = SNSEventPublisher(
            sns_client=client,
            topic_arn="arn:aws:sns:us-west-2:123456789012:test-topic",
        )

        events = [sample_file_event, sample_file_event, sample_file_event]
        message_ids = publisher.publish_batch(events)

        # Should have 2 successful publishes
        assert len(message_ids) == 2

    def test_publish_batch_empty(
        self, publisher: SNSEventPublisher
    ) -> None:
        """Test batch publishing with empty list."""
        message_ids = publisher.publish_batch([])
        assert message_ids == []


class TestSNSEventPublisherPublishToQueue:
    """Tests for publishing to specific queue."""

    @pytest.fixture
    def publisher(self) -> SNSEventPublisher:
        """Create a publisher for testing."""
        client = MagicMock()
        client.publish.return_value = {"MessageId": "test-message-id"}
        return SNSEventPublisher(
            sns_client=client,
            topic_arn="arn:aws:sns:us-west-2:123456789012:test-topic",
        )

    def test_publish_to_queue_success(
        self, publisher: SNSEventPublisher, sample_file_event: FileEvent
    ) -> None:
        """Test publishing to specific queue."""
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:target-queue"

        message_id = publisher.publish_to_queue(sample_file_event, queue_arn)

        assert message_id == "test-message-id"
        
        call_kwargs = publisher._client.publish.call_args.kwargs
        attrs = call_kwargs["MessageAttributes"]
        assert "targetQueue" in attrs
        assert attrs["targetQueue"]["StringValue"] == queue_arn

    def test_publish_to_queue_client_error(
        self, sample_file_event: FileEvent
    ) -> None:
        """Test publish to queue with client error."""
        client = MagicMock()
        client.publish.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}},
            "Publish",
        )

        publisher = SNSEventPublisher(
            sns_client=client,
            topic_arn="arn:aws:sns:us-west-2:123456789012:test-topic",
        )

        queue_arn = "arn:aws:sqs:us-west-2:123456789012:target-queue"

        with pytest.raises(MessageError) as exc_info:
            publisher.publish_to_queue(sample_file_event, queue_arn)

        assert "Failed to publish to queue" in str(exc_info.value)

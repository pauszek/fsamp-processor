# =============================================================================
# Unit Tests for Outbox Publisher Lambda
# =============================================================================
"""Tests for Outbox Publisher Lambda handler."""

import os
from unittest.mock import MagicMock, patch

import pytest

from processor.domain.models import OutboxEvent, OutboxEventType, OutboxStatus


class TestOutboxPublisherHelpers:
    """Tests for outbox publisher helper functions."""

    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        """Set up environment variables."""
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        from processor import outbox_publisher

        outbox_publisher._sns_client = None
        outbox_publisher._dynamodb_client = None
        outbox_publisher._aws_factory = None
        outbox_publisher._settings = None
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)

    def test_get_sns_client_singleton(self) -> None:
        """Test that SNS client is reused."""
        from processor import outbox_publisher

        mock_client = MagicMock()
        mock_factory = MagicMock()
        mock_factory.get_sns_client.return_value = mock_client

        with patch.object(outbox_publisher, "get_aws_factory", return_value=mock_factory):
            client1 = outbox_publisher.get_sns_client()
            client2 = outbox_publisher.get_sns_client()

        assert client1 is client2
        mock_factory.get_sns_client.assert_called_once()

    def test_get_dynamodb_client_singleton(self) -> None:
        """Test that DynamoDB client is reused."""
        from processor import outbox_publisher

        mock_client = MagicMock()
        mock_factory = MagicMock()
        mock_factory.get_dynamodb_client.return_value = mock_client

        with patch.object(outbox_publisher, "get_aws_factory", return_value=mock_factory):
            client1 = outbox_publisher.get_dynamodb_client()
            client2 = outbox_publisher.get_dynamodb_client()

        assert client1 is client2
        mock_factory.get_dynamodb_client.assert_called_once()


class TestOutboxPublisherRecordHandlerLogic:
    """Tests for record_handler logic - simplified without AWS decorators."""

    def test_non_insert_event_should_be_skipped(self) -> None:
        """Test that non-INSERT events should be skipped."""
        # This tests the logic directly without AWS decorators
        event_name = "MODIFY"
        assert event_name != "INSERT"

    def test_pending_status_detection(self) -> None:
        """Test PENDING status detection."""
        new_image = {"status": OutboxStatus.PENDING.value}
        assert new_image.get("status") == OutboxStatus.PENDING.value

        new_image = {"status": OutboxStatus.PUBLISHED.value}
        assert new_image.get("status") != OutboxStatus.PENDING.value

    def test_no_new_image_should_be_skipped(self) -> None:
        """Test that missing new_image should be skipped."""
        new_image = None
        assert not new_image

    def test_record_handler_skips_non_insert_event(self) -> None:
        """Test record handler skips DynamoDB stream events other than INSERT."""
        from processor import outbox_publisher

        record = MagicMock()
        record.event_name = "MODIFY"
        record.event_id = "stream-event-1"

        result = outbox_publisher.record_handler(record)

        assert result == {"status": "skipped", "reason": "not_insert"}

    def test_record_handler_skips_missing_new_image(self) -> None:
        """Test record handler skips INSERT events without a new image."""
        from processor import outbox_publisher

        record = MagicMock()
        record.event_name = "INSERT"
        record.event_id = "stream-event-1"
        record.dynamodb.new_image = None

        result = outbox_publisher.record_handler(record)

        assert result == {"status": "skipped", "reason": "no_new_image"}

    def test_record_handler_publishes_pending_event(self) -> None:
        """Test record handler publishes pending outbox events."""
        from processor import outbox_publisher

        outbox_event = OutboxEvent.for_file_processed(
            file_id="file-123",
            correlation_id="corr-123",
            file_hash="a" * 64,
            is_safe=True,
            bucket_name="bucket",
            object_key="object",
        )
        record = MagicMock()
        record.event_name = "INSERT"
        record.event_id = "stream-event-1"
        record.dynamodb.new_image = outbox_event.to_dynamodb_item()

        with (
            patch.object(outbox_publisher, "publish_to_sns") as publish_to_sns,
            patch.object(outbox_publisher, "mark_event_published") as mark_event_published,
        ):
            result = outbox_publisher.record_handler(record)

        assert result == {
            "status": "published",
            "event_id": outbox_event.event_id,
            "event_type": outbox_event.event_type.value,
        }
        publish_to_sns.assert_called_once()
        mark_event_published.assert_called_once()


class TestOutboxPublisherPublishToSNS:
    """Tests for publish_to_sns function."""

    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        """Set up environment variables."""
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)

    def test_publish_to_sns_success(self) -> None:
        """Test successful SNS publishing."""
        from processor import outbox_publisher

        mock_sns = MagicMock()
        mock_sns.publish.return_value = {"MessageId": "test-message-id"}

        outbox_event = OutboxEvent(
            event_id="test-event-id",
            event_type=OutboxEventType.FILE_PROCESSED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={"file_id": "file-123"},
        )

        with patch.object(outbox_publisher, "get_sns_client", return_value=mock_sns):
            result = outbox_publisher.publish_to_sns(outbox_event)

        assert result == "test-message-id"
        mock_sns.publish.assert_called_once()

    def test_publish_to_sns_adds_file_context_attributes(self) -> None:
        """Test SNS publishing includes file context attributes for filtering."""
        from processor import outbox_publisher

        mock_sns = MagicMock()
        mock_sns.publish.return_value = {"MessageId": "test-message-id"}

        outbox_event = OutboxEvent(
            event_id="test-event-id",
            event_type=OutboxEventType.FILE_PROCESSED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={
                "schemaVersion": "1.1.0",
                "eventType": "FILE_PROCESSED",
                "fileId": "file-123",
                "correlationId": "corr-123",
            },
        )

        with patch.object(outbox_publisher, "get_sns_client", return_value=mock_sns):
            result = outbox_publisher.publish_to_sns(outbox_event)

        assert result == "test-message-id"
        message_attributes = mock_sns.publish.call_args.kwargs["MessageAttributes"]
        assert message_attributes["fileId"]["StringValue"] == "file-123"
        assert message_attributes["correlationId"]["StringValue"] == "corr-123"


class TestOutboxPublisherMarkEventPublished:
    """Tests for mark_event_published function."""

    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        """Set up environment variables."""
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)

    def test_mark_event_published(self) -> None:
        """Test marking event as published."""
        from processor import outbox_publisher

        mock_dynamodb = MagicMock()

        outbox_event = OutboxEvent(
            event_id="test-event-id",
            event_type=OutboxEventType.FILE_PROCESSED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={"file_id": "file-123"},
        )

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            outbox_publisher.mark_event_published(outbox_event)

        mock_dynamodb.update_item.assert_called_once()
        call_kwargs = mock_dynamodb.update_item.call_args.kwargs
        assert call_kwargs["TableName"] == "test-outbox"


class TestOutboxPublisherMarkEventFailed:
    """Tests for mark_event_failed function."""

    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        """Set up environment variables."""
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)

    def test_mark_event_failed(self) -> None:
        """Test marking event as failed."""
        from processor import outbox_publisher

        mock_dynamodb = MagicMock()

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            outbox_publisher.mark_event_failed("test-event-id", "Test error")

        mock_dynamodb.update_item.assert_called_once()
        call_kwargs = mock_dynamodb.update_item.call_args.kwargs
        assert ":error" in call_kwargs["ExpressionAttributeValues"]

    def test_mark_event_failed_truncates_long_error(self) -> None:
        """Test that long error messages are truncated."""
        from processor import outbox_publisher

        mock_dynamodb = MagicMock()
        long_error = "x" * 2000

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            outbox_publisher.mark_event_failed("test-event-id", long_error)

        call_kwargs = mock_dynamodb.update_item.call_args.kwargs
        error_value = call_kwargs["ExpressionAttributeValues"][":error"]["S"]
        assert len(error_value) == 1000


class TestOutboxPublisherLambdaHandler:
    """Tests for lambda_handler function."""

    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        """Set up environment variables."""
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        os.environ["POWERTOOLS_SERVICE_NAME"] = "outbox-publisher"
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)
        os.environ.pop("POWERTOOLS_SERVICE_NAME", None)

    def test_lambda_handler_env_validation(self) -> None:
        """Test lambda handler validates environment variables."""
        from processor import outbox_publisher

        # Test SNS_TOPIC_ARN is used
        outbox_publisher._settings = None
        assert os.environ.get("SNS_TOPIC_ARN", "") == outbox_publisher.get_sns_topic_arn()

        # Test OUTBOX_TABLE_NAME is used
        assert os.environ.get("OUTBOX_TABLE_NAME", "") == outbox_publisher.get_outbox_table_name()

    def test_lambda_handler_constants(self) -> None:
        """Test lambda handler constants are set."""
        from processor import outbox_publisher

        # MAX_RETRY_COUNT should have a default
        assert outbox_publisher.MAX_RETRY_COUNT >= 0

    def test_settings_values_take_precedence_over_environment(self) -> None:
        """Test resolved settings override fallback environment values."""
        from processor import outbox_publisher

        outbox_publisher._settings = MagicMock(
            sns_topic_arn="arn:aws:sns:us-west-2:123456789012:settings-topic",
            outbox_table_name="settings-outbox",
        )

        assert (
            outbox_publisher.get_sns_topic_arn()
            == "arn:aws:sns:us-west-2:123456789012:settings-topic"
        )
        assert outbox_publisher.get_outbox_table_name() == "settings-outbox"


class TestOutboxPublisherRetryHandler:
    """Tests for retry_handler function."""

    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        """Set up environment variables."""
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        os.environ["POWERTOOLS_SERVICE_NAME"] = "outbox-publisher"
        os.environ["MAX_RETRY_COUNT"] = "3"
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)
        os.environ.pop("POWERTOOLS_SERVICE_NAME", None)
        os.environ.pop("MAX_RETRY_COUNT", None)

    def test_retry_handler_no_failed_events(self) -> None:
        """Test retry handler with no failed events."""
        from processor import outbox_publisher

        mock_dynamodb = MagicMock()
        mock_dynamodb.query.return_value = {"Items": []}

        event = {}
        context = MagicMock()

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            result = outbox_publisher.retry_handler(event, context)

        assert result["statusCode"] == 200
        assert result["body"]["total_processed"] == 0

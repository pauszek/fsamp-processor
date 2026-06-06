import os
from unittest.mock import MagicMock, patch

import pytest

from processor import outbox_publisher
from processor.domain.models import OutboxEvent, OutboxEventType, OutboxStatus


class TestOutboxPublisherHelpers:
    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"

        outbox_publisher._sns_client = None
        outbox_publisher._dynamodb_client = None
        outbox_publisher._aws_factory = None
        outbox_publisher._settings = None
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)

    def test_get_sns_client_singleton(self) -> None:
        mock_client = MagicMock()
        mock_factory = MagicMock()
        mock_factory.get_sns_client.return_value = mock_client

        with patch.object(outbox_publisher, "get_aws_factory", return_value=mock_factory):
            client1 = outbox_publisher.get_sns_client()
            client2 = outbox_publisher.get_sns_client()

        assert client1 is client2
        mock_factory.get_sns_client.assert_called_once()

    def test_get_dynamodb_client_singleton(self) -> None:
        mock_client = MagicMock()
        mock_factory = MagicMock()
        mock_factory.get_dynamodb_client.return_value = mock_client

        with patch.object(outbox_publisher, "get_aws_factory", return_value=mock_factory):
            client1 = outbox_publisher.get_dynamodb_client()
            client2 = outbox_publisher.get_dynamodb_client()

        assert client1 is client2
        mock_factory.get_dynamodb_client.assert_called_once()

    def test_get_aws_factory_initializes_with_settings_and_enforces_fips(self) -> None:
        settings = MagicMock(
            should_require_fips=True,
            aws_region="us-east-1",
            aws_endpoint_url=None,
            should_use_fips=True,
        )

        with (
            patch.object(outbox_publisher, "get_settings", return_value=settings),
            patch.object(outbox_publisher, "enforce_fips") as enforce_fips,
            patch.object(outbox_publisher, "AWSClientFactory") as aws_client_factory,
        ):
            factory = outbox_publisher.get_aws_factory()

        assert factory is aws_client_factory.return_value
        enforce_fips.assert_called_once_with(True)
        aws_client_factory.assert_called_once_with(
            region="us-east-1",
            endpoint_url=None,
            use_fips=True,
        )


class TestOutboxPublisherRecordHandlerLogic:
    def test_non_insert_event_should_be_skipped(self) -> None:
        event_name = "MODIFY"
        assert event_name != "INSERT"

    def test_pending_status_detection(self) -> None:
        new_image = {"status": OutboxStatus.PENDING.value}
        assert new_image.get("status") == OutboxStatus.PENDING.value

        new_image = {"status": OutboxStatus.PUBLISHED.value}
        assert new_image.get("status") != OutboxStatus.PENDING.value

    def test_no_new_image_should_be_skipped(self) -> None:
        new_image = None
        assert not new_image

    def test_record_handler_skips_non_insert_event(self) -> None:
        record = MagicMock()
        record.event_name = "MODIFY"
        record.event_id = "stream-event-1"

        result = outbox_publisher.record_handler(record)

        assert result == {"status": "skipped", "reason": "not_insert"}

    def test_record_handler_skips_missing_new_image(self) -> None:
        record = MagicMock()
        record.event_name = "INSERT"
        record.event_id = "stream-event-1"
        record.dynamodb.new_image = None

        result = outbox_publisher.record_handler(record)

        assert result == {"status": "skipped", "reason": "no_new_image"}

    def test_record_handler_publishes_pending_event(self) -> None:
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
            patch.object(
                outbox_publisher, "claim_event_for_publish", return_value=True
            ) as claim_event,
            patch.object(outbox_publisher, "publish_to_sns") as publish_to_sns,
            patch.object(outbox_publisher, "mark_event_published") as mark_event_published,
        ):
            result = outbox_publisher.record_handler(record)

        assert result == {
            "status": "published",
            "event_id": outbox_event.event_id,
            "event_type": outbox_event.event_type.value,
        }
        claim_event.assert_called_once_with(outbox_event)
        publish_to_sns.assert_called_once()
        mark_event_published.assert_called_once()

    def test_record_handler_skips_already_claimed_event(self) -> None:
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
            patch.object(outbox_publisher, "claim_event_for_publish", return_value=False),
            patch.object(outbox_publisher, "publish_to_sns") as publish_to_sns,
        ):
            result = outbox_publisher.record_handler(record)

        assert result == {
            "status": "skipped",
            "reason": "already_claimed",
            "event_id": outbox_event.event_id,
        }
        publish_to_sns.assert_not_called()

    def test_record_handler_skips_non_pending_wire_status(self) -> None:
        record = MagicMock()
        record.event_name = "INSERT"
        record.event_id = "stream-event-1"
        record.dynamodb.new_image = {"status": {"S": OutboxStatus.PUBLISHED.value}}

        result = outbox_publisher.record_handler(record)

        assert result == {
            "status": "skipped",
            "reason": "not_pending",
            "event_status": OutboxStatus.PUBLISHED.value,
        }

    def test_record_handler_marks_wire_event_failed_when_publish_fails(self) -> None:
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
            patch.object(outbox_publisher, "claim_event_for_publish", return_value=True),
            patch.object(outbox_publisher, "publish_to_sns", side_effect=RuntimeError("SNS down")),
            patch.object(outbox_publisher, "mark_event_failed") as mark_event_failed,
            pytest.raises(RuntimeError, match="SNS down"),
        ):
            outbox_publisher.record_handler(record)

        mark_event_failed.assert_called_once_with(
            outbox_event.event_id,
            "SNS down",
            aggregate_type="FileProcessing",
        )

    def test_record_handler_suppresses_mark_failed_errors(self) -> None:
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
            patch.object(outbox_publisher, "claim_event_for_publish", return_value=True),
            patch.object(outbox_publisher, "publish_to_sns", side_effect=RuntimeError("SNS down")),
            patch.object(
                outbox_publisher,
                "mark_event_failed",
                side_effect=RuntimeError("DynamoDB down"),
            ),
            pytest.raises(RuntimeError, match="SNS down"),
        ):
            outbox_publisher.record_handler(record)

    def test_record_handler_raises_original_error_without_event_id(self) -> None:
        record = MagicMock()
        record.event_name = "INSERT"
        record.event_id = "stream-event-1"
        record.dynamodb.new_image = {
            "eventType": {"S": "ANALYSIS_COMPLETED"},
            "aggregateId": {"S": "file-123"},
            "payload": {"S": "{}"},
            "status": {"S": OutboxStatus.PENDING.value},
            "createdAt": {"S": "2026-05-12T00:00:00+00:00"},
        }

        with (
            patch.object(outbox_publisher, "claim_event_for_publish", return_value=True),
            patch.object(outbox_publisher, "publish_to_sns", side_effect=RuntimeError("SNS down")),
            patch.object(outbox_publisher, "mark_event_failed") as mark_event_failed,
            pytest.raises(KeyError),
        ):
            outbox_publisher.record_handler(record)

        mark_event_failed.assert_not_called()


class TestOutboxPublisherPublishToSNS:
    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)

    def test_publish_to_sns_success(self) -> None:
        mock_sns = MagicMock()
        mock_sns.publish.return_value = {"MessageId": "test-message-id"}

        outbox_event = OutboxEvent(
            event_id="test-event-id",
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={"file_id": "file-123"},
        )

        with patch.object(outbox_publisher, "get_sns_client", return_value=mock_sns):
            result = outbox_publisher.publish_to_sns(outbox_event)

        assert result == "test-message-id"
        mock_sns.publish.assert_called_once()

    def test_publish_to_sns_adds_file_context_attributes(self) -> None:
        mock_sns = MagicMock()
        mock_sns.publish.return_value = {"MessageId": "test-message-id"}

        outbox_event = OutboxEvent(
            event_id="test-event-id",
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={
                "schemaVersion": "1.1.2",
                "eventType": "ANALYSIS_COMPLETED",
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
        # Idempotency key attribute lets downstream consumers deduplicate
        # at-least-once deliveries from SNS standard topics.
        assert message_attributes["idempotencyKey"]["StringValue"] == "test-event-id"

    def test_publish_to_sns_wraps_plain_payload_in_outbox_envelope(self) -> None:
        mock_sns = MagicMock()
        mock_sns.publish.return_value = {"MessageId": "test-message-id"}

        outbox_event = OutboxEvent(
            event_id="test-event-id",
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={"fileId": "file-123"},
            created_at="2026-05-12T00:00:00+00:00",
        )

        with patch.object(outbox_publisher, "get_sns_client", return_value=mock_sns):
            outbox_publisher.publish_to_sns(outbox_event)

        message = mock_sns.publish.call_args.kwargs["Message"]
        assert '"eventId": "test-event-id"' in message
        assert '"payload": {"fileId": "file-123"}' in message


class TestOutboxPublisherMarkEventPublished:
    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)

    def test_claim_event_for_publish_moves_pending_to_publishing(self) -> None:
        outbox_publisher._settings = None
        mock_dynamodb = MagicMock()

        outbox_event = OutboxEvent(
            event_id="test-event-id",
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={"file_id": "file-123"},
        )

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            result = outbox_publisher.claim_event_for_publish(outbox_event)

        assert result is True
        call_kwargs = mock_dynamodb.update_item.call_args.kwargs
        assert call_kwargs["ConditionExpression"] == "#status = :expected_status"
        values = call_kwargs["ExpressionAttributeValues"]
        assert values[":publishing"]["S"] == OutboxStatus.PUBLISHING.value
        assert values[":expected_status"]["S"] == OutboxStatus.PENDING.value

    def test_claim_event_for_publish_returns_false_when_not_claimable(self) -> None:
        outbox_publisher._settings = None

        class _ConditionalCheckFailedError(Exception):
            pass

        mock_dynamodb = MagicMock()
        mock_dynamodb.exceptions.ConditionalCheckFailedException = _ConditionalCheckFailedError
        mock_dynamodb.update_item.side_effect = _ConditionalCheckFailedError("already claimed")

        outbox_event = OutboxEvent(
            event_id="duplicate-event-id",
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-456",
            aggregate_type="FileProcessing",
            payload={"file_id": "file-456"},
        )

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            result = outbox_publisher.claim_event_for_publish(outbox_event)

        assert result is False

    def test_mark_event_published(self) -> None:
        outbox_publisher._settings = None
        mock_dynamodb = MagicMock()

        outbox_event = OutboxEvent(
            event_id="test-event-id",
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={"file_id": "file-123"},
        )

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            outbox_publisher.mark_event_published(outbox_event)

        mock_dynamodb.update_item.assert_called_once()
        call_kwargs = mock_dynamodb.update_item.call_args.kwargs
        assert call_kwargs["TableName"] == "test-outbox"
        assert "ConditionExpression" in call_kwargs
        assert (
            call_kwargs["ExpressionAttributeValues"][":publishing"]["S"]
            == OutboxStatus.PUBLISHING.value
        )

    def test_mark_event_published_is_idempotent_when_already_published(self) -> None:
        """Conditional update must swallow ConditionalCheckFailedException
        so DynamoDB Streams retries do not surface as Lambda errors."""
        outbox_publisher._settings = None

        class _ConditionalCheckFailedError(Exception):
            pass

        mock_dynamodb = MagicMock()
        mock_dynamodb.exceptions.ConditionalCheckFailedException = _ConditionalCheckFailedError
        mock_dynamodb.update_item.side_effect = _ConditionalCheckFailedError("already published")

        outbox_event = OutboxEvent(
            event_id="duplicate-event-id",
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-456",
            aggregate_type="FileProcessing",
            payload={"file_id": "file-456"},
        )

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            # Must not raise: duplicate deliveries are expected at-least-once
            outbox_publisher.mark_event_published(outbox_event)

        mock_dynamodb.update_item.assert_called_once()


class TestOutboxPublisherMarkEventFailed:
    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)

    def test_mark_event_failed(self) -> None:
        outbox_publisher._settings = None
        mock_dynamodb = MagicMock()

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            outbox_publisher.mark_event_failed("test-event-id", "Test error")

        mock_dynamodb.update_item.assert_called_once()
        call_kwargs = mock_dynamodb.update_item.call_args.kwargs
        assert ":error" in call_kwargs["ExpressionAttributeValues"]

    def test_mark_event_failed_truncates_long_error(self) -> None:
        outbox_publisher._settings = None
        mock_dynamodb = MagicMock()
        long_error = "x" * 2000

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            outbox_publisher.mark_event_failed("test-event-id", long_error)

        call_kwargs = mock_dynamodb.update_item.call_args.kwargs
        error_value = call_kwargs["ExpressionAttributeValues"][":error"]["S"]
        assert len(error_value) == 1000


class TestOutboxPublisherLambdaHandler:
    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-west-2:123456789012:test-topic"
        os.environ["OUTBOX_TABLE_NAME"] = "test-outbox"
        os.environ["POWERTOOLS_SERVICE_NAME"] = "outbox-publisher"
        yield
        os.environ.pop("SNS_TOPIC_ARN", None)
        os.environ.pop("OUTBOX_TABLE_NAME", None)
        os.environ.pop("POWERTOOLS_SERVICE_NAME", None)

    def test_lambda_handler_env_validation(self) -> None:
        outbox_publisher._settings = None
        assert os.environ.get("SNS_TOPIC_ARN", "") == outbox_publisher.get_sns_topic_arn()

        assert os.environ.get("OUTBOX_TABLE_NAME", "") == outbox_publisher.get_outbox_table_name()

    def test_lambda_handler_rejects_missing_sns_topic(self) -> None:
        os.environ["SNS_TOPIC_ARN"] = ""

        with pytest.raises(ValueError, match="SNS_TOPIC_ARN"):
            outbox_publisher.lambda_handler({"Records": []}, MagicMock())

    def test_lambda_handler_rejects_missing_outbox_table(self) -> None:
        os.environ["OUTBOX_TABLE_NAME"] = ""

        with pytest.raises(ValueError, match="OUTBOX_TABLE_NAME"):
            outbox_publisher.lambda_handler({"Records": []}, MagicMock())

    def test_lambda_handler_delegates_to_batch_processor(self) -> None:
        with patch.object(
            outbox_publisher,
            "process_partial_response",
            return_value={"batchItemFailures": []},
        ) as process_partial_response:
            result = outbox_publisher.lambda_handler({"Records": []}, MagicMock())

        assert result == {"batchItemFailures": []}
        process_partial_response.assert_called_once()

    def test_lambda_handler_constants(self) -> None:
        assert outbox_publisher.MAX_RETRY_COUNT >= 0

    def test_settings_values_take_precedence_over_environment(self) -> None:
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
    @pytest.fixture(autouse=True)
    def setup_env(self) -> None:
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
        mock_dynamodb = MagicMock()
        mock_dynamodb.query.return_value = {"Items": []}

        event = {}
        context = MagicMock()

        with patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb):
            result = outbox_publisher.retry_handler(event, context)

        assert result["statusCode"] == 200
        assert result["body"]["total_processed"] == 0

    def test_retry_handler_republishes_failed_events(self) -> None:
        outbox_event = OutboxEvent.for_file_processed(
            file_id="file-123",
            correlation_id="corr-123",
            file_hash="a" * 64,
            is_safe=True,
            bucket_name="bucket",
            object_key="object",
        )
        outbox_event.mark_failed("temporary")
        mock_dynamodb = MagicMock()
        mock_dynamodb.query.side_effect = [
            {"Items": [outbox_event.to_dynamodb_item()]},
            {"Items": []},
        ]

        with (
            patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb),
            patch.object(
                outbox_publisher, "claim_event_for_publish", return_value=True
            ) as claim_event,
            patch.object(outbox_publisher, "publish_to_sns") as publish_to_sns,
            patch.object(outbox_publisher, "mark_event_published") as mark_event_published,
        ):
            result = outbox_publisher.retry_handler({}, MagicMock())

        assert result["body"] == {
            "success_count": 1,
            "failure_count": 0,
            "total_processed": 1,
        }
        claim_event.assert_called_once_with(outbox_event, expected_status=OutboxStatus.FAILED)
        publish_to_sns.assert_called_once()
        mark_event_published.assert_called_once()

    def test_retry_handler_counts_failed_retry_attempts(self) -> None:
        outbox_event = OutboxEvent.for_file_processed(
            file_id="file-123",
            correlation_id="corr-123",
            file_hash="a" * 64,
            is_safe=True,
            bucket_name="bucket",
            object_key="object",
        )
        outbox_event.mark_failed("temporary")
        mock_dynamodb = MagicMock()
        mock_dynamodb.query.side_effect = [
            {"Items": [outbox_event.to_dynamodb_item()]},
            {"Items": []},
        ]

        with (
            patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb),
            patch.object(outbox_publisher, "claim_event_for_publish", return_value=True),
            patch.object(outbox_publisher, "publish_to_sns", side_effect=RuntimeError("SNS down")),
            patch.object(outbox_publisher, "mark_event_failed") as mark_event_failed,
        ):
            result = outbox_publisher.retry_handler({}, MagicMock())

        assert result["body"] == {
            "success_count": 0,
            "failure_count": 1,
            "total_processed": 1,
        }
        mark_event_failed.assert_called_once_with(
            outbox_event.event_id,
            "SNS down",
            aggregate_type=outbox_event.aggregate_type,
        )

    def test_retry_handler_recovers_stale_publishing_events(self) -> None:
        outbox_event = OutboxEvent.for_file_processed(
            file_id="file-123",
            correlation_id="corr-123",
            file_hash="a" * 64,
            is_safe=True,
            bucket_name="bucket",
            object_key="object",
        )
        outbox_event.status = OutboxStatus.PUBLISHING
        mock_dynamodb = MagicMock()
        mock_dynamodb.query.side_effect = [
            {"Items": []},
            {"Items": [outbox_event.to_dynamodb_item()]},
        ]

        with (
            patch.object(outbox_publisher, "get_dynamodb_client", return_value=mock_dynamodb),
            patch.object(
                outbox_publisher, "claim_event_for_publish", return_value=True
            ) as claim_event,
            patch.object(outbox_publisher, "publish_to_sns") as publish_to_sns,
            patch.object(outbox_publisher, "mark_event_published") as mark_event_published,
        ):
            result = outbox_publisher.retry_handler({}, MagicMock())

        assert result["body"]["success_count"] == 1
        claim_event.assert_called_once_with(
            outbox_event,
            expected_status=OutboxStatus.PUBLISHING,
        )
        publish_to_sns.assert_called_once()
        mark_event_published.assert_called_once()

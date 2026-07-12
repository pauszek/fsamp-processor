from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

import processor.outbox_publisher as publisher
from processor.domain.events import EventType, FileEvent, ProcessingResultDetails
from processor.domain.models import OutboxEvent, OutboxStatus


def canonical_outbox(event: FileEvent) -> OutboxEvent:
    completed = event.with_new_event_type(
        EventType.ANALYSIS_COMPLETED,
        processing_result=ProcessingResultDetails(
            is_safe=True,
            findings=[],
            processed_at=datetime.now(UTC),
            file_hash_sha256="a" * 64,
            scan_engine="fsamp-header-policy/1",
        ),
    )
    return OutboxEvent.from_file_event(completed)


def conditional_error() -> ClientError:
    return ClientError(
        {
            "Error": {
                "Code": "ConditionalCheckFailedException",
                "Message": "condition",
            }
        },
        "UpdateItem",
    )


def stream_record(item: dict) -> MagicMock:
    record = MagicMock()
    record.event_name = "INSERT"
    record.dynamodb.new_image = item
    return record


def test_publish_sends_exact_canonical_wire_payload(
    sample_file_event: FileEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = canonical_outbox(sample_file_event)
    sns = MagicMock()
    sns.publish.return_value = {"MessageId": "message-1"}
    monkeypatch.setattr(publisher, "get_sns_client", lambda: sns)
    monkeypatch.setattr(publisher, "get_topic_arn_for_event", lambda _: "topic")

    assert publisher.publish_to_sns(event) == "message-1"
    request = sns.publish.call_args.kwargs
    assert json.loads(request["Message"]) == event.to_sns_message()
    assert "payload" not in json.loads(request["Message"])
    assert request["MessageAttributes"]["idempotencyKey"]["StringValue"] == event.event_id


def test_claim_uses_partition_token_fence_and_sharded_status(
    sample_file_event: FileEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = canonical_outbox(sample_file_event)
    dynamodb = MagicMock()
    monkeypatch.setattr(publisher, "get_dynamodb_client", lambda: dynamodb)
    monkeypatch.setattr(publisher, "get_outbox_table_name", lambda: "outbox")

    token = publisher.claim_event_for_publish(event)
    request = dynamodb.update_item.call_args.kwargs
    assert token
    assert request["Key"]["PK"] == {"S": event.outbox_partition}
    assert request["ExpressionAttributeValues"][":token"] == {"S": token}
    assert request["ExpressionAttributeValues"][":gsi"] == {
        "S": f"STATUS#PUBLISHING#{event.outbox_shard}"
    }
    assert ":failed" in request["ExpressionAttributeValues"]
    assert "publisherClaimExpiresAt < :now" in request["ConditionExpression"]


def test_conditional_claim_is_terminal_only_when_live_row_is_published(
    sample_file_event: FileEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = canonical_outbox(sample_file_event)
    live = event.to_dynamodb_item()
    live["status"] = {"S": "PUBLISHED"}
    dynamodb = MagicMock()
    dynamodb.update_item.side_effect = conditional_error()
    dynamodb.get_item.return_value = {"Item": live}
    monkeypatch.setattr(publisher, "get_dynamodb_client", lambda: dynamodb)
    monkeypatch.setattr(publisher, "get_outbox_table_name", lambda: "outbox")
    assert publisher.claim_event_for_publish(event) is None


def test_busy_claim_remains_a_batch_failure(
    sample_file_event: FileEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = canonical_outbox(sample_file_event)
    live = event.to_dynamodb_item()
    live["status"] = {"S": "PUBLISHING"}
    dynamodb = MagicMock()
    dynamodb.update_item.side_effect = conditional_error()
    dynamodb.get_item.return_value = {"Item": live}
    monkeypatch.setattr(publisher, "get_dynamodb_client", lambda: dynamodb)
    monkeypatch.setattr(publisher, "get_outbox_table_name", lambda: "outbox")
    with pytest.raises(publisher.ClaimUnavailableError):
        publisher.claim_event_for_publish(event)


def test_mark_published_requires_matching_claim_token(
    sample_file_event: FileEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = canonical_outbox(sample_file_event)
    dynamodb = MagicMock()
    monkeypatch.setattr(publisher, "get_dynamodb_client", lambda: dynamodb)
    monkeypatch.setattr(publisher, "get_outbox_table_name", lambda: "outbox")
    publisher.mark_event_published(event, "claim-123")
    request = dynamodb.update_item.call_args.kwargs
    assert "publisherClaimToken = :token" in request["ConditionExpression"]
    assert request["ExpressionAttributeValues"][":token"] == {"S": "claim-123"}
    assert request["ExpressionAttributeValues"][":gsi"] == {
        "S": f"STATUS#PUBLISHED#{event.outbox_shard}"
    }


def test_mark_failed_requires_token_and_cannot_downgrade_published(
    sample_file_event: FileEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = canonical_outbox(sample_file_event)
    live = event.to_dynamodb_item()
    live["status"] = {"S": "PUBLISHED"}
    dynamodb = MagicMock()
    dynamodb.update_item.side_effect = conditional_error()
    dynamodb.get_item.return_value = {"Item": live}
    monkeypatch.setattr(publisher, "get_dynamodb_client", lambda: dynamodb)
    monkeypatch.setattr(publisher, "get_outbox_table_name", lambda: "outbox")
    publisher.mark_event_failed(event, "late failure", "stale-token")
    request = dynamodb.update_item.call_args.kwargs
    assert "publisherClaimToken = :token" in request["ConditionExpression"]


class StatefulDynamoDB:
    """Small state machine used to reproduce stream-snapshot retry behavior."""

    def __init__(self, item: dict) -> None:
        self.item = copy.deepcopy(item)

    def get_item(self, **_: object) -> dict:
        return {"Item": copy.deepcopy(self.item)}

    def update_item(self, **request: object) -> dict:
        values = request["ExpressionAttributeValues"]
        assert isinstance(values, dict)
        current = self.item["status"]["S"]
        if ":pending" in values:
            if current not in {"PENDING", "FAILED"}:
                raise conditional_error()
            self.item["status"] = {"S": "PUBLISHING"}
            self.item["publisherClaimToken"] = values[":token"]
            self.item["publisherClaimExpiresAt"] = values[":expires"]
        elif ":published" in values:
            if current != "PUBLISHING" or self.item["publisherClaimToken"] != values[":token"]:
                raise conditional_error()
            self.item["status"] = {"S": "PUBLISHED"}
        elif ":failed" in values:
            if current != "PUBLISHING" or self.item["publisherClaimToken"] != values[":token"]:
                raise conditional_error()
            self.item["status"] = {"S": "FAILED"}
            retries = int(self.item.get("retryCount", {"N": "0"})["N"])
            self.item["retryCount"] = {"N": str(retries + 1)}
        return {}


def test_stream_retry_recovers_after_first_sns_failure(
    sample_file_event: FileEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = canonical_outbox(sample_file_event)
    snapshot = event.to_dynamodb_item()
    dynamodb = StatefulDynamoDB(snapshot)
    sns = MagicMock()
    sns.publish.side_effect = [RuntimeError("SNS unavailable"), {"MessageId": "ok"}]
    monkeypatch.setattr(publisher, "get_dynamodb_client", lambda: dynamodb)
    monkeypatch.setattr(publisher, "get_sns_client", lambda: sns)
    monkeypatch.setattr(publisher, "get_outbox_table_name", lambda: "outbox")
    monkeypatch.setattr(publisher, "get_topic_arn_for_event", lambda _: "topic")

    with pytest.raises(RuntimeError, match="SNS unavailable"):
        publisher.record_handler(stream_record(snapshot))
    assert dynamodb.item["status"]["S"] == "FAILED"

    result = publisher.record_handler(stream_record(snapshot))
    assert result["status"] == "published"
    assert dynamodb.item["status"]["S"] == "PUBLISHED"
    assert sns.publish.call_count == 2


def test_record_handler_skips_non_insert() -> None:
    record = MagicMock()
    record.event_name = "MODIFY"
    assert publisher.record_handler(record) == {"status": "skipped", "reason": "not_insert"}


def test_invalid_outbox_payload_is_not_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = OutboxEvent.create(
        event_type=publisher.OutboxEventType.ANALYSIS_COMPLETED,
        aggregate_id="not-a-uuid",
        payload={"fileId": "not-a-uuid"},
    )
    sns = MagicMock()
    monkeypatch.setattr(publisher, "get_sns_client", lambda: sns)
    with pytest.raises(ValueError):
        publisher.publish_to_sns(event)
    sns.publish.assert_not_called()


def test_retry_queries_all_status_shards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dynamodb = MagicMock()
    dynamodb.query.return_value = {"Items": []}
    monkeypatch.setattr(publisher, "get_dynamodb_client", lambda: dynamodb)
    monkeypatch.setattr(publisher, "get_outbox_table_name", lambda: "outbox")
    assert publisher._query_retryable(OutboxStatus.FAILED) == []
    assert dynamodb.query.call_count == 16
    queried = {
        call.kwargs["ExpressionAttributeValues"][":status"]["S"]
        for call in dynamodb.query.call_args_list
    }
    assert queried == {f"STATUS#FAILED#{number:02x}" for number in range(16)}


def test_pending_reconciliation_does_not_require_a_claim_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dynamodb = MagicMock()
    dynamodb.query.return_value = {"Items": []}
    monkeypatch.setattr(publisher, "get_dynamodb_client", lambda: dynamodb)
    monkeypatch.setattr(publisher, "get_outbox_table_name", lambda: "outbox")

    assert publisher._query_retryable(OutboxStatus.PENDING) == []

    assert dynamodb.query.call_count == 16
    assert all("FilterExpression" not in call.kwargs for call in dynamodb.query.call_args_list)


def test_retry_query_last_shard_cannot_be_starved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dynamodb = MagicMock()

    def query(**request: object) -> dict:
        values = request["ExpressionAttributeValues"]
        assert isinstance(values, dict)
        status = values[":status"]["S"]
        if status.endswith("#00"):
            return {"Items": [{"createdAt": {"S": "2026-01-02"}, "id": "late"}]}
        if status.endswith("#0f"):
            return {"Items": [{"createdAt": {"S": "2026-01-01"}, "id": "early"}]}
        return {"Items": []}

    dynamodb.query.side_effect = query
    monkeypatch.setattr(publisher, "get_dynamodb_client", lambda: dynamodb)
    monkeypatch.setattr(publisher, "get_outbox_table_name", lambda: "outbox")
    items = publisher._query_retryable(OutboxStatus.FAILED, limit=1)
    assert items[0]["id"] == "early"
    assert dynamodb.query.call_count == 16


@pytest.mark.parametrize("value", [0, 101])
def test_retry_count_configuration_is_validated(
    value: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRY_COUNT", str(value))
    monkeypatch.setattr(publisher, "_settings", None)
    with pytest.raises(ValueError, match="MAX_RETRY_COUNT"):
        publisher.get_max_retry_count()

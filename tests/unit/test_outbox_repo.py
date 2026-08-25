from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from processor.adapters.outbound.outbox_repo import DynamoDBOutboxRepository
from processor.domain.events import EventType, FileEvent, ProcessingResultDetails
from processor.domain.exceptions import StorageError
from processor.domain.models import (
    MetadataRecord,
    OutboxEvent,
    ProcessingClaim,
    ProcessingStatus,
)


@pytest.fixture
def client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def repo(client: MagicMock) -> DynamoDBOutboxRepository:
    return DynamoDBOutboxRepository(client, "metadata", "outbox")


@pytest.fixture
def record(sample_file_event: FileEvent) -> MetadataRecord:
    return MetadataRecord(
        file_id=sample_file_event.file_id_str,
        timestamp=datetime.now(UTC).isoformat(),
        correlation_id=sample_file_event.correlation_id_str,
        original_filename=sample_file_event.file_metadata.original_filename,
        file_size_bytes=sample_file_event.file_metadata.file_size_bytes,
        mime_type=sample_file_event.file_metadata.mime_type,
        bucket_name=sample_file_event.storage_location.bucket_name,
        object_key=sample_file_event.storage_location.object_key,
        status=ProcessingStatus.COMPLETED,
        checksum_sha256=sample_file_event.file_metadata.checksum_sha256,
        last_processed_event_id=sample_file_event.event_id_str,
    )


@pytest.fixture
def outbox(sample_file_event: FileEvent) -> OutboxEvent:
    completed = sample_file_event.with_new_event_type(
        EventType.ANALYSIS_COMPLETED,
        processing_result=ProcessingResultDetails(
            is_safe=True,
            findings=[],
            processed_at=datetime.now(UTC),
        ),
    )
    return OutboxEvent.from_file_event(completed)


@pytest.fixture
def claim(sample_file_event: FileEvent) -> ProcessingClaim:
    return ProcessingClaim(
        event_id=sample_file_event.event_id_str,
        token="claim-token",
        version=3,
        expires_at_epoch=1234567890,
    )


def transaction_cancelled() -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [{"Code": "None"}, {"Code": "ConditionalCheckFailed"}],
        },
        "TransactWriteItems",
    )


def internal_server_error() -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "InternalServerError", "Message": "retry"},
            "ResponseMetadata": {"HTTPStatusCode": 500},
        },
        "TransactWriteItems",
    )


def test_save_updates_shared_metadata_and_conditionally_puts_event(
    repo: DynamoDBOutboxRepository,
    client: MagicMock,
    record: MetadataRecord,
    outbox: OutboxEvent,
) -> None:
    repo.save_with_outbox(record, outbox)
    request = client.transact_write_items.call_args.kwargs
    assert request["ClientRequestToken"] == outbox.event_id
    transaction = request["TransactItems"]
    metadata_update = transaction[0]["Update"]
    event_put = transaction[1]["Put"]
    assert metadata_update["Key"]["SK"] == {"S": "METADATA"}
    assert "attribute_exists(PK)" in metadata_update["ConditionExpression"]
    assert event_put["Item"]["PK"] == {"S": outbox.outbox_partition}
    assert "attribute_not_exists(PK)" in event_put["ConditionExpression"]


def test_transient_retry_reuses_identical_idempotency_request(
    repo: DynamoDBOutboxRepository,
    client: MagicMock,
    record: MetadataRecord,
    outbox: OutboxEvent,
) -> None:
    client.transact_write_items.side_effect = [internal_server_error(), {}]

    repo.save_with_outbox(record, outbox)

    assert client.transact_write_items.call_count == 2
    first_request, second_request = client.transact_write_items.call_args_list
    assert first_request.kwargs == second_request.kwargs


def test_claimed_transaction_is_fenced_and_releases_the_lease(
    repo: DynamoDBOutboxRepository,
    client: MagicMock,
    record: MetadataRecord,
    outbox: OutboxEvent,
    claim: ProcessingClaim,
) -> None:
    repo.save_with_outbox(record, outbox, claim=claim)

    metadata_update = client.transact_write_items.call_args.kwargs["TransactItems"][0]["Update"]
    assert "#claimToken = :claimToken" in metadata_update["ConditionExpression"]
    assert "#claimVersion = :claimVersion" in metadata_update["ConditionExpression"]
    assert "REMOVE #claimToken" in metadata_update["UpdateExpression"]
    assert metadata_update["ExpressionAttributeNames"]["#claimToken"] == ("processorClaimToken")


def test_duplicate_transaction_is_idempotent_and_restores_metadata_state(
    repo: DynamoDBOutboxRepository,
    client: MagicMock,
    record: MetadataRecord,
    outbox: OutboxEvent,
) -> None:
    client.transact_write_items.side_effect = transaction_cancelled()
    client.get_item.return_value = {"Item": outbox.to_dynamodb_item()}
    repo.save_with_outbox(record, outbox)
    client.update_item.assert_called_once()
    assert client.update_item.call_args.kwargs["Key"]["SK"] == {"S": "METADATA"}


def test_transaction_cancellation_with_different_event_is_not_hidden(
    repo: DynamoDBOutboxRepository,
    client: MagicMock,
    record: MetadataRecord,
    outbox: OutboxEvent,
) -> None:
    client.transact_write_items.side_effect = transaction_cancelled()
    persisted = outbox.to_dynamodb_item()
    persisted["payload"] = {"S": "{}"}
    client.get_item.return_value = {"Item": persisted}
    with pytest.raises(StorageError):
        repo.save_with_outbox(record, outbox)


def test_mark_published_uses_sharded_status_and_long_retention(
    repo: DynamoDBOutboxRepository,
    client: MagicMock,
    outbox: OutboxEvent,
) -> None:
    repo.mark_published(
        outbox.event_id,
        aggregate_type=outbox.aggregate_type,
        aggregate_id=outbox.aggregate_id,
    )
    request = client.update_item.call_args.kwargs
    assert request["Key"]["PK"] == {"S": outbox.outbox_partition}
    assert request["ExpressionAttributeValues"][":gsi"] == {
        "S": f"STATUS#PUBLISHED#{outbox.outbox_shard}"
    }
    ttl = int(request["ExpressionAttributeValues"][":ttl"]["N"])
    assert ttl - int(datetime.now(UTC).timestamp()) > 24 * 60 * 60


def test_mark_failed_never_downgrades_published(
    repo: DynamoDBOutboxRepository,
    client: MagicMock,
    outbox: OutboxEvent,
) -> None:
    repo.mark_failed(
        outbox.event_id,
        "SNS down",
        aggregate_type=outbox.aggregate_type,
        aggregate_id=outbox.aggregate_id,
    )
    request = client.update_item.call_args.kwargs
    assert request["ConditionExpression"] == "#status <> :published"
    assert request["ExpressionAttributeValues"][":gsi"] == {
        "S": f"STATUS#FAILED#{outbox.outbox_shard}"
    }

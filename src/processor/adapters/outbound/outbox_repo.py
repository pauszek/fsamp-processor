"""Transactional DynamoDB outbox repository."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from math import ceil
from typing import Any

import structlog
from botocore.exceptions import ClientError

from processor.adapters.outbound.aws_retry import aws_retry
from processor.adapters.outbound.dynamodb_repo import (
    apply_claim_fence,
    build_metadata_update,
    is_conditional_failure,
)
from processor.domain.exceptions import StorageError
from processor.domain.models import (
    OUTBOX_DEFAULT_RETENTION_SECONDS,
    OUTBOX_SHARD_COUNT,
    MetadataRecord,
    OutboxEvent,
    OutboxStatus,
    ProcessingClaim,
    outbox_shard,
)
from processor.ports.outbound import OutboxRepository

logger = structlog.get_logger(__name__)

_STATUS_NAME = "#status"
_STATUS_VALUE = ":status"


class DynamoDBOutboxRepository(OutboxRepository):
    """Atomically update current metadata and insert one canonical event."""

    def __init__(
        self,
        dynamodb_client: Any,
        metadata_table_name: str,
        outbox_table_name: str,
        retention_seconds: int = OUTBOX_DEFAULT_RETENTION_SECONDS,
    ) -> None:
        self._client = dynamodb_client
        self._metadata_table_name = metadata_table_name
        self._outbox_table_name = outbox_table_name
        self._retention_seconds = retention_seconds

    def save_with_outbox(
        self,
        record: MetadataRecord,
        outbox_event: OutboxEvent,
        claim: ProcessingClaim | None = None,
    ) -> None:
        """Update ``FILE#id/METADATA`` and conditionally insert the outbox row."""
        update, names, values = build_metadata_update(record)
        condition = "attribute_exists(PK) AND attribute_exists(SK)"
        if claim is not None:
            update, names, values, claim_condition = apply_claim_fence(
                update,
                names,
                values,
                claim,
            )
            condition += f" AND {claim_condition}"
        outbox_item = outbox_event.to_dynamodb_item()
        transaction_request: dict[str, Any] = {
            "ClientRequestToken": outbox_event.event_id,
            "TransactItems": [
                {
                    "Update": {
                        "TableName": self._metadata_table_name,
                        "Key": {
                            "PK": {"S": f"FILE#{record.file_id}"},
                            "SK": {"S": "METADATA"},
                        },
                        "UpdateExpression": update,
                        "ExpressionAttributeNames": names,
                        "ExpressionAttributeValues": values,
                        "ConditionExpression": condition,
                    }
                },
                {
                    "Put": {
                        "TableName": self._outbox_table_name,
                        "Item": outbox_item,
                        "ConditionExpression": (
                            "attribute_not_exists(PK) AND attribute_not_exists(SK)"
                        ),
                    }
                },
            ],
        }
        try:
            self._transact_write(transaction_request)
        except ClientError as error:
            if self._is_idempotent_replay(error, outbox_event):
                # The first transaction already inserted the canonical event. A later
                # retry may still need to restore the current metadata state after a
                # PROCESSING update, so apply only the idempotent metadata update.
                try:
                    self._client.update_item(
                        TableName=self._metadata_table_name,
                        Key={
                            "PK": {"S": f"FILE#{record.file_id}"},
                            "SK": {"S": "METADATA"},
                        },
                        UpdateExpression=update,
                        ExpressionAttributeNames=names,
                        ExpressionAttributeValues=values,
                        ConditionExpression=condition,
                    )
                except ClientError as update_error:
                    if not (
                        claim is not None
                        and is_conditional_failure(update_error)
                        and self._metadata_has_result(record)
                    ):
                        raise StorageError(
                            message=f"Failed to restore idempotent metadata state: {update_error}",
                            storage_type="dynamodb",
                            operation="conditional_update",
                            resource=f"{self._metadata_table_name}/{record.file_id}",
                            cause=update_error,
                        ) from update_error
                logger.info(
                    "Idempotent outbox replay ignored",
                    event_id=outbox_event.event_id,
                    file_id=record.file_id,
                )
                return
            raise StorageError(
                message=f"Failed to save metadata with outbox: {error}",
                storage_type="dynamodb",
                operation="transact_write",
                resource=f"{self._metadata_table_name}/{record.file_id}",
                cause=error,
            ) from error

    @aws_retry()
    def _transact_write(self, request: dict[str, Any]) -> None:
        """Retry one byte-for-byte stable idempotent transaction request."""
        self._client.transact_write_items(**request)

    def _metadata_has_result(self, record: MetadataRecord) -> bool:
        try:
            response = self._client.get_item(
                TableName=self._metadata_table_name,
                Key={
                    "PK": {"S": f"FILE#{record.file_id}"},
                    "SK": {"S": "METADATA"},
                },
                ConsistentRead=True,
            )
        except ClientError:
            return False
        item = response.get("Item", {})
        return bool(
            item.get("status", {}).get("S") == record.status.value
            and item.get("lastProcessedEventId", {}).get("S") == record.last_processed_event_id
        )

    def _is_idempotent_replay(
        self,
        error: ClientError,
        outbox_event: OutboxEvent,
    ) -> bool:
        if error.response.get("Error", {}).get("Code") != "TransactionCanceledException":
            return False
        try:
            response = self._client.get_item(
                TableName=self._outbox_table_name,
                Key={
                    "PK": {"S": str(outbox_event.outbox_partition)},
                    "SK": {"S": f"EVENT#{outbox_event.event_id}"},
                },
                ConsistentRead=True,
            )
        except ClientError:
            return False
        item = response.get("Item")
        if not item:
            return False
        persisted = OutboxEvent.from_dynamodb_item(item)
        return (
            persisted.event_id == outbox_event.event_id
            and persisted.aggregate_id == outbox_event.aggregate_id
            and persisted.payload == outbox_event.payload
        )

    def _get_events(self, status: OutboxStatus, limit: int) -> list[OutboxEvent]:
        events: list[OutboxEvent] = []
        per_shard_limit = max(1, ceil(limit / OUTBOX_SHARD_COUNT))
        for shard_number in range(OUTBOX_SHARD_COUNT):
            shard = f"{shard_number:02x}"
            start_key: dict[str, Any] | None = None
            shard_count = 0
            while shard_count < per_shard_limit:
                request: dict[str, Any] = {
                    "TableName": self._outbox_table_name,
                    "IndexName": "GSI1",
                    "KeyConditionExpression": "GSI1PK = :status",
                    "ExpressionAttributeValues": {
                        _STATUS_VALUE: {"S": f"STATUS#{status.value}#{shard}"}
                    },
                    "ScanIndexForward": True,
                    "Limit": per_shard_limit - shard_count,
                }
                if start_key:
                    request["ExclusiveStartKey"] = start_key
                response = self._client.query(**request)
                page = [OutboxEvent.from_dynamodb_item(item) for item in response.get("Items", [])]
                events.extend(page)
                shard_count += len(page)
                start_key = response.get("LastEvaluatedKey")
                if not start_key:
                    break
        return sorted(events, key=lambda event: event.created_at)[:limit]

    @aws_retry()
    def get_pending_events(self, limit: int = 100) -> list[OutboxEvent]:
        return self._get_events(OutboxStatus.PENDING, limit)

    @aws_retry()
    def get_failed_events(self, limit: int = 100) -> list[OutboxEvent]:
        return self._get_events(OutboxStatus.FAILED, limit)

    def _outbox_key(
        self,
        event_id: str,
        aggregate_type: str,
        aggregate_id: str | None,
    ) -> dict[str, dict[str, str]]:
        partition = (
            f"OUTBOX#{aggregate_type}#{aggregate_id}"
            if aggregate_id is not None
            else f"OUTBOX#{aggregate_type}"
        )
        return {"PK": {"S": partition}, "SK": {"S": f"EVENT#{event_id}"}}

    @aws_retry()
    def mark_published(
        self,
        event_id: str,
        aggregate_type: str = "FileProcessing",
        aggregate_id: str | None = None,
    ) -> None:
        """Compatibility API; the stream publisher uses token-fenced updates."""
        now = datetime.now(UTC)
        shard = outbox_shard(aggregate_id) if aggregate_id else "00"
        try:
            self._client.update_item(
                TableName=self._outbox_table_name,
                Key=self._outbox_key(event_id, aggregate_type, aggregate_id),
                UpdateExpression=(
                    "SET #status = :published, publishedAt = :now, " "GSI1PK = :gsi, #ttl = :ttl"
                ),
                ExpressionAttributeNames={_STATUS_NAME: "status", "#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":published": {"S": OutboxStatus.PUBLISHED.value},
                    ":now": {"S": now.isoformat()},
                    ":gsi": {"S": f"STATUS#PUBLISHED#{shard}"},
                    ":ttl": {"N": str(int(now.timestamp()) + self._retention_seconds)},
                },
                ConditionExpression="#status <> :published",
            )
        except ClientError as error:
            raise StorageError(
                message=f"Failed to mark outbox event published: {error}",
                storage_type="dynamodb",
                operation="update",
                resource=f"{self._outbox_table_name}/{event_id}",
                cause=error,
            ) from error

    @aws_retry()
    def mark_failed(
        self,
        event_id: str,
        error: str,
        aggregate_type: str = "FileProcessing",
        aggregate_id: str | None = None,
    ) -> None:
        """Compatibility API that never downgrades a published event."""
        shard = outbox_shard(aggregate_id) if aggregate_id else "00"
        try:
            self._client.update_item(
                TableName=self._outbox_table_name,
                Key=self._outbox_key(event_id, aggregate_type, aggregate_id),
                UpdateExpression=(
                    "SET #status = :failed, lastError = :error, "
                    "retryCount = if_not_exists(retryCount, :zero) + :inc, GSI1PK = :gsi"
                ),
                ExpressionAttributeNames={_STATUS_NAME: "status"},
                ExpressionAttributeValues={
                    ":failed": {"S": OutboxStatus.FAILED.value},
                    ":published": {"S": OutboxStatus.PUBLISHED.value},
                    ":error": {"S": error[:2000]},
                    ":zero": {"N": "0"},
                    ":inc": {"N": "1"},
                    ":gsi": {"S": f"STATUS#FAILED#{shard}"},
                },
                ConditionExpression="#status <> :published",
            )
        except ClientError as client_error:
            raise StorageError(
                message=f"Failed to mark outbox event failed: {client_error}",
                storage_type="dynamodb",
                operation="update",
                resource=f"{self._outbox_table_name}/{event_id}",
                cause=client_error,
            ) from client_error

    @aws_retry()
    def delete_old_published(self, older_than_hours: int = 24) -> int:
        """Delete published rows across all status shards, following pagination."""
        cutoff = (datetime.now(UTC) - timedelta(hours=older_than_hours)).isoformat()
        keys: list[dict[str, Any]] = []
        for shard_number in range(OUTBOX_SHARD_COUNT):
            shard = f"{shard_number:02x}"
            start_key: dict[str, Any] | None = None
            while True:
                request: dict[str, Any] = {
                    "TableName": self._outbox_table_name,
                    "IndexName": "GSI1",
                    "KeyConditionExpression": "GSI1PK = :status AND GSI1SK < :cutoff",
                    "ExpressionAttributeValues": {
                        _STATUS_VALUE: {"S": f"STATUS#PUBLISHED#{shard}"},
                        ":cutoff": {"S": cutoff},
                    },
                    "ProjectionExpression": "PK, SK",
                }
                if start_key:
                    request["ExclusiveStartKey"] = start_key
                response = self._client.query(**request)
                keys.extend(response.get("Items", []))
                start_key = response.get("LastEvaluatedKey")
                if not start_key:
                    break

        for offset in range(0, len(keys), 25):
            pending = [
                {"DeleteRequest": {"Key": {"PK": item["PK"], "SK": item["SK"]}}}
                for item in keys[offset : offset + 25]
            ]
            while pending:
                response = self._client.batch_write_item(
                    RequestItems={self._outbox_table_name: pending}
                )
                pending = response.get("UnprocessedItems", {}).get(self._outbox_table_name, [])
        return len(keys)

    def update_metadata_with_outbox(
        self,
        file_id: str,
        timestamp: str,
        status: str,
        outbox_event: OutboxEvent,
        error_message: str | None = None,
    ) -> None:
        """Legacy helper retained with the shared fixed metadata key."""
        del timestamp
        now = datetime.now(UTC).isoformat()
        values: dict[str, Any] = {
            _STATUS_VALUE: {"S": status},
            ":updated": {"S": now},
        }
        update = "SET #status = :status, updatedAt = :updated"
        if error_message:
            update += ", errorMessage = :error"
            values[":error"] = {"S": error_message}
        self._client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": self._metadata_table_name,
                        "Key": {
                            "PK": {"S": f"FILE#{file_id}"},
                            "SK": {"S": "METADATA"},
                        },
                        "UpdateExpression": update,
                        "ExpressionAttributeNames": {_STATUS_NAME: "status"},
                        "ExpressionAttributeValues": values,
                    }
                },
                {
                    "Put": {
                        "TableName": self._outbox_table_name,
                        "Item": outbox_event.to_dynamodb_item(),
                        "ConditionExpression": (
                            "attribute_not_exists(PK) AND attribute_not_exists(SK)"
                        ),
                    }
                },
            ]
        )

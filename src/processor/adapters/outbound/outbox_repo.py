"""
DynamoDB implementation of the Outbox Pattern for reliable event publishing.

The Outbox Pattern ensures that database writes and event publishing are atomic.
Events are first written to the outbox as part of the same DynamoDB transaction
as the business data, then published asynchronously by a separate Lambda
triggered by DynamoDB Streams.

Architecture:
    FileProcessor -> [DynamoDB Transaction] -> Metadata + Outbox
                                                    |
                                            DynamoDB Streams
                                                    |
                                            Outbox Publisher Lambda -> SNS
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from botocore.exceptions import ClientError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from processor.domain.exceptions import StorageError
from processor.domain.models import MetadataRecord, OutboxEvent, OutboxStatus
from processor.ports.outbound import OutboxRepository

logger = structlog.get_logger(__name__)


class DynamoDBOutboxRepository(OutboxRepository):
    """
    DynamoDB implementation of Outbox Repository.

    Uses DynamoDB transactions for atomic writes of metadata and outbox events.
    The outbox events are stored in a separate table that triggers DynamoDB Streams
    for the outbox publisher Lambda.

    Table Design (single-table design for metadata, separate for outbox):

    Metadata Table:
        PK: FILE#<file_id>
        SK: TS#<timestamp>
        GSI1PK: STATUS#<status>
        GSI1SK: <timestamp>

    Outbox Table:
        PK: OUTBOX#<aggregate_type>
        SK: EVENT#<event_id>
        GSI1PK: STATUS#<status>
        GSI1SK: <created_at>
    """

    def __init__(
        self,
        dynamodb_client: Any,
        metadata_table_name: str,
        outbox_table_name: str,
    ) -> None:
        """
        Initialize DynamoDB Outbox Repository.

        Args:
            dynamodb_client: Boto3 DynamoDB client.
            metadata_table_name: Name of the metadata table.
            outbox_table_name: Name of the outbox table.
        """
        self._client = dynamodb_client
        self._metadata_table_name = metadata_table_name
        self._outbox_table_name = outbox_table_name
        logger.debug(
            "DynamoDB outbox repository initialized",
            metadata_table=metadata_table_name,
            outbox_table=outbox_table_name,
        )

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def save_with_outbox(
        self,
        record: MetadataRecord,
        outbox_event: OutboxEvent,
    ) -> None:
        """
        Save metadata record and outbox event in a single transaction.

        This uses DynamoDB TransactWriteItems to ensure atomicity.
        Either both writes succeed, or both fail.
        """
        log = logger.bind(
            file_id=record.file_id,
            event_id=outbox_event.event_id,
            event_type=outbox_event.event_type,
        )

        try:
            log.info("Saving metadata with outbox event (transactional)")

            metadata_item = record.to_dynamodb_item()
            outbox_item = outbox_event.to_dynamodb_item()

            self._client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": self._metadata_table_name,
                            "Item": metadata_item,
                            "ConditionExpression": "attribute_not_exists(PK) OR attribute_not_exists(SK)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": self._outbox_table_name,
                            "Item": outbox_item,
                        }
                    },
                ]
            )

            log.info(
                "Transactional write succeeded",
                metadata_pk=metadata_item["PK"]["S"],
                outbox_pk=outbox_item["PK"]["S"],
            )

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")

            if error_code == "TransactionCanceledException":
                reasons = e.response.get("CancellationReasons", [])
                log.warning(
                    "Transaction cancelled",
                    reasons=[r.get("Code") for r in reasons],
                )

                if any(r.get("Code") == "ConditionalCheckFailed" for r in reasons):
                    log.info("Metadata record already exists, likely duplicate event")
                    return

            log.exception("Failed to save with outbox")
            raise StorageError(
                message=f"Failed to save with outbox: {e}",
                storage_type="dynamodb",
                operation="transact_write",
                resource=f"{self._metadata_table_name}/{record.file_id}",
                cause=e,
            ) from e

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def get_pending_events(
        self,
        limit: int = 100,
    ) -> list[OutboxEvent]:
        """Get pending outbox events for publishing."""
        log = logger.bind(limit=limit)

        try:
            response = self._client.query(
                TableName=self._outbox_table_name,
                IndexName="GSI1",
                KeyConditionExpression="GSI1PK = :status",
                ExpressionAttributeValues={
                    ":status": {"S": f"STATUS#{OutboxStatus.PENDING.value}"},
                },
                ScanIndexForward=True,  # Oldest first (FIFO processing)
                Limit=limit,
            )

            items = response.get("Items", [])
            events = [OutboxEvent.from_dynamodb_item(item) for item in items]

            log.debug("Retrieved pending outbox events", count=len(events))
            return events

        except ClientError as e:
            log.exception("Failed to get pending events")
            raise StorageError(
                message=f"Failed to get pending events: {e}",
                storage_type="dynamodb",
                operation="query",
                resource=self._outbox_table_name,
                cause=e,
            ) from e

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def mark_published(
        self,
        event_id: str,
        aggregate_type: str = "FileProcessing",
    ) -> None:
        """Mark an outbox event as published."""
        log = logger.bind(event_id=event_id)

        try:
            now = datetime.now(UTC).isoformat()
            ttl = int((datetime.now(UTC) + timedelta(hours=24)).timestamp())

            self._client.update_item(
                TableName=self._outbox_table_name,
                Key={
                    "PK": {"S": f"OUTBOX#{aggregate_type}"},
                    "SK": {"S": f"EVENT#{event_id}"},
                },
                UpdateExpression=(
                    "SET #status = :status, publishedAt = :published, GSI1PK = :gsi1pk, #ttl = :ttl"
                ),
                ExpressionAttributeNames={
                    "#status": "status",
                    "#ttl": "ttl",
                },
                ExpressionAttributeValues={
                    ":status": {"S": OutboxStatus.PUBLISHED.value},
                    ":published": {"S": now},
                    ":gsi1pk": {"S": f"STATUS#{OutboxStatus.PUBLISHED.value}"},
                    ":ttl": {"N": str(ttl)},
                },
            )

            log.info("Outbox event marked as published")

        except ClientError as e:
            log.exception("Failed to mark event as published")
            raise StorageError(
                message=f"Failed to mark event published: {e}",
                storage_type="dynamodb",
                operation="update",
                resource=f"{self._outbox_table_name}/{event_id}",
                cause=e,
            ) from e

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def mark_failed(
        self,
        event_id: str,
        error: str,
        aggregate_type: str = "FileProcessing",
    ) -> None:
        """Mark an outbox event as failed."""
        log = logger.bind(event_id=event_id, error=error)

        try:
            self._client.update_item(
                TableName=self._outbox_table_name,
                Key={
                    "PK": {"S": f"OUTBOX#{aggregate_type}"},
                    "SK": {"S": f"EVENT#{event_id}"},
                },
                UpdateExpression=(
                    "SET #status = :status, lastError = :error, "
                    "retryCount = retryCount + :inc, GSI1PK = :gsi1pk"
                ),
                ExpressionAttributeNames={
                    "#status": "status",
                },
                ExpressionAttributeValues={
                    ":status": {"S": OutboxStatus.FAILED.value},
                    ":error": {"S": error},
                    ":inc": {"N": "1"},
                    ":gsi1pk": {"S": f"STATUS#{OutboxStatus.FAILED.value}"},
                },
            )

            log.warning("Outbox event marked as failed")

        except ClientError as e:
            log.exception("Failed to mark event as failed")
            raise StorageError(
                message=f"Failed to mark event failed: {e}",
                storage_type="dynamodb",
                operation="update",
                resource=f"{self._outbox_table_name}/{event_id}",
                cause=e,
            ) from e

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def get_failed_events(
        self,
        limit: int = 100,
    ) -> list[OutboxEvent]:
        """Get failed outbox events for retry."""
        log = logger.bind(limit=limit)

        try:
            response = self._client.query(
                TableName=self._outbox_table_name,
                IndexName="GSI1",
                KeyConditionExpression="GSI1PK = :status",
                ExpressionAttributeValues={
                    ":status": {"S": f"STATUS#{OutboxStatus.FAILED.value}"},
                },
                ScanIndexForward=True,
                Limit=limit,
            )

            items = response.get("Items", [])
            events = [OutboxEvent.from_dynamodb_item(item) for item in items]

            log.debug("Retrieved failed outbox events", count=len(events))
            return events

        except ClientError as e:
            log.exception("Failed to get failed events")
            raise StorageError(
                message=f"Failed to get failed events: {e}",
                storage_type="dynamodb",
                operation="query",
                resource=self._outbox_table_name,
                cause=e,
            ) from e

    def delete_old_published(
        self,
        older_than_hours: int = 24,
    ) -> int:
        """
        Delete old published events (manual cleanup).

        Note: DynamoDB TTL should handle this automatically,
        but this provides manual cleanup capability.
        """
        log = logger.bind(older_than_hours=older_than_hours)
        deleted_count = 0

        try:
            cutoff = (datetime.now(UTC) - timedelta(hours=older_than_hours)).isoformat()

            response = self._client.query(
                TableName=self._outbox_table_name,
                IndexName="GSI1",
                KeyConditionExpression="GSI1PK = :status AND GSI1SK < :cutoff",
                ExpressionAttributeValues={
                    ":status": {"S": f"STATUS#{OutboxStatus.PUBLISHED.value}"},
                    ":cutoff": {"S": cutoff},
                },
                ProjectionExpression="PK, SK",
                Limit=1000,
            )

            items = response.get("Items", [])

            for i in range(0, len(items), 25):
                batch = items[i : i + 25]
                delete_requests = [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "PK": item["PK"],
                                "SK": item["SK"],
                            }
                        }
                    }
                    for item in batch
                ]

                self._client.batch_write_item(
                    RequestItems={self._outbox_table_name: delete_requests}
                )
                deleted_count += len(batch)

            log.info("Deleted old published events", count=deleted_count)
            return deleted_count

        except ClientError as e:
            log.exception("Failed to delete old events")
            raise StorageError(
                message=f"Failed to delete old events: {e}",
                storage_type="dynamodb",
                operation="batch_delete",
                resource=self._outbox_table_name,
                cause=e,
            ) from e

    def update_metadata_with_outbox(
        self,
        file_id: str,
        timestamp: str,
        status: str,
        outbox_event: OutboxEvent,
        error_message: str | None = None,
    ) -> None:
        """
        Update existing metadata and add outbox event atomically.

        Used for status updates that also need to emit events.
        """
        log = logger.bind(
            file_id=file_id,
            status=status,
            event_id=outbox_event.event_id,
        )

        try:
            now = datetime.now(UTC).isoformat()
            outbox_item = outbox_event.to_dynamodb_item()

            update_expr = "SET #status = :status, updatedAt = :updated, GSI1PK = :gsi1pk"
            expr_names = {"#status": "status"}
            expr_values = {
                ":status": {"S": status},
                ":updated": {"S": now},
                ":gsi1pk": {"S": f"STATUS#{status}"},
            }

            if error_message:
                update_expr += ", errorMessage = :error"
                expr_values[":error"] = {"S": error_message}

            if status == "COMPLETED":
                update_expr += ", processedAt = :processed"
                expr_values[":processed"] = {"S": now}

            self._client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": self._metadata_table_name,
                            "Key": {
                                "PK": {"S": f"FILE#{file_id}"},
                                "SK": {"S": f"TS#{timestamp}"},
                            },
                            "UpdateExpression": update_expr,
                            "ExpressionAttributeNames": expr_names,
                            "ExpressionAttributeValues": expr_values,
                        }
                    },
                    {
                        "Put": {
                            "TableName": self._outbox_table_name,
                            "Item": outbox_item,
                        }
                    },
                ]
            )

            log.info("Metadata updated with outbox event")

        except ClientError as e:
            log.exception("Failed to update metadata with outbox")
            raise StorageError(
                message=f"Failed to update with outbox: {e}",
                storage_type="dynamodb",
                operation="transact_write",
                resource=f"{self._metadata_table_name}/{file_id}",
                cause=e,
            ) from e

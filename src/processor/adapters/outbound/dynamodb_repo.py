# =============================================================================
# DynamoDB Metadata Repository Adapter
# =============================================================================
"""
DynamoDB implementation of the MetadataRepository port.
Stores file metadata with single-table design.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any

import structlog
from botocore.exceptions import ClientError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from processor.domain.exceptions import StorageError
from processor.domain.models import MetadataRecord, ProcessingStatus
from processor.ports.outbound import MetadataRepository

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

logger = structlog.get_logger(__name__)


class DynamoDBMetadataRepository(MetadataRepository):
    """
    DynamoDB Metadata Repository adapter.

    Uses single-table design with:
    - PK: FILE#<file_id>
    - SK: TS#<timestamp>
    - GSI1PK: STATUS#<status> (for querying by status)
    - GSI1SK: <timestamp>
    """

    def __init__(
        self,
        dynamodb_client: DynamoDBClient,
        table_name: str,
    ) -> None:
        """
        Initialize DynamoDB Repository.

        Args:
            dynamodb_client: Boto3 DynamoDB client.
            table_name: Name of the DynamoDB table.
        """
        self._client = dynamodb_client
        self._table_name = table_name
        logger.info("DynamoDB Repository initialized", table=table_name)

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def save(self, record: MetadataRecord) -> None:
        """Save a metadata record to DynamoDB."""
        log = logger.bind(file_id=record.file_id, status=record.status)

        try:
            now = datetime.utcnow().isoformat()
            record.updated_at = now
            if not record.created_at:
                record.created_at = now

            item = record.to_dynamodb_item()

            # Add GSI keys for status-based queries
            item["GSI1PK"] = {"S": f"STATUS#{record.status.value}"}
            item["GSI1SK"] = {"S": record.timestamp}

            self._client.put_item(
                TableName=self._table_name,
                Item=item,
            )

            log.info("Metadata record saved")

        except ClientError as e:
            log.exception("Failed to save metadata record")
            raise StorageError(
                message=f"Failed to save metadata record: {e}",
                storage_type="dynamodb",
                operation="save",
                resource=f"{self._table_name}/{record.file_id}",
                cause=e,
            ) from e

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def get_by_id(self, file_id: str) -> MetadataRecord | None:
        """Get the latest metadata record by file ID."""
        log = logger.bind(file_id=file_id)

        try:
            # Query for the latest record (newest timestamp first)
            response = self._client.query(
                TableName=self._table_name,
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={
                    ":pk": {"S": f"FILE#{file_id}"},
                },
                ScanIndexForward=False,  # Descending order (newest first)
                Limit=1,
            )

            items = response.get("Items", [])
            if not items:
                log.debug("Metadata record not found")
                return None

            record = MetadataRecord.from_dynamodb_item(items[0])
            log.debug("Metadata record retrieved", status=record.status)
            return record

        except ClientError as e:
            log.exception("Failed to get metadata record")
            raise StorageError(
                message=f"Failed to get metadata record: {e}",
                storage_type="dynamodb",
                operation="get",
                resource=f"{self._table_name}/{file_id}",
                cause=e,
            ) from e

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def get_history(self, file_id: str, limit: int = 10) -> list[MetadataRecord]:
        """Get metadata record history for a file."""
        log = logger.bind(file_id=file_id)

        try:
            response = self._client.query(
                TableName=self._table_name,
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={
                    ":pk": {"S": f"FILE#{file_id}"},
                },
                ScanIndexForward=False,  # Descending order (newest first)
                Limit=limit,
            )

            items = response.get("Items", [])
            records = [MetadataRecord.from_dynamodb_item(item) for item in items]

            log.debug("Retrieved metadata history", count=len(records))
            return records

        except ClientError as e:
            log.exception("Failed to get metadata history")
            raise StorageError(
                message=f"Failed to get metadata history: {e}",
                storage_type="dynamodb",
                operation="query",
                resource=f"{self._table_name}/{file_id}",
                cause=e,
            ) from e

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def update_status(
        self,
        file_id: str,
        timestamp: str,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """Update the status of a metadata record."""
        log = logger.bind(file_id=file_id, status=status)

        try:
            update_expr = "SET #status = :status, updatedAt = :updated"
            expr_names: dict[str, str] = {"#status": "status"}
            expr_values: dict[str, Any] = {
                ":status": {"S": status},
                ":updated": {"S": datetime.utcnow().isoformat()},
            }

            # Also update GSI1PK for status queries
            update_expr += ", GSI1PK = :gsi1pk"
            expr_values[":gsi1pk"] = {"S": f"STATUS#{status}"}

            if error_message:
                update_expr += ", errorMessage = :error"
                expr_values[":error"] = {"S": error_message}

            if status == ProcessingStatus.COMPLETED.value:
                update_expr += ", processedAt = :processed"
                expr_values[":processed"] = {"S": datetime.utcnow().isoformat()}

            self._client.update_item(
                TableName=self._table_name,
                Key={
                    "PK": {"S": f"FILE#{file_id}"},
                    "SK": {"S": f"TS#{timestamp}"},
                },
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values,
            )

            log.info("Metadata status updated")

        except ClientError as e:
            log.exception("Failed to update metadata status")
            raise StorageError(
                message=f"Failed to update status: {e}",
                storage_type="dynamodb",
                operation="update",
                resource=f"{self._table_name}/{file_id}",
                cause=e,
            ) from e

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def query_by_status(
        self,
        status: str,
        limit: int = 100,
    ) -> list[MetadataRecord]:
        """Query metadata records by status using GSI."""
        log = logger.bind(status=status)

        try:
            response = self._client.query(
                TableName=self._table_name,
                IndexName="GSI1",
                KeyConditionExpression="GSI1PK = :status",
                ExpressionAttributeValues={
                    ":status": {"S": f"STATUS#{status}"},
                },
                ScanIndexForward=False,
                Limit=limit,
            )

            items = response.get("Items", [])
            records = [MetadataRecord.from_dynamodb_item(item) for item in items]

            log.debug("Retrieved records by status", count=len(records))
            return records

        except ClientError as e:
            log.exception("Failed to query by status")
            raise StorageError(
                message=f"Failed to query by status: {e}",
                storage_type="dynamodb",
                operation="query",
                resource=self._table_name,
                cause=e,
            ) from e

    def increment_retry_count(self, file_id: str, timestamp: str) -> int:
        """Increment retry count and return new value."""
        log = logger.bind(file_id=file_id)

        try:
            response = self._client.update_item(
                TableName=self._table_name,
                Key={
                    "PK": {"S": f"FILE#{file_id}"},
                    "SK": {"S": f"TS#{timestamp}"},
                },
                UpdateExpression="SET retryCount = retryCount + :inc, updatedAt = :updated",
                ExpressionAttributeValues={
                    ":inc": {"N": "1"},
                    ":updated": {"S": datetime.utcnow().isoformat()},
                },
                ReturnValues="UPDATED_NEW",
            )

            new_count = int(response["Attributes"]["retryCount"]["N"])
            log.debug("Retry count incremented", retry_count=new_count)
            return new_count

        except ClientError as e:
            log.exception("Failed to increment retry count")
            raise StorageError(
                message=f"Failed to increment retry count: {e}",
                storage_type="dynamodb",
                operation="update",
                resource=f"{self._table_name}/{file_id}",
                cause=e,
            ) from e

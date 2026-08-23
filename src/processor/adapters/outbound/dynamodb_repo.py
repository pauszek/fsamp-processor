"""DynamoDB current-state implementation of the metadata repository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import structlog
from botocore.exceptions import ClientError

from processor.adapters.outbound.aws_retry import aws_retry
from processor.domain.exceptions import StorageError
from processor.domain.models import MetadataRecord, ProcessingClaim, ProcessingStatus
from processor.ports.outbound import MetadataRepository

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

logger = structlog.get_logger(__name__)

_PROCESSOR_CLAIM_TOKEN_ATTRIBUTE = "processorClaimToken"  # noqa: S105  # nosec B105
_PROCESSOR_CLAIM_VERSION_ATTRIBUTE = "processorClaimVersion"
_PROCESSOR_CLAIM_EXPIRY_ATTRIBUTE = "processorClaimExpiresAt"
_PROCESSING_EVENT_ID = "processingEventId"


def apply_claim_fence(
    update: str,
    names: dict[str, str],
    values: dict[str, dict[str, Any]],
    claim: ProcessingClaim,
) -> tuple[str, dict[str, str], dict[str, dict[str, Any]], str]:
    """Fence a final state write and release its active lease atomically."""
    names.update(
        {
            "#claimToken": _PROCESSOR_CLAIM_TOKEN_ATTRIBUTE,
            "#claimVersion": _PROCESSOR_CLAIM_VERSION_ATTRIBUTE,
            "#claimExpiresAt": _PROCESSOR_CLAIM_EXPIRY_ATTRIBUTE,
            "#processingEventId": _PROCESSING_EVENT_ID,
        }
    )
    values.update(
        {
            ":claimToken": {"S": claim.token},
            ":claimVersion": {"N": str(claim.version)},
            ":claimEventId": {"S": claim.event_id},
        }
    )
    update += " REMOVE #claimToken, #claimExpiresAt, #processingEventId"
    condition = (
        "#claimToken = :claimToken AND #claimVersion = :claimVersion "
        "AND #processingEventId = :claimEventId"
    )
    return update, names, values, condition


def is_conditional_failure(error: ClientError) -> bool:
    """Return whether DynamoDB rejected a compare-and-set condition."""
    return error.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


def build_metadata_update(
    record: MetadataRecord,
) -> tuple[str, dict[str, str], dict[str, dict[str, Any]]]:
    """Build an update that preserves gateway-owned audit and optional fields."""
    now = datetime.now(UTC).isoformat()
    record.updated_at = now
    created_at = record.created_at or now

    item = record.to_dynamodb_item()
    item["GSI1PK"] = {"S": f"STATUS#{record.status.value}"}
    item["GSI1SK"] = {"S": now}
    item["updatedAt"] = {"S": now}

    names: dict[str, str] = {}
    values: dict[str, dict[str, Any]] = {}
    assignments: list[str] = []
    index = 0
    for attribute, value in item.items():
        if attribute in {"PK", "SK", "createdAt", "createdBy"}:
            continue
        name = f"#n{index}"
        placeholder = f":v{index}"
        names[name] = attribute
        values[placeholder] = value
        assignments.append(f"{name} = {placeholder}")
        index += 1

    names["#createdAt"] = "createdAt"
    values[":createdAt"] = {"S": created_at}
    assignments.append("#createdAt = if_not_exists(#createdAt, :createdAt)")
    if record.created_by:
        names["#createdBy"] = "createdBy"
        values[":createdBy"] = {"S": record.created_by}
        assignments.append("#createdBy = if_not_exists(#createdBy, :createdBy)")

    return "SET " + ", ".join(assignments), names, values


class DynamoDBMetadataRepository(MetadataRepository):
    """Store one shared current-state row at ``FILE#id`` / ``METADATA``."""

    def __init__(self, dynamodb_client: DynamoDBClient, table_name: str) -> None:
        self._client = dynamodb_client
        self._table_name = table_name
        logger.info("DynamoDB metadata repository initialized", table=table_name)

    @aws_retry()
    def save(
        self,
        record: MetadataRecord,
        claim: ProcessingClaim | None = None,
    ) -> None:
        """Update the shared metadata item without replacing gateway-owned fields."""
        expression, names, values = build_metadata_update(record)
        condition: str | None = None
        if claim is not None:
            expression, names, values, condition = apply_claim_fence(
                expression,
                names,
                values,
                claim,
            )
        try:
            request: dict[str, Any] = {
                "TableName": self._table_name,
                "Key": {
                    "PK": {"S": f"FILE#{record.file_id}"},
                    "SK": {"S": "METADATA"},
                },
                "UpdateExpression": expression,
                "ExpressionAttributeNames": names,
                "ExpressionAttributeValues": values,
            }
            if condition is not None:
                request["ConditionExpression"] = condition
            self._client.update_item(**request)
            logger.info(
                "Metadata current state saved",
                file_id=record.file_id,
                status=record.status.value,
            )
        except ClientError as error:
            raise StorageError(
                message=f"Failed to save metadata record: {error}",
                storage_type="dynamodb",
                operation="update",
                resource=f"{self._table_name}/{record.file_id}",
                cause=error,
            ) from error

    @aws_retry()
    def claim_processing(
        self,
        initial_record: MetadataRecord,
        event_id: str,
        lease_seconds: int,
    ) -> ProcessingClaim | None:
        """Initialize metadata and acquire or take over a token-fenced lease."""
        now = datetime.now(UTC)
        now_epoch = int(now.timestamp())
        expires_at_epoch = now_epoch + lease_seconds
        token = str(uuid4())
        initial_names: dict[str, str] = {}
        initial_values: dict[str, dict[str, Any]] = {}
        initial_assignments: list[str] = []
        for index, (attribute, value) in enumerate(initial_record.to_dynamodb_item().items()):
            if attribute in {
                "PK",
                "SK",
                "status",
                "updatedAt",
                "errorMessage",
                "errorCode",
                "lastProcessedEventId",
            }:
                continue
            name = f"#initial{index}"
            placeholder = f":initial{index}"
            initial_names[name] = attribute
            initial_values[placeholder] = value
            initial_assignments.append(f"{name} = if_not_exists({name}, {placeholder})")

        try:
            response = self._client.update_item(
                TableName=self._table_name,
                Key={
                    "PK": {"S": f"FILE#{initial_record.file_id}"},
                    "SK": {"S": "METADATA"},
                },
                UpdateExpression=(
                    "SET #status = :processing, GSI1PK = :gsi, GSI1SK = :now, "
                    "updatedAt = :now, #claimToken = :claimToken, "
                    "#claimExpiresAt = :claimExpiresAt, "
                    "#claimVersion = if_not_exists(#claimVersion, :zero) + :one, "
                    "#processingEventId = :eventId, " + ", ".join(initial_assignments) + " "
                    "REMOVE errorMessage, errorCode"
                ),
                ConditionExpression=(
                    "(attribute_not_exists(#lastProcessedEventId) "
                    "OR #lastProcessedEventId <> :eventId) "
                    "AND (attribute_not_exists(#claimToken) "
                    "OR #claimExpiresAt <= :nowEpoch)"
                ),
                ExpressionAttributeNames={
                    **initial_names,
                    "#status": "status",
                    "#lastProcessedEventId": "lastProcessedEventId",
                    "#claimToken": _PROCESSOR_CLAIM_TOKEN_ATTRIBUTE,
                    "#claimVersion": _PROCESSOR_CLAIM_VERSION_ATTRIBUTE,
                    "#claimExpiresAt": _PROCESSOR_CLAIM_EXPIRY_ATTRIBUTE,
                    "#processingEventId": _PROCESSING_EVENT_ID,
                },
                ExpressionAttributeValues={
                    **initial_values,
                    ":processing": {"S": ProcessingStatus.PROCESSING.value},
                    ":gsi": {"S": f"STATUS#{ProcessingStatus.PROCESSING.value}"},
                    ":now": {"S": now.isoformat()},
                    ":nowEpoch": {"N": str(now_epoch)},
                    ":claimToken": {"S": token},
                    ":claimExpiresAt": {"N": str(expires_at_epoch)},
                    ":zero": {"N": "0"},
                    ":one": {"N": "1"},
                    ":eventId": {"S": event_id},
                },
                ReturnValues="ALL_NEW",
            )
            attributes = response["Attributes"]
            return ProcessingClaim(
                event_id=event_id,
                token=token,
                version=int(attributes[_PROCESSOR_CLAIM_VERSION_ATTRIBUTE]["N"]),
                expires_at_epoch=int(attributes[_PROCESSOR_CLAIM_EXPIRY_ATTRIBUTE]["N"]),
            )
        except ClientError as error:
            if is_conditional_failure(error):
                return None
            raise StorageError(
                message=f"Failed to claim metadata record: {error}",
                storage_type="dynamodb",
                operation="conditional_update",
                resource=f"{self._table_name}/{initial_record.file_id}",
                cause=error,
            ) from error

    @aws_retry()
    def get_by_id(self, file_id: str) -> MetadataRecord | None:
        """Read the shared current-state metadata item consistently."""
        try:
            response = self._client.get_item(
                TableName=self._table_name,
                Key={
                    "PK": {"S": f"FILE#{file_id}"},
                    "SK": {"S": "METADATA"},
                },
                ConsistentRead=True,
            )
            item = response.get("Item")
            return MetadataRecord.from_dynamodb_item(item) if item else None
        except ClientError as error:
            raise StorageError(
                message=f"Failed to get metadata record: {error}",
                storage_type="dynamodb",
                operation="get",
                resource=f"{self._table_name}/{file_id}",
                cause=error,
            ) from error

    def get_history(self, file_id: str, limit: int = 10) -> list[MetadataRecord]:
        """Return the current record; timestamp history is intentionally not stored."""
        if limit <= 0:
            return []
        record = self.get_by_id(file_id)
        return [record] if record is not None else []

    @aws_retry()
    def update_status(
        self,
        file_id: str,
        timestamp: str,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """Update current status; ``timestamp`` remains for port compatibility."""
        del timestamp
        now = datetime.now(UTC).isoformat()
        expression = "SET #status = :status, updatedAt = :updated, GSI1PK = :gsi"
        values: dict[str, dict[str, Any]] = {
            ":status": {"S": status},
            ":updated": {"S": now},
            ":gsi": {"S": f"STATUS#{status}"},
        }
        if error_message:
            expression += ", errorMessage = :error"
            values[":error"] = {"S": error_message}
        if status == ProcessingStatus.COMPLETED.value:
            expression += ", processedAt = :processed"
            values[":processed"] = {"S": now}
        try:
            self._client.update_item(
                TableName=self._table_name,
                Key={"PK": {"S": f"FILE#{file_id}"}, "SK": {"S": "METADATA"}},
                UpdateExpression=expression,
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues=values,
            )
        except ClientError as error:
            raise StorageError(
                message=f"Failed to update metadata status: {error}",
                storage_type="dynamodb",
                operation="update",
                resource=f"{self._table_name}/{file_id}",
                cause=error,
            ) from error

    @aws_retry()
    def query_by_status(self, status: str, limit: int = 100) -> list[MetadataRecord]:
        """Query current-state rows by status."""
        try:
            response = self._client.query(
                TableName=self._table_name,
                IndexName="GSI1",
                KeyConditionExpression="GSI1PK = :status",
                ExpressionAttributeValues={":status": {"S": f"STATUS#{status}"}},
                ScanIndexForward=False,
                Limit=limit,
            )
            return [
                MetadataRecord.from_dynamodb_item(item)
                for item in response.get("Items", [])
                if item.get("entityType", {}).get("S") == "FILE_METADATA"
            ]
        except ClientError as error:
            raise StorageError(
                message=f"Failed to query metadata status: {error}",
                storage_type="dynamodb",
                operation="query",
                resource=self._table_name,
                cause=error,
            ) from error

    @aws_retry()
    def increment_retry_count(self, file_id: str, timestamp: str) -> int:
        """Atomically increment retry count on the current-state item."""
        del timestamp
        try:
            response = self._client.update_item(
                TableName=self._table_name,
                Key={"PK": {"S": f"FILE#{file_id}"}, "SK": {"S": "METADATA"}},
                UpdateExpression=(
                    "SET retryCount = if_not_exists(retryCount, :zero) + :inc, updatedAt = :updated"
                ),
                ExpressionAttributeValues={
                    ":zero": {"N": "0"},
                    ":inc": {"N": "1"},
                    ":updated": {"S": datetime.now(UTC).isoformat()},
                },
                ReturnValues="UPDATED_NEW",
            )
            return int(response["Attributes"]["retryCount"]["N"])
        except ClientError as error:
            raise StorageError(
                message=f"Failed to increment retry count: {error}",
                storage_type="dynamodb",
                operation="update",
                resource=f"{self._table_name}/{file_id}",
                cause=error,
            ) from error

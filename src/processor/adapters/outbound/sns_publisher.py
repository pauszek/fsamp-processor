# =============================================================================
# SNS Event Publisher Adapter
# =============================================================================
"""
SNS implementation of the EventPublisher port.
Publishes file events to SNS topics.
"""

from typing import TYPE_CHECKING

import orjson
import structlog
from botocore.exceptions import ClientError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from processor.domain.events import FileEvent
from processor.domain.exceptions import MessageError
from processor.ports.outbound import EventPublisher

if TYPE_CHECKING:
    from mypy_boto3_sns import SNSClient

logger = structlog.get_logger(__name__)


class SNSEventPublisher(EventPublisher):
    """
    SNS Event Publisher adapter.

    Features:
    - JSON serialization with orjson
    - Message attributes for filtering
    - Retry logic with exponential backoff
    - Batch publishing support
    """

    def __init__(
        self,
        sns_client: SNSClient,
        topic_arn: str,
    ) -> None:
        """
        Initialize SNS Publisher.

        Args:
            sns_client: Boto3 SNS client.
            topic_arn: ARN of the SNS topic.
        """
        self._client = sns_client
        self._topic_arn = topic_arn
        logger.info("SNS Publisher initialized", topic_arn=topic_arn)

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def publish(self, event: FileEvent) -> str:
        """Publish a single event to SNS."""
        log = logger.bind(
            event_id=str(event.event_id),
            correlation_id=event.correlation_id,
            event_type=event.event_type,
        )

        try:
            # Serialize event to JSON (camelCase for compatibility)
            message = orjson.dumps(event.model_dump(mode="json", by_alias=True)).decode("utf-8")

            # Message attributes for filtering subscriptions
            message_attributes = {
                "eventType": {
                    "DataType": "String",
                    "StringValue": event.event_type.value,
                },
                "correlationId": {
                    "DataType": "String",
                    "StringValue": event.correlation_id,
                },
            }

            log.info("Publishing event to SNS")

            response = self._client.publish(
                TopicArn=self._topic_arn,
                Message=message,
                MessageAttributes=message_attributes,
            )

            message_id = response["MessageId"]
            log.info("Event published successfully", message_id=message_id)

            return message_id

        except ClientError as e:
            log.exception("Failed to publish event")
            raise MessageError(
                message=f"Failed to publish event: {e}",
                queue_url=self._topic_arn,
                message_id=str(event.event_id),
                cause=e,
            ) from e

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def publish_batch(self, events: list[FileEvent]) -> list[str]:
        """
        Publish multiple events in a batch.

        Note: SNS doesn't have native batch publishing like SQS,
        so this publishes sequentially. For high-throughput,
        consider using SNS FIFO or direct SQS batch.
        """
        message_ids: list[str] = []

        for event in events:
            try:
                message_id = self.publish(event)
                message_ids.append(message_id)
            except MessageError:
                # Log and continue with other events
                logger.error(
                    "Failed to publish event in batch",
                    event_id=str(event.event_id),
                )

        logger.info(
            "Batch publish completed",
            total=len(events),
            successful=len(message_ids),
        )

        return message_ids

    def publish_to_queue(
        self,
        event: FileEvent,
        queue_arn: str,
    ) -> str:
        """
        Publish event directly to a specific SQS queue via SNS.

        Args:
            event: The event to publish.
            queue_arn: ARN of the target SQS queue.

        Returns:
            The message ID.
        """
        log = logger.bind(
            event_id=str(event.event_id),
            queue_arn=queue_arn,
        )

        try:
            message = orjson.dumps(event.model_dump(mode="json", by_alias=True)).decode("utf-8")

            message_attributes = {
                "eventType": {
                    "DataType": "String",
                    "StringValue": event.event_type.value,
                },
                "targetQueue": {
                    "DataType": "String",
                    "StringValue": queue_arn,
                },
            }

            response = self._client.publish(
                TopicArn=self._topic_arn,
                Message=message,
                MessageAttributes=message_attributes,
                MessageStructure="string",
            )

            message_id = response["MessageId"]
            log.info("Event published to queue", message_id=message_id)

            return message_id

        except ClientError as e:
            log.exception("Failed to publish to queue")
            raise MessageError(
                message=f"Failed to publish to queue: {e}",
                queue_url=queue_arn,
                message_id=str(event.event_id),
                cause=e,
            ) from e

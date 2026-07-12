"""
SQS Consumer implementation for receiving file events.
Implements the MessageConsumer port with long-polling and graceful shutdown.
"""

import signal
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import structlog
from botocore.exceptions import ClientError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from processor.domain.events import FileEvent, SQSMessageWrapper
from processor.domain.exceptions import (
    EventValidationError,
    MessageError,
    NonRetryableError,
)
from processor.ports.inbound import MessageConsumer

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

logger = structlog.get_logger(__name__)


class SQSConsumer(MessageConsumer):
    """
    SQS Message Consumer with long-polling and graceful shutdown.

    Features:
    - Long-polling for efficient message retrieval
    - Graceful shutdown on SIGTERM/SIGINT
    - Automatic message acknowledgment/rejection
    - Retry logic with exponential backoff
    - Dead Letter Queue support
    """

    def __init__(
        self,
        sqs_client: SQSClient,
        queue_url: str,
        handler: Callable[[FileEvent], Any],
        max_messages: int = 10,
        wait_time_seconds: int = 20,
        visibility_timeout: int = 300,
    ) -> None:
        """
        Initialize SQS Consumer.

        Args:
            sqs_client: Boto3 SQS client.
            queue_url: URL of the SQS queue.
            handler: Callback function to process FileEvent.
            max_messages: Max messages per poll (1-10).
            wait_time_seconds: Long-polling wait time.
            visibility_timeout: Message visibility timeout.
        """
        self._client = sqs_client
        self._queue_url = queue_url
        self._handler = handler
        self._max_messages = min(max(1, max_messages), 10)
        self._wait_time_seconds = wait_time_seconds
        self._visibility_timeout = visibility_timeout

        self._running = False
        self._shutdown_event = threading.Event()
        self._consumer_thread: threading.Thread | None = None

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        logger.info(
            "SQS Consumer initialized",
            queue_url=queue_url,
            max_messages=max_messages,
            wait_time_seconds=wait_time_seconds,
        )

    def _signal_handler(self, signum: int, _frame: Any) -> None:
        """Handle shutdown signals gracefully."""
        logger.info("Received shutdown signal", signal=signum)
        self.stop()

    def start(self) -> None:
        """Start consuming messages in a background thread."""
        if self._running:
            logger.warning("Consumer is already running")
            return

        self._running = True
        self._shutdown_event.clear()
        self._consumer_thread = threading.Thread(
            target=self._consume_loop,
            name="SQSConsumer",
            daemon=True,
        )
        self._consumer_thread.start()
        logger.info("SQS Consumer started")

    def start_blocking(self) -> None:
        """Start consuming messages in the current thread (blocking)."""
        if self._running:
            logger.warning("Consumer is already running")
            return

        self._running = True
        self._shutdown_event.clear()
        logger.info("SQS Consumer started (blocking mode)")
        self._consume_loop()

    def stop(self) -> None:
        """Stop consuming messages gracefully."""
        if not self._running:
            return

        logger.info("Stopping SQS Consumer...")
        self._running = False
        self._shutdown_event.set()

        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=30)

        logger.info("SQS Consumer stopped")

    def is_running(self) -> bool:
        """Check if consumer is running."""
        return self._running

    def _consume_loop(self) -> None:
        """Main consumption loop with long-polling."""
        consecutive_errors = 0
        max_consecutive_errors = 5

        while self._running and not self._shutdown_event.is_set():
            try:
                messages = self._receive_messages()

                if messages:
                    self._process_messages(messages)

                consecutive_errors = 0

            except ClientError as e:
                consecutive_errors += 1
                logger.error(
                    "AWS error in consume loop",
                    error=str(e),
                    consecutive_errors=consecutive_errors,
                )

                if consecutive_errors >= max_consecutive_errors:
                    logger.critical(
                        "Too many consecutive errors, stopping consumer",
                        consecutive_errors=consecutive_errors,
                    )
                    self._running = False
                    break

                backoff = min(2**consecutive_errors, 60)
                time.sleep(backoff)

            except Exception as e:
                logger.exception("Unexpected error in consume loop", error=str(e))
                time.sleep(5)

    def _process_messages(self, messages: list[dict[str, Any]]) -> None:
        """Process received messages until the consumer is asked to stop."""
        for message in messages:
            if self._shutdown_event.is_set():
                break
            self._process_message(message)

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def _receive_messages(self) -> list[dict[str, Any]]:
        """Receive messages from SQS with long-polling."""
        response = self._client.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=self._max_messages,
            WaitTimeSeconds=self._wait_time_seconds,
            VisibilityTimeout=self._visibility_timeout,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        return cast(list[dict[str, Any]], response.get("Messages", []))

    def _process_message(self, raw_message: dict[str, Any]) -> None:
        """Process a single SQS message."""
        message_id = raw_message.get("MessageId", "unknown")
        receipt_handle = raw_message.get("ReceiptHandle")

        log = logger.bind(message_id=message_id)

        try:
            wrapper = SQSMessageWrapper(
                message_id=message_id,
                receipt_handle=receipt_handle,
                body=raw_message["Body"],
                attributes=raw_message.get("Attributes", {}),
                MessageAttributes=raw_message.get("MessageAttributes", {}),
            )

            event = wrapper.get_file_event()
            log = log.bind(
                event_id=event.event_id_str,
                correlation_id=event.correlation_id_str,
                event_type=event.event_type,
            )
            log.info("Processing event")

            self._handler(event)

            self.acknowledge(receipt_handle)
            log.info("Event processed successfully")

        except EventValidationError as e:
            log.error("Event validation failed", error=str(e))
            self.reject(receipt_handle, requeue=False)

        except NonRetryableError as e:
            log.error("Non-retryable error", error=str(e))
            self.reject(receipt_handle, requeue=False)

        except Exception as e:
            log.exception("Processing failed", error=str(e))
            self.reject(receipt_handle, requeue=True)

    def acknowledge(self, receipt_handle: str | None) -> None:
        """Delete message from queue (acknowledge successful processing)."""
        if not receipt_handle:
            logger.warning("No receipt handle to acknowledge")
            return

        try:
            self._client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=receipt_handle,
            )
            logger.debug("Message acknowledged", receipt_handle=receipt_handle[:20])
        except ClientError as e:
            logger.error(
                "Failed to acknowledge message",
                error=str(e),
                receipt_handle=receipt_handle[:20],
            )
            raise MessageError(
                message="Failed to acknowledge message",
                queue_url=self._queue_url,
                receipt_handle=receipt_handle,
                cause=e,
            ) from e

    def reject(self, receipt_handle: str | None, requeue: bool = True) -> None:
        """
        Reject a message.

        If requeue=True, changes visibility timeout to 0 (immediate retry).
        If requeue=False, leaves it unacknowledged so the queue redrive policy can
        move it to the DLQ. Deleting a message would bypass the DLQ entirely.
        """
        if not receipt_handle:
            logger.warning("No receipt handle to reject")
            return

        try:
            if requeue:
                self._client.change_message_visibility(
                    QueueUrl=self._queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=0,
                )
                logger.debug("Message requeued for retry")
            else:
                logger.warning("Message left unacknowledged for configured DLQ redrive")

        except ClientError as e:
            logger.error(
                "Failed to reject message",
                error=str(e),
                requeue=requeue,
            )
            raise MessageError(
                message="Failed to reject message",
                queue_url=self._queue_url,
                receipt_handle=receipt_handle,
                cause=e,
            ) from e

    def get_queue_attributes(self) -> dict[str, str]:
        """Get queue attributes for monitoring."""
        try:
            response = self._client.get_queue_attributes(
                QueueUrl=self._queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            )
            return cast(dict[str, str], response.get("Attributes", {}))
        except ClientError as e:
            logger.error("Failed to get queue attributes", error=str(e))
            return {}

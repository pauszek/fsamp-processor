"""
Inbound ports define interfaces for external actors to interact with the domain.
These are implemented by input adapters (e.g., SQS Consumer, HTTP Controller).
"""

from abc import ABC, abstractmethod
from typing import Protocol

from processor.domain.events import FileEvent
from processor.domain.models import ProcessingResult


class EventHandler(Protocol):
    """
    Port for handling incoming file events.
    Implemented by the application service (use case).
    """

    def handle(self, event: FileEvent) -> ProcessingResult:
        """
        Handle a single file event.

        Args:
            event: The file event to process.

        Returns:
            ProcessingResult with the outcome.

        Raises:
            ProcessingError: If processing fails.
            EventValidationError: If event is invalid.
        """
        ...


class MessageConsumer(ABC):
    """
    Port for consuming messages from a queue.
    Implemented by adapters like SQS Consumer.
    """

    @abstractmethod
    def start(self) -> None:
        """Start consuming messages."""
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stop consuming messages gracefully."""
        ...

    @abstractmethod
    def is_running(self) -> bool:
        """Check if consumer is currently running."""
        ...

    @abstractmethod
    def acknowledge(self, receipt_handle: str) -> None:
        """
        Acknowledge successful processing of a message.

        Args:
            receipt_handle: The receipt handle of the message to acknowledge.
        """
        ...

    @abstractmethod
    def reject(self, receipt_handle: str, requeue: bool = True) -> None:
        """
        Reject a message, optionally requeuing it.

        Args:
            receipt_handle: The receipt handle of the message to reject.
            requeue: Whether to requeue the message for retry.
        """
        ...


class HealthCheck(Protocol):
    """Port for health check endpoints."""

    def is_healthy(self) -> bool:
        """Check if the service is healthy."""
        ...

    def get_status(self) -> dict[str, str]:
        """Get detailed health status."""
        ...

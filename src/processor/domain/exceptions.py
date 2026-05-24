"""
Custom exceptions for the FSAMP Processor domain.
Follows enterprise patterns with error codes and context.
"""

from typing import Any


class DomainError(Exception):
    """
    Base exception for all domain errors.
    Provides error code and context for observability.
    """

    error_code: str = "DOMAIN_ERROR"

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.error_code
        self.context = context or {}
        self.cause = cause

    def __str__(self) -> str:
        return f"[{self.error_code}] {self.message}"

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for logging/serialization."""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "context": self.context,
            "cause": str(self.cause) if self.cause else None,
        }


class EventValidationError(DomainError):
    """Raised when event validation fails."""

    error_code: str = "EVENT_VALIDATION_ERROR"

    def __init__(
        self,
        message: str,
        event_id: str | None = None,
        validation_errors: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        context = kwargs.pop("context", {})
        context["event_id"] = event_id
        context["validation_errors"] = validation_errors or []
        super().__init__(message, context=context, **kwargs)


class ProcessingError(DomainError):
    """Raised when file processing fails."""

    error_code: str = "PROCESSING_ERROR"

    def __init__(
        self,
        message: str,
        event_id: str | None = None,
        correlation_id: str | None = None,
        retryable: bool = True,
        **kwargs: Any,
    ) -> None:
        context = kwargs.pop("context", {})
        context["event_id"] = event_id
        context["correlation_id"] = correlation_id
        context["retryable"] = retryable
        super().__init__(message, context=context, **kwargs)
        self.retryable = retryable


class StorageError(DomainError):
    """Raised when storage operations fail (S3, DynamoDB)."""

    error_code: str = "STORAGE_ERROR"

    def __init__(
        self,
        message: str,
        storage_type: str = "unknown",
        operation: str = "unknown",
        resource: str | None = None,
        **kwargs: Any,
    ) -> None:
        context = kwargs.pop("context", {})
        context["storage_type"] = storage_type
        context["operation"] = operation
        context["resource"] = resource
        super().__init__(message, context=context, **kwargs)


class CryptoError(DomainError):
    """Raised when cryptographic operations fail."""

    error_code: str = "CRYPTO_ERROR"

    def __init__(
        self,
        message: str,
        operation: str = "unknown",
        algorithm: str | None = None,
        **kwargs: Any,
    ) -> None:
        context = kwargs.pop("context", {})
        context["operation"] = operation
        context["algorithm"] = algorithm
        super().__init__(message, context=context, **kwargs)


class MessageError(DomainError):
    """Raised when message handling fails (SQS, SNS)."""

    error_code: str = "MESSAGE_ERROR"

    def __init__(
        self,
        message: str,
        queue_url: str | None = None,
        message_id: str | None = None,
        receipt_handle: str | None = None,
        **kwargs: Any,
    ) -> None:
        context = kwargs.pop("context", {})
        context["queue_url"] = queue_url
        context["message_id"] = message_id
        context["receipt_handle"] = receipt_handle
        super().__init__(message, context=context, **kwargs)


class ConfigurationError(DomainError):
    """Raised when configuration is invalid or missing."""

    error_code: str = "CONFIGURATION_ERROR"

    def __init__(
        self,
        message: str,
        config_key: str | None = None,
        expected_type: str | None = None,
        **kwargs: Any,
    ) -> None:
        context = kwargs.pop("context", {})
        context["config_key"] = config_key
        context["expected_type"] = expected_type
        super().__init__(message, context=context, **kwargs)


class RetryableError(DomainError):
    """
    Base exception for errors that should trigger a retry.
    Used by the retry mechanism to determine retry strategy.
    """

    error_code: str = "RETRYABLE_ERROR"

    def __init__(
        self,
        message: str,
        max_retries: int = 3,
        retry_delay_seconds: int = 5,
        **kwargs: Any,
    ) -> None:
        context = kwargs.pop("context", {})
        context["max_retries"] = max_retries
        context["retry_delay_seconds"] = retry_delay_seconds
        super().__init__(message, context=context, **kwargs)
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds


class NonRetryableError(DomainError):
    """
    Base exception for errors that should NOT trigger a retry.
    Message should go directly to DLQ.
    """

    error_code: str = "NON_RETRYABLE_ERROR"

    def __init__(self, message: str, **kwargs: Any) -> None:
        context = kwargs.pop("context", {})
        context["retryable"] = False
        super().__init__(message, context=context, **kwargs)

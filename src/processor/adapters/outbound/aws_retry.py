"""Shared retry policy for transient AWS SDK failures."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from processor.domain.exceptions import DomainError

_TRANSIENT_CODES = {
    "InternalError",
    "InternalServerError",
    "PriorRequestNotComplete",
    "ProvisionedThroughputExceededException",
    "RequestLimitExceeded",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
    "TransactionInProgressException",
}


def _root_cause(error: BaseException) -> BaseException:
    """Unwrap domain adapters without losing the original SDK exception."""
    current = error
    seen: set[int] = set()
    while isinstance(current, DomainError) and current.cause is not None:
        if id(current) in seen:
            break
        seen.add(id(current))
        current = current.cause
    return current


def is_retryable_aws_error(error: BaseException) -> bool:
    """Return whether an SDK failure is safe and useful to retry."""
    cause = _root_cause(error)
    if isinstance(cause, ClientError):
        response = cause.response
        code = str(response.get("Error", {}).get("Code", ""))
        status = int(response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        return code in _TRANSIENT_CODES or code.startswith("Throttl") or status >= 500
    return isinstance(cause, BotoCoreError)


def aws_retry(*, attempts: int = 3) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Tenacity decorator that also sees SDK errors wrapped in domain errors."""
    return retry(
        retry=retry_if_exception(is_retryable_aws_error),
        stop=stop_after_attempt(attempts),
        wait=wait_exponential_jitter(initial=0.1, max=2),
        reraise=True,
    )

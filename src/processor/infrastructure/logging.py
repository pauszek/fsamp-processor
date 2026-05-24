"""
Structured logging setup using structlog.
Outputs JSON in production, pretty console in development.
"""

import logging
import sys
from typing import Any, cast

import structlog


def configure_logging(
    level: str = "INFO",
    json_format: bool = True,
    service_name: str = "fsamp-processor",
) -> None:
    """
    Configure structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR).
        json_format: If True, output JSON logs. If False, pretty console output.
        service_name: Service name to include in all logs.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if json_format:
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            ),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service=service_name)

    logger = structlog.get_logger()
    logger.info(
        "Logging configured",
        level=level,
        format="json" if json_format else "console",
    )


def get_correlation_logger(correlation_id: str) -> structlog.stdlib.BoundLogger:
    """
    Get a logger bound with a correlation ID.

    Args:
        correlation_id: The correlation ID to bind.

    Returns:
        A bound logger with the correlation ID.
    """
    return cast(
        structlog.stdlib.BoundLogger, structlog.get_logger().bind(correlation_id=correlation_id)
    )


def bind_context(**kwargs: Any) -> None:
    """
    Bind additional context to all subsequent log messages.

    Args:
        **kwargs: Key-value pairs to bind to the logging context.
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all bound context variables."""
    structlog.contextvars.clear_contextvars()

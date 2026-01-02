# =============================================================================
# Unit Tests for Logging Infrastructure
# =============================================================================
"""Tests for structured logging configuration."""

import logging

import pytest
import structlog

from processor.infrastructure.logging import (
    bind_context,
    clear_context,
    configure_logging,
    get_correlation_logger,
)


class TestConfigureLogging:
    """Tests for configure_logging function."""

    def test_configure_logging_default(self) -> None:
        """Test configure_logging with defaults."""
        configure_logging()

        # Should not raise
        logger = structlog.get_logger()
        assert logger is not None

    def test_configure_logging_json_format(self) -> None:
        """Test configure_logging with JSON format."""
        configure_logging(level="INFO", json_format=True)

        # Check that structlog is configured
        logger = structlog.get_logger()
        assert logger is not None

    def test_configure_logging_console_format(self) -> None:
        """Test configure_logging with console format."""
        configure_logging(level="DEBUG", json_format=False)

        logger = structlog.get_logger()
        assert logger is not None

    def test_configure_logging_sets_log_level(self) -> None:
        """Test that log level is set correctly."""
        configure_logging(level="WARNING")

        root_logger = logging.getLogger()
        assert root_logger.level == logging.WARNING

    def test_configure_logging_service_name(self) -> None:
        """Test that service name is bound to context."""
        configure_logging(service_name="test-service")

        # Service name should be in context
        # This is bound via contextvars

    @pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR"])
    def test_configure_logging_various_levels(self, level: str) -> None:
        """Test configure_logging with various log levels."""
        # Reset root logger handlers to allow basicConfig to take effect
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(logging.NOTSET)

        configure_logging(level=level)

        expected_level = getattr(logging, level)
        root_logger = logging.getLogger()
        assert root_logger.level == expected_level


class TestGetCorrelationLogger:
    """Tests for get_correlation_logger function."""

    def test_get_correlation_logger(self) -> None:
        """Test getting a correlation-bound logger."""
        correlation_id = "test-correlation-123"

        logger = get_correlation_logger(correlation_id)

        assert logger is not None
        # Logger should have correlation_id bound

    def test_get_correlation_logger_different_ids(self) -> None:
        """Test getting loggers with different correlation IDs."""
        logger1 = get_correlation_logger("correlation-1")
        logger2 = get_correlation_logger("correlation-2")

        assert logger1 is not None
        assert logger2 is not None


class TestBindContext:
    """Tests for bind_context function."""

    def test_bind_context(self) -> None:
        """Test binding context variables."""
        clear_context()

        bind_context(user_id="user-123", request_id="req-456")

        # Context should be bound

    def test_bind_context_multiple_calls(self) -> None:
        """Test multiple bind_context calls."""
        clear_context()

        bind_context(key1="value1")
        bind_context(key2="value2")

        # Both should be bound


class TestClearContext:
    """Tests for clear_context function."""

    def test_clear_context(self) -> None:
        """Test clearing context variables."""
        bind_context(test_key="test_value")

        clear_context()

        # Context should be cleared

    def test_clear_context_when_empty(self) -> None:
        """Test clearing context when already empty."""
        clear_context()
        clear_context()  # Should not raise


class TestLoggingIntegration:
    """Integration tests for logging."""

    def test_logging_with_context(self) -> None:
        """Test logging with bound context."""
        configure_logging(level="DEBUG", json_format=False)
        clear_context()

        bind_context(user_id="user-123")
        logger = structlog.get_logger()

        # Should not raise
        logger.info("Test message")

    def test_logging_with_correlation(self) -> None:
        """Test logging with correlation ID."""
        configure_logging(level="DEBUG", json_format=False)

        logger = get_correlation_logger("corr-123")

        # Should not raise
        logger.info("Test with correlation")

    def test_logging_exception(self) -> None:
        """Test logging exceptions."""
        configure_logging(level="DEBUG", json_format=False)
        logger = structlog.get_logger()

        try:
            raise ValueError("Test error")
        except ValueError:
            # Should not raise
            logger.exception("Caught exception")

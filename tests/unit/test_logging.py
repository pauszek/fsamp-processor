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
    def test_configure_logging_default(self) -> None:
        configure_logging()

        logger = structlog.get_logger()
        assert logger is not None

    def test_configure_logging_json_format(self) -> None:
        configure_logging(level="INFO", json_format=True)

        logger = structlog.get_logger()
        assert logger is not None

    def test_configure_logging_console_format(self) -> None:
        configure_logging(level="DEBUG", json_format=False)

        logger = structlog.get_logger()
        assert logger is not None

    def test_configure_logging_sets_log_level(self) -> None:
        configure_logging(level="WARNING")

        root_logger = logging.getLogger()
        assert root_logger.level == logging.WARNING

    def test_configure_logging_service_name(self) -> None:
        configure_logging(service_name="test-service")

    @pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR"])
    def test_configure_logging_various_levels(self, level: str) -> None:
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(logging.NOTSET)

        configure_logging(level=level)

        expected_level = getattr(logging, level)
        root_logger = logging.getLogger()
        assert root_logger.level == expected_level


class TestGetCorrelationLogger:
    def test_get_correlation_logger(self) -> None:
        correlation_id = "test-correlation-123"

        logger = get_correlation_logger(correlation_id)

        assert logger is not None

    def test_get_correlation_logger_different_ids(self) -> None:
        logger1 = get_correlation_logger("correlation-1")
        logger2 = get_correlation_logger("correlation-2")

        assert logger1 is not None
        assert logger2 is not None


class TestBindContext:
    def test_bind_context(self) -> None:
        clear_context()

        bind_context(user_id="user-123", request_id="req-456")

    def test_bind_context_multiple_calls(self) -> None:
        clear_context()

        bind_context(key1="value1")
        bind_context(key2="value2")


class TestClearContext:
    def test_clear_context(self) -> None:
        bind_context(test_key="test_value")

        clear_context()

    def test_clear_context_when_empty(self) -> None:
        clear_context()
        clear_context()  # Should not raise


class TestLoggingIntegration:
    def test_logging_with_context(self) -> None:
        configure_logging(level="DEBUG", json_format=False)
        clear_context()

        bind_context(user_id="user-123")
        logger = structlog.get_logger()

        logger.info("Test message")

    def test_logging_with_correlation(self) -> None:
        configure_logging(level="DEBUG", json_format=False)

        logger = get_correlation_logger("corr-123")

        logger.info("Test with correlation")

    def test_logging_exception(self) -> None:
        configure_logging(level="DEBUG", json_format=False)
        logger = structlog.get_logger()

        try:
            raise ValueError("Test error")
        except ValueError:
            logger.exception("Caught exception")

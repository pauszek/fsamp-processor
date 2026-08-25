import logging

import pytest
import structlog

from processor.infrastructure.logging import configure_logging


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


class TestLoggingIntegration:
    def test_logging_with_context(self) -> None:
        configure_logging(level="DEBUG", json_format=False)
        logger = structlog.get_logger().bind(user_id="user-123")

        logger.info("Test message")

    def test_logging_exception(self) -> None:
        configure_logging(level="DEBUG", json_format=False)
        logger = structlog.get_logger()

        try:
            raise ValueError("Test error")
        except ValueError:
            logger.exception("Caught exception")

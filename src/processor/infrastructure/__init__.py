# =============================================================================
# Infrastructure Layer
# =============================================================================
"""
Infrastructure concerns: logging, AWS clients, configuration.
"""

from processor.infrastructure.aws_clients import AWSClientFactory
from processor.infrastructure.logging import configure_logging

__all__ = ["AWSClientFactory", "configure_logging"]

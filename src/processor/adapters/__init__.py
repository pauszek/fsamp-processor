# =============================================================================
# Adapters Layer
# =============================================================================
"""
Adapters implement the ports to connect the domain with external services.
"""

from processor.adapters.inbound import SQSConsumer
from processor.adapters.outbound import (
    DynamoDBMetadataRepository,
    KMSCryptoProvider,
    S3FileStorage,
    SNSEventPublisher,
)

__all__ = [
    "DynamoDBMetadataRepository",
    "KMSCryptoProvider",
    "S3FileStorage",
    "SNSEventPublisher",
    "SQSConsumer",
]

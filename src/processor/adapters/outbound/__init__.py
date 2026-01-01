# =============================================================================
# Outbound Adapters
# =============================================================================
"""Outbound adapters for interacting with external services."""

from processor.adapters.outbound.dynamodb_repo import DynamoDBMetadataRepository
from processor.adapters.outbound.kms_crypto import KMSCryptoProvider
from processor.adapters.outbound.s3_storage import S3FileStorage
from processor.adapters.outbound.sns_publisher import SNSEventPublisher

__all__ = [
    "S3FileStorage",
    "DynamoDBMetadataRepository",
    "SNSEventPublisher",
    "KMSCryptoProvider",
]

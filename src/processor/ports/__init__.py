"""
Ports define the interfaces (contracts) between the domain and external world.
Following Hexagonal Architecture (Ports & Adapters) pattern.
"""

from processor.ports.inbound import EventHandler, MessageConsumer
from processor.ports.outbound import (
    CryptoProvider,
    EventPublisher,
    FileStorage,
    MetadataRepository,
)

__all__ = [
    "CryptoProvider",
    "EventHandler",
    "EventPublisher",
    "FileStorage",
    "MessageConsumer",
    "MetadataRepository",
]

# =============================================================================
# Domain Layer
# =============================================================================
"""Domain models, events, and business logic."""

from processor.domain.events import (
    EventType,
    FileEvent,
    FileMetadata,
    SecurityContext,
    StorageLocation,
)
from processor.domain.exceptions import (
    CryptoError,
    DomainError,
    EventValidationError,
    ProcessingError,
    StorageError,
)
from processor.domain.models import ProcessingResult, ProcessingStatus

__all__ = [
    # Events
    "EventType",
    "FileEvent",
    "FileMetadata",
    "SecurityContext",
    "StorageLocation",
    # Models
    "ProcessingResult",
    "ProcessingStatus",
    # Exceptions
    "DomainError",
    "EventValidationError",
    "ProcessingError",
    "StorageError",
    "CryptoError",
]

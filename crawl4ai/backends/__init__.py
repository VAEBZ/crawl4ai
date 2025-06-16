"""
Clean backend architecture for crawl4ai storage systems.

This module provides a clean, extensible architecture for different storage
backends including DynamoDB, Mem0, and future storage systems. All backends
implement the StorageBackend protocol for consistent interfaces.
"""

from typing import Optional, Protocol, runtime_checkable
from enum import Enum

from ..models import CrawlResult


class BackendType(Enum):
    """Supported backend types for crawl4ai storage."""

    DYNAMODB = "dynamodb"
    MEM0 = "mem0"
    SQLITE = "sqlite"  # Legacy support


@runtime_checkable
class StorageBackend(Protocol):
    """
    Protocol defining the storage backend interface.

    All storage backends must implement these methods to ensure
    consistent behavior across different storage systems.
    """

    async def store_result(self, url: str, result: CrawlResult) -> None:
        """
        Store a crawl result.

        Args:
            url: The URL that was crawled
            result: The CrawlResult object to store

        Raises:
            StorageError: If storage operation fails
        """
        ...

    async def retrieve_result(self, url: str) -> Optional[CrawlResult]:
        """
        Retrieve a cached crawl result.

        Args:
            url: The URL to retrieve results for

        Returns:
            CrawlResult if found, None otherwise

        Raises:
            StorageError: If retrieval operation fails
        """
        ...

    async def close(self) -> None:
        """
        Clean up resources and close connections.

        This method should be called when the backend is no longer needed
        to ensure proper cleanup of connections, sessions, etc.
        """
        ...


class StorageError(Exception):
    """Base exception for storage backend errors."""

    def __init__(self, message: str, backend_type: Optional[str] = None) -> None:
        super().__init__(message)
        self.backend_type = backend_type


class BackendConfigurationError(StorageError):
    """Raised when backend configuration is invalid."""

    pass


class BackendConnectionError(StorageError):
    """Raised when backend connection fails."""

    pass


# Export main interfaces
__all__ = [
    "StorageBackend",
    "BackendType",
    "StorageError",
    "BackendConfigurationError",
    "BackendConnectionError",
]

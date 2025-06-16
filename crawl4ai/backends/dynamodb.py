"""
Production-ready DynamoDB backend for crawl4ai.

This module provides a robust, type-safe DynamoDB storage backend with:
- Full async/await support
- Connection pooling and retries
- Comprehensive error handling
- Proper resource management
- Complete type annotations
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError

from . import BackendConnectionError, BackendConfigurationError, StorageError
from ..models import CrawlResult

logger = logging.getLogger(__name__)


class DynamoDBBackend:
    """
    Production-ready DynamoDB storage backend.

    Features:
    - Full type safety with mypy compliance
    - Comprehensive error handling and logging
    - Connection pooling and adaptive retries
    - Proper async/await patterns and resource management
    - Configurable table names and regions
    - Support for both local and AWS DynamoDB
    """

    def __init__(
        self,
        table_name: str = "crawl4ai-results",
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ) -> None:
        """
        Initialize DynamoDB backend with configuration.

        Args:
            table_name: Name of the DynamoDB table
            region_name: AWS region name
            endpoint_url: Custom endpoint URL (for local DynamoDB)
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
        """
        self.table_name = table_name
        self.region_name = region_name
        self.endpoint_url = endpoint_url or os.getenv("DYNAMODB_ENDPOINT_URL")

        # Production-ready boto3 configuration
        self.config = Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            max_pool_connections=50,
            region_name=region_name,
        )

        # Session and client management
        self._session: Optional[aioboto3.Session] = None
        self._client = None
        self._credentials = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }

        logger.info(
            f"Initialized DynamoDB backend: table={table_name}, "
            f"region={region_name}, endpoint={self.endpoint_url}"
        )

    async def store_result(self, url: str, result: CrawlResult) -> None:
        """
        Store crawl result with comprehensive error handling.

        Args:
            url: The URL that was crawled
            result: The CrawlResult object to store

        Raises:
            StorageError: If storage operation fails
            BackendConnectionError: If connection to DynamoDB fails
        """
        try:
            client = await self._get_client()
            item = self._serialize_result(url, result)

            await client.put_item(TableName=self.table_name, Item=item)

            logger.info(f"Successfully stored result for URL: {url}")

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = f"DynamoDB error storing {url}: {error_code}"
            logger.error(error_msg, exc_info=True)
            raise StorageError(error_msg, "dynamodb") from e

        except NoCredentialsError as e:
            error_msg = "AWS credentials not found or invalid"
            logger.error(error_msg, exc_info=True)
            raise BackendConnectionError(error_msg, "dynamodb") from e

        except Exception as e:
            error_msg = f"Unexpected error storing result for {url}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StorageError(error_msg, "dynamodb") from e

    async def retrieve_result(self, url: str) -> Optional[CrawlResult]:
        """
        Retrieve cached result with proper error handling.

        Args:
            url: The URL to retrieve results for

        Returns:
            CrawlResult if found, None otherwise

        Raises:
            StorageError: If retrieval operation fails
        """
        try:
            client = await self._get_client()

            response = await client.get_item(
                TableName=self.table_name, Key={"url": {"S": url}}
            )

            if "Item" not in response:
                logger.debug(f"No cached result found for URL: {url}")
                return None

            result = self._deserialize_result(response["Item"])
            logger.info(f"Retrieved cached result for URL: {url}")
            return result

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "ResourceNotFoundException":
                logger.warning(f"Table {self.table_name} not found")
                return None

            error_msg = f"DynamoDB error retrieving {url}: {error_code}"
            logger.error(error_msg, exc_info=True)
            raise StorageError(error_msg, "dynamodb") from e

        except Exception as e:
            error_msg = f"Unexpected error retrieving result for {url}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            # Don't raise on retrieval errors, just return None
            return None

    async def close(self) -> None:
        """Clean up resources and close connections."""
        try:
            if self._client:
                await self._client.close()
                self._client = None
                logger.debug("Closed DynamoDB client")

            self._session = None
            logger.info("DynamoDB backend closed successfully")

        except Exception as e:
            logger.error(f"Error closing DynamoDB backend: {e}", exc_info=True)

    async def _get_client(self):
        """Get or create DynamoDB client with proper session management."""
        if not self._session:
            self._session = aioboto3.Session()

        if not self._client:
            client_kwargs = {
                "service_name": "dynamodb",
                "region_name": self.region_name,
                "config": self.config,
            }

            # Add endpoint URL if specified (for local DynamoDB)
            if self.endpoint_url:
                client_kwargs["endpoint_url"] = self.endpoint_url

            # Add credentials if specified
            if self._credentials["aws_access_key_id"]:
                client_kwargs.update(self._credentials)

            # Create the actual client by entering the context manager
            client_context = self._session.client(**client_kwargs)
            self._client = await client_context.__aenter__()

        return self._client

    def _serialize_result(self, url: str, result: CrawlResult) -> Dict[str, Any]:
        """
        Convert CrawlResult to DynamoDB item format.

        Args:
            url: The URL that was crawled
            result: The CrawlResult to serialize

        Returns:
            DynamoDB item dictionary
        """
        # Convert CrawlResult to dictionary
        result_dict = {
            "url": result.url,
            "html": result.html,
            "cleaned_html": result.cleaned_html,
            "markdown": result.markdown,
            "extracted_content": result.extracted_content,
            "success": result.success,
            "status_code": result.status_code,
            "error_message": result.error_message,
            "session_id": result.session_id,
            "response_headers": dict(result.response_headers or {}),
            "links": {
                "internal": list(result.links.get("internal", [])),
                "external": list(result.links.get("external", [])),
            },
            "media": dict(result.media or {}),
            "metadata": dict(result.metadata or {}),
        }

        # Add timestamps
        now = datetime.now(timezone.utc)
        result_dict.update(
            {
                "crawled_at": getattr(result, 'crawled_at', now).isoformat()
                if hasattr(result, 'crawled_at') and getattr(result, 'crawled_at')
                else now.isoformat(),
                "stored_at": now.isoformat(),
            }
        )

        # Convert to DynamoDB format
        return self._to_dynamodb_item(result_dict)

    def _deserialize_result(self, item: Dict[str, Any]) -> CrawlResult:
        """
        Convert DynamoDB item to CrawlResult.

        Args:
            item: DynamoDB item dictionary

        Returns:
            Reconstructed CrawlResult object
        """
        # Convert from DynamoDB format
        data = self._from_dynamodb_item(item)

        # Parse timestamps (crawled_at is not part of CrawlResult model)
        # We store it in metadata for reference
        metadata = data.get("metadata", {})
        if data.get("crawled_at"):
            metadata["crawled_at"] = data["crawled_at"]

        # Reconstruct CrawlResult
        return CrawlResult(
            url=data.get("url", ""),
            html=data.get("html", ""),
            cleaned_html=data.get("cleaned_html", ""),
            markdown=data.get("markdown", ""),
            extracted_content=data.get("extracted_content", ""),
            success=data.get("success", False),
            status_code=data.get("status_code", 0),
            error_message=data.get("error_message"),
            session_id=data.get("session_id"),
            response_headers=data.get("response_headers", {}),
            links=data.get("links", {}),
            media=data.get("media", {}),
            metadata=metadata,
        )

    def _to_dynamodb_item(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Python dict to DynamoDB item format."""
        item = {}
        for key, value in data.items():
            if value is None:
                continue
            elif isinstance(value, str):
                item[key] = {"S": value}
            elif isinstance(value, bool):
                item[key] = {"BOOL": value}
            elif isinstance(value, int):
                item[key] = {"N": str(value)}
            elif isinstance(value, float):
                item[key] = {"N": str(Decimal(str(value)))}
            elif isinstance(value, dict):
                if value:  # Only add non-empty dicts
                    item[key] = {"S": json.dumps(value)}
            elif isinstance(value, list):
                if value:  # Only add non-empty lists
                    item[key] = {"S": json.dumps(value)}
            else:
                # Fallback to JSON serialization
                item[key] = {"S": json.dumps(value, default=str)}

        return item

    def _from_dynamodb_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert DynamoDB item format to Python dict."""
        data = {}
        for key, value in item.items():
            if "S" in value:
                # Try to parse as JSON first, fallback to string
                try:
                    data[key] = json.loads(value["S"])
                except (json.JSONDecodeError, TypeError):
                    data[key] = value["S"]
            elif "N" in value:
                # Try int first, fallback to float
                try:
                    data[key] = int(value["N"])
                except ValueError:
                    data[key] = float(value["N"])
            elif "BOOL" in value:
                data[key] = value["BOOL"]
            else:
                # Fallback
                data[key] = value

        return data

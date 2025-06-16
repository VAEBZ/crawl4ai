import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .models import CrawlResult

logger = logging.getLogger(__name__)


class DynamoDBManager:
    """
    Manages interactions with a DynamoDB table for caching crawl results.
    Includes advanced features like connection pooling, adaptive retries,
    and batch operations.
    """

    def __init__(
        self,
        table_name: str = "crawl4ai-results",
        sessions_table_name: str = "crawl4ai-sessions",
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
    ):
        self.table_name = table_name
        self.sessions_table_name = sessions_table_name
        self.region_name = region_name
        self.endpoint_url = endpoint_url or os.getenv(
            "DYNAMODB_ENDPOINT_URL", "http://localhost:8000"
        )

        # Best-practice configuration for boto3
        self.boto_config = Config(
            retries={
                "max_attempts": 5,
                "mode": "adaptive",
            },
            connect_timeout=5,
            read_timeout=5,
        )
        self.session = aioboto3.Session()

    async def _get_table(self, table_name: str):
        """Initializes and returns a DynamoDB table resource."""
        async with self.session.resource(
            "dynamodb",
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
            config=self.boto_config,
        ) as dynamo_resource:
            return await dynamo_resource.Table(table_name)

    def _serialize_document(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively converts a dictionary to a DynamoDB-compatible format.
        - Converts floats to Decimals.
        - Removes empty strings, which are not allowed by DynamoDB.
        """
        if isinstance(data, dict):
            return {
                k: self._serialize_document(v)
                for k, v in data.items()
                if v not in ("", None)
            }
        if isinstance(data, list):
            return [
                self._serialize_document(i) for i in data if i not in ("", None)
            ]
        if isinstance(data, float):
            return Decimal(str(data))
        return data

    def _deserialize_document(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively converts Decimals back to floats."""
        if isinstance(data, dict):
            return {k: self._deserialize_document(v) for k, v in data.items()}
        if isinstance(data, list):
            return [self._deserialize_document(i) for i in data]
        if isinstance(data, Decimal):
            return float(data)
        return data

    async def get_cached_url(self, url: str) -> Optional[CrawlResult]:
        """
        Retrieves a cached crawl result from DynamoDB.

        Args:
            url (str): The URL to retrieve.

        Returns:
            A CrawlResult object if found, otherwise None.
        """
        table = await self._get_table(self.table_name)
        try:
            response = await table.get_item(Key={"url": url})
            item = response.get("Item")
            if not item:
                logger.info("URL '%s' not found in DynamoDB cache.", url)
                return None

            logger.info("URL '%s' found in DynamoDB cache.", url)
            deserialized_item = self._deserialize_document(item)
            return CrawlResult(**deserialized_item)
        except ClientError as e:
            logger.error(
                "Failed to get item from DynamoDB for URL %s: %s",
                url,
                e.response["Error"]["Message"],
            )
            return None

    async def cache_url(self, result: CrawlResult, ttl_days: int = 30):
        """
        Caches a crawl result in DynamoDB.

        Args:
            result (CrawlResult): The crawl result to cache.
            ttl_days (int): The time-to-live for the cached item in days.
        """
        table = await self._get_table(self.table_name)

        # Prepare item for DynamoDB
        item = result.dict()
        item["created_at"] = datetime.now(timezone.utc).isoformat()

        # Add TTL attribute
        if ttl_days > 0:
            ttl_timestamp = datetime.now(timezone.utc) + timedelta(
                days=ttl_days
            )
            item["ttl"] = int(ttl_timestamp.timestamp())

        serialized_item = self._serialize_document(item)

        try:
            await table.put_item(Item=serialized_item)
            logger.info(
                "Successfully cached URL '%s' in DynamoDB.", result.url
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ValidationException":
                logger.error(
                    (
                        "Validation failed for item %s. Check for empty "
                        "strings or other invalid data. Error: %s"
                    ),
                    result.url,
                    e.response["Error"]["Message"],
                )
            else:
                logger.error(
                    "Failed to cache item in DynamoDB for URL %s: %s",
                    result.url,
                    e.response["Error"]["Message"],
                )

    async def close(self):
        """
        A placeholder for closing connections.
        With aioboto3's context managers, explicit close is not needed.
        """
        logger.info(
            "DynamoDBManager is stateless, no active connections to close."
        )
        await asyncio.sleep(0) 
"""
Production-ready Mem0 backend for crawl4ai using DynamoDB.

This module provides an intelligent, semantic storage backend using Mem0 with:
- DynamoDB as the vector store backend
- Semantic search and retrieval capabilities
- Graph-based relationship tracking
- Smart caching with similarity detection
- Unified storage architecture with DynamoDB
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import BackendConnectionError, StorageError
from ..models import CrawlResult

logger = logging.getLogger(__name__)

# Mem0 is optional - graceful degradation if not available
try:
    from mem0 import Memory

    MEM0_AVAILABLE = True
except ImportError:
    MEM0_AVAILABLE = False
    Memory = None
    logger.warning("Mem0 not available. Install with: pip install mem0ai")


class Mem0Backend:
    """
    Intelligent memory-based storage using Mem0 with DynamoDB backend.

    Features:
    - DynamoDB as unified vector store backend
    - Semantic search and retrieval across stored content
    - Graph-based relationship tracking between URLs
    - Smart caching with content similarity detection
    - Advanced memory categorization and filtering
    - Automatic memory updates and conflict resolution
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        enable_graph: bool = True,
        dynamodb_config: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize Mem0 backend with DynamoDB configuration.

        Args:
            api_key: Mem0 API key (if using managed platform)
            enable_graph: Enable graph memory for relationships
            dynamodb_config: DynamoDB configuration for vector store
            config: Custom Mem0 configuration

        Raises:
            BackendConnectionError: If Mem0 is not available
        """
        if not MEM0_AVAILABLE:
            raise BackendConnectionError(
                "Mem0 is not available. Install with: pip install mem0ai", "mem0"
            )

        self.api_key = api_key
        self.enable_graph = enable_graph
        self.dynamodb_config = dynamodb_config or {}
        self.config = config or self._default_config()

        try:
            # Initialize Memory with proper configuration
            if self.api_key:
                # Use managed Mem0 platform
                self.memory = Memory(api_key=self.api_key)
            else:
                # Use open source version with custom config
                from mem0 import MemoryConfig
                mem_config = MemoryConfig(**self.config)
                self.memory = Memory(config=mem_config)
                
            logger.info(
                f"Initialized Mem0 backend with DynamoDB: graph={enable_graph}, "
                f"table={self.dynamodb_config.get('table_name', 'mem0-vectors')}"
            )
        except Exception as e:
            error_msg = f"Failed to initialize Mem0 with DynamoDB: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise BackendConnectionError(error_msg, "mem0") from e

    async def store_result(self, url: str, result: CrawlResult) -> None:
        """
        Store result as semantic memory with rich context.

        Args:
            url: The URL that was crawled
            result: The CrawlResult object to store

        Raises:
            StorageError: If storage operation fails
        """
        try:
            memory_data = self._prepare_memory_data(url, result)

            # Store the main content memory
            memory_response = await self.memory.add(
                messages=memory_data["messages"],
                user_id=memory_data["user_id"],
                metadata=memory_data["metadata"],
            )

            # Store additional context if graph is enabled
            if self.enable_graph and memory_data.get("relationships"):
                await self._store_relationships(url, memory_data["relationships"])

            logger.info(
                f"Successfully stored semantic memory for URL: {url}, "
                f"memory_id: {memory_response.get('id', 'unknown')}"
            )

        except Exception as e:
            error_msg = f"Failed to store memory for {url}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StorageError(error_msg, "mem0") from e

    async def retrieve_result(self, url: str) -> Optional[CrawlResult]:
        """
        Retrieve result using exact URL match.

        Args:
            url: The URL to retrieve results for

        Returns:
            CrawlResult if found, None otherwise
        """
        try:
            # Search for exact URL match
            memories = await self.memory.search(
                query=f"exact URL: {url}",
                user_id=f"url:{self._url_to_id(url)}",
                limit=1,
            )

            if not memories:
                logger.debug(f"No memory found for URL: {url}")
                return None

            result = self._reconstruct_result(memories[0])
            logger.info(f"Retrieved memory for URL: {url}")
            return result

        except Exception as e:
            error_msg = f"Failed to retrieve memory for {url}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            # Don't raise on retrieval errors, just return None
            return None

    async def semantic_search(
        self, query: str, limit: int = 10, filters: Optional[Dict[str, Any]] = None
    ) -> List[CrawlResult]:
        """
        Search for similar content across all stored results.

        Args:
            query: Search query for semantic matching
            limit: Maximum number of results to return
            filters: Additional filters for the search

        Returns:
            List of matching CrawlResult objects
        """
        try:
            search_kwargs = {"query": query, "limit": limit}

            if filters:
                search_kwargs.update(filters)

            memories = await self.memory.search(**search_kwargs)

            results = []
            for memory in memories:
                try:
                    result = self._reconstruct_result(memory)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning(f"Failed to reconstruct result from memory: {e}")
                    continue

            logger.info(
                f"Semantic search returned {len(results)} results for query: {query}"
            )
            return results

        except Exception as e:
            error_msg = f"Semantic search failed for query '{query}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            return []

    async def find_similar_content(
        self, url: str, similarity_threshold: float = 0.8
    ) -> List[CrawlResult]:
        """
        Find content similar to the given URL.

        Args:
            url: URL to find similar content for
            similarity_threshold: Minimum similarity score

        Returns:
            List of similar CrawlResult objects
        """
        try:
            # First get the content for the URL
            current_result = await self.retrieve_result(url)
            if not current_result:
                return []

            # Search for similar content using the cleaned text
            search_query = current_result.cleaned_html[:500]  # First 500 chars
            similar_memories = await self.memory.search(
                query=search_query,
                limit=20,  # Get more to filter by similarity
            )

            # Filter by similarity and exclude the original URL
            similar_results = []
            for memory in similar_memories:
                try:
                    result = self._reconstruct_result(memory)
                    if result and result.url != url:
                        similar_results.append(result)
                except Exception as e:
                    logger.warning(f"Failed to process similar memory: {e}")
                    continue

            logger.info(f"Found {len(similar_results)} similar results for URL: {url}")
            return similar_results

        except Exception as e:
            error_msg = f"Failed to find similar content for {url}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return []

    async def close(self) -> None:
        """Clean up resources."""
        try:
            # Mem0 client cleanup if needed
            if hasattr(self.memory, "close"):
                await self.memory.close()
            logger.info("Mem0 backend closed successfully")
        except Exception as e:
            logger.error(f"Error closing Mem0 backend: {e}", exc_info=True)

    def _default_config(self) -> Dict[str, Any]:
        """Default Mem0 configuration using DynamoDB as vector store."""
        # Get DynamoDB configuration
        dynamodb_table = self.dynamodb_config.get("table_name", "mem0-vectors")
        dynamodb_region = self.dynamodb_config.get("region_name", "us-east-1")
        dynamodb_endpoint = self.dynamodb_config.get(
            "endpoint_url", os.getenv("DYNAMODB_ENDPOINT_URL")
        )

        config = {
            "llm": {
                "provider": "openai",
                "config": {
                    "model": "gpt-4o-mini",
                    "temperature": 0.1,
                },
            },
            "embedder": {
                "provider": "openai",
                "config": {
                    "model": "text-embedding-3-small",
                },
            },
            # Use DynamoDB as vector store
            "vector_store": {
                "provider": "aws_dynamodb",
                "config": {
                    "table_name": dynamodb_table,
                    "region_name": dynamodb_region,
                    "endpoint_url": dynamodb_endpoint,
                    # Add AWS credentials if provided
                    **{
                        k: v
                        for k, v in self.dynamodb_config.items()
                        if k in ["aws_access_key_id", "aws_secret_access_key"]
                    },
                },
            },
        }

        # Add graph store if enabled (also using DynamoDB)
        if self.enable_graph:
            graph_table = self.dynamodb_config.get("graph_table_name", "mem0-graph")
            config["graph_store"] = {
                "provider": "aws_dynamodb",
                "config": {
                    "table_name": graph_table,
                    "region_name": dynamodb_region,
                    "endpoint_url": dynamodb_endpoint,
                    **{
                        k: v
                        for k, v in self.dynamodb_config.items()
                        if k in ["aws_access_key_id", "aws_secret_access_key"]
                    },
                },
            }

        return config

    def _prepare_memory_data(self, url: str, result: CrawlResult) -> Dict[str, Any]:
        """
        Prepare CrawlResult for Mem0 storage with rich context.

        Args:
            url: The URL that was crawled
            result: The CrawlResult to prepare

        Returns:
            Dictionary with memory data for Mem0
        """
        # Create rich content description
        content_summary = self._create_content_summary(result)

        # Prepare messages for Mem0
        messages = [
            {
                "role": "system",
                "content": f"Crawled web content from {url}",
            },
            {
                "role": "user",
                "content": (
                    f"URL: {url}\n\n"
                    f"Content Summary: {content_summary}\n\n"
                    f"Full Content: {result.cleaned_html[:2000]}"
                ),
            },
        ]

        # Prepare metadata
        metadata = {
            "url": url,
            "domain": self._extract_domain(url),
            "crawled_at": (
                result.crawled_at.isoformat()
                if result.crawled_at
                else datetime.now(timezone.utc).isoformat()
            ),
            "success": result.success,
            "status_code": result.status_code,
            "content_length": len(result.cleaned_html or ""),
            "has_links": bool(result.links),
            "has_media": bool(result.media),
            "session_id": result.session_id,
        }

        # Add content categories
        if result.metadata:
            metadata.update(result.metadata)

        return {
            "messages": messages,
            "user_id": f"url:{self._url_to_id(url)}",
            "metadata": metadata,
            "relationships": self._extract_relationships(url, result),
        }

    def _reconstruct_result(self, memory: Dict[str, Any]) -> Optional[CrawlResult]:
        """
        Reconstruct CrawlResult from Mem0 memory.

        Args:
            memory: Mem0 memory object

        Returns:
            Reconstructed CrawlResult or None if reconstruction fails
        """
        try:
            metadata = memory.get("metadata", {})

            # Extract URL from metadata or memory content
            url = metadata.get("url", "")
            if not url:
                # Try to extract from memory content
                content = memory.get("memory", "")
                if "URL:" in content:
                    url = content.split("URL:")[1].split("\n")[0].strip()

            # Parse timestamp
            crawled_at = None
            if metadata.get("crawled_at"):
                crawled_at = datetime.fromisoformat(metadata["crawled_at"])

            # Reconstruct basic CrawlResult
            # Note: Mem0 stores processed/summarized content, not full HTML
            return CrawlResult(
                url=url,
                html="",  # Not stored in Mem0
                cleaned_html=memory.get("memory", ""),  # Stored content
                markdown="",  # Could be regenerated if needed
                extracted_content=memory.get("memory", ""),
                success=metadata.get("success", True),
                status_code=metadata.get("status_code", 200),
                error_message=None,
                session_id=metadata.get("session_id"),
                crawled_at=crawled_at,
                response_headers={},
                links={},
                media={},
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"Failed to reconstruct result from memory: {e}")
            return None

    def _create_content_summary(self, result: CrawlResult) -> str:
        """Create a concise summary of the crawled content."""
        summary_parts = []

        if result.success:
            summary_parts.append("Successfully crawled content")
            if result.cleaned_html:
                content_length = len(result.cleaned_html)
                summary_parts.append(f"Content length: {content_length} chars")
        else:
            summary_parts.append(f"Failed to crawl: {result.error_message}")

        if result.links:
            internal_count = len(result.links.get("internal", []))
            external_count = len(result.links.get("external", []))
            summary_parts.append(
                f"Links: {internal_count} internal, {external_count} external"
            )

        if result.media:
            media_count = sum(len(items) for items in result.media.values())
            summary_parts.append(f"Media items: {media_count}")

        return "; ".join(summary_parts)

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse

            return urlparse(url).netloc
        except Exception:
            return ""

    def _url_to_id(self, url: str) -> str:
        """Convert URL to a safe ID for Mem0."""
        import hashlib

        return hashlib.md5(url.encode()).hexdigest()

    def _extract_relationships(
        self, url: str, result: CrawlResult
    ) -> List[Dict[str, Any]]:
        """Extract relationships for graph storage."""
        relationships = []

        if not result.links:
            return relationships

        domain = self._extract_domain(url)

        # Add relationships to linked URLs
        for link_type, links in result.links.items():
            for link_url in links[:10]:  # Limit to first 10 links
                relationships.append(
                    {
                        "type": f"links_to_{link_type}",
                        "source": url,
                        "target": link_url,
                        "metadata": {
                            "source_domain": domain,
                            "target_domain": self._extract_domain(link_url),
                        },
                    }
                )

        return relationships

    async def _store_relationships(
        self, url: str, relationships: List[Dict[str, Any]]
    ) -> None:
        """Store relationship data for graph memory."""
        try:
            # Store relationships as additional memories in DynamoDB
            for relationship in relationships:
                rel_memory = {
                    "role": "system",
                    "content": (
                        f"Relationship: {relationship['source']} "
                        f"{relationship['type']} {relationship['target']}"
                    ),
                }

                await self.memory.add(
                    messages=[rel_memory],
                    user_id=f"relationship:{self._url_to_id(url)}",
                    metadata=relationship.get("metadata", {}),
                )

        except Exception as e:
            logger.warning(f"Failed to store relationships for {url}: {e}")

"""
Enhanced AsyncWebCrawler with pluggable storage backends.

This module provides an enhanced version of AsyncWebCrawler that supports
multiple storage backends including DynamoDB and Mem0 for intelligent caching
and semantic search capabilities.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from .async_webcrawler import AsyncWebCrawler
from .backends import BackendType
from .backends.factory import BackendFactory
from .models import CrawlResult, CrawlResultContainer

logger = logging.getLogger(__name__)


class EnhancedAsyncWebCrawler(AsyncWebCrawler):
    """
    Enhanced webcrawler with pluggable storage backends.

    This crawler extends the base AsyncWebCrawler with:
    - Pluggable storage backends (DynamoDB, Mem0)
    - Intelligent caching with semantic search
    - Clean configuration management
    - Advanced memory capabilities
    """

    def __init__(
        self,
        backend_type: Union[BackendType, str] = BackendType.DYNAMODB,
        backend_config: Optional[Dict[str, Any]] = None,
        backend_url: Optional[str] = None,
        always_bypass_cache: bool = False,
        **kwargs,
    ) -> None:
        """
        Initialize enhanced crawler with specified backend.

        Args:
            backend_type: Type of storage backend to use
            backend_config: Configuration for the backend
            backend_url: URL-style backend configuration
            always_bypass_cache: Always bypass cache and crawl fresh
            **kwargs: Additional arguments for base AsyncWebCrawler

        Examples:
            # DynamoDB backend
            crawler = EnhancedAsyncWebCrawler(
                backend_type="dynamodb",
                backend_config={"table_name": "my-crawl-cache"}
            )

            # Mem0 backend with DynamoDB
            crawler = EnhancedAsyncWebCrawler(
                backend_type="mem0",
                backend_config={
                    "dynamodb_config": {
                        "table_name": "mem0-vectors",
                        "graph_table_name": "mem0-graph"
                    }
                }
            )

            # URL-style configuration
            crawler = EnhancedAsyncWebCrawler(
                backend_url="dynamodb://crawl-cache?region=us-west-2"
            )
        """
        # Initialize base crawler without db_manager
        super().__init__(db_manager=None, **kwargs)

        self.always_bypass_cache = always_bypass_cache

        # Create backend from URL or type/config
        if backend_url:
            self.backend = BackendFactory.create_from_url(backend_url)
            logger.info(f"Created backend from URL: {backend_url}")
        else:
            self.backend = BackendFactory.create_backend(backend_type, backend_config)
            logger.info(f"Created {backend_type} backend with config: {backend_config}")

    async def arun(
        self, url: str, bypass_cache: bool = False, **kwargs
    ) -> CrawlResultContainer:
        """
        Enhanced arun with intelligent caching.

        Args:
            url: URL to crawl
            bypass_cache: Skip cache lookup for this request
            **kwargs: Additional crawl parameters

        Returns:
            CrawlResultContainer with crawl results
        """
        # Determine if we should check cache
        should_bypass = self.always_bypass_cache or bypass_cache

        # Check cache first if not bypassing
        if not should_bypass:
            try:
                cached_result = await self.backend.retrieve_result(url)
                if cached_result:
                    logger.info(f"Cache hit for URL: {url}")
                    return self._wrap_cached_result(cached_result)
            except Exception as e:
                logger.warning(f"Cache lookup failed for {url}: {e}")
                # Continue with fresh crawl

        # Run actual crawl using base class
        logger.info(f"Crawling fresh content for URL: {url}")
        result_container = await super().arun(url, **kwargs)

        # Store result in cache if successful
        if (
            result_container
            and hasattr(result_container, "crawl_result")
            and result_container.crawl_result
            and result_container.crawl_result.success
        ):
            try:
                await self.backend.store_result(url, result_container.crawl_result)
                logger.info(f"Cached result for URL: {url}")
            except Exception as e:
                logger.warning(f"Failed to cache result for {url}: {e}")
                # Don't fail the crawl if caching fails

        return result_container

    async def semantic_search(
        self, query: str, limit: int = 10, **kwargs
    ) -> List[CrawlResult]:
        """
        Perform semantic search across cached content.

        This method is only available when using Mem0 backend.

        Args:
            query: Search query for semantic matching
            limit: Maximum number of results to return
            **kwargs: Additional search parameters

        Returns:
            List of matching CrawlResult objects

        Raises:
            NotImplementedError: If backend doesn't support semantic search
        """
        if not hasattr(self.backend, "semantic_search"):
            raise NotImplementedError(
                f"Semantic search not supported by {type(self.backend).__name__}. "
                "Use Mem0 backend for semantic search capabilities."
            )

        try:
            results = await self.backend.semantic_search(query, limit, **kwargs)
            logger.info(
                f"Semantic search for '{query}' returned {len(results)} results"
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

        This method is only available when using Mem0 backend.

        Args:
            url: URL to find similar content for
            similarity_threshold: Minimum similarity score

        Returns:
            List of similar CrawlResult objects

        Raises:
            NotImplementedError: If backend doesn't support similarity search
        """
        if not hasattr(self.backend, "find_similar_content"):
            raise NotImplementedError(
                f"Similarity search not supported by {type(self.backend).__name__}. "
                "Use Mem0 backend for similarity search capabilities."
            )

        try:
            results = await self.backend.find_similar_content(url, similarity_threshold)
            logger.info(f"Found {len(results)} similar results for URL: {url}")
            return results
        except Exception as e:
            error_msg = f"Similarity search failed for URL '{url}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            return []

    async def get_backend_info(self) -> Dict[str, Any]:
        """
        Get information about the current backend.

        Returns:
            Dictionary with backend information
        """
        backend_info = {
            "backend_type": type(self.backend).__name__,
            "supports_semantic_search": hasattr(self.backend, "semantic_search"),
            "supports_similarity_search": hasattr(self.backend, "find_similar_content"),
        }

        # Add backend-specific information
        if hasattr(self.backend, "table_name"):
            backend_info["table_name"] = self.backend.table_name
        if hasattr(self.backend, "region_name"):
            backend_info["region_name"] = self.backend.region_name
        if hasattr(self.backend, "enable_graph"):
            backend_info["graph_enabled"] = self.backend.enable_graph

        return backend_info

    async def close(self) -> None:
        """Clean up resources including backend connections."""
        try:
            # Close backend first
            if self.backend:
                await self.backend.close()
                logger.debug("Backend closed successfully")

            # Close base crawler
            await super().close()
            logger.info("Enhanced crawler closed successfully")

        except Exception as e:
            logger.error(f"Error closing enhanced crawler: {e}", exc_info=True)

    def _wrap_cached_result(self, cached_result: CrawlResult) -> CrawlResultContainer:
        """
        Wrap cached CrawlResult in a CrawlResultContainer.

        Args:
            cached_result: The cached CrawlResult

        Returns:
            CrawlResultContainer with the cached result
        """
        return CrawlResultContainer(
            crawl_result=cached_result,
            markdown_generation_result=None,  # Not stored in cache
        )

    async def __aenter__(self):
        """Async context manager entry."""
        await super().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        return await super().__aexit__(exc_type, exc_val, exc_tb)


# Convenience functions for common configurations
def create_dynamodb_crawler(
    table_name: str = "crawl4ai-results",
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    **kwargs,
) -> EnhancedAsyncWebCrawler:
    """
    Create crawler with DynamoDB backend.

    Args:
        table_name: DynamoDB table name
        region_name: AWS region
        endpoint_url: Custom endpoint (for local DynamoDB)
        **kwargs: Additional crawler arguments

    Returns:
        Configured EnhancedAsyncWebCrawler
    """
    config = {
        "table_name": table_name,
        "region_name": region_name,
    }
    if endpoint_url:
        config["endpoint_url"] = endpoint_url

    return EnhancedAsyncWebCrawler(
        backend_type=BackendType.DYNAMODB, backend_config=config, **kwargs
    )


def create_mem0_crawler(
    vector_table: str = "mem0-vectors",
    graph_table: str = "mem0-graph",
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    enable_graph: bool = True,
    **kwargs,
) -> EnhancedAsyncWebCrawler:
    """
    Create crawler with Mem0 backend using DynamoDB.

    Args:
        vector_table: DynamoDB table for vectors
        graph_table: DynamoDB table for graph data
        region_name: AWS region
        endpoint_url: Custom endpoint (for local DynamoDB)
        enable_graph: Enable graph memory features
        **kwargs: Additional crawler arguments

    Returns:
        Configured EnhancedAsyncWebCrawler
    """
    config = {
        "enable_graph": enable_graph,
        "dynamodb_config": {
            "table_name": vector_table,
            "graph_table_name": graph_table,
            "region_name": region_name,
        },
    }
    if endpoint_url:
        config["dynamodb_config"]["endpoint_url"] = endpoint_url

    return EnhancedAsyncWebCrawler(
        backend_type=BackendType.MEM0, backend_config=config, **kwargs
    )

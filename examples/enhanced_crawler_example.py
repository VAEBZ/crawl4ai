"""
Enhanced Crawler Example: DynamoDB + Mem0 Integration

This example demonstrates the enhanced crawler with both DynamoDB and Mem0 backends,
showing how they work together with DynamoDB as the unified storage layer.
"""

import asyncio
import logging
import os
from typing import List

from crawl4ai import create_dynamodb_crawler, create_mem0_crawler
from crawl4ai.models import CrawlResult

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def demo_dynamodb_backend():
    """Demonstrate DynamoDB backend for fast caching."""
    print("\n" + "=" * 60)
    print("DYNAMODB BACKEND DEMO")
    print("=" * 60)

    # Create crawler with DynamoDB backend (local)
    crawler = create_dynamodb_crawler(
        table_name="crawl4ai-demo",
        endpoint_url="http://localhost:8000",  # Local DynamoDB
        region_name="us-east-1",
    )

    try:
        # Get backend info
        backend_info = await crawler.get_backend_info()
        print(f"Backend Info: {backend_info}")

        # Test URLs
        test_urls = [
            "https://example.com",
            "https://httpbin.org/json",
            "https://jsonplaceholder.typicode.com/posts/1",
        ]

        print(f"\nCrawling {len(test_urls)} URLs...")

        # First crawl - should be fresh
        for url in test_urls:
            print(f"\nCrawling: {url}")
            result = await crawler.arun(url)
            if result.crawl_result and result.crawl_result.success:
                print(f"✓ Success: {len(result.crawl_result.cleaned_html)} chars")
            else:
                print(
                    f"✗ Failed: {result.crawl_result.error_message if result.crawl_result else 'No result'}"
                )

        print(f"\nSecond crawl - should hit cache...")

        # Second crawl - should hit cache
        for url in test_urls:
            print(f"\nCrawling (cached): {url}")
            result = await crawler.arun(url)
            if result.crawl_result and result.crawl_result.success:
                print(f"✓ Cache hit: {len(result.crawl_result.cleaned_html)} chars")

    finally:
        await crawler.close()


async def demo_mem0_backend():
    """Demonstrate Mem0 backend with semantic capabilities."""
    print("\n" + "=" * 60)
    print("MEM0 BACKEND DEMO")
    print("=" * 60)

    # Create crawler with Mem0 backend using DynamoDB
    crawler = create_mem0_crawler(
        vector_table="mem0-vectors-demo",
        graph_table="mem0-graph-demo",
        endpoint_url="http://localhost:8000",  # Local DynamoDB
        region_name="us-east-1",
        enable_graph=True,
    )

    try:
        # Get backend info
        backend_info = await crawler.get_backend_info()
        print(f"Backend Info: {backend_info}")

        # Test URLs with different content types
        tech_urls = [
            "https://docs.python.org/3/tutorial/",
            "https://fastapi.tiangolo.com/",
            "https://docs.djangoproject.com/en/stable/",
        ]

        news_urls = [
            "https://example.com/news/tech",
            "https://example.com/news/science",
        ]

        print(f"\nCrawling tech documentation...")

        # Crawl tech documentation
        for url in tech_urls:
            print(f"\nCrawling: {url}")
            try:
                result = await crawler.arun(url)
                if result.crawl_result and result.crawl_result.success:
                    print(
                        f"✓ Stored in Mem0: {len(result.crawl_result.cleaned_html)} chars"
                    )
                else:
                    print(
                        f"✗ Failed: {result.crawl_result.error_message if result.crawl_result else 'No result'}"
                    )
            except Exception as e:
                print(f"✗ Error: {e}")

        print(f"\nCrawling news articles...")

        # Crawl news articles
        for url in news_urls:
            print(f"\nCrawling: {url}")
            try:
                result = await crawler.arun(url)
                if result.crawl_result and result.crawl_result.success:
                    print(
                        f"✓ Stored in Mem0: {len(result.crawl_result.cleaned_html)} chars"
                    )
                else:
                    print(
                        f"✗ Failed: {result.crawl_result.error_message if result.crawl_result else 'No result'}"
                    )
            except Exception as e:
                print(f"✗ Error: {e}")

        # Demonstrate semantic search
        print(f"\n" + "-" * 40)
        print("SEMANTIC SEARCH DEMO")
        print("-" * 40)

        search_queries = [
            "Python web framework tutorial",
            "API documentation and examples",
            "technology news and updates",
        ]

        for query in search_queries:
            print(f"\nSearching: '{query}'")
            try:
                results = await crawler.semantic_search(query, limit=3)
                print(f"Found {len(results)} results:")
                for i, result in enumerate(results, 1):
                    print(f"  {i}. {result.url} ({len(result.cleaned_html)} chars)")
            except Exception as e:
                print(f"✗ Search error: {e}")

        # Demonstrate similarity search
        print(f"\n" + "-" * 40)
        print("SIMILARITY SEARCH DEMO")
        print("-" * 40)

        if tech_urls:
            reference_url = tech_urls[0]
            print(f"\nFinding content similar to: {reference_url}")
            try:
                similar_results = await crawler.find_similar_content(
                    reference_url, similarity_threshold=0.7
                )
                print(f"Found {len(similar_results)} similar results:")
                for i, result in enumerate(similar_results, 1):
                    print(f"  {i}. {result.url} ({len(result.cleaned_html)} chars)")
            except Exception as e:
                print(f"✗ Similarity search error: {e}")

    finally:
        await crawler.close()


async def demo_unified_architecture():
    """Demonstrate how DynamoDB and Mem0 work together."""
    print("\n" + "=" * 60)
    print("UNIFIED ARCHITECTURE DEMO")
    print("=" * 60)

    # Create both crawlers
    dynamodb_crawler = create_dynamodb_crawler(
        table_name="unified-cache",
        endpoint_url="http://localhost:8000",
    )

    mem0_crawler = create_mem0_crawler(
        vector_table="unified-vectors",
        graph_table="unified-graph",
        endpoint_url="http://localhost:8000",
    )

    try:
        test_url = "https://example.com/unified-test"

        print(f"1. Crawling with DynamoDB backend: {test_url}")
        dynamodb_result = await dynamodb_crawler.arun(test_url)

        print(f"2. Crawling same URL with Mem0 backend: {test_url}")
        mem0_result = await mem0_crawler.arun(test_url)

        print(f"3. Both backends store in DynamoDB but serve different purposes:")
        print(f"   - DynamoDB: Fast key-value cache")
        print(f"   - Mem0: Semantic search + graph relationships")

        # Show backend information
        dynamodb_info = await dynamodb_crawler.get_backend_info()
        mem0_info = await mem0_crawler.get_backend_info()

        print(f"\nDynamoDB Backend: {dynamodb_info}")
        print(f"Mem0 Backend: {mem0_info}")

    finally:
        await dynamodb_crawler.close()
        await mem0_crawler.close()


async def main():
    """Run all demos."""
    print("Enhanced Crawler Demo: DynamoDB + Mem0 Integration")
    print("=" * 60)

    # Check if local DynamoDB is running
    try:
        import boto3

        dynamodb = boto3.client(
            "dynamodb",
            endpoint_url="http://localhost:8000",
            region_name="us-east-1",
            aws_access_key_id="dummy",
            aws_secret_access_key="dummy",
        )
        dynamodb.list_tables()
        print("✓ Local DynamoDB is running")
    except Exception as e:
        print(f"✗ Local DynamoDB not available: {e}")
        print(
            "Please start local DynamoDB with: docker-compose -f docker-compose.aws.yml up -d"
        )
        return

    try:
        # Run demos
        await demo_dynamodb_backend()
        await demo_mem0_backend()
        await demo_unified_architecture()

        print("\n" + "=" * 60)
        print("DEMO COMPLETED SUCCESSFULLY!")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)


if __name__ == "__main__":
    # Set environment variables for demo
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "dummy")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "dummy")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    asyncio.run(main())

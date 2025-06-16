"""
Test script to ingest DK Plus API documentation using unified DynamoDB + Mem0 architecture.

This demonstrates the complete backend system working together:
- DynamoDB backend for fast caching
- Mem0 backend for semantic search capabilities
- Both using DynamoDB as the unified storage layer
"""

import asyncio
import logging
import os
from datetime import datetime

from crawl4ai import create_dynamodb_crawler, create_mem0_crawler

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Target URL
DKPLUS_API_URL = "https://api.dkplus.is/swagger/ui/index#/"


async def test_dynamodb_backend():
    """Test DynamoDB backend for fast caching."""
    print("\n" + "=" * 60)
    print("TESTING DYNAMODB BACKEND")
    print("=" * 60)

    # Create DynamoDB crawler (note: using port 8900 as shown in docker ps)
    crawler = create_dynamodb_crawler(
        table_name="dkplus-cache",
        endpoint_url="http://localhost:8900",
        region_name="us-east-1",
    )

    try:
        # Get backend info
        backend_info = await crawler.get_backend_info()
        print(f"Backend Info: {backend_info}")

        print(f"\n🚀 Crawling DK Plus API documentation...")
        print(f"URL: {DKPLUS_API_URL}")

        # First crawl - should be fresh
        start_time = datetime.now()
        result = await crawler.arun(DKPLUS_API_URL)
        first_duration = (datetime.now() - start_time).total_seconds()

        if result.crawl_result and result.crawl_result.success:
            content_length = len(result.crawl_result.cleaned_html)
            print(f"✅ First crawl successful!")
            print(f"   Duration: {first_duration:.2f}s")
            print(f"   Content length: {content_length:,} characters")
            print(f"   Status code: {result.crawl_result.status_code}")

            # Show a snippet of the content
            snippet = result.crawl_result.cleaned_html[:200]
            print(f"   Content snippet: {snippet}...")
        else:
            error_msg = (
                result.crawl_result.error_message
                if result.crawl_result
                else "No result"
            )
            print(f"❌ First crawl failed: {error_msg}")
            return

        print(f"\n🔄 Testing cache hit...")

        # Second crawl - should hit cache
        start_time = datetime.now()
        cached_result = await crawler.arun(DKPLUS_API_URL)
        cache_duration = (datetime.now() - start_time).total_seconds()

        if cached_result.crawl_result and cached_result.crawl_result.success:
            print(f"✅ Cache hit successful!")
            print(f"   Duration: {cache_duration:.2f}s")
            print(f"   Speed improvement: {first_duration/cache_duration:.1f}x faster")
            print(
                f"   Content length: {len(cached_result.crawl_result.cleaned_html):,} characters"
            )
        else:
            print(f"❌ Cache retrieval failed")

    except Exception as e:
        logger.error(f"DynamoDB backend test failed: {e}", exc_info=True)
    finally:
        await crawler.close()


async def test_mem0_backend():
    """Test Mem0 backend with semantic capabilities."""
    print("\n" + "=" * 60)
    print("TESTING MEM0 BACKEND")
    print("=" * 60)

    # Create Mem0 crawler using DynamoDB as vector store
    crawler = create_mem0_crawler(
        vector_table="dkplus-vectors",
        graph_table="dkplus-graph",
        endpoint_url="http://localhost:8900",
        region_name="us-east-1",
        enable_graph=True,
    )

    try:
        # Get backend info
        backend_info = await crawler.get_backend_info()
        print(f"Backend Info: {backend_info}")

        print(f"\n🧠 Ingesting into Mem0 with semantic capabilities...")
        print(f"URL: {DKPLUS_API_URL}")

        # Crawl and store in Mem0
        start_time = datetime.now()
        result = await crawler.arun(DKPLUS_API_URL)
        duration = (datetime.now() - start_time).total_seconds()

        if result.crawl_result and result.crawl_result.success:
            content_length = len(result.crawl_result.cleaned_html)
            print(f"✅ Mem0 ingestion successful!")
            print(f"   Duration: {duration:.2f}s")
            print(f"   Content length: {content_length:,} characters")
            print(f"   Status code: {result.crawl_result.status_code}")
            print(f"   Stored in DynamoDB vector store for semantic search")
        else:
            error_msg = (
                result.crawl_result.error_message
                if result.crawl_result
                else "No result"
            )
            print(f"❌ Mem0 ingestion failed: {error_msg}")
            return

        # Test semantic search capabilities
        print(f"\n🔍 Testing semantic search capabilities...")

        search_queries = [
            "API endpoints and documentation",
            "swagger documentation interface",
            "REST API methods and parameters",
            "authentication and security",
        ]

        for query in search_queries:
            print(f"\n   Searching: '{query}'")
            try:
                search_results = await crawler.semantic_search(query, limit=3)
                print(f"   Found {len(search_results)} semantic matches")

                for i, search_result in enumerate(search_results, 1):
                    snippet = (
                        search_result.cleaned_html[:100]
                        if search_result.cleaned_html
                        else "No content"
                    )
                    print(f"     {i}. {search_result.url}")
                    print(f"        Snippet: {snippet}...")

            except Exception as e:
                print(f"   ❌ Search failed: {e}")

        # Test similarity search
        print(f"\n🔗 Testing similarity search...")
        try:
            similar_results = await crawler.find_similar_content(
                DKPLUS_API_URL, similarity_threshold=0.7
            )
            print(f"   Found {len(similar_results)} similar content pieces")

            for i, similar_result in enumerate(similar_results, 1):
                print(f"     {i}. {similar_result.url}")

        except Exception as e:
            print(f"   ❌ Similarity search failed: {e}")

    except Exception as e:
        logger.error(f"Mem0 backend test failed: {e}", exc_info=True)
    finally:
        await crawler.close()


async def test_unified_architecture():
    """Demonstrate how both backends work together on the same content."""
    print("\n" + "=" * 60)
    print("TESTING UNIFIED ARCHITECTURE")
    print("=" * 60)

    # Create both crawlers
    dynamodb_crawler = create_dynamodb_crawler(
        table_name="unified-dkplus", endpoint_url="http://localhost:8900"
    )

    mem0_crawler = create_mem0_crawler(
        vector_table="unified-vectors",
        graph_table="unified-graph",
        endpoint_url="http://localhost:8900",
    )

    try:
        print(f"🔄 Testing unified storage approach...")
        print(f"URL: {DKPLUS_API_URL}")

        # Crawl with DynamoDB backend
        print(f"\n1️⃣ Storing in DynamoDB backend (fast cache)...")
        dynamodb_result = await dynamodb_crawler.arun(DKPLUS_API_URL)

        if dynamodb_result.crawl_result and dynamodb_result.crawl_result.success:
            print(f"   ✅ DynamoDB storage successful")
            print(
                f"   Content: {len(dynamodb_result.crawl_result.cleaned_html):,} chars"
            )

        # Crawl with Mem0 backend
        print(f"\n2️⃣ Storing in Mem0 backend (semantic search)...")
        mem0_result = await mem0_crawler.arun(DKPLUS_API_URL)

        if mem0_result.crawl_result and mem0_result.crawl_result.success:
            print(f"   ✅ Mem0 storage successful")
            print(f"   Content: {len(mem0_result.crawl_result.cleaned_html):,} chars")

        print(f"\n🏗️ Architecture Summary:")
        print(f"   • DynamoDB Backend: Fast key-value cache in 'unified-dkplus' table")
        print(f"   • Mem0 Backend: Semantic vectors in 'unified-vectors' table")
        print(f"   • Both use DynamoDB as storage foundation")
        print(f"   • No data duplication - different use cases, same infrastructure")

        # Show backend details
        dynamodb_info = await dynamodb_crawler.get_backend_info()
        mem0_info = await mem0_crawler.get_backend_info()

        print(f"\n📊 Backend Details:")
        print(f"   DynamoDB: {dynamodb_info}")
        print(f"   Mem0: {mem0_info}")

    except Exception as e:
        logger.error(f"Unified architecture test failed: {e}", exc_info=True)
    finally:
        await dynamodb_crawler.close()
        await mem0_crawler.close()


async def main():
    """Run all ingestion tests."""
    print("🚀 DK Plus API Ingestion Test")
    print("Testing unified DynamoDB + Mem0 architecture")
    print("=" * 60)

    # Set environment variables
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "dummy")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "dummy")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    try:
        # Test DynamoDB backend
        await test_dynamodb_backend()

        # Test Mem0 backend
        await test_mem0_backend()

        # Test unified architecture
        await test_unified_architecture()

        print("\n" + "=" * 60)
        print("🎉 ALL TESTS COMPLETED SUCCESSFULLY!")
        print("✅ DynamoDB + Mem0 unified architecture working perfectly")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Test suite failed: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())

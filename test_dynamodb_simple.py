"""
Simple test for DynamoDB backend only.
This tests the core functionality without Mem0 complications.
"""

import asyncio
import logging
import os
from datetime import datetime

from crawl4ai import create_dynamodb_crawler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Target URL
DKPLUS_API_URL = "https://api.dkplus.is/swagger/ui/index#/"


async def test_dynamodb_simple():
    """Simple test of DynamoDB backend."""
    print("🚀 Testing DynamoDB Backend")
    print("=" * 50)

    # Set environment variables
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "dummy")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "dummy")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    # Create DynamoDB crawler
    crawler = create_dynamodb_crawler(
        table_name="simple-test",
        endpoint_url="http://localhost:8900",
        region_name="us-east-1",
    )

    try:
        # Get backend info
        backend_info = await crawler.get_backend_info()
        print(f"Backend Info: {backend_info}")

        print(f"\n📡 Crawling: {DKPLUS_API_URL}")

        # First crawl
        start_time = datetime.now()
        result = await crawler.arun(DKPLUS_API_URL)
        duration = (datetime.now() - start_time).total_seconds()

        if result.crawl_result and result.crawl_result.success:
            content_length = len(result.crawl_result.cleaned_html)
            print(f"✅ Crawl successful!")
            print(f"   Duration: {duration:.2f}s")
            print(f"   Content: {content_length:,} characters")
            print(f"   Status: {result.crawl_result.status_code}")

            # Show snippet
            snippet = result.crawl_result.cleaned_html[:200]
            print(f"   Snippet: {snippet}...")

            print(f"\n🔄 Testing cache...")

            # Second crawl - should hit cache
            start_time = datetime.now()
            cached_result = await crawler.arun(DKPLUS_API_URL)
            cache_duration = (datetime.now() - start_time).total_seconds()

            if cached_result.crawl_result and cached_result.crawl_result.success:
                print(f"✅ Cache hit!")
                print(f"   Duration: {cache_duration:.2f}s")
                print(f"   Speed improvement: {duration/cache_duration:.1f}x")
            else:
                print(f"❌ Cache miss")

        else:
            error_msg = (
                result.crawl_result.error_message
                if result.crawl_result
                else "No result"
            )
            print(f"❌ Crawl failed: {error_msg}")

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
    finally:
        await crawler.close()
        print(f"\n✅ Test completed")


if __name__ == "__main__":
    asyncio.run(test_dynamodb_simple())

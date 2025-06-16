#!/usr/bin/env python3
import asyncio
import logging
import sys
import os
from pathlib import Path

# Ensure no conflicting node options
if 'NODE_OPTIONS' in os.environ:
    del os.environ['NODE_OPTIONS']

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from crawl4ai.async_webcrawler_dynamodb import AsyncWebCrawlerDynamoDB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    """Main function to perform the crawl test."""
    url = 'https://api.dkplus.is/swagger/ui/index#/'
    print(f"🚀 Let's crawl {url} and see what happens...")

    try:
        async with AsyncWebCrawlerDynamoDB() as crawler:
            # First run - should crawl and cache
            print("\\n--- FIRST RUN (expecting a live crawl) ---")
            result_container = await crawler.arun(url)
            if result_container and result_container.crawl_result:
                print(f"✅ Crawl successful. URL: {result_container.crawl_result.url}")
                print(f"  - Success: {result_container.crawl_result.success}")
                print(f"  - HTML length: {len(result_container.crawl_result.html)}")
            else:
                print("❌ Crawl failed on first run.")
                return

            # Second run - should hit the cache
            print("\\n--- SECOND RUN (expecting a cache hit) ---")
            result_container_cached = await crawler.arun(url)
            if result_container_cached and result_container_cached.crawl_result:
                print(f"✅ Cache hit successful. URL: {result_container_cached.crawl_result.url}")
                print(f"  - Success: {result_container_cached.crawl_result.success}")
                print(f"  - HTML length: {len(result_container_cached.crawl_result.html)}")
            else:
                print("❌ Cache hit failed on second run.")

    except Exception as e:
        print("\\n" + "="*60)
        print("💥 An unexpected error occurred during the test.")
        print("="*60)
        print(f"  Error details: {e}")
        print("\\nPlease ensure the Docker environment is running:")
        print("  docker-compose -f docker-compose.aws.yml up -d")

if __name__ == "__main__":
    asyncio.run(main()) 
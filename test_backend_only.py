"""
Test just the backend storage without the full crawler.
This isolates the DynamoDB functionality.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from crawl4ai.backends.dynamodb import DynamoDBBackend
from crawl4ai.models import CrawlResult, MarkdownGenerationResult

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_backend_storage():
    """Test DynamoDB backend storage directly."""
    print("🚀 Testing DynamoDB Backend Storage")
    print("=" * 50)

    # Set environment variables
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "dummy")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "dummy")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    # Create backend directly
    backend = DynamoDBBackend(
        table_name="backend-test",
        endpoint_url="http://localhost:8900",
        region_name="us-east-1",
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )

    try:
        # Create a test CrawlResult
        test_url = "https://api.dkplus.is/swagger/ui/index#/"
        test_result = CrawlResult(
            url=test_url,
            html="<html><body>Test content</body></html>",
            cleaned_html="Test content from DK Plus API",
            markdown=MarkdownGenerationResult(
                raw_markdown="# Test Content\nDK Plus API documentation",
                markdown_with_citations="# Test Content\nDK Plus API documentation",
                references_markdown="",
            ),
            extracted_content="API documentation for DK Plus services",
            success=True,
            status_code=200,
            error_message=None,
            session_id="test-session",
            response_headers={"content-type": "text/html"},
            links={"internal": [{"href": "#/", "text": "Home"}], "external": []},
            media={},
            metadata={"test": True, "source": "dkplus"},
        )

        print(f"📝 Storing test result for: {test_url}")

        # Test storage
        await backend.store_result(test_url, test_result)
        print("✅ Storage successful!")

        print(f"🔍 Retrieving stored result...")

        # Test retrieval
        retrieved_result = await backend.retrieve_result(test_url)

        if retrieved_result:
            print("✅ Retrieval successful!")
            print(f"   URL: {retrieved_result.url}")
            print(f"   Success: {retrieved_result.success}")
            print(f"   Status: {retrieved_result.status_code}")
            print(f"   Content length: {len(retrieved_result.cleaned_html)}")
            print(f"   Metadata: {retrieved_result.metadata}")
        else:
            print("❌ No result retrieved")

        # Test cache hit
        print(f"🔄 Testing cache hit...")
        cached_result = await backend.retrieve_result(test_url)

        if cached_result:
            print("✅ Cache hit successful!")
            print(
                f"   Content matches: {cached_result.cleaned_html == test_result.cleaned_html}"
            )
        else:
            print("❌ Cache miss")

    except Exception as e:
        logger.error(f"Backend test failed: {e}", exc_info=True)
    finally:
        await backend.close()
        print(f"\n✅ Backend test completed")


if __name__ == "__main__":
    asyncio.run(test_backend_storage())

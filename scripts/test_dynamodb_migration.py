#!/usr/bin/env python3
"""
Test Script for DynamoDB Migration
Tests the DynamoDB components and migration functionality
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add the parent directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from crawl4ai.dynamodb_manager import DynamoDBManager
from crawl4ai.async_webcrawler_dynamodb import AsyncWebCrawlerDynamoDB
from crawl4ai.mcp_server_dynamodb import MCPServerDynamoDB

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_dynamodb_manager():
    """Test basic DynamoDB manager functionality"""
    print("\n" + "="*60)
    print("TESTING DYNAMODB MANAGER")
    print("="*60)
    
    manager = DynamoDBManager(
        endpoint_url='http://localhost:8000',
        table_name='crawl4ai-results'
    )
    
    try:
        await manager.initialize()
        print("✓ DynamoDB manager initialized")
        
        # Test document storage
        test_doc = {
            'url': 'https://example.com/test',
            'success': True,
            'html': '<html><body>Test content</body></html>',
            'cleaned_html': 'Test content',
            'markdown': {
                'raw_markdown': '# Test\nContent',
                'markdown_with_citations': '',
                'references_markdown': '',
                'fit_markdown': '',
                'fit_html': ''
            },
            'extracted_content': 'Test content',
            'media': {'images': []},
            'links': {'internal': [], 'external': []},
            'metadata': {'title': 'Test Page'},
            'created_at': datetime.utcnow().isoformat()
        }
        
        # Store document
        await manager.cache_url(test_doc['url'], test_doc)
        print(f"✓ Stored test document: {test_doc['url']}")
        
        # Retrieve document
        retrieved = await manager.get_cached_url(test_doc['url'])
        if retrieved and retrieved['url'] == test_doc['url']:
            print("✓ Retrieved test document successfully")
        else:
            print("✗ Failed to retrieve test document")
            return False
        
        # Test cache stats
        count = await manager.get_total_count()
        print(f"✓ Total documents in cache: {count}")
        
        return True
        
    except Exception as e:
        print(f"✗ DynamoDB manager test failed: {e}")
        return False
    
    finally:
        await manager.close()


async def test_async_webcrawler():
    """Test DynamoDB-integrated AsyncWebCrawler"""
    print("\n" + "="*60)
    print("TESTING DYNAMODB WEBCRAWLER")
    print("="*60)
    
    try:
        async with AsyncWebCrawlerDynamoDB() as crawler:
            print("✓ DynamoDB webcrawler initialized")
            
            # Test a simple crawl (using a reliable test URL)
            test_url = "https://httpbin.org/html"
            
            try:
                result = await crawler.arun(test_url)
                
                if result.success:
                    print(f"✓ Successfully crawled: {test_url}")
                    print(f"  - HTML length: {len(result.html) if result.html else 0}")
                    print(f"  - Markdown length: {len(result.markdown) if result.markdown else 0}")
                else:
                    print(f"✗ Crawl failed for: {test_url}")
                    return False
                
                # Test cache retrieval
                cached = await crawler.aget_cached_url(test_url)
                if cached and cached.url == test_url:
                    print("✓ Cache retrieval working")
                else:
                    print("✗ Cache retrieval failed")
                    return False
                
                # Test cache stats
                cache_size = await crawler.aget_cache_size()
                print(f"✓ Cache contains {cache_size} documents")
                
                return True
                
            except Exception as e:
                print(f"✗ Crawling test failed: {e}")
                return False
        
    except Exception as e:
        print(f"✗ Webcrawler initialization failed: {e}")
        return False


async def test_mcp_server():
    """Test MCP server with DynamoDB"""
    print("\n" + "="*60)
    print("TESTING MCP SERVER")
    print("="*60)
    
    server = MCPServerDynamoDB()
    
    try:
        await server.initialize()
        print("✓ MCP server initialized")
        
        # Test crawling via MCP
        test_url = "https://httpbin.org/json"
        result = await server.crawl_url(test_url, session_id="test-session")
        
        if result['success']:
            print(f"✓ MCP crawl successful: {test_url}")
        else:
            print(f"✗ MCP crawl failed: {result.get('error', 'Unknown error')}")
            return False
        
        # Test getting cached content
        content = await server.get_cached_content(test_url, 'markdown')
        if content['success']:
            print("✓ MCP cached content retrieval working")
        else:
            print("✗ MCP cached content retrieval failed")
            return False
        
        # Test cache stats
        stats = await server.get_cache_stats()
        if stats['success']:
            print(f"✓ MCP cache stats: {stats['total_cached_urls']} documents")
        else:
            print("✗ MCP cache stats failed")
            return False
        
        return True
        
    except Exception as e:
        print(f"✗ MCP server test failed: {e}")
        return False
    
    finally:
        await server.close()


async def test_environment():
    """Test if the environment is properly set up"""
    print("\n" + "="*60)
    print("TESTING ENVIRONMENT")
    print("="*60)
    
    # Check environment variables
    env_vars = [
        'AWS_REGION',
        'DYNAMODB_ENDPOINT_URL',
        'DYNAMODB_TABLE_NAME',
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY'
    ]
    
    print("Environment variables:")
    for var in env_vars:
        value = os.getenv(var, 'NOT SET')
        print(f"  {var}: {value}")
    
    # Test DynamoDB connection
    try:
        import boto3
        
        dynamodb = boto3.client(
            'dynamodb',
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            endpoint_url=os.getenv('DYNAMODB_ENDPOINT_URL', 'http://localhost:8000'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'dummy'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'dummy')
        )
        
        # Try to list tables
        response = dynamodb.list_tables()
        tables = response.get('TableNames', [])
        
        print(f"\n✓ DynamoDB connection successful")
        print(f"Available tables: {tables}")
        
        # Check for required tables
        required_tables = ['crawl4ai-results', 'crawl4ai-sessions']
        missing_tables = [table for table in required_tables if table not in tables]
        
        if missing_tables:
            print(f"⚠ Missing required tables: {missing_tables}")
            print("Run the Docker setup to create tables automatically")
        else:
            print("✓ All required tables exist")
        
        return len(missing_tables) == 0
        
    except Exception as e:
        print(f"✗ DynamoDB connection failed: {e}")
        print("Make sure DynamoDB Local is running (docker-compose up)")
        return False


async def run_all_tests():
    """Run all tests"""
    print("CRAWL4AI DYNAMODB MIGRATION TESTS")
    print("="*60)
    
    tests = [
        ("Environment Setup", test_environment),
        ("DynamoDB Manager", test_dynamodb_manager),
        ("AsyncWebCrawler", test_async_webcrawler),
        ("MCP Server", test_mcp_server)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\nRunning {test_name} test...")
        try:
            results[test_name] = await test_func()
        except Exception as e:
            print(f"✗ {test_name} test crashed: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        emoji = "✓" if result else "✗"
        print(f"{emoji} {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! DynamoDB migration is ready to use.")
        return True
    else:
        print(f"\n⚠ {total - passed} test(s) failed. Check the setup and try again.")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test DynamoDB migration components')
    parser.add_argument('--test', choices=['env', 'db', 'crawler', 'mcp', 'all'],
                        default='all', help='Which test to run')
    
    args = parser.parse_args()
    
    if args.test == 'env':
        asyncio.run(test_environment())
    elif args.test == 'db':
        asyncio.run(test_dynamodb_manager())
    elif args.test == 'crawler':
        asyncio.run(test_async_webcrawler())
    elif args.test == 'mcp':
        asyncio.run(test_mcp_server())
    else:
        success = asyncio.run(run_all_tests())
        sys.exit(0 if success else 1) 
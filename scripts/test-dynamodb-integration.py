#!/usr/bin/env python3
"""
DynamoDB Integration Testing Script
Validates the hybrid database implementation following code of conduct
"""

import asyncio
import sys
import os
import time
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawl4ai.models import CrawlResult
from crawl4ai.hybrid_database_manager import HybridDatabaseManager
from crawl4ai.async_logger import AsyncLogger


class IntegrationTester:
    """Test the DynamoDB integration thoroughly"""
    
    def __init__(self):
        self.logger = AsyncLogger(
            log_file=os.path.join(
                os.getenv("CRAWL4_AI_BASE_DIRECTORY", ".crawl4ai"),
                "integration_test.log"
            ),
            verbose=True,
            tag_width=10
        )
        
        self.test_results = {
            "passed": 0,
            "failed": 0,
            "errors": []
        }
    
    def log_test(self, test_name: str, success: bool, error: str = None):
        """Log test result"""
        if success:
            self.test_results["passed"] += 1
            self.logger.success(f"✅ {test_name}", tag="TEST")
        else:
            self.test_results["failed"] += 1
            self.logger.error(f"❌ {test_name}: {error}", tag="TEST")
            self.test_results["errors"].append(f"{test_name}: {error}")
    
    async def test_hybrid_manager_initialization(self):
        """Test hybrid manager can initialize"""
        try:
            manager = HybridDatabaseManager()
            await manager.initialize()
            
            status = manager.get_status()
            self.log_test("Hybrid Manager Initialization", True)
            
            self.logger.info(
                f"Status: {status}",
                tag="STATUS"
            )
            
            return manager
            
        except Exception as e:
            self.log_test("Hybrid Manager Initialization", False, str(e))
            return None
    
    async def test_cache_operations(self, manager):
        """Test basic cache operations"""
        try:
            # Create test crawl result
            test_result = CrawlResult(
                url="https://test.example.com",
                html="<html><body>Test content</body></html>",
                cleaned_html="Test content",
                success=True,
                markdown="# Test Content",
                extracted_content="Test extracted content",
                media={},
                links={},
                metadata={"test": "value"},
                response_headers={"content-type": "text/html"},
                downloaded_files=[],
                screenshot="",
                status_code=200,
                error_message="",
                session_id=""
            )
            
            # Test caching
            await manager.acache_url(test_result)
            self.log_test("Cache URL", True)
            
            # Test retrieval
            cached = await manager.aget_cached_url("https://test.example.com")
            if cached and cached.url == test_result.url:
                self.log_test("Retrieve Cached URL", True)
            else:
                self.log_test("Retrieve Cached URL", False, "Retrieved data doesn't match")
            
            # Test count
            count = await manager.aget_total_count()
            if count >= 1:
                self.log_test("Get Total Count", True)
            else:
                self.log_test("Get Total Count", False, f"Count is {count}, expected >= 1")
                
        except Exception as e:
            self.log_test("Cache Operations", False, str(e))
    
    async def test_fallback_behavior(self):
        """Test fallback behavior between SQLite and DynamoDB"""
        try:
            # Test with SQLite-only configuration
            os.environ.pop('DYNAMODB_ENDPOINT', None)
            os.environ['FORCE_SQLITE'] = 'true'
            
            sqlite_manager = HybridDatabaseManager()
            await sqlite_manager.initialize()
            
            status = sqlite_manager.get_status()
            if not status['dynamodb_enabled']:
                self.log_test("SQLite Fallback", True)
            else:
                self.log_test("SQLite Fallback", False, "DynamoDB unexpectedly enabled")
                
        except Exception as e:
            self.log_test("Fallback Behavior", False, str(e))
        finally:
            # Clean up environment
            os.environ.pop('FORCE_SQLITE', None)
    
    async def test_performance(self, manager):
        """Test basic performance characteristics"""
        try:
            urls = [f"https://perf-test-{i}.example.com" for i in range(10)]
            
            # Create test results
            results = []
            for url in urls:
                results.append(CrawlResult(
                    url=url,
                    html=f"<html><body>Content for {url}</body></html>",
                    cleaned_html=f"Content for {url}",
                    success=True,
                    markdown=f"# {url}",
                    extracted_content=f"Extracted: {url}",
                    media={},
                    links={},
                    metadata={"index": urls.index(url)},
                    response_headers={"content-type": "text/html"},
                    downloaded_files=[],
                    screenshot="",
                    status_code=200,
                    error_message="",
                    session_id=""
                ))
            
            # Time batch operations
            start_time = time.time()
            
            # Cache all URLs
            for result in results:
                await manager.acache_url(result)
            
            cache_time = time.time() - start_time
            
            # Retrieve all URLs
            start_time = time.time()
            retrieved = []
            for url in urls:
                cached = await manager.aget_cached_url(url)
                retrieved.append(cached)
            
            retrieve_time = time.time() - start_time
            
            # Validate all were retrieved
            if len([r for r in retrieved if r is not None]) == len(urls):
                self.log_test("Performance Test", True)
                self.logger.info(
                    f"Cache time: {cache_time:.3f}s, Retrieve time: {retrieve_time:.3f}s",
                    tag="PERF"
                )
            else:
                self.log_test("Performance Test", False, "Some URLs not retrieved")
                
        except Exception as e:
            self.log_test("Performance Test", False, str(e))
    
    async def test_error_handling(self, manager):
        """Test error handling and recovery"""
        try:
            # Test with invalid URL
            cached = await manager.aget_cached_url("")
            if cached is None:
                self.log_test("Invalid URL Handling", True)
            else:
                self.log_test("Invalid URL Handling", False, "Should return None for empty URL")
            
            # Test clear operations
            await manager.aclear_db()
            count = await manager.aget_total_count()
            if count == 0:
                self.log_test("Clear Database", True)
            else:
                self.log_test("Clear Database", False, f"Count after clear: {count}")
                
        except Exception as e:
            self.log_test("Error Handling", False, str(e))
    
    async def run_all_tests(self):
        """Run all integration tests"""
        self.logger.info("Starting DynamoDB Integration Tests", tag="START")
        
        # Test 1: Initialization
        manager = await self.test_hybrid_manager_initialization()
        if not manager:
            self.logger.error("Cannot continue tests without manager", tag="ABORT")
            return
        
        # Test 2: Basic operations
        await self.test_cache_operations(manager)
        
        # Test 3: Fallback behavior
        await self.test_fallback_behavior()
        
        # Test 4: Performance
        await self.test_performance(manager)
        
        # Test 5: Error handling
        await self.test_error_handling(manager)
        
        # Summary
        self.print_summary()
    
    def print_summary(self):
        """Print test summary"""
        total = self.test_results["passed"] + self.test_results["failed"]
        
        print("\n" + "="*60)
        print("INTEGRATION TEST SUMMARY")
        print("="*60)
        print(f"Total Tests:    {total}")
        print(f"Passed:         {self.test_results['passed']}")
        print(f"Failed:         {self.test_results['failed']}")
        
        if total > 0:
            success_rate = (self.test_results['passed'] / total) * 100
            print(f"Success Rate:   {success_rate:.1f}%")
        
        if self.test_results['errors']:
            print("\nFAILURES:")
            for error in self.test_results['errors']:
                print(f"  - {error}")
        
        print("="*60)
        
        return self.test_results['failed'] == 0


async def main():
    """Main test runner"""
    print("Testing DynamoDB Integration...")
    
    os.environ['DYNAMODB_ENDPOINT'] = 'http://localhost:8000'
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['AWS_ACCESS_KEY_ID'] = 'dummy'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'dummy'
    
    try:
        manager = HybridDatabaseManager()
        await manager.initialize()
        
        status = manager.get_status()
        print(f"Manager Status: {status}")
        
        test_result = CrawlResult(
            url="https://test.example.com",
            html="<html><body>Test content</body></html>",
            cleaned_html="Test content",
            success=True,
            markdown="# Test Content",
            extracted_content="Test extracted content",
            media={},
            links={},
            metadata={"test": "value"},
            response_headers={"content-type": "text/html"},
            downloaded_files=[],
            screenshot="",
            status_code=200,
            error_message="",
            session_id=""
        )
        
        await manager.acache_url(test_result)
        cached = await manager.aget_cached_url("https://test.example.com")
        
        if cached and cached.url == test_result.url:
            print("✅ Integration test passed")
        else:
            print("❌ Integration test failed")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 
"""
Hybrid Database Manager for Crawl4AI
Gracefully transitions from SQLite to DynamoDB while maintaining compatibility
Following the code of conduct: responsible contribution for community benefit
"""

import os
import asyncio
from typing import Optional, Dict, Any, List
from .async_database import AsyncDatabaseManager
from .dynamodb_manager import DynamoDBDocumentManager
from .models import CrawlResult
from .async_logger import AsyncLogger


class HybridDatabaseManager:
    """
    Hybrid manager that supports both SQLite and DynamoDB
    Allows for gradual migration and fallback capabilities
    """
    
    def __init__(
        self,
        migration_percentage: int = None,
        force_dynamodb: bool = False,
        force_sqlite: bool = False
    ):
        """
        Initialize hybrid manager with migration control
        
        Args:
            migration_percentage: 0-100, percentage of traffic to route to DynamoDB
            force_dynamodb: Force all operations to use DynamoDB
            force_sqlite: Force all operations to use SQLite (rollback mode)
        """
        self.migration_percentage = (
            migration_percentage 
            if migration_percentage is not None 
            else int(os.getenv('DYNAMODB_MIGRATION_PCT', '0'))
        )
        self.force_dynamodb = force_dynamodb or os.getenv('FORCE_DYNAMODB', '').lower() == 'true'
        self.force_sqlite = force_sqlite or os.getenv('FORCE_SQLITE', '').lower() == 'true'
        
        # Initialize both managers
        self.sqlite_manager = AsyncDatabaseManager()
        self.dynamodb_manager = None
        self._dynamodb_available = False
        self._initialized = False
        
        # Initialize logger
        self.logger = AsyncLogger(
            log_file=os.path.join(
                os.getenv("CRAWL4_AI_BASE_DIRECTORY", ".crawl4ai"), 
                "hybrid_db.log"
            ),
            verbose=False,
            tag_width=10,
        )
        
        # Initialize DynamoDB if configured
        if self._should_use_dynamodb() or self.force_dynamodb:
            try:
                self.dynamodb_manager = DynamoDBDocumentManager()
                self._dynamodb_available = True
                self.logger.info("DynamoDB manager initialized", tag="INIT")
            except Exception as e:
                self.logger.warning(
                    message="DynamoDB initialization failed, falling back to SQLite: {error}",
                    tag="WARN",
                    params={"error": str(e)}
                )
                self._dynamodb_available = False
    
    def _should_use_dynamodb(self) -> bool:
        """Determine if DynamoDB should be used based on configuration"""
        if self.force_sqlite:
            return False
        if self.force_dynamodb:
            return True
        
        # Check if DynamoDB is configured
        dynamodb_endpoint = os.getenv('DYNAMODB_ENDPOINT')
        return dynamodb_endpoint is not None
    
    def _route_to_dynamodb(self, url: str) -> bool:
        """Determine if this URL should be routed to DynamoDB"""
        if self.force_sqlite:
            return False
        if self.force_dynamodb:
            return True
        if not self._dynamodb_available:
            return False
        
        # Use hash-based routing for consistent behavior
        if self.migration_percentage > 0:
            url_hash = hash(url) % 100
            return url_hash < self.migration_percentage
        
        return False
    
    async def initialize(self):
        """Initialize both database managers"""
        if self._initialized:
            return
        
        try:
            # Always initialize SQLite for fallback
            await self.sqlite_manager.initialize()
            
            # Initialize DynamoDB if available
            if self._dynamodb_available and self.dynamodb_manager:
                await self.dynamodb_manager.initialize()
            
            self._initialized = True
            self.logger.success(
                message="Hybrid database manager initialized (SQLite: {sqlite}, DynamoDB: {dynamodb})",
                tag="INIT",
                params={
                    "sqlite": "enabled",
                    "dynamodb": "enabled" if self._dynamodb_available else "disabled"
                }
            )
            
        except Exception as e:
            self.logger.error(
                message="Hybrid database initialization failed: {error}",
                tag="ERROR",
                params={"error": str(e)}
            )
            raise
    
    async def aget_cached_url(self, url: str) -> Optional[CrawlResult]:
        """Retrieve cached URL data with intelligent routing"""
        if not self._initialized:
            await self.initialize()
        
        use_dynamodb = self._route_to_dynamodb(url)
        
        try:
            if use_dynamodb:
                result = await self.dynamodb_manager.aget_cached_url(url)
                
                # If not found in DynamoDB, try SQLite as fallback
                if result is None and self.migration_percentage < 100:
                    result = await self.sqlite_manager.aget_cached_url(url)
                
                return result
            else:
                return await self.sqlite_manager.aget_cached_url(url)
                
        except Exception as e:
            self.logger.error(
                message="Error retrieving cached URL {url}: {error}",
                tag="ERROR",
                params={"url": url, "error": str(e)}
            )
            
            # Fallback to the other system
            try:
                if use_dynamodb:
                    return await self.sqlite_manager.aget_cached_url(url)
                elif self._dynamodb_available:
                    return await self.dynamodb_manager.aget_cached_url(url)
            except Exception:
                pass
            
            return None
    
    async def acache_url(self, result: CrawlResult):
        """Cache CrawlResult data with intelligent routing and dual-write option"""
        if not self._initialized:
            await self.initialize()
        
        use_dynamodb = self._route_to_dynamodb(result.url)
        
        # Dual-write mode during migration for data consistency
        dual_write = (
            self.migration_percentage > 0 and 
            self.migration_percentage < 100 and 
            self._dynamodb_available
        )
        
        try:
            if use_dynamodb:
                await self.dynamodb_manager.acache_url(result)
                
                # Also cache to SQLite during migration
                if dual_write:
                    try:
                        await self.sqlite_manager.acache_url(result)
                    except Exception as e:
                        self.logger.warning(
                            message="Dual-write to SQLite failed: {url}, {error}",
                            tag="WARN",
                            params={"url": result.url, "error": str(e)}
                        )
            else:
                await self.sqlite_manager.acache_url(result)
                
        except Exception as e:
            self.logger.error(
                message="Error caching URL {url}: {error}",
                tag="ERROR",
                params={"url": result.url, "error": str(e)}
            )
            
            # Fallback to the other system
            try:
                if use_dynamodb:
                    await self.sqlite_manager.acache_url(result)
                elif self._dynamodb_available:
                    await self.dynamodb_manager.acache_url(result)
            except Exception:
                raise
    
    async def aget_total_count(self) -> int:
        """Get total number of cached URLs from both systems"""
        if not self._initialized:
            await self.initialize()
        
        try:
            sqlite_count = await self.sqlite_manager.aget_total_count()
            
            if self._dynamodb_available:
                dynamodb_count = await self.dynamodb_manager.aget_total_count()
                return max(sqlite_count, dynamodb_count)
            
            return sqlite_count
            
        except Exception as e:
            self.logger.error(
                message="Error getting total count: {error}",
                tag="ERROR",
                params={"error": str(e)}
            )
            return 0
    
    async def aclear_db(self):
        """Clear all data from both database systems"""
        if not self._initialized:
            await self.initialize()
        
        try:
            await self.sqlite_manager.aclear_db()
            
            if self._dynamodb_available:
                await self.dynamodb_manager.aclear_db()
            
            self.logger.info("All databases cleared successfully", tag="CLEAR")
            
        except Exception as e:
            self.logger.error(
                message="Error clearing databases: {error}",
                tag="ERROR",
                params={"error": str(e)}
            )
    
    async def aflush_db(self):
        """Drop all tables from both database systems"""
        if not self._initialized:
            await self.initialize()
        
        try:
            await self.sqlite_manager.aflush_db()
            
            if self._dynamodb_available:
                await self.dynamodb_manager.aflush_db()
            
            self.logger.info("All databases flushed successfully", tag="FLUSH")
            
        except Exception as e:
            self.logger.error(
                message="Error flushing databases: {error}",
                tag="ERROR",
                params={"error": str(e)}
            )
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of the hybrid database manager"""
        return {
            "initialized": self._initialized,
            "sqlite_enabled": True,
            "dynamodb_enabled": self._dynamodb_available,
            "migration_percentage": self.migration_percentage,
            "force_dynamodb": self.force_dynamodb,
            "force_sqlite": self.force_sqlite,
            "dual_write_active": (
                self.migration_percentage > 0 and 
                self.migration_percentage < 100 and 
                self._dynamodb_available
            )
        } 
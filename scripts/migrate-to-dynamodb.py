#!/usr/bin/env python3
"""
SQLite to DynamoDB Migration Script
Gradually migrate crawl data from SQLite to DynamoDB
Following code of conduct: responsible migration with user control
"""

import asyncio
import argparse
import sys
import os
from typing import List, Optional

# Add the parent directory to sys.path to import crawl4ai modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawl4ai.async_database import AsyncDatabaseManager
from crawl4ai.dynamodb_manager import DynamoDBDocumentManager
from crawl4ai.async_logger import AsyncLogger


class DatabaseMigrator:
    """Manages migration from SQLite to DynamoDB"""
    
    def __init__(self, batch_size: int = 10, dry_run: bool = False):
        self.batch_size = batch_size
        self.dry_run = dry_run
        
        self.sqlite_manager = AsyncDatabaseManager()
        self.dynamodb_manager = DynamoDBDocumentManager()
        
        self.logger = AsyncLogger(
            log_file=os.path.join(
                os.getenv("CRAWL4_AI_BASE_DIRECTORY", ".crawl4ai"),
                "migration.log"
            ),
            verbose=True,
            tag_width=10
        )
        
        self.stats = {
            "total_urls": 0,
            "migrated": 0,
            "failed": 0,
            "skipped": 0
        }
    
    async def initialize(self):
        """Initialize both database managers"""
        await self.sqlite_manager.initialize()
        await self.dynamodb_manager.initialize()
        
        self.logger.info("Migration tools initialized", tag="INIT")
    
    async def get_all_urls(self) -> List[str]:
        """Get all URLs from SQLite database"""
        async def _get_urls(db):
            async with db.execute("SELECT url FROM crawled_data") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
        
        return await self.sqlite_manager.execute_with_retry(_get_urls)
    
    async def migrate_url(self, url: str) -> bool:
        """Migrate a single URL from SQLite to DynamoDB"""
        try:
            # Get data from SQLite
            sqlite_result = await self.sqlite_manager.aget_cached_url(url)
            if not sqlite_result:
                self.logger.warning(
                    message="URL not found in SQLite: {url}",
                    tag="SKIP",
                    params={"url": url}
                )
                self.stats["skipped"] += 1
                return False
            
            # Check if already exists in DynamoDB
            existing = await self.dynamodb_manager.aget_cached_url(url)
            if existing and not self.args.force:
                self.logger.info(
                    message="URL already exists in DynamoDB: {url}",
                    tag="SKIP",
                    params={"url": url}
                )
                self.stats["skipped"] += 1
                return True
            
            # Migrate to DynamoDB
            if not self.dry_run:
                await self.dynamodb_manager.acache_url(sqlite_result)
            
            self.logger.success(
                message="Migrated URL: {url}",
                tag="MIGRATE",
                params={"url": url}
            )
            self.stats["migrated"] += 1
            return True
            
        except Exception as e:
            self.logger.error(
                message="Migration failed for URL {url}: {error}",
                tag="ERROR",
                params={"url": url, "error": str(e)}
            )
            self.stats["failed"] += 1
            return False
    
    async def migrate_batch(self, urls: List[str]):
        """Migrate a batch of URLs concurrently"""
        tasks = [self.migrate_url(url) for url in urls]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def run_migration(self, urls: Optional[List[str]] = None):
        """Run the complete migration process"""
        if urls is None:
            self.logger.info("Discovering URLs from SQLite database", tag="SCAN")
            urls = await self.get_all_urls()
        
        self.stats["total_urls"] = len(urls)
        
        if self.dry_run:
            self.logger.info(
                message="DRY RUN: Would migrate {count} URLs",
                tag="DRY_RUN",
                params={"count": len(urls)}
            )
        else:
            self.logger.info(
                message="Starting migration of {count} URLs",
                tag="START",
                params={"count": len(urls)}
            )
        
        # Process in batches
        for i in range(0, len(urls), self.batch_size):
            batch = urls[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(urls) + self.batch_size - 1) // self.batch_size
            
            self.logger.info(
                message="Processing batch {batch}/{total} ({count} URLs)",
                tag="BATCH",
                params={
                    "batch": batch_num,
                    "total": total_batches,
                    "count": len(batch)
                }
            )
            
            await self.migrate_batch(batch)
            
            # Progress update
            progress = (self.stats["migrated"] + self.stats["failed"] + self.stats["skipped"])
            percentage = (progress / self.stats["total_urls"]) * 100
            
            self.logger.info(
                message="Progress: {progress}/{total} ({percentage:.1f}%)",
                tag="PROGRESS",
                params={
                    "progress": progress,
                    "total": self.stats["total_urls"],
                    "percentage": percentage
                }
            )
    
    def print_summary(self):
        """Print migration summary"""
        print("\n" + "="*60)
        print("MIGRATION SUMMARY")
        print("="*60)
        print(f"Total URLs:     {self.stats['total_urls']}")
        print(f"Migrated:       {self.stats['migrated']}")
        print(f"Failed:         {self.stats['failed']}")
        print(f"Skipped:        {self.stats['skipped']}")
        
        if self.stats['total_urls'] > 0:
            success_rate = (self.stats['migrated'] / self.stats['total_urls']) * 100
            print(f"Success Rate:   {success_rate:.1f}%")
        
        if self.dry_run:
            print("\nNOTE: This was a dry run. No data was actually migrated.")
        
        print("="*60)


async def main():
    parser = argparse.ArgumentParser(description="Migrate data from SQLite to DynamoDB")
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=10,
        help="Number of URLs to process concurrently (default: 10)"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Show what would be migrated without actually doing it"
    )
    parser.add_argument(
        "--force", 
        action="store_true",
        help="Overwrite existing entries in DynamoDB"
    )
    parser.add_argument(
        "--urls", 
        nargs="+",
        help="Specific URLs to migrate (default: migrate all)"
    )
    
    args = parser.parse_args()
    
    migrator = DatabaseMigrator(
        batch_size=args.batch_size,
        dry_run=args.dry_run
    )
    migrator.args = args  # Store args for access in methods
    
    try:
        await migrator.initialize()
        await migrator.run_migration(args.urls)
        migrator.print_summary()
        
    except KeyboardInterrupt:
        print("\n\nMigration interrupted by user")
        migrator.print_summary()
        
    except Exception as e:
        print(f"\nMigration failed: {e}")
        migrator.print_summary()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 
#!/usr/bin/env python3
"""
SQLite to DynamoDB Migration Script
Migrates existing Crawl4AI data from SQLite to DynamoDB
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

import aiofiles
import sqlite3
from tqdm.asyncio import tqdm

# Add the parent directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from crawl4ai.dynamodb_manager import DynamoDBManager
from crawl4ai.database_manager import AsyncDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQLiteToDynamoDBMigrator:
    """Migrates data from SQLite to DynamoDB"""
    
    def __init__(self, 
                 sqlite_db_path: str = None,
                 batch_size: int = 25):
        self.sqlite_db_path = sqlite_db_path or self._find_sqlite_db()
        self.batch_size = batch_size
        
        # Initialize DynamoDB manager
        self.dynamodb_manager = DynamoDBManager(
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            endpoint_url=os.getenv(
                'DYNAMODB_ENDPOINT_URL', 'http://localhost:8000'
            ),
            table_name=os.getenv('DYNAMODB_TABLE_NAME', 'crawl4ai-results')
        )
        
        # Initialize SQLite manager (for compatibility)
        self.sqlite_manager = None
    
    def _find_sqlite_db(self) -> Optional[str]:
        """Find the SQLite database file"""
        possible_paths = [
            '.crawl4ai/cache.db',
            'cache.db',
            '.crawl4ai_cache/cache.db',
            os.path.expanduser('~/.crawl4ai/cache.db')
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"Found SQLite database at: {path}")
                return path
        
        logger.warning("No SQLite database found. Checked paths:")
        for path in possible_paths:
            logger.warning(f"  - {path}")
        
        return None
    
    async def initialize(self):
        """Initialize the migration environment"""
        await self.dynamodb_manager.initialize()
        logger.info("DynamoDB manager initialized")
        
        if self.sqlite_db_path and os.path.exists(self.sqlite_db_path):
            logger.info(f"Using SQLite database: {self.sqlite_db_path}")
        else:
            raise FileNotFoundError(
                f"SQLite database not found: {self.sqlite_db_path}"
            )
    
    async def close(self):
        """Close connections"""
        await self.dynamodb_manager.close()
    
    def _get_sqlite_data(self) -> List[Dict[str, Any]]:
        """Extract data from SQLite database"""
        if not self.sqlite_db_path or not os.path.exists(self.sqlite_db_path):
            logger.error("SQLite database not found")
            return []
        
        try:
            conn = sqlite3.connect(self.sqlite_db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            cursor = conn.cursor()
            
            # Get all cached URLs
            cursor.execute("""
                SELECT url, html, cleaned_html, markdown, extracted_content, 
                       success, media, links, metadata, timestamp
                FROM cached_urls
                ORDER BY timestamp DESC
            """)
            
            rows = cursor.fetchall()
            results = []
            
            for row in rows:
                try:
                    # Convert row to dictionary
                    data = dict(row)
                    
                    # Parse JSON fields
                    for json_field in ['media', 'links', 'metadata']:
                        if data.get(json_field):
                            try:
                                data[json_field] = json.loads(data[json_field])
                            except (json.JSONDecodeError, TypeError):
                                data[json_field] = {}
                        else:
                            data[json_field] = {}
                    
                    # Ensure success is boolean
                    data['success'] = bool(data.get('success', True))
                    
                    # Add created_at timestamp
                    if data.get('timestamp'):
                        data['created_at'] = data['timestamp']
                    else:
                        data['created_at'] = datetime.utcnow().isoformat()
                    
                    # Create markdown structure
                    markdown_content = data.get('markdown', '')
                    data['markdown'] = {
                        'raw_markdown': markdown_content,
                        'markdown_with_citations': '',
                        'references_markdown': '',
                        'fit_markdown': '',
                        'fit_html': ''
                    }
                    
                    results.append(data)
                    
                except Exception as e:
                    logger.error(f"Error processing row {row}: {e}")
                    continue
            
            conn.close()
            logger.info(f"Extracted {len(results)} records from SQLite")
            return results
            
        except sqlite3.Error as e:
            logger.error(f"SQLite error: {e}")
            return []
        except Exception as e:
            logger.error(f"Error reading SQLite data: {e}")
            return []
    
    async def _migrate_batch(self, batch: List[Dict[str, Any]]) -> int:
        """Migrate a batch of records to DynamoDB"""
        success_count = 0
        
        for record in batch:
            try:
                url = record['url']
                await self.dynamodb_manager.cache_url(url, record)
                success_count += 1
                logger.debug(f"Migrated: {url}")
                
            except Exception as e:
                logger.error(f"Failed to migrate {record.get('url', 'unknown')}: {e}")
        
        return success_count
    
    async def migrate(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Perform the migration
        
        Args:
            dry_run: If True, only analyze the data without migrating
            
        Returns:
            Dictionary with migration statistics
        """
        logger.info("Starting SQLite to DynamoDB migration")
        
        if dry_run:
            logger.info("DRY RUN MODE - No data will be migrated")
        
        # Extract data from SQLite
        sqlite_data = self._get_sqlite_data()
        
        if not sqlite_data:
            return {
                'success': False,
                'error': 'No data found in SQLite database',
                'total_records': 0,
                'migrated_records': 0
            }
        
        total_records = len(sqlite_data)
        logger.info(f"Found {total_records} records to migrate")
        
        if dry_run:
            # Analyze the data
            urls = [record['url'] for record in sqlite_data]
            unique_urls = set(urls)
            
            return {
                'success': True,
                'dry_run': True,
                'total_records': total_records,
                'unique_urls': len(unique_urls),
                'duplicate_urls': total_records - len(unique_urls),
                'sample_urls': list(unique_urls)[:10],
                'estimated_migration_time_minutes': (total_records / 100) * 2
            }
        
        # Perform actual migration
        migrated_count = 0
        
        # Process in batches
        batches = [
            sqlite_data[i:i + self.batch_size] 
            for i in range(0, total_records, self.batch_size)
        ]
        
        logger.info(f"Processing {len(batches)} batches of {self.batch_size} records each")
        
        for i, batch in enumerate(tqdm(batches, desc="Migrating batches")):
            try:
                batch_success = await self._migrate_batch(batch)
                migrated_count += batch_success
                
                logger.info(
                    f"Batch {i+1}/{len(batches)}: "
                    f"{batch_success}/{len(batch)} records migrated"
                )
                
                # Small delay to avoid overwhelming DynamoDB
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error processing batch {i+1}: {e}")
        
        # Verify migration
        final_count = await self.dynamodb_manager.get_total_count()
        
        return {
            'success': True,
            'total_records': total_records,
            'migrated_records': migrated_count,
            'final_dynamodb_count': final_count,
            'migration_efficiency': (migrated_count / total_records) * 100,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    async def verify_migration(self, sample_size: int = 10) -> Dict[str, Any]:
        """Verify the migration by comparing sample records"""
        logger.info(f"Verifying migration with {sample_size} sample records")
        
        sqlite_data = self._get_sqlite_data()
        if not sqlite_data:
            return {'success': False, 'error': 'No SQLite data to verify'}
        
        # Take a sample
        sample_records = sqlite_data[:sample_size]
        verification_results = []
        
        for record in sample_records:
            url = record['url']
            
            try:
                # Get from DynamoDB
                dynamo_record = await self.dynamodb_manager.get_cached_url(url)
                
                if dynamo_record:
                    verification_results.append({
                        'url': url,
                        'found_in_dynamodb': True,
                        'data_matches': (
                            dynamo_record.get('html') == record.get('html') and
                            dynamo_record.get('success') == record.get('success')
                        )
                    })
                else:
                    verification_results.append({
                        'url': url,
                        'found_in_dynamodb': False,
                        'data_matches': False
                    })
                    
            except Exception as e:
                verification_results.append({
                    'url': url,
                    'found_in_dynamodb': False,
                    'data_matches': False,
                    'error': str(e)
                })
        
        found_count = sum(1 for r in verification_results if r['found_in_dynamodb'])
        matching_count = sum(1 for r in verification_results if r['data_matches'])
        
        return {
            'success': True,
            'sample_size': sample_size,
            'found_in_dynamodb': found_count,
            'data_matches': matching_count,
            'verification_rate': (found_count / sample_size) * 100,
            'data_accuracy': (matching_count / sample_size) * 100,
            'results': verification_results
        }


async def main():
    """Main migration function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate Crawl4AI data from SQLite to DynamoDB')
    parser.add_argument('--sqlite-db', help='Path to SQLite database file')
    parser.add_argument('--dry-run', action='store_true', help='Analyze data without migrating')
    parser.add_argument('--verify', action='store_true', help='Verify migration after completion')
    parser.add_argument('--batch-size', type=int, default=25, help='Batch size for migration')
    parser.add_argument('--sample-size', type=int, default=10, help='Sample size for verification')
    
    args = parser.parse_args()
    
    migrator = SQLiteToDynamoDBMigrator(
        sqlite_db_path=args.sqlite_db,
        batch_size=args.batch_size
    )
    
    try:
        await migrator.initialize()
        
        # Perform migration
        result = await migrator.migrate(dry_run=args.dry_run)
        
        print("\n" + "="*60)
        print("MIGRATION RESULTS")
        print("="*60)
        print(json.dumps(result, indent=2))
        
        # Verify if requested
        if args.verify and not args.dry_run and result.get('success'):
            print("\n" + "="*60)
            print("VERIFICATION RESULTS")
            print("="*60)
            
            verification = await migrator.verify_migration(args.sample_size)
            print(json.dumps(verification, indent=2))
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        print(f"\nMigration failed: {e}")
        sys.exit(1)
    
    finally:
        await migrator.close()


if __name__ == "__main__":
    asyncio.run(main()) 
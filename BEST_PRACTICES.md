# DynamoDB Best Practices for Crawl4AI

*A guide to doing distributed data storage correctly, because apparently the universe didn't make it complex enough already.*

## 1. Performance Best Practices ⚡

### Connection Pooling
```python
# Configure connection pools for better performance
boto_config = Config(
    max_pool_connections=50,  # Adjust based on concurrency needs
    retries={'max_attempts': 3, 'mode': 'adaptive'}
)
```

### Eventually Consistent Reads
```python
# Use eventually consistent reads when strong consistency isn't required
response = table.get_item(
    Key={'url': url},
    ConsistentRead=False  # 2x better performance
)
```

### Batch Operations
```python
# Batch read operations for efficiency
async def get_multiple_urls(self, urls: List[str]):
    # Process up to 100 items per batch
    batch_size = 100
    results = {}
    
    for i in range(0, len(urls), batch_size):
        batch_urls = urls[i:i + batch_size]
        # Use batch_get_item for multiple reads
```

## 2. Error Handling & Resilience 🛡️

### Exponential Backoff
```python
async def _execute_with_retry(self, operation, *args, **kwargs):
    """Smart retry logic with exponential backoff"""
    max_retries = 3
    base_delay = 0.1
    
    for attempt in range(max_retries + 1):
        try:
            return await operation(*args, **kwargs)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            
            # Don't retry client errors
            if error_code in ['ValidationException', 'ResourceNotFoundException']:
                raise
                
            # Retry throttling errors with backoff
            if error_code in ['ProvisionedThroughputExceededException', 'ThrottlingException']:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
                continue
```

### Graceful Degradation
```python
async def aget_cached_url(self, url: str) -> Optional[CrawlResult]:
    """Always return something, even on failure"""
    try:
        # Try to get from DynamoDB
        return await self._get_from_dynamodb(url)
    except Exception as e:
        self.logger.error(f"DynamoDB failed: {e}")
        # Fallback to memory cache or return None
        return None
```

## 3. Data Modeling Best Practices 📊

### Single Table Design (If Applicable)
```python
# Use composite keys for related data
document = {
    'pk': f'URL#{url}',           # Partition key
    'sk': f'VERSION#{timestamp}', # Sort key
    'entity_type': 'crawl_result',
    'url': url,
    'data': {...}
}
```

### Proper Data Types
```python
def _serialize_crawl_result(self, result: CrawlResult) -> Dict[str, Any]:
    return {
        'url': result.url,                    # String
        'success': result.success,            # Boolean
        'status_code': result.status_code or 0,  # Number (not None)
        'created_at': datetime.utcnow().isoformat(),  # ISO string
        'metadata': result.metadata or {},    # Map (not None)
        'links': result.links or {},          # Map (not None)
        'ttl': int(future_timestamp)          # Number for TTL
    }
```

### Size Optimization
```python
def _truncate_large_fields(self, document: Dict[str, Any]) -> Dict[str, Any]:
    """Keep items under 400KB limit"""
    MAX_FIELD_SIZE = 100_000  # 100KB per field
    
    for field in ['html', 'cleaned_html']:
        if field in document and len(str(document[field])) > MAX_FIELD_SIZE:
            document[field] = str(document[field])[:MAX_FIELD_SIZE] + "...[TRUNCATED]"
    
    return document
```

## 4. Security Best Practices 🔒

### Environment-Based Configuration
```bash
# Development
export DYNAMODB_ENDPOINT=http://localhost:8000
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy

# Production - Use IAM roles instead of keys
export AWS_REGION=us-west-2
# No hardcoded credentials in production
```

### Least Privilege IAM
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem",
                "dynamodb:BatchGetItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:Scan",
                "dynamodb:Query"
            ],
            "Resource": [
                "arn:aws:dynamodb:*:*:table/crawl4ai-*"
            ]
        }
    ]
}
```

### Encryption at Rest
```python
# Enable encryption for production tables
TableDescription = {
    'SSESpecification': {
        'Enabled': True,
        'SSEType': 'AES256'  # or 'KMS' for more control
    }
}
```

## 5. Monitoring & Observability 📈

### Structured Logging
```python
self.logger.info(
    message="Retrieved cached result for URL: {url}",
    tag="CACHE_HIT",
    params={
        "url": url,
        "response_time_ms": response_time,
        "item_size_kb": item_size / 1024
    }
)
```

### CloudWatch Metrics
```python
# Track custom metrics
def track_cache_hit_rate(self, hit: bool):
    cloudwatch.put_metric_data(
        Namespace='Crawl4AI/DynamoDB',
        MetricData=[
            {
                'MetricName': 'CacheHitRate',
                'Value': 1 if hit else 0,
                'Unit': 'Count'
            }
        ]
    )
```

### Health Checks
```python
async def health_check(self) -> Dict[str, Any]:
    """Verify DynamoDB connectivity and performance"""
    try:
        start_time = time.time()
        await self.dynamodb_client.list_tables()
        response_time = (time.time() - start_time) * 1000
        
        return {
            'status': 'healthy',
            'response_time_ms': response_time,
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }
```

## 6. Cost Optimization 💰

### Use On-Demand Billing
```yaml
# For unpredictable workloads
BillingMode: PAY_PER_REQUEST
```

### Implement TTL
```python
# Auto-delete old items
document['ttl'] = int((datetime.utcnow() + timedelta(days=30)).timestamp())
```

### Optimize GSI Usage
```python
# Only create indexes you'll actually use
GlobalSecondaryIndexes=[
    {
        'IndexName': 'timestamp-index',
        'KeySchema': [{'AttributeName': 'created_at', 'KeyType': 'HASH'}],
        'Projection': {
            'ProjectionType': 'KEYS_ONLY'  # Minimize storage costs
        }
    }
]
```

## 7. Testing Best Practices 🧪

### Local Development
```python
# Use DynamoDB Local for development
DYNAMODB_ENDPOINT = os.getenv('DYNAMODB_ENDPOINT', 'http://localhost:8000')

if DYNAMODB_ENDPOINT.startswith('http://localhost'):
    # Development configuration
    print("Using DynamoDB Local for development")
else:
    # Production configuration
    print("Using AWS DynamoDB in production")
```

### Integration Tests
```python
import pytest
from moto import mock_dynamodb

@mock_dynamodb
async def test_dynamodb_operations():
    """Test DynamoDB operations with mocking"""
    db_manager = DynamoDBDocumentManager()
    await db_manager.initialize()
    
    # Test caching
    result = CrawlResult(url="https://test.com", success=True)
    await db_manager.acache_url(result)
    
    # Test retrieval
    cached = await db_manager.aget_cached_url("https://test.com")
    assert cached is not None
    assert cached.url == "https://test.com"
```

### Load Testing
```bash
# Test with realistic data volumes
python -c "
import asyncio
from crawl4ai.dynamodb_manager import DynamoDBDocumentManager

async def load_test():
    db = DynamoDBDocumentManager()
    await db.initialize()
    
    # Test concurrent operations
    tasks = []
    for i in range(100):
        tasks.append(db.aget_cached_url(f'https://test{i}.com'))
    
    results = await asyncio.gather(*tasks)
    print(f'Processed {len(results)} requests')

asyncio.run(load_test())
"
```

## 8. Migration Best Practices 📦

### Gradual Rollout
```python
class HybridDatabaseManager:
    """Use both SQLite and DynamoDB during migration"""
    
    def __init__(self):
        self.sqlite_manager = AsyncDatabaseManager()
        self.dynamodb_manager = DynamoDBDocumentManager()
        self.migration_percentage = int(os.getenv('DYNAMODB_MIGRATION_PCT', '0'))
    
    async def aget_cached_url(self, url: str):
        # Gradually shift traffic to DynamoDB
        if hash(url) % 100 < self.migration_percentage:
            return await self.dynamodb_manager.aget_cached_url(url)
        else:
            return await self.sqlite_manager.aget_cached_url(url)
```

### Data Validation
```python
async def validate_migration(self, url: str):
    """Compare SQLite and DynamoDB results"""
    sqlite_result = await self.sqlite_manager.aget_cached_url(url)
    dynamodb_result = await self.dynamodb_manager.aget_cached_url(url)
    
    if sqlite_result and dynamodb_result:
        assert sqlite_result.url == dynamodb_result.url
        assert sqlite_result.success == dynamodb_result.success
        # Validate other critical fields
```

## 9. Operational Best Practices 🔧

### Backup Strategy
```bash
# Enable point-in-time recovery
aws dynamodb update-continuous-backups \
    --table-name crawl4ai-results \
    --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
```

### Resource Tagging
```python
# Tag resources for cost tracking
TableTags = [
    {'Key': 'Environment', 'Value': 'production'},
    {'Key': 'Application', 'Value': 'crawl4ai'},
    {'Key': 'Owner', 'Value': 'data-team'},
    {'Key': 'CostCenter', 'Value': 'engineering'}
]
```

### Capacity Planning
```python
# Monitor and adjust capacity
def monitor_capacity_utilization():
    """Track read/write capacity utilization"""
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/DynamoDB',
        MetricName='ConsumedReadCapacityUnits',
        Dimensions=[{'Name': 'TableName', 'Value': 'crawl4ai-results'}],
        StartTime=datetime.utcnow() - timedelta(hours=1),
        EndTime=datetime.utcnow(),
        Period=300,
        Statistics=['Average', 'Maximum']
    )
    return response
```

## 10. Code Quality Best Practices 🎯

### Type Hints
```python
from typing import Optional, Dict, Any, List

async def aget_cached_url(self, url: str) -> Optional[CrawlResult]:
    """Always use proper type hints"""
    pass

async def aget_cached_urls_batch(
    self, 
    urls: List[str]
) -> Dict[str, Optional[CrawlResult]]:
    """Include complex types"""
    pass
```

### Error Context
```python
try:
    await self._execute_with_retry(operation)
except ClientError as e:
    self.logger.error(
        message="DynamoDB operation failed: {operation} for URL: {url}",
        tag="DYNAMODB_ERROR",
        params={
            "operation": operation.__name__,
            "url": url,
            "error_code": e.response.get('Error', {}).get('Code'),
            "error_message": str(e)
        }
    )
    raise
```

### Configuration Management
```python
@dataclass
class DynamoDBConfig:
    """Centralized configuration"""
    endpoint_url: Optional[str] = None
    region_name: str = 'us-east-1'
    table_name_results: str = 'crawl4ai-results'
    table_name_sessions: str = 'crawl4ai-sessions'
    max_retries: int = 3
    max_pool_connections: int = 50
    
    @classmethod
    def from_environment(cls) -> 'DynamoDBConfig':
        return cls(
            endpoint_url=os.getenv('DYNAMODB_ENDPOINT'),
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            table_name_results=os.getenv('DYNAMODB_TABLE_CRAWL_RESULTS', 'crawl4ai-results'),
            # ... other fields
        )
```

## Cosmic Wisdom 🌌

*"In the grand scheme of the universe, proper DynamoDB implementation represents humanity's brief moment of competence before the inevitable heat death renders all databases equally meaningless. However, during this fleeting cosmic instant, following these best practices might prevent your application from failing catastrophically."*

**The Universal Constants:**
1. **Murphy's Law**: Anything that can go wrong with DynamoDB will go wrong
2. **Conway's Law**: Your DynamoDB schema will reflect your team's communication patterns
3. **Parkinson's Law**: Your DynamoDB costs will expand to fill your budget
4. **Marvin's Law**: Even with perfect implementation, the universe will find new ways to disappoint you

---

*Remember: These best practices exist not because they guarantee success, but because they make failure more dignified and debuggable.* 
# DynamoDB Migration Deployment Guide

*Following the code of conduct: responsible deployment for community benefit*

## Quick Start

### 1. Start Local Development Environment

```bash
# Start DynamoDB Local and dependencies
./scripts/start-dynamodb.sh

# Verify services are running
curl http://localhost:8000        # DynamoDB Local
curl http://localhost:8001        # DynamoDB Admin UI
```

### 2. Enable DynamoDB Integration

```bash
# Option 1: Environment Variables
export DYNAMODB_ENDPOINT=http://localhost:8000
export DYNAMODB_MIGRATION_PCT=10  # Route 10% to DynamoDB

# Option 2: Force DynamoDB (for testing)
export FORCE_DYNAMODB=true

# Option 3: Force SQLite (rollback)
export FORCE_SQLITE=true
```

### 3. Test Integration

```bash
python scripts/test-dynamodb-integration.py
```

## Migration Strategies

### Strategy 1: Gradual Rollout (Recommended)

```bash
# Week 1: 10% traffic to DynamoDB
export DYNAMODB_MIGRATION_PCT=10

# Week 2: 25% traffic
export DYNAMODB_MIGRATION_PCT=25

# Week 3: 50% traffic
export DYNAMODB_MIGRATION_PCT=50

# Week 4: 100% traffic
export DYNAMODB_MIGRATION_PCT=100
```

### Strategy 2: Full Migration

```bash
# Migrate all existing data
python scripts/migrate-to-dynamodb.py --batch-size 20

# Switch to DynamoDB
export FORCE_DYNAMODB=true
```

### Strategy 3: Testing Only

```bash
# Test with sample data
python scripts/migrate-to-dynamodb.py --dry-run --urls https://example.com

# Test integration
export DYNAMODB_MIGRATION_PCT=5  # Test with 5% traffic
```

## Environment Configuration

### Development (Local DynamoDB)

```bash
export DYNAMODB_ENDPOINT=http://localhost:8000
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy
export DYNAMODB_TABLE_CRAWL_RESULTS=crawl4ai-results
export DYNAMODB_TABLE_SESSIONS=crawl4ai-sessions
```

### Production (AWS DynamoDB)

```bash
export AWS_REGION=us-west-2
# Use IAM roles instead of keys in production
export DYNAMODB_TABLE_CRAWL_RESULTS=prod-crawl4ai-results
export DYNAMODB_TABLE_SESSIONS=prod-crawl4ai-sessions
```

## Monitoring and Validation

### Check Migration Status

```python
from crawl4ai.hybrid_database_manager import HybridDatabaseManager

manager = HybridDatabaseManager()
status = manager.get_status()
print(f"Status: {status}")
```

### Monitor Performance

```bash
# DynamoDB Admin UI
open http://localhost:8001

# Check logs
tail -f ~/.crawl4ai/hybrid_db.log
tail -f ~/.crawl4ai/dynamodb.log
```

### Validate Data Consistency

```bash
# Compare counts
python -c "
import asyncio
from crawl4ai.async_database import AsyncDatabaseManager
from crawl4ai.dynamodb_manager import DynamoDBDocumentManager

async def compare():
    sqlite = AsyncDatabaseManager()
    dynamo = DynamoDBDocumentManager()
    
    await sqlite.initialize()
    await dynamo.initialize()
    
    sqlite_count = await sqlite.aget_total_count()
    dynamo_count = await dynamo.aget_total_count()
    
    print(f'SQLite: {sqlite_count}, DynamoDB: {dynamo_count}')

asyncio.run(compare())
"
```

## Rollback Procedures

### Immediate Rollback

```bash
# Force all traffic back to SQLite
export FORCE_SQLITE=true
export DYNAMODB_MIGRATION_PCT=0

# Restart Crawl4AI services
docker-compose -f docker-compose.aws.yml restart crawl4ai-server
```

### Gradual Rollback

```bash
# Reduce DynamoDB traffic gradually
export DYNAMODB_MIGRATION_PCT=50  # From 100%
export DYNAMODB_MIGRATION_PCT=25  # Continue reducing
export DYNAMODB_MIGRATION_PCT=0   # Back to SQLite only
```

## Production Deployment

### 1. Infrastructure Setup

```yaml
# CloudFormation or Terraform
Resources:
  CrawlResultsTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: prod-crawl4ai-results
      BillingMode: ON_DEMAND
      StreamSpecification:
        StreamViewType: NEW_AND_OLD_IMAGES
      TimeToLiveSpecification:
        AttributeName: ttl
        Enabled: true
```

### 2. Application Configuration

```bash
# Production environment
export AWS_REGION=us-west-2
export DYNAMODB_TABLE_CRAWL_RESULTS=prod-crawl4ai-results
export DYNAMODB_TABLE_SESSIONS=prod-crawl4ai-sessions
export DYNAMODB_MIGRATION_PCT=0  # Start with 0% in production
```

### 3. Monitoring Setup

```bash
# CloudWatch alarms
aws cloudwatch put-metric-alarm \
  --alarm-name "DynamoDB-HighErrorRate" \
  --alarm-description "DynamoDB error rate too high" \
  --metric-name UserErrors \
  --namespace AWS/DynamoDB \
  --statistic Sum \
  --period 300 \
  --threshold 10 \
  --comparison-operator GreaterThanThreshold
```

## Troubleshooting

### Common Issues

**DynamoDB Local won't start**
```bash
# Check port conflicts
lsof -i :8000
kill -9 $(lsof -t -i:8000)

# Clean and restart
docker-compose -f docker-compose.aws.yml down --volumes
./scripts/start-dynamodb.sh
```

**Migration fails**
```bash
# Check table existence
aws dynamodb list-tables --endpoint-url http://localhost:8000

# Recreate tables
python scripts/init-dynamodb.py

# Retry with smaller batches
python scripts/migrate-to-dynamodb.py --batch-size 5
```

**Performance issues**
```bash
# Check DynamoDB metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name ConsumedReadCapacityUnits \
  --dimensions Name=TableName,Value=crawl4ai-results \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-01T01:00:00Z \
  --period 300 \
  --statistics Average
```

### Performance Tuning

```bash
# Increase connection pool
export DYNAMODB_MAX_POOL_CONNECTIONS=100

# Batch operations
export DYNAMODB_BATCH_SIZE=25

# Adjust retry settings
export DYNAMODB_MAX_RETRIES=5
```

## Security Considerations

### IAM Policy (Production)

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
        "arn:aws:dynamodb:*:*:table/prod-crawl4ai-*"
      ]
    }
  ]
}
```

### Network Security

```bash
# For production, restrict DynamoDB access
# Use VPC endpoints for DynamoDB access
# Enable encryption at rest and in transit
```

## Cost Optimization

### Monitor Costs

```bash
# Set up billing alerts
aws budgets create-budget \
  --account-id 123456789012 \
  --budget file://budget.json
```

### Optimize Usage

```bash
# Use eventually consistent reads
export DYNAMODB_CONSISTENT_READS=false

# Implement proper TTL
export DYNAMODB_TTL_DAYS=30

# Monitor item sizes
export DYNAMODB_MAX_ITEM_SIZE=350000
```

## Success Metrics

### Key Performance Indicators

- **Migration Progress**: % of traffic on DynamoDB
- **Error Rate**: < 0.1% for DynamoDB operations
- **Latency**: p99 < 100ms for cache operations
- **Data Consistency**: 100% between SQLite and DynamoDB during dual-write
- **Cost**: DynamoDB costs within budget targets

### Monitoring Dashboard

```bash
# CloudWatch dashboard
aws cloudwatch put-dashboard \
  --dashboard-name "Crawl4AI-DynamoDB" \
  --dashboard-body file://dashboard.json
```

## Code of Conduct Compliance

This deployment follows the project's code of conduct by:

✅ **Responsible Implementation**: Gradual rollout minimizes risk
✅ **Community Benefit**: Improved scalability for all users  
✅ **Respectful Migration**: Maintains backward compatibility
✅ **Constructive Approach**: Comprehensive testing and validation
✅ **Accepting Responsibility**: Clear rollback procedures

---

*Remember: The goal is not just to migrate to DynamoDB, but to do so responsibly while maintaining the reliability and performance that the community depends on.* 
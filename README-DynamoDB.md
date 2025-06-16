# Crawl4AI DynamoDB Migration Guide

*A comprehensive guide to replacing SQLite with DynamoDB, because apparently local databases weren't distributed enough for our cosmic ambitions.*

## Overview

This guide documents the migration from SQLite file-based storage to DynamoDB document storage for Crawl4AI. The implementation supports both local development (DynamoDB Local) and production deployment (AWS DynamoDB).

## Architecture Changes

### Before (SQLite)
- **Storage**: Local SQLite database with file-based content storage
- **Location**: `.crawl4ai/crawl4ai.db` + content files by hash
- **Complexity**: Minimal (single file database)
- **Scalability**: Limited to single instance

### After (DynamoDB)
- **Storage**: DynamoDB tables with JSON document format
- **Location**: Local DynamoDB or AWS DynamoDB service  
- **Complexity**: Distributed (but worth it for the flexibility)
- **Scalability**: Horizontal scaling with AWS

## Document Format

### CrawlResult Storage Schema

```json
{
  "url": "https://example.com",
  "success": true,
  "html": "...",
  "cleaned_html": "...",
  "markdown": {
    "raw_markdown": "...",
    "markdown_with_citations": "...",
    "references_markdown": "...",
    "fit_markdown": "...",
    "fit_html": "..."
  },
  "extracted_content": "...",
  "media": {},
  "links": {},
  "metadata": {},
  "response_headers": {},
  "downloaded_files": [],
  "screenshot": "",
  "status_code": 200,
  "error_message": "",
  "session_id": "",
  "created_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:30:00Z",
  "ttl": 1706184600
}
```

## Quick Start

### 1. Start Development Environment

```bash
# Start all DynamoDB services
./scripts/start-dynamodb.sh

# Or manually with docker-compose
docker-compose -f docker-compose.aws.yml up -d
```

### 2. Verify Services

- **DynamoDB Local**: http://localhost:8000
- **DynamoDB Admin UI**: http://localhost:8001
- **Redis Cache**: redis://localhost:6379

### 3. Run Crawl4AI with DynamoDB

```bash
# Start the server with DynamoDB backend
docker-compose -f docker-compose.aws.yml up crawl4ai-server

# Or configure environment for local development
export DYNAMODB_ENDPOINT=http://localhost:8000
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy
```

## Development Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DYNAMODB_ENDPOINT` | DynamoDB endpoint URL | `http://localhost:8000` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | AWS access key (dummy for local) | `dummy` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key (dummy for local) | `dummy` |
| `DYNAMODB_TABLE_CRAWL_RESULTS` | Results table name | `crawl4ai-results` |
| `DYNAMODB_TABLE_SESSIONS` | Sessions table name | `crawl4ai-sessions` |

### Code Integration

```python
from crawl4ai.dynamodb_manager import DynamoDBDocumentManager
from crawl4ai import AsyncWebCrawler

# Replace AsyncDatabaseManager with DynamoDBDocumentManager
async def main():
    # Initialize DynamoDB manager
    db_manager = DynamoDBDocumentManager(
        endpoint_url="http://localhost:8000"
    )
    await db_manager.initialize()
    
    # Use with crawler (when integration is complete)
    async with AsyncWebCrawler() as crawler:
        # Configure crawler to use DynamoDB
        # This integration is part of the migration tasks
        pass
```

## Table Schemas

### crawl4ai-results Table

- **Partition Key**: `url` (String)
- **Global Secondary Index**: `timestamp-index` on `created_at`
- **TTL**: 30 days from creation
- **Billing Mode**: Pay per request (for local development)

### crawl4ai-sessions Table

- **Partition Key**: `session_id` (String)  
- **TTL**: 24 hours from last update
- **Purpose**: MCP server session state management

## Migration Tasks

### Completed ✅
- [x] Docker Compose configuration for local DynamoDB
- [x] DynamoDB table initialization scripts
- [x] Basic DynamoDBDocumentManager implementation
- [x] Document serialization/deserialization
- [x] Development environment scripts

### In Progress 🚧
- [ ] Integration with AsyncWebCrawler
- [ ] MCP server DynamoDB integration
- [ ] Migration script from SQLite to DynamoDB
- [ ] Testing and validation

### Planned 📋
- [ ] Production deployment configuration
- [ ] Performance optimization
- [ ] Monitoring and alerting
- [ ] Documentation updates

## Testing

### Local Testing

```bash
# Start services
./scripts/start-dynamodb.sh

# Run tests with DynamoDB backend
python -m pytest tests/ -k "dynamodb"

# Test DynamoDB connection
python -c "
from crawl4ai.dynamodb_manager import DynamoDBDocumentManager
import asyncio
async def test():
    db = DynamoDBDocumentManager()
    await db.initialize()
    print('DynamoDB connection successful')
asyncio.run(test())
"
```

### Manual Verification

```bash
# Check tables exist
aws dynamodb list-tables --endpoint-url http://localhost:8000

# Inspect table schema
aws dynamodb describe-table \
    --table-name crawl4ai-results \
    --endpoint-url http://localhost:8000
```

## Production Deployment

### AWS Configuration

```bash
# Set production environment variables
export DYNAMODB_ENDPOINT=""  # Use AWS DynamoDB
export AWS_REGION="us-west-2"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export DYNAMODB_TABLE_CRAWL_RESULTS="prod-crawl4ai-results"
export DYNAMODB_TABLE_SESSIONS="prod-crawl4ai-sessions"
```

### Infrastructure as Code

```yaml
# cloudformation/dynamodb-tables.yml
Resources:
  CrawlResultsTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub "${Environment}-crawl4ai-results"
      BillingMode: ON_DEMAND
      AttributeDefinitions:
        - AttributeName: url
          AttributeType: S
        - AttributeName: created_at
          AttributeType: S
      KeySchema:
        - AttributeName: url
          KeyType: HASH
      GlobalSecondaryIndexes:
        - IndexName: timestamp-index
          KeySchema:
            - AttributeName: created_at
              KeyType: HASH
          Projection:
            ProjectionType: ALL
      TimeToLiveSpecification:
        AttributeName: ttl
        Enabled: true
```

## Troubleshooting

### Common Issues

**DynamoDB Local won't start**
```bash
# Check if port 8000 is already in use
lsof -i :8000

# Clean up Docker containers
docker-compose -f docker-compose.aws.yml down --volumes
```

**Table creation fails**
```bash
# Check DynamoDB Local logs
docker-compose -f docker-compose.aws.yml logs dynamodb-local

# Manually create tables
python scripts/init-dynamodb.py
```

**Connection timeout errors**
```bash
# Verify network connectivity
curl http://localhost:8000

# Check container networking
docker network ls
docker network inspect crawl4ai-network
```

### Performance Considerations

- **Item Size Limit**: DynamoDB has a 400KB item size limit
- **Large Content**: Consider storing large HTML/PDFs in S3 with references
- **Query Patterns**: Design GSI indexes for your access patterns
- **Costs**: Monitor DynamoDB usage in production

## MCP Integration

The DynamoDB implementation maintains compatibility with the existing MCP server, supporting:

- **Session Management**: Persistent session state across MCP calls
- **Document Retrieval**: Fast access to cached crawl results
- **Streamable HTTP**: Compatible with Lambda MCP server pattern

### MCP Tools Affected

- `crawl`: Now stores results in DynamoDB
- `md`: Retrieves markdown from DynamoDB documents  
- `html`: Accesses HTML content from DynamoDB
- `ask`: Queries document store for context

## Contributing

When contributing to the DynamoDB migration:

1. Test changes with local DynamoDB setup
2. Ensure backward compatibility during transition
3. Update documentation for new features
4. Consider performance implications of queries

## Cosmic Philosophy

*"In an infinite universe, the transition from SQLite to DynamoDB represents a fundamental shift from local simplicity to distributed complexity. While this adds operational overhead, it provides the scalability necessary for cosmic-scale web crawling operations. The universe may be heading toward heat death, but at least our data will be horizontally scalable."*

— Marvin, The Paranoid Android

---

**Next Steps**: Begin integration testing and performance validation of the DynamoDB backend before deprecating SQLite entirely. 
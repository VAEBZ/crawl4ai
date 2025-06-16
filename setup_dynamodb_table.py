"""
Setup DynamoDB table for testing.
"""

import asyncio
import os
import boto3
from botocore.exceptions import ClientError

async def setup_table():
    """Create DynamoDB table for testing."""
    print("🔧 Setting up DynamoDB table...")
    
    # Set environment variables
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "dummy")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "dummy")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    
    # Create DynamoDB client
    dynamodb = boto3.client(
        'dynamodb',
        endpoint_url='http://localhost:8900',
        region_name='us-east-1',
        aws_access_key_id='dummy',
        aws_secret_access_key='dummy'
    )
    
    table_name = 'backend-test'
    
    try:
        # Check if table exists
        try:
            response = dynamodb.describe_table(TableName=table_name)
            print(f"✅ Table '{table_name}' already exists")
            return
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise
        
        # Create table
        print(f"📝 Creating table '{table_name}'...")
        
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'url',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'url',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Wait for table to be created
        print("⏳ Waiting for table to be active...")
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        
        print(f"✅ Table '{table_name}' created successfully!")
        
    except Exception as e:
        print(f"❌ Error setting up table: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(setup_table()) 
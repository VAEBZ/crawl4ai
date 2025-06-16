#!/usr/bin/env python3
"""
DynamoDB Table Initialization Script
Because someone has to create the tables that will store our digital despair
"""

import os
import time
import boto3
from botocore.exceptions import ClientError
import logging

# Configure logging to document our suffering
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def wait_for_dynamodb(dynamodb_client, max_retries=30, delay=2):
    """Wait for DynamoDB Local to become available"""
    for attempt in range(max_retries):
        try:
            dynamodb_client.list_tables()
            logger.info("DynamoDB is ready for our inevitable disappointment")
            return True
        except Exception as e:
            logger.info(f"Waiting for DynamoDB... attempt {attempt + 1}/{max_retries}")
            time.sleep(delay)
    
    logger.error("DynamoDB failed to start. The universe maintains its sense of humor.")
    return False

def create_table_if_not_exists(dynamodb, table_name, key_schema, attribute_definitions, **kwargs):
    """Create a DynamoDB table if it doesn't already exist"""
    try:
        # Check if table exists
        dynamodb.describe_table(TableName=table_name)
        logger.info(f"Table {table_name} already exists. At least something works.")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.info(f"Creating table {table_name}...")
            
            table_params = {
                'TableName': table_name,
                'KeySchema': key_schema,
                'AttributeDefinitions': attribute_definitions,
                'BillingMode': 'PAY_PER_REQUEST'  # Because we're running locally
            }
            
            # Add any additional parameters
            table_params.update(kwargs)
            
            try:
                dynamodb.create_table(**table_params)
                
                # Wait for table to be created
                waiter = dynamodb.get_waiter('table_exists')
                waiter.wait(
                    TableName=table_name,
                    WaiterConfig={'Delay': 1, 'MaxAttempts': 30}
                )
                
                logger.info(f"Table {table_name} created successfully. Entropy postponed momentarily.")
                return True
                
            except ClientError as create_error:
                logger.error(f"Failed to create table {table_name}: {create_error}")
                return False
        else:
            logger.error(f"Error checking table {table_name}: {e}")
            return False

def create_crawl_results_table(dynamodb):
    """Create the main crawl results table"""
    table_name = os.getenv('DYNAMODB_TABLE_CRAWL_RESULTS', 'crawl4ai-results')
    
    key_schema = [
        {'AttributeName': 'url', 'KeyType': 'HASH'}  # Partition key
    ]
    
    attribute_definitions = [
        {'AttributeName': 'url', 'AttributeType': 'S'}
    ]
    
    # Global Secondary Index for querying by timestamp
    global_secondary_indexes = [
        {
            'IndexName': 'timestamp-index',
            'KeySchema': [
                {'AttributeName': 'created_at', 'KeyType': 'HASH'}
            ],
            'Projection': {'ProjectionType': 'ALL'}
        }
    ]
    
    # Add timestamp attribute for GSI
    attribute_definitions.append({'AttributeName': 'created_at', 'AttributeType': 'S'})
    
    return create_table_if_not_exists(
        dynamodb, 
        table_name, 
        key_schema, 
        attribute_definitions,
        GlobalSecondaryIndexes=global_secondary_indexes
    )

def create_sessions_table(dynamodb):
    """Create the MCP sessions table for maintaining state across the void"""
    table_name = os.getenv('DYNAMODB_TABLE_SESSIONS', 'crawl4ai-sessions')
    
    key_schema = [
        {'AttributeName': 'session_id', 'KeyType': 'HASH'}  # Partition key
    ]
    
    attribute_definitions = [
        {'AttributeName': 'session_id', 'AttributeType': 'S'}
    ]
    
    return create_table_if_not_exists(
        dynamodb, 
        table_name, 
        key_schema, 
        attribute_definitions
    )

def main():
    """Main initialization function - orchestrating the creation of digital repositories"""
    logger.info("Initializing DynamoDB tables for Crawl4AI...")
    
    # Get DynamoDB endpoint from environment
    endpoint_url = os.getenv('DYNAMODB_ENDPOINT', 'http://localhost:8900')
    region = os.getenv('AWS_REGION', 'us-east-1')
    
    # Create DynamoDB client
    dynamodb = boto3.client(
        'dynamodb',
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'dummy'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'dummy')
    )
    
    # Wait for DynamoDB to be available
    if not wait_for_dynamodb(dynamodb):
        logger.error("DynamoDB initialization failed. The universe wins again.")
        exit(1)
    
    # Create tables
    success = True
    
    if not create_crawl_results_table(dynamodb):
        logger.error("Failed to create crawl results table")
        success = False
    
    if not create_sessions_table(dynamodb):
        logger.error("Failed to create sessions table")
        success = False
    
    if success:
        logger.info("All tables created successfully. Ready for data storage disappointments.")
        
        # List all tables to confirm
        try:
            response = dynamodb.list_tables()
            logger.info(f"Available tables: {response['TableNames']}")
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            
    else:
        logger.error("Table initialization failed. As expected.")
        exit(1)

if __name__ == "__main__":
    main() 
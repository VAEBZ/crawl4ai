"""
MCP Server with DynamoDB Integration
Extends the existing MCP server to use DynamoDB for document storage and session management
"""

import asyncio
import json
import logging
import os
from typing import Dict, Any, Optional, List
from datetime import datetime

from .dynamodb_manager import DynamoDBManager
from .async_webcrawler_dynamodb import AsyncWebCrawlerDynamoDB
from .models import CrawlResult

logger = logging.getLogger(__name__)


class MCPServerDynamoDB:
    """MCP Server with DynamoDB storage backend"""
    
    def __init__(self):
        self.db_manager = DynamoDBManager(
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            endpoint_url=os.getenv(
                'DYNAMODB_ENDPOINT_URL', 'http://localhost:8000'
            ),
            table_name=os.getenv('DYNAMODB_TABLE_NAME', 'crawl4ai-results'),
            session_table_name=os.getenv(
                'DYNAMODB_SESSION_TABLE_NAME', 'crawl4ai-sessions'
            )
        )
        self.crawler = None
        self.sessions: Dict[str, Dict[str, Any]] = {}
    
    async def initialize(self):
        """Initialize the MCP server and database connections"""
        await self.db_manager.initialize()
        self.crawler = AsyncWebCrawlerDynamoDB()
        await self.crawler.__aenter__()
        logger.info("MCP Server with DynamoDB initialized")
    
    async def close(self):
        """Close the MCP server and cleanup resources"""
        if self.crawler:
            await self.crawler.__aexit__(None, None, None)
        await self.db_manager.close()
        logger.info("MCP Server with DynamoDB closed")
    
    async def crawl_url(self, 
                       url: str, 
                       session_id: str = None,
                       extraction_strategy: str = None,
                       chunking_strategy: str = None,
                       **kwargs) -> Dict[str, Any]:
        """
        Crawl a URL and store result in DynamoDB
        
        Args:
            url: The URL to crawl
            session_id: Optional session ID for tracking
            extraction_strategy: Optional extraction strategy
            chunking_strategy: Optional chunking strategy
            **kwargs: Additional crawling parameters
            
        Returns:
            Dictionary containing crawl result and metadata
        """
        try:
            # Perform the crawl
            result = await self.crawler.arun(
                url=url,
                extraction_strategy=extraction_strategy,
                chunking_strategy=chunking_strategy,
                **kwargs
            )
            
            # Store session information if provided
            if session_id:
                await self._update_session(session_id, {
                    'last_crawled_url': url,
                    'last_crawl_time': datetime.utcnow().isoformat(),
                    'success': result.success
                })
            
            return {
                'success': True,
                'url': url,
                'result': {
                    'success': result.success,
                    'html_length': len(result.html) if result.html else 0,
                    'markdown_length': len(result.markdown) if result.markdown else 0,
                    'links_count': len(result.links.get('internal', []) + 
                                    result.links.get('external', [])) if result.links else 0,
                    'media_count': len(result.media.get('images', [])) if result.media else 0,
                    'extracted_content': result.extracted_content,
                    'metadata': result.metadata
                },
                'session_id': session_id,
                'timestamp': datetime.utcnow().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error crawling URL {url}: {e}")
            return {
                'success': False,
                'url': url,
                'error': str(e),
                'session_id': session_id,
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def get_cached_content(self, 
                               url: str,
                               content_type: str = 'markdown') -> Dict[str, Any]:
        """
        Get cached content for a URL
        
        Args:
            url: The URL to retrieve
            content_type: Type of content ('markdown', 'html', 'text', 'json')
            
        Returns:
            Dictionary containing the requested content
        """
        try:
            result = await self.crawler.aget_cached_url(url)
            
            if not result:
                return {
                    'success': False,
                    'error': f'No cached content found for URL: {url}',
                    'url': url
                }
            
            content = ''
            if content_type == 'markdown':
                content = result.markdown or ''
            elif content_type == 'html':
                content = result.cleaned_html or result.html or ''
            elif content_type == 'text':
                content = result.extracted_content or ''
            elif content_type == 'json':
                content = {
                    'url': result.url,
                    'success': result.success,
                    'markdown': result.markdown,
                    'html': result.html,
                    'cleaned_html': result.cleaned_html,
                    'extracted_content': result.extracted_content,
                    'media': result.media,
                    'links': result.links,
                    'metadata': result.metadata
                }
            
            return {
                'success': True,
                'url': url,
                'content_type': content_type,
                'content': content,
                'metadata': result.metadata,
                'timestamp': datetime.utcnow().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error retrieving cached content for {url}: {e}")
            return {
                'success': False,
                'error': str(e),
                'url': url,
                'content_type': content_type
            }
    
    async def search_cached_urls(self, 
                               query: str = None,
                               limit: int = 10) -> Dict[str, Any]:
        """
        Search cached URLs (basic implementation)
        
        Args:
            query: Optional search query
            limit: Maximum number of results
            
        Returns:
            Dictionary containing search results
        """
        try:
            # This is a basic implementation - in production you'd want
            # to use DynamoDB GSI or ElasticSearch for better search
            
            # For now, just return recent cached URLs
            # In a real implementation, you'd add search capabilities
            
            total_count = await self.crawler.aget_cache_size()
            
            return {
                'success': True,
                'query': query,
                'total_cached_urls': total_count,
                'limit': limit,
                'message': ('Basic search not implemented yet. '
                          'Use get_cached_content for specific URLs.'),
                'timestamp': datetime.utcnow().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error searching cached URLs: {e}")
            return {
                'success': False,
                'error': str(e),
                'query': query
            }
    
    async def clear_cache(self, 
                         session_id: str = None) -> Dict[str, Any]:
        """
        Clear cached content
        
        Args:
            session_id: Optional session ID for tracking
            
        Returns:
            Dictionary containing operation result
        """
        try:
            success = await self.crawler.aclear_cache()
            
            if session_id:
                await self._update_session(session_id, {
                    'last_action': 'clear_cache',
                    'last_action_time': datetime.utcnow().isoformat()
                })
            
            return {
                'success': success,
                'message': 'Cache cleared successfully' if success else 'Failed to clear cache',
                'session_id': session_id,
                'timestamp': datetime.utcnow().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return {
                'success': False,
                'error': str(e),
                'session_id': session_id
            }
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics
        
        Returns:
            Dictionary containing cache statistics
        """
        try:
            size = await self.crawler.aget_cache_size()
            
            return {
                'success': True,
                'total_cached_urls': size,
                'cache_type': 'DynamoDB',
                'timestamp': datetime.utcnow().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _update_session(self, 
                            session_id: str, 
                            data: Dict[str, Any]) -> None:
        """Update session data in DynamoDB"""
        try:
            existing_session = self.sessions.get(session_id, {})
            existing_session.update(data)
            existing_session['session_id'] = session_id
            existing_session['updated_at'] = datetime.utcnow().isoformat()
            
            self.sessions[session_id] = existing_session
            
            # Store in DynamoDB sessions table
            await self.db_manager.store_session_data(session_id, existing_session)
            
        except Exception as e:
            logger.error(f"Error updating session {session_id}: {e}")
    
    async def get_session_data(self, session_id: str) -> Dict[str, Any]:
        """Get session data from DynamoDB"""
        try:
            # Check local cache first
            if session_id in self.sessions:
                return {
                    'success': True,
                    'session_id': session_id,
                    'data': self.sessions[session_id]
                }
            
            # Fetch from DynamoDB
            session_data = await self.db_manager.get_session_data(session_id)
            if session_data:
                self.sessions[session_id] = session_data
                return {
                    'success': True,
                    'session_id': session_id,
                    'data': session_data
                }
            
            return {
                'success': False,
                'session_id': session_id,
                'error': 'Session not found'
            }
        
        except Exception as e:
            logger.error(f"Error getting session data for {session_id}: {e}")
            return {
                'success': False,
                'session_id': session_id,
                'error': str(e)
            }


# MCP Tools implementation for the server
class MCPTools:
    """MCP Tools interface for DynamoDB-backed crawler"""
    
    def __init__(self, server: MCPServerDynamoDB):
        self.server = server
    
    async def crawl(self, url: str, **kwargs) -> Dict[str, Any]:
        """MCP crawl tool"""
        return await self.server.crawl_url(url, **kwargs)
    
    async def get_markdown(self, url: str) -> Dict[str, Any]:
        """MCP get markdown tool"""
        return await self.server.get_cached_content(url, 'markdown')
    
    async def get_html(self, url: str) -> Dict[str, Any]:
        """MCP get HTML tool"""
        return await self.server.get_cached_content(url, 'html')
    
    async def get_text(self, url: str) -> Dict[str, Any]:
        """MCP get text tool"""
        return await self.server.get_cached_content(url, 'text')
    
    async def search(self, query: str = None, limit: int = 10) -> Dict[str, Any]:
        """MCP search tool"""
        return await self.server.search_cached_urls(query, limit)
    
    async def clear_cache(self) -> Dict[str, Any]:
        """MCP clear cache tool"""
        return await self.server.clear_cache()
    
    async def cache_stats(self) -> Dict[str, Any]:
        """MCP cache stats tool"""
        return await self.server.get_cache_stats()


# Factory function for creating the server
async def create_mcp_server() -> MCPServerDynamoDB:
    """Create and initialize MCP server with DynamoDB"""
    server = MCPServerDynamoDB()
    await server.initialize()
    return server 
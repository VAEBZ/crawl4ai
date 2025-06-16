"""
DynamoDB-integrated AsyncWebCrawler
Extends the standard AsyncWebCrawler to use DynamoDB for document storage
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from .dynamodb_manager import DynamoDBManager
from .models import CrawlResult, MarkdownGenerationResult, CrawlResultContainer
from .async_webcrawler import AsyncWebCrawler as BaseAsyncWebCrawler

logger = logging.getLogger(__name__)


class AsyncWebCrawlerDynamoDB(BaseAsyncWebCrawler):
    """AsyncWebCrawler with DynamoDB storage backend"""
    
    def __init__(self, 
                 always_bypass_cache: bool = False,
                 base_directory: str = ".",
                 **kwargs):
        
        # Initialize base crawler without a db_manager; we'll manage it here.
        super().__init__(db_manager=None, **kwargs)
        
        self.always_bypass_cache = always_bypass_cache
        self.base_directory = base_directory
        self.db_manager = DynamoDBManager()
    
    async def __aenter__(self):
        """Async context manager enter."""
        # The base class __aenter__ might try to init its own db_manager
        # we we skip it and call its parent's __aenter__ if necessary.
        # In our case, the super().super() has no __aenter__, so we just return self.
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.db_manager.close()
        # Now call the base class's exit logic, which handles browser shutdown
        await super().__aexit__(exc_type, exc_val, exc_tb)
    
    async def arun(self, url: str, **kwargs) -> CrawlResultContainer:
        """
        Enhanced arun with DynamoDB caching.
        """
        bypass_cache = self.always_bypass_cache or kwargs.get(
            "bypass_cache", False
        )
        if not bypass_cache:
            cached_result = await self.db_manager.get_cached_url(url)
            if cached_result:
                # Wrap the cached result in a container to match the return type
                return CrawlResultContainer(
                    crawl_result=cached_result,
                    markdown_generation_result=None,
                )

        # If not cached, run the actual crawl from the base class
        result_container = await super().arun(url, **kwargs)

        # Cache the result
        if result_container and result_container.crawl_result and result_container.crawl_result.success:
            await self.db_manager.cache_url(result_container.crawl_result)

        return result_container
    
    async def aget_cached_url(self, url: str) -> Optional[CrawlResult]:
        """Get cached URL result from DynamoDB"""
        try:
            cached_doc = await self.db_manager.get_cached_url(url)
            if cached_doc:
                return self._deserialize_crawl_result(cached_doc)
            return None
        except Exception as e:
            logger.error(f"Error retrieving cached URL {url}: {e}")
            return None
    
    async def aget_cached_urls_batch(
        self, urls: List[str]
    ) -> Dict[str, Optional[CrawlResult]]:
        """Get multiple cached URLs in batch from DynamoDB"""
        try:
            cached_docs = await self.db_manager.aget_cached_urls_batch(urls)
            results = {}
            for url in urls:
                if url in cached_docs and cached_docs[url]:
                    results[url] = self._deserialize_crawl_result(
                        cached_docs[url]
                    )
                else:
                    results[url] = None
            return results
        except Exception as e:
            logger.error(f"Error retrieving cached URLs batch: {e}")
            return {url: None for url in urls}
    
    async def aclear_cache(self) -> bool:
        """Clear all cached results"""
        try:
            await self.db_manager.clear_cache()
            logger.info("Cache cleared successfully")
            return True
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return False
    
    async def aget_cache_size(self) -> int:
        """Get number of cached documents"""
        try:
            return await self.db_manager.get_total_count()
        except Exception as e:
            logger.error(f"Error getting cache size: {e}")
            return 0
    
    def _serialize_crawl_result(self, result: CrawlResult) -> Dict[str, Any]:
        """Serialize CrawlResult to DynamoDB document format"""
        doc = {
            'url': result.url,
            'success': result.success,
            'html': result.html or '',
            'cleaned_html': result.cleaned_html or '',
            'extracted_content': result.extracted_content or '',
            'media': result.media or {},
            'links': result.links or {},
            'metadata': result.metadata or {},
            'created_at': datetime.utcnow().isoformat()
        }
        
        # Handle markdown generation result
        if hasattr(result, 'markdown_v2') and result.markdown_v2:
            markdown_result = result.markdown_v2
            doc['markdown'] = {
                'raw_markdown': markdown_result.raw_markdown or '',
                'markdown_with_citations': (
                    markdown_result.markdown_with_citations or ''
                ),
                'references_markdown': (
                    markdown_result.references_markdown or ''
                ),
                'fit_markdown': markdown_result.fit_markdown or '',
                'fit_html': markdown_result.fit_html or ''
            }
        else:
            doc['markdown'] = {
                'raw_markdown': result.markdown or '',
                'markdown_with_citations': '',
                'references_markdown': '',
                'fit_markdown': '',
                'fit_html': ''
            }
        
        return doc
    
    def _deserialize_crawl_result(self, doc: Dict[str, Any]) -> CrawlResult:
        """Deserialize DynamoDB document to CrawlResult"""
        # Create markdown generation result
        markdown_data = doc.get('markdown', {})
        markdown_result = MarkdownGenerationResult(
            raw_markdown=markdown_data.get('raw_markdown', ''),
            markdown_with_citations=markdown_data.get(
                'markdown_with_citations', ''
            ),
            references_markdown=markdown_data.get(
                'references_markdown', ''
            ),
            fit_markdown=markdown_data.get('fit_markdown', ''),
            fit_html=markdown_data.get('fit_html', '')
        )
        
        # Create crawl result
        result = CrawlResult(
            url=doc['url'],
            html=doc.get('html', ''),
            success=doc.get('success', True),
            cleaned_html=doc.get('cleaned_html', ''),
            media=doc.get('media', {}),
            links=doc.get('links', {}),
            markdown=markdown_data.get('raw_markdown', ''),
            extracted_content=doc.get('extracted_content', ''),
            metadata=doc.get('metadata', {}),
            screenshot=None  # Screenshots not stored in DynamoDB
        )
        
        # Attach the markdown generation result
        result.markdown_v2 = markdown_result
        
        return result


# Convenience function for backward compatibility
async def create_async_crawler(**kwargs) -> AsyncWebCrawlerDynamoDB:
    """Create and initialize a DynamoDB-backed AsyncWebCrawler"""
    crawler = AsyncWebCrawlerDynamoDB(**kwargs)
    await crawler.db_manager.initialize()
    return crawler 
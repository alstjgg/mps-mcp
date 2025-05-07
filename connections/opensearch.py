"""OpenSearch connection management."""
from typing import Any, Dict, List, Optional, AsyncIterator
from contextlib import asynccontextmanager

from opensearchpy import AsyncOpenSearch, exceptions

import config


class OpenSearchConnection:
    """Manages connections to OpenSearch."""

    def __init__(self) -> None:
        """Initialize the OpenSearch connection manager."""
        self.client: Optional[AsyncOpenSearch] = None

    async def connect(self) -> None:
        """Establish connection to OpenSearch."""
        if not self.client:
            self.client = AsyncOpenSearch(
                hosts=[{
                    'host': config.OPENSEARCH_HOST,
                    'port': config.OPENSEARCH_PORT
                }],
                http_auth=(config.OPENSEARCH_USERNAME, config.OPENSEARCH_PASSWORD),
                use_ssl=config.OPENSEARCH_USE_SSL,
                verify_certs=False,  # Note: In production, consider verifying certs
                ssl_show_warn=False
            )

    async def disconnect(self) -> None:
        """Close the OpenSearch connection."""
        if self.client:
            await self.client.close()
            self.client = None

    async def get_indices(self) -> List[str]:
        """Get list of indices in OpenSearch."""
        if not self.client:
            await self.connect()
        response = await self.client.indices.get('*')
        return list(response.keys())

    async def get_index_mapping(self, index_name: str) -> Dict[str, Any]:
        """Get mapping for a specific index."""
        if not self.client:
            await self.connect()
        try:
            mapping = await self.client.indices.get_mapping(index=index_name)
            return mapping
        except exceptions.NotFoundError:
            return {"error": f"Index {index_name} not found"}

    async def search(self, index_name: str, query: Dict[str, Any], 
                    size: int = 100) -> Dict[str, Any]:
        """Execute a search query against an index."""
        if not self.client:
            await self.connect()
        try:
            result = await self.client.search(
                index=index_name,
                body=query,
                size=size
            )
            return result
        except exceptions.NotFoundError:
            return {"error": f"Index {index_name} not found"}


@asynccontextmanager
async def get_opensearch_connection() -> AsyncIterator[OpenSearchConnection]:
    """Context manager for OpenSearch connections."""
    connection = OpenSearchConnection()
    await connection.connect()
    try:
        yield connection
    finally:
        await connection.disconnect()
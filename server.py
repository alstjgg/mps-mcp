"""MCP Server with MongoDB and OpenSearch integration."""
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

from mcp.server.fastmcp import Context, FastMCP
from connections.mongodb import MongoDBConnection, get_mongodb_connection
from connections.opensearch import OpenSearchConnection, get_opensearch_connection


@dataclass
class AppContext:
    """Application context with database connections."""
    mongodb: MongoDBConnection
    opensearch: OpenSearchConnection


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage application lifecycle with database connections."""
    # Initialize connections on startup
    async with get_mongodb_connection() as mongodb, get_opensearch_connection() as opensearch:
        # Provide both connections in the context
        yield AppContext(mongodb=mongodb, opensearch=opensearch)


# Create the MCP server with lifespan management
mcp = FastMCP(
    "MongoDB-OpenSearch Explorer",
    lifespan=app_lifespan,
    dependencies=[
        "motor", "pymongo", "opensearch-py", "pandas", 
        "matplotlib", "python-dotenv"
    ]
)


# Example resource to expose MongoDB collections
@mcp.resource("mongodb://collections")
async def list_mongodb_collections(ctx: Context) -> str:
    """List all MongoDB collections."""
    mongodb = ctx.request_context.lifespan_context.mongodb
    collections = await mongodb.get_collections()
    return "\n".join(collections)


# Example resource to expose MongoDB schema for a specific collection
@mcp.resource("mongodb://{collection}/schema")
async def get_collection_schema(collection: str, ctx: Context) -> str:
    """Get schema for a specific MongoDB collection."""
    mongodb = ctx.request_context.lifespan_context.mongodb
    schema = await mongodb.get_schema(collection)
    return str(schema)


# Example resource to expose OpenSearch indices
@mcp.resource("opensearch://indices")
async def list_opensearch_indices(ctx: Context) -> str:
    """List all OpenSearch indices."""
    opensearch = ctx.request_context.lifespan_context.opensearch
    indices = await opensearch.get_indices()
    return "\n".join(indices)


# Example tool to query MongoDB collection
@mcp.tool()
async def query_mongodb(collection: str, query: str, ctx: Context, limit: int = 100) -> str:
    """
    Query a MongoDB collection with a read-only filter query.
    
    Args:
        collection: The name of the collection to query
        query: A JSON string representing a MongoDB query filter
        limit: Maximum number of results to return (default: 100)
    
    Returns:
        The query results as a formatted string
    """
    import json
    
    mongodb = ctx.request_context.lifespan_context.mongodb
    
    try:
        # Parse the query string to a dictionary
        query_dict = json.loads(query)
        
        # Execute the query
        results = await mongodb.execute_query(collection, query_dict, limit)
        
        # Format the results
        if not results:
            return "No results found."
        
        # Simple formatting for demonstration
        formatted_results = json.dumps(results, indent=2)
        return formatted_results
        
    except json.JSONDecodeError:
        return "Error: Invalid JSON query format."
    except Exception as e:
        return f"Error executing query: {str(e)}"



# Example tool to search OpenSearch
@mcp.tool()
async def search_opensearch(index: str, query: str, ctx: Context, size: int = 100) -> str:
    """
    Search an OpenSearch index.
    
    Args:
        index: The name of the index to search
        query: A JSON string representing an OpenSearch query
        size: Maximum number of results to return (default: 100)
    
    Returns:
        The search results as a formatted string
    """
    import json
    
    opensearch = ctx.request_context.lifespan_context.opensearch
    
    try:
        # Parse the query string to a dictionary
        query_dict = json.loads(query)
        
        # Execute the search
        results = await opensearch.search(index, query_dict, size)
        
        # Format the results
        formatted_results = json.dumps(results, indent=2)
        return formatted_results
        
    except json.JSONDecodeError:
        return "Error: Invalid JSON query format."
    except Exception as e:
        return f"Error executing search: {str(e)}"


if __name__ == "__main__":
    # Run the server
    mcp.run()
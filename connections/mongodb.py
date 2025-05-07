"""MongoDB connection management."""
from typing import Any, Dict, List, Optional, AsyncIterator
from contextlib import asynccontextmanager

import motor.motor_asyncio
from pymongo.database import Database
from pymongo.results import InsertOneResult

import config


class MongoDBConnection:
    """Manages connections to MongoDB."""

    def __init__(self) -> None:
        """Initialize the MongoDB connection manager."""
        self.client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
        self.db: Optional[motor.motor_asyncio.AsyncIOMotorDatabase] = None

    async def connect(self) -> None:
        """Establish connection to MongoDB."""
        if not self.client:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(config.MONGODB_URI)
            self.db = self.client[config.MONGODB_DB_NAME]

    async def disconnect(self) -> None:
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None

    async def get_collections(self) -> List[str]:
        """Get list of collection names in the database."""
        if not self.db:
            await self.connect()
        return await self.db.list_collection_names()

    async def get_schema(self, collection_name: str) -> Dict[str, Any]:
        """
        Generate a schema for a collection based on sampling documents.
        
        Note: This is a basic implementation. For production, consider
        using MongoDB schema validation or more sophisticated approaches.
        """
        if not self.db:
            await self.connect()
            
        collection = self.db[collection_name]
        sample = await collection.find_one()
        
        if not sample:
            return {"error": "No documents found in collection"}
            
        schema = {}
        for key, value in sample.items():
            schema[key] = type(value).__name__
            
        return schema

    async def execute_query(self, collection_name: str, query: Dict[str, Any], 
                           limit: int = 100) -> List[Dict[str, Any]]:
        """Execute a read-only query against a collection."""
        if not self.db:
            await self.connect()
            
        collection = self.db[collection_name]
        cursor = collection.find(query).limit(limit)
        return await cursor.to_list(length=limit)


@asynccontextmanager
async def get_mongodb_connection() -> AsyncIterator[MongoDBConnection]:
    """Context manager for MongoDB connections."""
    connection = MongoDBConnection()
    await connection.connect()
    try:
        yield connection
    finally:
        await connection.disconnect()
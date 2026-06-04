from pymongo import AsyncMongoClient

from config import CONNSTRING, DBNAME


client = AsyncMongoClient(CONNSTRING)
db = client[DBNAME]


async def close_database():
    await client.close()

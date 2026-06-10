from time import time
from typing import AsyncIterator

from pymongo import UpdateOne
from aiogram.types import User as TgUser

import parsing
from constants import STATUS_TIMEOUTERROR
from models import Product, Sku, User
from settings import AppSettings


class SettingsRepository:
    def __init__(self, database):
        self.collection = database.settings

    async def get(self) -> AppSettings:
        document = await self.collection.find_one({'_id': 'settings'})
        if document is None:
            raise RuntimeError('Settings document not found')
        return AppSettings.from_document(document)


class SkuRepository:
    def __init__(self, database):
        self.collection = database.sku

    async def exists(self, doc_id: str) -> bool:
        return await self.collection.find_one({'_id': doc_id}) is not None

    async def find(self, query: dict | None = None, sort=None) -> AsyncIterator[Sku]:
        cursor = self.collection.find(query or {})
        if sort is not None:
            cursor = cursor.sort(sort)
        async for document in cursor:
            yield Sku.from_document(document)

    async def count(self, query: dict | None = None) -> int:
        return await self.collection.count_documents(query or {})

    async def distinct(self, field: str, query: dict | None = None):
        return await self.collection.distinct(field, query or {})

    async def insert(self, sku: Sku):
        return await self.collection.insert_one(sku.to_json())

    async def save(self, sku: Sku):
        data = sku.to_json()
        data.pop('_id')
        return await self.collection.update_one(
            {'_id': sku.doc_id},
            {'$set': data}
        )

    async def delete(self, doc_id: str) -> bool:
        result = await self.collection.delete_one({'_id': doc_id})
        return result.deleted_count == 1

    async def delete_many(self, query: dict):
        return await self.collection.delete_many(query)

    async def delete_by_ids(self, chat_id: str, doc_ids: list[str]):
        return await self.collection.delete_many({
            '_id': {'$in': doc_ids},
            'chat_id': chat_id
        })

    async def update_many(self, query: dict, update: dict):
        return await self.collection.update_many(query, update)

    async def clear_notifications(self, doc_ids: list[str]):
        if not doc_ids:
            return
        
        requests = [
            UpdateOne(
                {'_id': doc_id},
                {'$set': {'price_prev': None, 'instock_prev': None}}
            )
            for doc_id in doc_ids
        ]
        await self.collection.bulk_write(requests)

class ProductRepository:
    cache_lifetime = 0
    http_timeout = 0

    def __init__(self, database):
        self.collection = database.skucache

    @classmethod
    def configure(cls, cache_lifetime: int, http_timeout: int):
        cls.cache_lifetime = cache_lifetime
        cls.http_timeout = http_timeout

    async def get(self, store: str, url: str) -> Product:
        timestamp_expired = int(time()) - self.cache_lifetime * 60
        document = await self.collection.find_one({
            'url': url,
            'timestamp': {'$gt': timestamp_expired}
        })
        if document:
            return Product(data=document['variants'], source='cache')

        parse_function = getattr(parsing, 'parse' + store)
        result = await parse_function(url, self.http_timeout)
        await self._cache(url, result)
        return Product(data=result['variants'], source='web')

    async def get_url(self, store: str, product_id: str) -> str | None:
        document = await self.collection.find_one({'_id': store + '_' + product_id})
        return document['url'] if document else None

    async def clear_sku_cache(self):
        timestamp_expired = int(time()) - self.cache_lifetime * 60
        await self.collection.delete_many({'timestamp': {'$lt': timestamp_expired}})

    async def _cache(self, url: str, result: dict):
        if result['status'] == STATUS_TIMEOUTERROR:
            return

        variants = result['variants']
        if variants:
            first_sku = list(variants.values())[0]
            query = {'_id': first_sku['store'] + '_' + first_sku['prodid']}
        else:
            query = {'url': url}

        data = {
            'variants': variants,
            'timestamp': int(time()),
            'url': url
        }
        await self.collection.update_one(query, {'$set': data}, upsert=True)


class UserRepository:
    def __init__(self, database):
        self.collection = database.users

    async def find(self, query: dict | None = None) -> AsyncIterator[User]:
        cursor = self.collection.find(query or {})
        async for document in cursor:
            yield User.from_document(document)

    async def find_one(self, chat_id: str | int) -> User | None:
        document = await self.collection.find_one({'_id': str(chat_id)})
        return User.from_document(document) if document else None
    
    async def save(self, user: User):
        data = {
            'first_name': user.first_name,
            'last_name': user.last_name,
            'username': user.username,
            'enable': user.enable,
            'broadcasts': user.broadcasts
        }
        await self.collection.update_one({'_id': user.id}, {'$set': data}, upsert=True)

    async def find_by_store(self, store: str) -> AsyncIterator[User]:
        cursor = await self.collection.aggregate([
            {
                '$lookup':
                {
                    'from': 'sku',
                    'localField': '_id',
                    'foreignField': 'chat_id',
                    'as': 'matched_skus'
                }
            },
            {
                '$match':
                {
                    'matched_skus.store': store,
                    'enable': True
                }
            }
        ])
        async for document in cursor:
            yield User.from_document(document)

    async def create_if_not_exists(self, tg_user: TgUser):
        if not await self.find_one(tg_user.id):
            new_user = User.from_aiogram_user(tg_user)
            await self.save(new_user)

    async def top_users(self, limit: int) -> AsyncIterator[User]:
        cursor = await self.collection.aggregate([
            {
                '$lookup':
                {
                    'from': "sku",
                    'localField': "_id",
                    'foreignField': "chat_id",
                    'as': "sku_docs"
                }
            },
            {
                '$addFields': { 'sku_count': { '$size': "$sku_docs" } }
            },
            {
                '$match': { 'sku_count': { '$ne': 0 } }
            },
            {
                '$sort': { "sku_count": -1 }
            },
            {
                '$limit': limit
            }
        ])
        
        async for document in cursor:
            yield User.from_document(document)

    async def count(self, query: dict | None = None) -> int:
        return await self.collection.count_documents(query or {})

    async def count_with_sku(self) -> int:
        cursor = await self.collection.aggregate([
            {
                '$match': {'enable': True}
            },
            {
                '$lookup':
                {
                    'from': 'sku',
                    'localField': '_id',
                    'foreignField': 'chat_id',
                    'as': 'sku_docs'
                }
            },
            {
                '$match': { 'sku_docs.0': { '$exists': True } }
            },
            {
                '$count': 'count'
            }
        ])
        result = await cursor.to_list(length=1)
        return result[0]['count'] if result else 0
    
    async def update_many(self, query: dict, update: dict):
        return await self.collection.update_many(query, update, upsert=True)


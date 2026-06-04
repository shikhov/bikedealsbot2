from time import time
from typing import AsyncIterator

from pymongo import UpdateOne

import parsing
from constants import STATUS_TIMEOUTERROR
from models import Product, Sku
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
            yield Sku(document)

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
    def __init__(self, database, cache_lifetime: int, http_timeout: int):
        self.collection = database.skucache
        self.cache_lifetime = cache_lifetime
        self.http_timeout = http_timeout

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

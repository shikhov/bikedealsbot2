from datetime import datetime
from time import time
from typing import Dict

from aiogram.types import User as TgUser
from pytz import timezone

from settings import StoreSettings

class User:
    def __init__(
        self,
        chat_id: str | int,
        first_name: str,
        last_name: str,
        username: str,
        enable: bool,
        max_items: int | None = None,
        broadcasts: list[str] | None = None,
        data: dict | None = None
    ):      
        self.id: str = str(chat_id)
        self.first_name: str = first_name
        self.last_name: str = last_name
        self.username: str = username
        self.enable: bool = enable
        self.max_items: int = max_items or self.max_items_per_user
        self.broadcasts: list[str] = broadcasts or []
        self.data: dict | None = data

    @property
    def full_name(self) -> str:
        return self.first_name + ' ' + self.last_name if self.last_name else self.first_name
    
    @property
    def display_name(self) -> str:
        username = f' ({self.username})' if self.username else ''
        return self.full_name + username

    @property
    def sku_count(self) -> int:
        return self.data.get('sku_count', 0) if self.data else 0
    
    @classmethod
    def configure(cls, max_items_per_user: int):
        cls.max_items_per_user = max_items_per_user

    @classmethod
    def from_document(cls, data: dict):
        return cls(
            chat_id=data['_id'],
            first_name=data['first_name'],
            last_name=data['last_name'],
            username=data['username'],
            enable=data['enable'],
            max_items=data.get('max_items'),
            broadcasts=data.get('broadcasts'),
            data=data
        )
    
    @classmethod
    def from_aiogram_user(cls, tg_user: TgUser):
        return cls(
            chat_id=tg_user.id,
            first_name=tg_user.first_name,
            last_name=tg_user.last_name,
            username=tg_user.username,
            enable=True
        )


class Variant:
    def __init__(self, data: dict):
        self.store: str = data['store']
        self.prodid: str = data['prodid']
        self.id: str = data['skuid']
        self.url: str = data['url']
        self.name: str = data['name']
        self.variant: str = data['variant']
        self.price: int = data['price']
        self.currency: str = data['currency']
        self.instock: bool = data['instock']
        self.key: str = self.store.lower() + '_' + self.prodid + '_' + self.id

    def _icon_str(self):
        return '✅ ' if self.instock else '🚫 '

    def _url_str(self):
        return f'<a href="{self.url}">{self.name}</a>\n'

    def _store_str(self):
        return f'<code>[{self.store}]</code> '

    def _price_str(self):
        return f' <b>{self.price} {self.currency}</b>'

    def _add_str(self):
        return f'\n<i>Добавить: /add_{self.key}</i>'

    def _del_str(self):
        return f'\n<i>Удалить: /del_{self.key}</i>'

    def get_string(self, *options):
        string_parts = []

        if 'store' in options:
            string_parts.append(self._store_str())
        if 'url' in options:
            string_parts.append(self._url_str())
        if 'icon' in options:
            string_parts.append(self._icon_str())
        if self.variant:
            string_parts.append(self.variant)
        if 'price' in options:
            string_parts.append(self._price_str())
        if 'add' in options:
            string_parts.append(self._add_str())
        if 'del' in options:
            string_parts.append(self._del_str())

        return ''.join(string_parts)


class Sku(Variant):
    error_min_threshold = 0
    stores: dict[str, StoreSettings] = {}

    def __init__(
        self,
        variant: Variant,
        doc_id: str,
        chat_id: str,
        errors: int,
        enable: bool,
        lastcheck: str,
        lastcheckts: int,
        lastgoodts: int,
        instock_prev: bool | None,
        price_prev: int | None
    ):
        self.store = variant.store
        self.prodid = variant.prodid
        self.id = variant.id
        self.url = variant.url
        self.name = variant.name
        self.variant = variant.variant
        self.price = variant.price
        self.currency = variant.currency
        self.instock = variant.instock
        self.key = variant.key

        self.doc_id = doc_id
        self.chat_id = chat_id
        self.errors = errors
        self.enable = enable
        self.lastcheck = lastcheck
        self.lastcheckts = lastcheckts
        self.lastgoodts = lastgoodts
        self.instock_prev = instock_prev
        self.price_prev = price_prev
        self.store_prodid = self.store + '_' + self.prodid

    @classmethod
    def from_document(cls, data: dict) -> 'Sku':
        return cls(
            variant=Variant(data),
            doc_id=data['_id'],
            chat_id=data['chat_id'],
            errors=data['errors'],
            enable=data['enable'],
            lastcheck=data['lastcheck'],
            lastcheckts=data['lastcheckts'],
            lastgoodts=data['lastgoodts'],
            instock_prev=data['instock_prev'],
            price_prev=data['price_prev']
        )

    @classmethod
    def from_variant(cls, variant: Variant, user_id: str) -> 'Sku':
        timestamp = int(time())
        return cls(
            variant=variant,
            doc_id=f'{user_id}_{variant.store}_{variant.prodid}_{variant.id}',
            chat_id=user_id,
            errors=0,
            enable=True,
            lastcheck=datetime.now(timezone('Asia/Yekaterinburg')).strftime('%d.%m.%Y %H:%M'),
            lastcheckts=timestamp,
            lastgoodts=timestamp,
            instock_prev=None,
            price_prev=None
        )

    @classmethod
    def configure(cls, error_min_threshold: int, stores: dict[str, StoreSettings]):
        cls.error_min_threshold = error_min_threshold
        cls.stores = stores

    def _icon_str(self):
        icon = '✅ ' if self.instock else '🚫 '
        if self.errors > self.error_min_threshold:
            icon = '⚠️ '
        if not self.stores[self.store].active:
            icon = '⏳ '
        return icon

    def _price_prev_str(self):
        return f' (было: {self.price_prev} {self.currency})'

    def get_string(self, *options):
        string_parts = []
        if 'store' in options:
            string_parts.append(self._store_str())
        if 'url' in options:
            string_parts.append(self._url_str())
        if 'icon' in options:
            string_parts.append(self._icon_str())
        if self.variant:
            string_parts.append(self.variant)
        if 'price' in options:
            string_parts.append(self._price_str())
        if 'price_prev' in options:
            string_parts.append(self._price_prev_str())
        if 'add' in options:
            string_parts.append(self._add_str())
        if 'del' in options:
            string_parts.append(self._del_str())

        return ''.join(string_parts)

    def to_json(self):
        return {
            '_id': self.doc_id,
            'store': self.store,
            'prodid': self.prodid,
            'skuid': self.id,
            'url': self.url,
            'name': self.name,
            'variant': self.variant,
            'price': self.price,
            'currency': self.currency,
            'instock': self.instock,
            'store_prodid': self.store_prodid,
            'chat_id': self.chat_id,
            'errors': self.errors,
            'enable': self.enable,
            'lastcheck': self.lastcheck,
            'lastcheckts': self.lastcheckts,
            'lastgoodts': self.lastgoodts,
            'instock_prev': self.instock_prev,
            'price_prev': self.price_prev
        }


class Product:
    def __init__(self, data: dict | None, source: str):
        self.variants: Dict[str, Variant] = {}
        self.source = source
        self.id = None
        self.first_skuid = None
        self.name = None
        self.store = None
        self.var_count = 0

        if data:
            for sku_id, sku_data in data.items():
                sku_data = dict(sku_data)
                sku_data['skuid'] = sku_id
                self.variants[sku_id] = Variant(sku_data)
            first_sku = list(self.variants.values())[0]
            self.id = first_sku.prodid
            self.first_skuid = first_sku.id
            self.name = first_sku.name
            self.store = first_sku.store
            self.var_count = len(data)

    def get_sku_add_list(self):
        text_array = [self.name]
        for variant in self.variants.values():
            line = variant.get_string('icon', 'price', 'add')
            text_array.append(line)
        return text_array

    def has_sku(self, skuid: str):
        return skuid in self.variants

from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StoreSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra='ignore')

    name: str
    url: str
    url_regex: str
    active: bool
    price_threshold: float


class AppSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra='ignore', populate_by_name=True)

    token: str = Field(alias='TOKEN')
    admin_chat_id: int = Field(alias='ADMINCHATID')
    best_deals_chat_id: int | None = Field(alias='BESTDEALSCHATID')
    best_deals_min_percentage: int = Field(alias='BESTDEALSMINPERCENTAGE')
    best_deals_warn_percentage: int = Field(alias='BESTDEALSWARNPERCENTAGE')
    best_deals_min_value: dict[str, int] = Field(alias='BESTDEALSMINVALUE')
    cache_lifetime: int = Field(alias='CACHELIFETIME')
    error_min_threshold: int = Field(alias='ERRORMINTHRESHOLD')
    error_max_days: int = Field(alias='ERRORMAXDAYS')
    max_items_per_user: int = Field(alias='MAXITEMSPERUSER')
    check_interval: int = Field(alias='CHECKINTERVAL')
    log_chat_id: int | None = Field(alias='LOGCHATID')
    log_filter: list[str] = Field(alias='LOGFILTER')
    banner_start: str = Field(alias='BANNERSTART')
    banner_help: str = Field(alias='BANNERHELP')
    banner_donate: str = Field(alias='BANNERDONATE')
    stores: dict[str, StoreSettings] = Field(alias='STORES')
    debug: bool = Field(alias='DEBUG')
    http_timeout: int = Field(alias='HTTPTIMEOUT')
    request_delay: int = Field(alias='REQUESTDELAY')

    @model_validator(mode='before')
    @classmethod
    def add_store_names(cls, data: Any) -> Any:
        new_data = dict(data)
        new_data['STORES'] = {
            store_name: {**store_data, 'name': store_name}
            for store_name, store_data in data['STORES'].items()
        }
        return new_data

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> 'AppSettings':
        return cls.model_validate(document)

    def get_store_urls(self) -> str:
        urls = []
        for store in self.stores.values():
            status = '' if store.active else ' <i>(временно недоступен)</i>'
            urls.append(store.url + status)
        return '\n'.join(urls)

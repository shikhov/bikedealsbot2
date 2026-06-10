from dataclasses import dataclass


@dataclass(frozen=True)
class AppSettings:
    token: str
    admin_chat_id: int
    best_deals_chat_id: int | None
    best_deals_min_percentage: int
    best_deals_warn_percentage: int
    best_deals_min_value: dict
    cache_lifetime: int
    error_min_threshold: int
    error_max_days: int
    max_items_per_user: int
    check_interval: int
    log_chat_id: int | None
    log_filter: list[str]
    banner_start: str
    banner_help: str
    banner_donate: str
    stores: dict
    debug: bool
    http_timeout: int
    request_delay: int

    def get_store_urls(self) -> str:
        urls = []
        for store in self.stores.values():
            status = '' if store['active'] else ' <i>(временно недоступен)</i>'
            urls.append(store['url'] + status)
        return '\n'.join(urls)

    @classmethod
    def from_document(cls, document: dict) -> 'AppSettings':
        return cls(
            token=document['TOKEN'],
            admin_chat_id=document['ADMINCHATID'],
            best_deals_chat_id=document['BESTDEALSCHATID'],
            best_deals_min_percentage=document['BESTDEALSMINPERCENTAGE'],
            best_deals_warn_percentage=document['BESTDEALSWARNPERCENTAGE'],
            best_deals_min_value=document['BESTDEALSMINVALUE'],
            cache_lifetime=document['CACHELIFETIME'],
            error_min_threshold=document['ERRORMINTHRESHOLD'],
            error_max_days=document['ERRORMAXDAYS'],
            max_items_per_user=document['MAXITEMSPERUSER'],
            check_interval=document['CHECKINTERVAL'],
            log_chat_id=document['LOGCHATID'],
            log_filter=document['LOGFILTER'],
            banner_start=document['BANNERSTART'],
            banner_help=document['BANNERHELP'],
            banner_donate=document['BANNERDONATE'],
            stores=document['STORES'],
            debug=document['DEBUG'],
            http_timeout=document['HTTPTIMEOUT'],
            request_delay=document['REQUESTDELAY']
        )

import asyncio
import logging
import re
from html import escape
from hashlib import md5
from datetime import datetime
from time import time
from collections import defaultdict
from typing import AsyncIterator, Callable, Dict, Any, Awaitable

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandObject, CommandStart, BaseFilter
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pydantic import ValidationError
from pytz import timezone
from aiohttp import web
from webapp.routes import list_handler, api_list_handler, api_delete_handler

from config import PORT, WEBAPP_URL
from database import close_database, db
from models import Sku, User
from repositories import ProductRepository, SettingsRepository, SkuRepository, UserRepository
from settings import AppSettings

settings: AppSettings
settings_repository = SettingsRepository(db)
sku_repository = SkuRepository(db)
product_repository = ProductRepository(db)
user_repository = UserRepository(db)


class IsAdmin(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user.id == settings.admin_chat_id


async def load_settings():
    global settings

    settings = await settings_repository.get()
    Sku.configure(
        error_min_threshold=settings.error_min_threshold,
        stores=settings.stores
    )
    User.configure(
        max_items_per_user=settings.max_items_per_user
    )
    ProductRepository.configure(
        cache_lifetime=settings.cache_lifetime,
        http_timeout=settings.http_timeout
    )


class LoggingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        if event.from_user.id == bot.id:
            return
        if isinstance(event, Message):
            if event.text != '/start' and event.chat.type == ChatType.PRIVATE:
                await user_repository.create_if_not_exists(event.from_user)
            result = await handler(event, data)
            await self.log_message(event)
            return result
        return await handler(event, data)

    async def log_message(self, message: Message):
        if not settings.log_chat_id:
            return
        if message.from_user.id == settings.admin_chat_id:
            return
        if not message.text:
            return
        if message.text in settings.log_filter:
            return
        
        user = User.from_aiogram_user(message.from_user)        
        logentry = '<b>' + user.display_name + ':</b> ' + message.text
        await bot.send_message(settings.log_chat_id, logentry)


# Configure logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("aiogram.event").setLevel(logging.WARNING) 

dp = Dispatcher()
dp.message.middleware(LoggingMiddleware())


async def processException(e: Exception, chat_id: str):
    error_codes = ['bot was blocked', 'user is deactivated']
    if e.message and any(code in e.message for code in error_codes):
        await disableUser(chat_id)


@dp.message(CommandStart(), F.chat.type == ChatType.PRIVATE)
async def processCmdStart(message: Message):
    await message.answer(settings.banner_start)

    user = User.from_aiogram_user(message.from_user)
    await user_repository.save(user)
    await sku_repository.update_many({'chat_id': user.id}, {'$set': {'enable': True}})


async def broadcast(message: Message, text, users: AsyncIterator[User], pin=False):
    text_hash = md5(text.encode('utf-8')).hexdigest()
    await message.answer('🟢 Начало рассылки')

    count = 0
    async for user in users:
        count += 1
        if count % 100 == 0:
            await message.answer('Обработано: ' + str(count))
                
        if text_hash in user.broadcasts:
            continue

        try:
            sent_message = await bot.send_message(chat_id=user.id, text=text)
            if pin:
                await bot.pin_chat_message(chat_id=user.id, message_id=sent_message.message_id)            
            user.broadcasts.append(text_hash)
            await user_repository.save(user)
        except Exception as e:
            await processException(e, user.id)
        await asyncio.sleep(0.1)

    await message.answer('🔴 Окончание рассылки')


@dp.message(Command('users'), IsAdmin())
async def processCmdUpdateUsers(message: Message):
    await message.answer('🟢 Начало обновления списка пользователей')
    count = 0
    async for user in user_repository.find({'enable': True}):
        count += 1
        if count % 100 == 0:
            await message.answer('Обработано: ' + str(count))
        
        try:
            await bot.send_chat_action(chat_id=user.id, action='typing')
        except Exception as e:
            await processException(e, user.id)
        await asyncio.sleep(0.1)

    await message.answer('🔴 Окончание обновления списка пользователей')


@dp.message(Command('bc'), IsAdmin())
async def processCmdBroadcast(message: Message, command: CommandObject):
    text = (command.args or '').strip()
    if not text:
        await message.answer('Empty broadcast text')
        return
    users = user_repository.find({'enable': True})
    await broadcast(message, text, users)


@dp.message(Command('bc_pin'), IsAdmin())
async def processCmdBroadcastAndPin(message: Message, command: CommandObject):
    text = (command.args or '').strip()
    if not text:
        await message.answer('Empty broadcast text')
        return
    users = user_repository.find({'enable': True})
    await broadcast(message, text, users, pin=True)


@dp.message(Command(re.compile(r'^bc_(\w+)$')), IsAdmin())
async def processCmdBroadcastByStore(message: Message, command: CommandObject):
    store = command.regexp_match.group(1).upper()
    text = (command.args or '').strip()
    if not text:
        await message.answer('Empty broadcast text')
        return
    users = user_repository.find_by_store(store)
    await broadcast(message, text, users)


@dp.message(Command('reload'), IsAdmin())
async def cmd_reload(message: Message):
    try:
        await load_settings()
    except ValidationError as error:
        errors = ['Settings validation failed:']
        for item in error.errors():
            field = '.'.join(str(part) for part in item['loc'])
            errors.append(f'<code>{escape(field)}</code>: {escape(item["msg"])}')
        await paginatedTgMsg(errors, message.chat.id)
        return

    await message.answer('Settings successfully reloaded')


@dp.message(F.text.regexp(r'https?://', mode='search'), F.chat.type == ChatType.PRIVATE)
async def processURLMsg(message: Message):
    for store in settings.stores.values():
        if re.search(store.url_regex, message.text):
            break
    else:
        await message.reply('⚠️ Этот сайт не поддерживается. Список поддерживаемых смотрите в /help')
        return

    if not store.active:
        await message.reply('😔 К сожалению, отслеживание этого сайта временно недоступно')
        return

    url = processURL(store.name, message.text)
    if not url:
        await message.reply('🤷‍♂️ Не могу понять. Кажется, это не ссылка на товар')
        return

    await showVariants(store.name, url, message)


def processURL(store, text):
    if store == 'BD':
        rg = re.search(r'https://www\.bike-discount\.de/.+?/([^?&\s]+)', text)
        if rg:
            return 'https://www.bike-discount.de/en/' + rg.group(1)

    if store == 'B24':
        rg = re.search(r'(https://www\.bike24\.(com|de)/p[12](\d+)\.html)', text)
        if rg:
            return 'https://www.bike24.com/p2' + rg.group(3) + '.html'

    if store == 'TI':
        rg = re.search(r'(https://www\.tradeinn\.com/)(.+?)/(.+?)(/\S+/\d+/p)', text)
        if rg:
            return rg.group(1) + 'bikeinn/en' + rg.group(4)

    if store == 'SB':
        rg = re.search(r'(https://www\.starbike\.com/en/\S+?/)', text)
        if rg:
            return rg.group(1)

    if store == 'CRC':
        rg = re.search(r'(https://www\.chainreactioncycles\.com/)(\S+/)?(p/[^?&\s]+)', text)
        if rg:
            return rg.group(1) + 'int/' + rg.group(3)

    if store == 'BC':
        rg = re.search(r'(https://www\.bike-components\.de/)(.+?)(/\S+p(\d+)\/)', text)
        if rg:
            return rg.group(1) + 'en' + rg.group(3)

    if store == 'A4C':
        rg = re.search(r'https://www\.all4cycling\.com/(.+?/)?products/([^?]+)', text)
        if rg:
            return 'https://www.all4cycling.com/en/products/' + rg.group(2)

    if store == 'LG':
        rg = re.search(r'https://www\.lordgun\.com/([^ ?]+)', text)
        if rg:
            return 'https://www.lordgun.com/' + rg.group(1)

    return None


@dp.message(F.text.regexp(r'^/add_\w+_\w+_\w+$'), F.chat.type == ChatType.PRIVATE)
async def processCmdAdd(message: Message):
    params = message.text.split('_')
    store = params[1].upper()
    prodid = params[2]
    skuid = params[3]
    await addVariant(store, prodid, skuid, message)


@dp.message(F.text.regexp(r'^/del_\w+_\w+_\w+$'), F.chat.type == ChatType.PRIVATE)
async def processCmdDel(message: Message):
    chat_id = str(message.from_user.id)
    docid = message.text.replace('/del', chat_id).upper()
    if await sku_repository.delete(docid):
        await message.answer('Удалено')
        return
    await message.answer('Какая-то ошибка 😧')


@dp.message(Command('help'), F.chat.type == ChatType.PRIVATE)
async def processCmdHelp(message: Message):
    await message.answer(settings.banner_help)


@dp.message(Command('donate'), F.chat.type == ChatType.PRIVATE)
async def processCmdDonate(message: Message):
    await message.answer(settings.banner_donate)


@dp.message(Command('list'), F.chat.type == ChatType.PRIVATE)
async def processCmdList(message: Message):
    text_array = []
    chat_id = str(message.from_user.id)
    query = {'chat_id': chat_id}
    async for sku in sku_repository.find(query):
        line = sku.get_string('store', 'url', 'icon', 'price', 'del')
        text_array.append(line)

    if text_array:
        text_array = ['Отслеживаемые товары:'] + text_array
    else:
        text_array = ['Ваш список пуст']

    await paginatedTgMsg(text_array, chat_id)


@dp.message(Command('listw'), F.chat.type == ChatType.PRIVATE)
async def command_list_web(message: Message):
    btn = InlineKeyboardButton(text='Открыть', web_app=WebAppInfo(url=f'{WEBAPP_URL}/list/'))
    kb = InlineKeyboardMarkup(inline_keyboard=[[btn]])
    await message.answer(
        'Нажмите кнопку ниже, чтобы открыть веб-интерфейс для управления отслеживаемыми товарами:',
        reply_markup=kb
    )


@dp.message(Command('stat'), IsAdmin())
async def processCmdStat(message: Message):
    sent_msg = await message.answer('Getting stat...')

    usersall = await user_repository.count()
    usersactive = await user_repository.count({'enable': True})
    skuall = await sku_repository.count()
    userswsku = await user_repository.count_with_sku()
    skuactive = await sku_repository.count({'enable': True})
    unique_urls = len(await sku_repository.distinct('url', {'enable': True}))

    msg = ''
    msg += f'<b>Total users:</b> {usersall}\n'
    msg += f'<b>Enabled users:</b> {usersactive}\n'
    msg += f'<b>Enabled users with SKU:</b> {userswsku}\n'
    msg += f'<b>Total SKU:</b> {skuall}\n'
    msg += f'<b>Active SKU:</b> {skuactive}\n'
    msg += f'<b>Unique active URLs:</b> {unique_urls}\n'

    for key in settings.stores.keys():
        num = await sku_repository.count({'store': key})
        msg += f'<b>{key}:</b> {num}\n'

    TOPNUMBER = 10
    msg += f'\n<b>Top {TOPNUMBER} users:</b>\n'
    async for user in user_repository.top_users(TOPNUMBER):
        msg += f'{user.display_name}: {user.sku_count}\n'

    await sent_msg.edit_text(msg)


@dp.message(F.chat.type == ChatType.PRIVATE)
async def processSearch(message: Message):
    text = message.text
    if not text:
        return

    chat_id = str(message.from_user.id)
    query = {'chat_id': chat_id}
    if await sku_repository.count(query) == 0:
        await message.answer('⚠️ Ваш список пуст, поиск невозможен')
        return

    try:
        pattern = re.compile(text, re.I)
    except Exception:
        await message.reply('⚠️ Некорректное выражение')
        return

    query = {'chat_id': chat_id, 'name': {'$regex': pattern}}
    text_array = []
    async for sku in sku_repository.find(query):
        line = sku.get_string('store', 'url', 'icon', 'price', 'del')
        text_array.append(line)

    header = f'Результаты поиска по строке <b>{text}</b>:'
    text_array = [header] + (text_array or ['Ничего не найдено'])
    await paginatedTgMsg(text_array, chat_id)


async def reply_or_edit_msg(text, message: Message):
    if message.from_user.id == bot.id:
        await message.edit_text(text)
    else:
        await message.reply(text)


async def showVariants(store, url, message: Message):
    sent_msg = await message.reply('🔎 Ищу информацию о товаре...')

    prod = await product_repository.get(store, url)
    if prod.var_count == 0:
        await sent_msg.edit_text('Не смог найти цену 😧')
    elif prod.var_count == 1:
        await addVariant(store, prod.id, prod.first_skuid, sent_msg)
    elif prod.var_count > 1:
        await paginatedTgMsg(prod.getSkuAddList(), message.chat.id, sent_msg.message_id)


async def addVariant(store, prodid, skuid, message: Message):
    user = await user_repository.find_one(message.chat.id)
    if not user:
        await reply_or_edit_msg('Какая-то ошибка 😧', message)
        return
    
    query = {'chat_id': user.id}
    if await sku_repository.count(query) >= user.max_items:
        await reply_or_edit_msg(f'⛔️ Увы, в данный момент добавить можно не более {user.max_items} позиций', message)
        return

    docid = user.id + '_' + store + '_' + prodid + '_' + skuid
    if await sku_repository.exists(docid):
        await reply_or_edit_msg('️☝️ Товар уже есть в вашем списке', message)
        return

    url = await product_repository.get_url(store, prodid)
    if not url:
        await reply_or_edit_msg('Какая-то ошибка 😧', message)
        return

    prod = await product_repository.get(store, url)
    if not prod.has_sku(skuid):
        await reply_or_edit_msg('Какая-то ошибка 😧', message)
        return

    sku = Sku.from_variant(prod.variants[skuid], user.id)
    await sku_repository.insert(sku)
    await reply_or_edit_msg(f'{sku.variant or sku.name}\n✔️ Добавлено к отслеживанию', message)


async def paginatedTgMsg(text_array, chat_id, message_id=0, delimiter='\n\n'):
    async def sendOrEditMsg():
        if message_id != 0 and first_page:
            await bot.edit_message_text(text=msg, chat_id=chat_id, message_id=message_id)
        else:
            await bot.send_message(chat_id, msg)

    first_page = True
    msg = ''

    for paragraph in text_array:
        if len(msg + paragraph) > 4090:
            await sendOrEditMsg()
            msg = ''
            first_page = False
        msg += paragraph + delimiter

    if msg:
        await sendOrEditMsg()


async def removeInvalidSKU():
    banner = f'ℹ️ Следующие позиции были удалены из вашего списка в связи с недоступностью более {settings.error_max_days} дней:'
    tsexpired = int(time()) - settings.error_max_days * 24 * 3600
    query = {'lastgoodts': {'$lt': tsexpired}}
    messages = {}
    async for sku in sku_repository.find(query):
        user = await user_repository.find_one(sku.chat_id)
        if not user.enable:
            continue
        line = sku.get_string('store', 'url')
        messages.setdefault(sku.chat_id, [banner]).append(line)

    await sku_repository.delete_many(query)

    for chat_id, message in messages.items():
        try:
            await paginatedTgMsg(message, chat_id)
        except Exception as e:
            await processException(e, chat_id)
        await asyncio.sleep(0.1)


async def notify():
    def addMsg(msg):
        messages.setdefault(sku.chat_id, []).append(msg)

    def processBestDeals():
        price_prev = sku.price_prev
        price = sku.price
        if price_prev == 0:
            return
        percents = int((1 - price/float(price_prev))*100)
        value = price_prev - price
        minvalue = settings.best_deals_min_value.get(sku.currency, 0)
        if percents >= settings.best_deals_min_percentage and value >= minvalue:
            bdkey = sku.store_prodid + '_' + sku.id
            bestdeals[bdkey] = skustring + ' ' + str(percents) + '%'
            if percents >= settings.best_deals_warn_percentage:
                bestdeals[bdkey] += '‼️'

    messages = {}
    bestdeals = {}
    notification_sku_ids = []

    query = {'$or': [{'price_prev': {'$ne': None}},{'instock_prev': {'$ne': None}}], 'enable': True}
    async for sku in sku_repository.find(query):
        if sku.instock_prev is not None:
            skustring = sku.get_string('store', 'url', 'price')
            if sku.instock:
                addMsg('✅ Снова в наличии!\n' + skustring)
            if not sku.instock:
                addMsg('🚫 Не в наличии\n' + skustring)
        elif sku.price_prev is not None and sku.instock:
            skustring = sku.get_string('store', 'url', 'price', 'price_prev')
            if sku.price < sku.price_prev:
                addMsg('📉 Снижение цены!\n' + skustring)
                processBestDeals()
            if sku.price > sku.price_prev:
                addMsg('📈 Повышение цены\n' + skustring)

        notification_sku_ids.append(sku.doc_id)

    for chat_id, message in messages.items():
        try:
            await paginatedTgMsg(message, chat_id)
        except Exception as e:
            await processException(e, chat_id)
        if settings.debug and settings.log_chat_id:
            await paginatedTgMsg(message, settings.log_chat_id)
        await asyncio.sleep(0.1)

    if settings.best_deals_chat_id:
        await paginatedTgMsg(bestdeals.values(), settings.best_deals_chat_id)

    await sku_repository.clear_notifications(notification_sku_ids)


async def disableUser(chat_id):
    await user_repository.update_many({'_id': chat_id}, {'$set': {'enable': False}})
    await sku_repository.update_many({'chat_id': chat_id}, {'$set': {'enable': False}})


async def checkSKU():
    now = int(time())
    query = {'enable': True, 'lastcheckts': {'$lt': now - settings.check_interval * 60}}
    prodlist = set()
    async for sku in sku_repository.find(query):
        prodlist.add(sku.store_prodid)
    
    prodlist = list(prodlist)
    if not prodlist:
        return

    query = {'store_prodid': {'$in': prodlist}, 'enable': True}
    async for sku in sku_repository.find(query, sort='store_prodid'):
        store = settings.stores[sku.store]
        if not store.active:
            continue

        logging.info(sku.doc_id + ' [' + sku.name + '][' + sku.variant + ']')

        prod = await product_repository.get(sku.store, sku.url)
        if prod.has_sku(sku.id):
            variant = prod.variants[sku.id]
            if variant.instock != sku.instock:
                sku.instock_prev = sku.instock
            
            if variant.currency == sku.currency:
                if sku.price * store.price_threshold < abs(variant.price - sku.price):
                    sku.price_prev = sku.price

            sku.instock = variant.instock
            sku.currency = variant.currency
            sku.price = variant.price
            sku.variant = variant.variant
            sku.errors = 0
            sku.lastgoodts = int(time())
        else:
            sku.errors += 1

        sku.lastcheck = datetime.now(timezone('Asia/Yekaterinburg')).strftime('%d.%m.%Y %H:%M')
        sku.lastcheckts = int(time())
        try:
            await sku_repository.save(sku)
        except Exception as e:
            logging.error(f'Error updating SKU: {e}')
        if prod.source == 'web':
            await asyncio.sleep(settings.request_delay)


async def errorsMonitor():
    bad = defaultdict(int)
    good = defaultdict(int)
    query = {'lastcheckts': {'$gt': int(time()) - settings.check_interval * 60}}
    
    async for sku in sku_repository.find(query):
        if sku.errors == 0:
            good[sku.store] += 1
        else:
            bad[sku.store] += 1

    for store in set(list(good) + list(bad)):
        if not settings.stores[store].active:
            continue
        good_count = good[store]
        bad_count = bad[store]
        if good_count == 0 or bad_count/float(good_count) > 0.8:
            await bot.send_message(
                settings.admin_chat_id,
                f'Problem with {store}!\nGood: {good_count}\nBad: {bad_count}'
            )


def create_webapp_server():
    app = web.Application()
    app.router.add_get('/list/', list_handler)
    app.router.add_post('/api/list', api_list_handler)
    app.router.add_post('/api/delete', api_delete_handler)
    # app.add_routes([web.static('/static', 'webapp/static')])
    
    return app


async def main():
    # settings
    await load_settings()

    # Initialize bot and dispatcher
    global bot
    botProperties = DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True)
    bot = Bot(token=settings.token, default=botProperties)

    web_app = create_webapp_server()
    web_app['bot'] = bot
    web_app['sku_repository'] = sku_repository
    web_runner = web.AppRunner(web_app)
    await web_runner.setup()
    site = web.TCPSite(web_runner, '0.0.0.0', PORT)
    await site.start()

    scheduler = AsyncIOScheduler(job_defaults={'misfire_grace_time': None})
    scheduler.start()

    scheduler.add_job(checkSKU, 'interval', minutes=5)
    scheduler.add_job(notify, 'interval', minutes=5)
    scheduler.add_job(errorsMonitor, 'interval', minutes=settings.check_interval)
    scheduler.add_job(product_repository.clear_sku_cache, 'cron', day_of_week='mon', hour=0, minute=0)
    scheduler.add_job(removeInvalidSKU, 'cron', day=1, hour=14, minute=0)

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()
        await web_runner.cleanup()
        await close_database()


if __name__ == '__main__':
    asyncio.run(main())
